using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace WeatherService;

/// <summary>
/// Punkt wejścia aplikacji typu Worker Service / Windows Service.
///
/// Klasa <c>Program</c> buduje hosta (IHost), konfiguruje:
/// - logowanie,
/// - wstrzykiwanie zależności (DI),
/// - uruchomienie serwisu jako Windows Service,
/// a następnie uruchamia pętlę życiową hosta.
/// </summary>
internal class Program
{
    /// <summary>
    /// Metoda główna aplikacji (entry point).
    /// 
    /// Odpowiada za:
    /// 1) Utworzenie i skonfigurowanie hosta (<see cref="IHost"/>),
    /// 2) Rejestrację serwisów w kontenerze DI:
    ///    - <see cref="WeatherSyncJob"/> jako singleton,
    ///    - <see cref="Worker"/> jako HostedService (serwis tła),
    /// 3) Uruchomienie hosta (RunAsync), co w praktyce uruchamia Worker Service
    ///    jako usługę Windows (dzięki UseWindowsService()).
    /// </summary>
    /// <param name="args">
    /// Argumenty wiersza poleceń przekazywane do HostBuildera.
    /// W typowym scenariuszu Worker Service są puste, ale mogą służyć
    /// np. do wyboru środowiska, konfiguracji itd.
    /// </param>
    private static async Task Main(string[] args)
    {
        // Tworzymy hosta aplikacji – standardowy szablon .NET Worker Service
        var host = Host.CreateDefaultBuilder(args)
            // Rejestruje aplikację jako Windows Service:
            // - pozwala instalować/uruchamiać jako usługę systemową,
            // - integruje lifecycle z SCM (Service Control Manager).
            .UseWindowsService()

            // Konfiguracja serwisów (Dependency Injection)
            .ConfigureServices((context, services) =>
            {
                // Rejestracja joba synchronizującego pogodę jako singleton:
                // - jedna instancja na cały czas życia hosta,
                // - wstrzykiwana m.in. do Worker.
                services.AddSingleton<WeatherSyncJob>();

                // Rejestracja Workera jako Hosted Service:
                // - Worker dziedziczy po BackgroundService,
                // - jest automatycznie uruchamiany po starcie hosta,
                // - działa w tle (pętla ExecuteAsync).
                services.AddHostedService<Worker>();
            })

            // Opcjonalnie można tu dopinać dodatkowe logowanie itd.
            .ConfigureLogging(logging =>
            {
                // Możesz dostosować poziomy logów, dodać dodatkowe providery itp.
                logging.SetMinimumLevel(LogLevel.Information);
            })

            // Budujemy gotowego hosta
            .Build();

        // Uruchamiamy hosta – blokuje aż do zatrzymania usługi / aplikacji.
        await host.RunAsync();
    }
}

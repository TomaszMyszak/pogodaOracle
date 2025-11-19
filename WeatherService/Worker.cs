using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace WeatherService;

/// <summary>
/// Główna usługa tła (Windows Service / Linux daemon),
/// uruchamiana przez Host Builder.
/// 
/// Worker:
///  - działa cyklicznie w nieskończonej pętli,
///  - co określony interwał uruchamia logikę synchronizacji pogody,
///  - reaguje na sygnał zatrzymania (CancellationToken).
/// 
/// W tym projekcie Worker wywołuje klasę <see cref="WeatherSyncJob"/>
/// odpowiedzialną za pobieranie oraz zapisywanie danych pogodowych.
/// </summary>
public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly WeatherSyncJob _weatherSyncJob;

    /// <summary>
    /// Konstruktor Worker Service.  
    /// Wstrzykiwany jest logger oraz instancja klasy WeatherSyncJob z DI.
    /// </summary>
    /// <param name="logger">Logger używany do zapisywania zdarzeń z pracy usługi.</param>
    /// <param name="weatherSyncJob">Logika joba synchronizującego pogodę.</param>
    public Worker(ILogger<Worker> logger, WeatherSyncJob weatherSyncJob)
    {
        _logger = logger;
        _weatherSyncJob = weatherSyncJob;
    }

    /// <summary>
    /// Metoda główna wykonywana po starcie serwisu.
    /// Pętla działa dopóki aplikacja nie zostanie zatrzymana.
    /// 
    /// Schemat:
    ///     1. Wywołaj _weatherSyncJob.RunOnce()
    ///     2. Poczekaj 10 minut
    ///     3. Powtórz
    /// </summary>
    /// <param name="stoppingToken">Token sygnalizujący zatrzymanie usługi.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("WeatherService Worker: start at {Time}", DateTime.Now);

        // Pętla pracująca do momentu zatrzymania usługi
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Wywołanie głównej logiki synchronizacji pogody
                await _weatherSyncJob.RunOnce(stoppingToken);
            }
            catch (Exception ex)
            {
                // Złapanie ewentualnych wyjątków, aby Worker nie umierał
                _logger.LogError(ex, "Błąd podczas wykonywania WeatherSyncJob.");
            }

            // Opóźnienie między cyklami (10 minut)
            _logger.LogInformation("WeatherService czeka 10 minut do kolejnej synchronizacji.");

            try
            {
                await Task.Delay(TimeSpan.FromMinutes(10), stoppingToken);
            }
            catch (TaskCanceledException)
            {
                // Worker został zatrzymany w trakcie Delay
                break;
            }
        }

        _logger.LogInformation("WeatherService Worker: stop at {Time}", DateTime.Now);
    }
}

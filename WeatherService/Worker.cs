using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace WeatherService;

/// <summary>
/// Usługa tła uruchamiana przez Host Builder (Windows Service / Linux daemon).
///
/// <para>
/// Worker działa jako cykliczny harmonogram testowy – jego pętla nie jest źródłem
/// danych produkcyjnych, ponieważ właściwy proces synchronizacji sterowany jest
/// po stronie Oracle (JOB → PL/SQL → UTL_HTTP → serwis .NET).
/// </para>
///
/// <para>
/// Rola Workera:
///  - działa w tle i może wykonywać testową synchronizację danych,
///  - zapewnia mechanizm sanity-check lub diagnostyczny w środowisku DEV/TEST,
///  - reaguje na sygnały zatrzymania aplikacji (CancellationToken),
///  - nigdy nie blokuje się na błędach (błędy loguje i kontynuuje pracę).
/// </para>
/// </summary>
public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly WeatherSyncJob _weatherSyncJob;

    /// <summary>
    /// Konstruktor. Wstrzykuje logger oraz instancję <see cref="WeatherSyncJob"/>.
    /// </summary>
    /// <param name="logger">Logger dedykowany dla Workera.</param>
    /// <param name="weatherSyncJob">Główny komponent zawierający logikę integracji.</param>
    public Worker(
        ILogger<Worker> logger,
        WeatherSyncJob weatherSyncJob)
    {
        _logger = logger;
        _weatherSyncJob = weatherSyncJob;
    }

    /// <summary>
    /// Główna pętla życiowa Workera.
    /// 
    /// <para>
    /// W wersji demonstracyjnej:
    ///     1. Wywołuje testowy cykl synchronizacji (<see cref="WeatherSyncJob.RunOnce"/>)
    ///     2. Czeka 10 minut
    ///     3. Powtarza aż do zatrzymania serwisu
    /// </para>
    ///
    /// <para>
    /// W środowisku produkcyjnym fakt pobierania danych powinien być sterowany
    /// wyłącznie z Oracle (PL/SQL → lokalny endpoint HTTP), a Worker może służyć
    /// jedynie do okresowych samo-testów, monitoringu lub fallbacku.
    /// </para>
    /// </summary>
    /// <param name="stoppingToken">Token sygnalizujący zatrzymanie usługi.</param>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation(
            "WeatherService Worker: started at {Time}",
            DateTime.Now);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Wykonanie jednorazowego cyklu testowego.
                // Produkcyjnie można to wyłączyć lub zastąpić funkcją diagnostyczną.
                await _weatherSyncJob.RunOnce(stoppingToken);
            }
            catch (Exception ex)
            {
                // Worker NIE MOŻE umrzeć — logujemy i pracujemy dalej.
                _logger.LogError(
                    ex,
                    "Błąd podczas wykonywania WeatherSyncJob w Worker Service.");
            }

            // Opóźnienie między cyklami (10 minut)
            _logger.LogInformation(
                "WeatherService Worker: oczekiwanie 10 minut do kolejnego cyklu.");

            try
            {
                await Task.Delay(TimeSpan.FromMinutes(10), stoppingToken);
            }
            catch (TaskCanceledException)
            {
                // Prawidłowe zakończenie pracy serwisu
                break;
            }
        }

        _logger.LogInformation(
            "WeatherService Worker: stopped at {Time}",
            DateTime.Now);
    }
}

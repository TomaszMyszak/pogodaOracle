using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Oracle.ManagedDataAccess.Client;
using Oracle.ManagedDataAccess.Types;

namespace WeatherService;

/// <summary>
/// Reprezentuje uproszczony pomiar pogodowy przekazywany z .NET do Oracla.
/// 
/// Ten DTO jest:
/// - zwracany z endpointu HTTP /weather/latest,
/// - łatwy do parsowania w PL/SQL przy użyciu JSON_TABLE,
/// - celowo ograniczony tylko do niezbędnych pól.
///
/// Pola:
/// - <see cref="MeasuredAt"/> – znacznik czasu pomiaru (UTC lub strefa z API),
/// - <see cref="TempC"/> – temperatura w °C (nullable),
/// - <see cref="Humidity"/> – wilgotność względna [%] (nullable),
/// - <see cref="WindSpeedMs"/> – prędkość wiatru [m/s] (nullable),
/// - <see cref="IsRain"/> – informacja, czy notowany jest opad (deszcz).
/// </summary>
public sealed record WeatherDto(
    DateTime MeasuredAt,
    double? TempC,
    double? Humidity,
    double? WindSpeedMs,
    bool IsRain);

/// <summary>
/// Główna klasa odpowiedzialna za:
/// 1) test połączeń z bazą Oracle i serwisem pogodowym,
/// 2) uruchomienie pojedynczego cyklu synchronizacji (tryb testowy),
/// 3) zapis danych testowych do pliku oraz do tabeli WEATHER_MEASUREMENTS w Oracle,
/// 4) udostępnienie danych pogodowych jako uproszczonego DTO (<see cref="WeatherDto"/>)
///    na potrzeby endpointu HTTP /weather/latest.
/// 
/// Klasa jest projektowana do użycia:
/// - wewnątrz Worker Service (BackgroundService),
/// - jako serwis wstrzykiwany przez DI do Minimal API.
/// </summary>
public sealed class WeatherSyncJob
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<WeatherSyncJob> _logger;

    // Można wynieść do appsettings, jeśli będzie potrzeba.
    private const int HttpTimeoutSeconds = 10;

    /// <summary>
    /// Konstruktor joba synchronizacji pogody.
    /// </summary>
    /// <param name="configuration">
    /// Dostarczone przez DI IConfiguration – używane do odczytu:
    /// - connection stringa "Oracle",
    /// - sekcji "WeatherApi" (BaseUrl, Params).
    /// </param>
    /// <param name="logger">
    /// Logger dla klasy WeatherSyncJob, używany do logowania informacji,
    /// ostrzeżeń i błędów.
    /// </param>
    public WeatherSyncJob(IConfiguration configuration, ILogger<WeatherSyncJob> logger)
    {
        _configuration = configuration;
        _logger = logger;
    }

    /// <summary>
    /// Jednorazowy test połączeń:
    /// - sprawdza możliwość połączenia z bazą Oracle,
    /// - sprawdza, czy serwis pogodowy odpowiada (HTTP 2xx).
    /// 
    /// Metoda wypisuje komunikaty na konsolę (Console.WriteLine)
    /// – przydatne przy odpalaniu z konsoli lub w logach serwisu.
    /// </summary>
    /// <param name="stoppingToken">
    /// Token anulowania przekazywany z Worker Service / BackgroundService.
    /// W przypadku przerwania pracy serwisu umożliwia przerwanie operacji async.
    /// </param>
    private async Task TestConnectionsAsync(CancellationToken stoppingToken)
    {
        string? connString = _configuration.GetConnectionString("Oracle");
        string? baseUrl = _configuration["WeatherApi:BaseUrl"];
        string? commonParams = _configuration["WeatherApi:Params"];

        Console.WriteLine("=== TEST POŁĄCZEŃ START ===");

        // --- TEST ORACLE ---
        if (string.IsNullOrWhiteSpace(connString))
        {
            Console.WriteLine("BŁĄD: Brak connection stringa 'Oracle' w appsettings.json");
        }
        else
        {
            try
            {
                using var conn = new OracleConnection(connString);
                await conn.OpenAsync(stoppingToken);
                Console.WriteLine("OK: Połączono z bazą Oracle.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"BŁĄD: Nie udało się połączyć z bazą Oracle: {ex.Message}");
            }
        }

        // --- TEST SERWISU POGODOWEGO ---
        if (string.IsNullOrWhiteSpace(baseUrl))
        {
            Console.WriteLine("BŁĄD: Brak 'WeatherApi:BaseUrl' w appsettings.json");
        }
        else
        {
            try
            {
                using var http = new HttpClient
                {
                    Timeout = TimeSpan.FromSeconds(HttpTimeoutSeconds)
                };

                // Prosty request testowy – przykładowa lokalizacja (Kraków)
                var url = new StringBuilder(baseUrl);

                if (!string.IsNullOrWhiteSpace(commonParams))
                {
                    url.Append($"?latitude=50.0647&longitude=19.9450&{commonParams}");
                }

                var resp = await http.GetAsync(url.ToString(), stoppingToken);

                if (resp.IsSuccessStatusCode)
                {
                    Console.WriteLine(
                        $"OK: Połączono z serwisem pogodowym. Status: {(int)resp.StatusCode} {resp.ReasonPhrase}");
                }
                else
                {
                    Console.WriteLine(
                        $"BŁĄD: Serwis pogodowy zwrócił status {(int)resp.StatusCode} {resp.ReasonPhrase}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"BŁĄD: Nie udało się połączyć z serwisem pogodowym: {ex.Message}");
            }
        }

        Console.WriteLine("=== TEST POŁĄCZEŃ KONIEC ===");
    }

    /// <summary>
    /// Uruchamia pojedynczy cykl "jobu" (tryb testowy).
    /// 
    /// 1) Generuje testowe prognozy (losowe dane),
    /// 2) zapisuje je do pliku tekstowego w C:\temp\weather_data.txt,
    /// 3) próbuje zapisać te same dane jako pomiary do tabeli WEATHER_MEASUREMENTS
    ///    w schemacie TMYSZAK (z użyciem <see cref="InsertMeasurementAsync"/>).
    /// 
    /// W produkcji może służyć jako:
    /// - narzędzie testowe,
    /// - ręczny sanity-check integracji z Oracle.
    /// </summary>
    /// <param name="cancellationToken">
    /// Token anulowania – umożliwia przerwanie pracy (np. przy zatrzymaniu serwisu).
    /// </param>
    public async Task RunOnce(CancellationToken cancellationToken)
    {
        _logger.LogInformation("[{Time}] WeatherSync start (tryb testowy + zapis do Oracle)", DateTime.Now);

        var startDate = DateOnly.FromDateTime(DateTime.Now);

        var summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild",
            "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        // Generowanie kolekcji testowych prognoz (5 dni w przód).
        var forecasts = Enumerable.Range(1, 5)
            .Select(index => new WeatherForecast
            {
                Date = startDate.AddDays(index),
                TemperatureC = Random.Shared.Next(-20, 55),
                Summary = summaries[Random.Shared.Next(summaries.Length)]
            })
            .ToArray();

        var filePath = @"C:\temp\weather_data.txt";

        // Utwórz katalog C:\temp, jeśli nie istnieje
        var directoryPath = Path.GetDirectoryName(filePath);
        if (!Directory.Exists(directoryPath))
        {
            Directory.CreateDirectory(directoryPath);
        }

        // Zapis do pliku TXT (append).
        foreach (var f in forecasts)
        {
            var line = $"[{DateTime.Now:O}] {f.Date}: {f.TemperatureC}C ({f.Summary}){Environment.NewLine}";

            Console.WriteLine(line.TrimEnd());
            _logger.LogInformation(
                "Prognoza: {Date} {TempC}C ({Summary})",
                f.Date, f.TemperatureC, f.Summary);

            await File.AppendAllTextAsync(filePath, line, cancellationToken);
        }

        // --- Próba zapisu prognoz do Oracle jako testowych pomiarów ---
        var connString = _configuration.GetConnectionString("Oracle");
        if (string.IsNullOrWhiteSpace(connString))
        {
            _logger.LogError("Brakuje connection stringa 'Oracle' w konfiguracji. Pomiary nie zostaną zapisane do bazy.");
            _logger.LogInformation("[{Time}] WeatherSync end (tryb testowy + zapis do Oracle)", DateTime.Now);
            return;
        }

        try
        {
            using var conn = new OracleConnection(connString);
            await conn.OpenAsync(cancellationToken);

            _logger.LogInformation("Połączono z bazą Oracle (tryb testowy wstawiania prognoz).");

            // Dwie lokalizacje testowe na sztywno: Kraków i Warszawa.
            // W produkcji powinno to być pobierane z tabeli WEATHER_LOCATIONS
            // przez metodę LoadLocationsAsync.
            var locations = new List<Location>
            {
                new(1, "PL", "Krakow",   50.0647, 19.9450),
                new(2, "PL", "Warszawa", 52.2297, 21.0122)
            };

            // Dla każdej lokalizacji zapisujemy każdą z testowych prognoz jako pomiar.
            foreach (var loc in locations)
            {
                foreach (var f in forecasts)
                {
                    var measurement = new WeatherMeasurement
                    {
                        MeasuredAt = f.Date.ToDateTime(TimeOnly.MinValue),
                        TempC = f.TemperatureC,
                        Humidity = null,
                        WindSpeedMs = null,
                        IsRain = false
                    };

                    await InsertMeasurementAsync(
                        conn,
                        loc.Id,
                        measurement,
                        $"Test forecast: {f.Summary}",
                        cancellationToken);

                    _logger.LogInformation(
                        "Zapisano TESTOWY pomiar dla lokalizacji {Id} {City}: {TempC}C ({Summary})",
                        loc.Id, loc.CityName, f.TemperatureC, f.Summary);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Błąd przy zapisie testowych prognoz do Oracle.");
        }

        _logger.LogInformation("[{Time}] WeatherSync end (tryb testowy + zapis do Oracle)", DateTime.Now);
    }

    /// <summary>
    /// Prosty rekord reprezentujący lokalizację (miasto / punkt na mapie),
    /// z informacją o identyfikatorze z bazy, kraju i współrzędnych geograficznych.
    /// </summary>
    private readonly record struct Location(
        int Id,
        string CountryCode,
        string CityName,
        double Latitude,
        double Longitude);

    /// <summary>
    /// Model pomiaru pogody, mapowany na jeden wiersz w tabeli WEATHER_MEASUREMENTS.
    /// </summary>
    private sealed class WeatherMeasurement
    {
        /// <summary>Data i czas wykonania pomiaru.</summary>
        public DateTime MeasuredAt { get; set; }

        /// <summary>Temperatura w stopniach Celsjusza (może być null, gdy brak danych).</summary>
        public double? TempC { get; set; }

        /// <summary>Wilgotność względna w %, może być null.</summary>
        public double? Humidity { get; set; }

        /// <summary>Prędkość wiatru [m/s], może być null.</summary>
        public double? WindSpeedMs { get; set; }

        /// <summary>
        /// Informacja czy pada deszcz (true/false); mapowane na 'Y'/'N' w tabeli.
        /// </summary>
        public bool IsRain { get; set; }
    }

    /// <summary>
    /// Model prognozy pogodowej używany tylko w trybie testowym:
    /// generuje się losowa temperatura oraz opis tekstowy.
    /// </summary>
    private sealed class WeatherForecast
    {
        /// <summary>Data prognozy (dzień bez czasu).</summary>
        public DateOnly Date { get; set; }

        /// <summary>Temperatura Celsjusza.</summary>
        public int TemperatureC { get; set; }

        /// <summary>Temperatura Fahrenheita (pochodna od TemperatureC).</summary>
        public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);

        /// <summary>Opis jakościowy (np. "Warm", "Chilly").</summary>
        public string? Summary { get; set; }
    }

    /// <summary>
    /// Ładuje listę aktywnych lokalizacji pogodowych z tabeli TMYSZAK.WEATHER_LOCATIONS.
    /// W tej wersji nie jest używane przez endpoint HTTP, ale może być wykorzystane
    /// np. w automatycznym jobie w Workerze.
    /// </summary>
    private static async Task<List<Location>> LoadLocationsAsync(
        OracleConnection conn,
        CancellationToken cancellationToken)
    {
        const string sql = @"
            SELECT ID_LOCATION, COUNTRY_CODE, CITY_NAME, LATITUDE, LONGITUDE
            FROM TMYSZAK.WEATHER_LOCATIONS
            WHERE ACTIVE_FLAG = 'Y'";

        var result = new List<Location>();

        using var cmd = new OracleCommand(sql, conn);

        using var rdr = await cmd.ExecuteReaderAsync(
            CommandBehavior.Default,
            cancellationToken);

        while (await rdr.ReadAsync(cancellationToken))
        {
            var loc = new Location(
                Id: rdr.GetInt32(0),
                CountryCode: rdr.GetString(1),
                CityName: rdr.GetString(2),
                Latitude: rdr.GetDouble(3),
                Longitude: rdr.GetDouble(4)
            );
            result.Add(loc);
        }

        return result;
    }

    /// <summary>
    /// Parsuje odpowiedź JSON z serwisu pogodowego (Open-Meteo) i zwraca
    /// ostatni dostępny pomiar z sekcji "hourly":
    /// - czas (time),
    /// - temperatura (temperature_2m),
    /// - wilgotność (relativehumidity_2m),
    /// - opady (precipitation),
    /// - prędkość wiatru (wind_speed_10m).
    /// </summary>
    /// <param name="json">
    /// Surowa odpowiedź JSON z API Open-Meteo.
    /// </param>
    /// <returns>
    /// Obiekt <see cref="WeatherMeasurement"/> z wypełnionymi danymi,
    /// albo null, jeśli w JSON nie ma oczekiwanej struktury.
    /// </returns>
    private static WeatherMeasurement? ParseLatestMeasurement(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        if (!root.TryGetProperty("hourly", out var hourly))
            return null;

        var times = hourly.GetProperty("time");
        var temps = hourly.GetProperty("temperature_2m");
        var humidity = hourly.GetProperty("relativehumidity_2m");
        var precip = hourly.GetProperty("precipitation");
        var windSpeed = hourly.GetProperty("windspeed_10m");

        // Bierzemy ostatni indeks (najświeższy pomiar).
        var lastIdx = times.GetArrayLength() - 1;

        var timeStr = times[lastIdx].GetString();
        if (string.IsNullOrEmpty(timeStr))
            return null;

        var temp = temps[lastIdx].GetDouble();
        var hum = humidity[lastIdx].GetDouble();
        var prec = precip[lastIdx].GetDouble();
        var wind = windSpeed[lastIdx].GetDouble();

        // Tu można doprecyzować format i strefę czasową, ale dla testów wystarczy Parse.
        var dt = DateTime.Parse(timeStr, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);

        return new WeatherMeasurement
        {
            MeasuredAt = dt,
            TempC = temp,
            Humidity = hum,
            WindSpeedMs = wind,
            IsRain = prec > 0.0
        };
    }

    /// <summary>
    /// Wstawia pojedynczy pomiar pogodowy do tabeli TMYSZAK.WEATHER_MEASUREMENTS.
    /// </summary>
    /// <param name="conn">
    /// Otwarte połączenie <see cref="OracleConnection"/> – metoda nie zarządza cyklem życia połączenia.
    /// </param>
    /// <param name="idLocation">
    /// ID lokalizacji (klucz obcy do WEATHER_LOCATIONS.ID_LOCATION).
    /// </param>
    /// <param name="m">
    /// Obiekt <see cref="WeatherMeasurement"/> z danymi pomiaru (czas, temperatura, wilgotność, wiatr, deszcz).
    /// </param>
    /// <param name="rawJson">
    /// Surowy JSON / opis – w tym wypadku w trybie testowym przekazywany jest prosty string.
    /// W wariancie produkcyjnym może to być pełna odpowiedź z API.
    /// </param>
    /// <param name="cancellationToken">
    /// Token anulowania do przerwania operacji INSERT.
    /// </param>
    private static async Task InsertMeasurementAsync(
        OracleConnection conn,
        int idLocation,
        WeatherMeasurement m,
        string rawJson,
        CancellationToken cancellationToken)
    {
        const string sql = @"
            INSERT INTO TMYSZAK.WEATHER_MEASUREMENTS
                (ID_LOCATION, MEASURED_AT, TEMP_C, IS_RAIN, HUMIDITY, WIND_SPEED_MS, RAW_JSON)
            VALUES
                (:p_loc, :p_time, :p_temp, :p_rain, :p_hum, :p_wind, :p_json)";

        using var cmd = new OracleCommand(sql, conn)
        {
            BindByName = true
        };

        cmd.Parameters.Add("p_loc", OracleDbType.Int32).Value = idLocation;
        cmd.Parameters.Add("p_time", OracleDbType.Date).Value = m.MeasuredAt;
        cmd.Parameters.Add("p_temp", OracleDbType.Decimal).Value = (object?)m.TempC ?? DBNull.Value;
        cmd.Parameters.Add("p_rain", OracleDbType.Char).Value = m.IsRain ? "Y" : "N";
        cmd.Parameters.Add("p_hum", OracleDbType.Decimal).Value = (object?)m.Humidity ?? DBNull.Value;
        cmd.Parameters.Add("p_wind", OracleDbType.Decimal).Value = (object?)m.WindSpeedMs ?? DBNull.Value;
        cmd.Parameters.Add("p_json", OracleDbType.Clob).Value = rawJson;

        await cmd.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// Pobiera zewnętrzny JSON z Open-Meteo, parsuje go lokalnie
    /// i zwraca uproszczony obiekt DTO (<see cref="WeatherDto"/>) do wysłania jako JSON do Oracla.
    /// 
    /// Metoda jest wykorzystywana przez endpoint HTTP:
    ///   GET /weather/latest?lat={lat}&lon={lon}
    /// </summary>
    /// <param name="latitude">Szerokość geograficzna (-90..90).</param>
    /// <param name="longitude">Długość geograficzna (-180..180).</param>
    /// <param name="cancellationToken">Token anulowania HTTP / operacji I/O.</param>
    /// <returns>
    /// Wypełniony <see cref="WeatherDto"/> albo null, jeśli:
    /// - brak konfiguracji API,
    /// - nie udało się pobrać danych,
    /// - nie udało się sparsować JSON.
    /// </returns>
    public async Task<WeatherDto?> GetLatestMeasurementDtoAsync(
        double latitude,
        double longitude,
        CancellationToken cancellationToken)
    {
        string? baseUrl = _configuration["WeatherApi:BaseUrl"];
        string? commonParms = _configuration["WeatherApi:Params"];

        if (string.IsNullOrWhiteSpace(baseUrl))
        {
            _logger.LogError("Brak konfiguracji WeatherApi:BaseUrl");
            return null;
        }

        // Budujemy URL z culture invariant (kropka jako separator dziesiętny).
        var sb = new StringBuilder();
        sb.Append(baseUrl);
        sb.Append("?latitude=");
        sb.Append(latitude.ToString(CultureInfo.InvariantCulture));
        sb.Append("&longitude=");
        sb.Append(longitude.ToString(CultureInfo.InvariantCulture));

        if (!string.IsNullOrWhiteSpace(commonParms))
        {
            sb.Append('&');
            sb.Append(commonParms);
        }

        using var http = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(HttpTimeoutSeconds)
        };

        HttpResponseMessage resp;
        try
        {
            resp = await http.GetAsync(sb.ToString(), cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Błąd przy wywołaniu serwisu pogodowego.");
            return null;
        }

        if (!resp.IsSuccessStatusCode)
        {
            _logger.LogError(
                "Weather API zwrócił status {Code} {Reason}",
                (int)resp.StatusCode,
                resp.ReasonPhrase);
            return null;
        }

        var rawJson = await resp.Content.ReadAsStringAsync(cancellationToken);

        // Parsujemy „gruby” JSON Open-Meteo do wewnętrznego modelu
        var measurement = ParseLatestMeasurement(rawJson);
        if (measurement is null)
        {
            _logger.LogWarning("Nie udało się sparsować JSON z Open-Meteo.");
            return null;
        }

        // Uproszczony DTO dla Oracla
        return new WeatherDto(
            MeasuredAt: measurement.MeasuredAt,
            TempC: measurement.TempC,
            Humidity: measurement.Humidity,
            WindSpeedMs: measurement.WindSpeedMs,
            IsRain: measurement.IsRain);
    }
}

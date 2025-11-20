using WeatherService;
using Microsoft.Extensions.Logging;

//
// Program.cs
// Główny punkt wejścia aplikacji .NET (Minimal API + Worker Service).
//
// Rola:
//   - Buduje hosta webowego (WebApplication),
//   - Rejestruje serwisy w DI (WeatherSyncJob, Worker),
//   - Wystawia lokalne endpointy HTTP używane przez Oracle (UTL_HTTP):
//       * GET /health
//       * GET /weather/latest?lat={lat}&lon={lon}
//   - Uruchamia pętlę życia aplikacji (app.Run()).
//
var builder = WebApplication.CreateBuilder(args);

#region Konfiguracja hosta / Kestrel

// Wystaw nasłuch na wszystkich interfejsach (dostępne z sieci lokalnej / innego hosta).
builder.WebHost.UseUrls("http://0.0.0.0:5005");

#endregion

#region Logowanie

// Czyścimy domyślnych providerów i ustawiamy to, czego realnie potrzebujemy.
// W praktyce: konsola + poziom INFO wystarczą dla serwisu systemowego.
builder.Logging.ClearProviders();
builder.Logging.AddConsole();
builder.Logging.SetMinimumLevel(LogLevel.Information);

#endregion

#region Rejestracja serwisów (Dependency Injection)

// Główna logika pobierania pogody i zapisu do Oracle
builder.Services.AddSingleton<WeatherSyncJob>();

// Worker (BackgroundService), który może uruchamiać joby cykliczne / testowe.
// W naszym scenariuszu głównym sterownikiem jest Oracle, ale Worker może
// być użyty np. do dodatkowego logowania / sanity-checków.
builder.Services.AddHostedService<Worker>();

#endregion

var app = builder.Build();

#region Endpointy HTTP

// ---------------------------------------------------------------------------
// GET /health
// Prosty endpoint zdrowia – wykorzystywany głównie:
//   - do szybkiego testu UTL_HTTP po stronie Oracle,
//   - do monitoringów typu "czy proces żyje".
// Odpowiedź 200 OK + body "OK".
// ---------------------------------------------------------------------------
app.MapGet("/health", () => Results.Ok("OK"));


// ---------------------------------------------------------------------------
// GET /weather/latest?lat=50.0647&lon=19.9450
//
// Główny endpoint dla Oracla.
// Parametry:
//   - lat: szerokość geograficzna (-90..90)
//   - lon: długość geograficzna (-180..180)
//
// Zwraca:
//   - 200 OK + JSON (uproszczony DTO z danymi pomiaru),
//   - 400 Bad Request przy nieprawidłowych współrzędnych,
//   - 502 Bad Gateway, jeżeli serwis pogodowy jest niedostępny
//     lub logika WeatherSyncJob nie zwróciła żadnych danych.
// ---------------------------------------------------------------------------
app.MapGet(
    "/weather/latest",
    async (
        double lat,
        double lon,
        WeatherSyncJob job,
        CancellationToken cancellationToken) =>
    {
        // Walidacja współrzędnych – lepiej odrzucić błąd wejścia od razu.
        if (lat is < -90 or > 90)
        {
            return Results.BadRequest("Nieprawidłowa szerokość geograficzna (lat). Zakres: -90..90.");
        }

        if (lon is < -180 or > 180)
        {
            return Results.BadRequest("Nieprawidłowa długość geograficzna (lon). Zakres: -180..180.");
        }

        // Obsługa anulowania żądania (np. podczas stopu serwisu)
        if (cancellationToken.IsCancellationRequested)
        {
            // 499 – "Client Closed Request" (niestandardowy, ale stosowany np. w Nginx).
            return Results.StatusCode(StatusCodes.Status499ClientClosedRequest);
        }

        // Delegujemy logikę do WeatherSyncJob – dzięki temu:
        //   - Program.cs pozostaje cienkim "composition root",
        //   - cała logika techniczna jest w klasie serwisowej.
        var dto = await job.GetLatestMeasurementDtoAsync(lat, lon, cancellationToken);

        if (dto is null)
        {
            // Brak danych = problem po stronie "downstream" (zewnętrzny serwis pogodowy).
            return Results.Problem(
                detail: "Brak danych z serwisu pogodowego lub błąd podczas pobierania.",
                statusCode: StatusCodes.Status502BadGateway,
                title: "Błąd proxy pogodowego");
        }

        // Zwracamy mały, prosty JSON, który jest łatwy do przetworzenia w PL/SQL
        // przy użyciu JSON_TABLE.
        return Results.Ok(dto);
    });

#endregion

// Uruchomienie aplikacji (blokuje wątki robocze aż do zatrzymania serwisu).
app.Run();

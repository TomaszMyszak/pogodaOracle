--------------------------------------------------------------------------------
--  P_WEATHER_SYNC_ONE_LOC
--  Autor:      TMYSZAK
--  Schemat:    TMYSZAK
--  Opis:
--      Procedura pobiera pojedynczy pomiar pogody dla wskazanej lokalizacji
--      (ID_LOCATION) z lokalnego serwisu Windows (proxy HTTP), który z kolei
--      odpyta zewnętrzny serwis pogodowy (np. Open-Meteo).
--
--      Flow:
--        1) Odczyt LAT/LON z TMYSZAK.WEATHER_LOCATIONS
--        2) Wywołanie HTTP GET: http://127.0.0.1:5005/weather/latest?lat=..&lon=..
--        3) Odbiór JSON (CLOB)
--        4) Parsowanie JSON_TABLE
--        5) Insert do TMYSZAK.WEATHER_MEASUREMENTS
--
--      Założenia:
--        - Serwis Windows nasłuchuje na 127.0.0.1:5005
--        - Endpoint /weather/latest zwraca JSON:
--            {
--                "measuredAt": "2025-11-20T09:00:00Z",
--                "tempC": 4.5,
--                "humidity": 73.2,
--                "windSpeedMs": 1.8,
--                "isRain": false
--            }
--        - Konfiguracja ACL (DBMS_NETWORK_ACL_ADMIN) zezwala TMYSZAK na HTTP do 127.0.0.1
--
--  Uwaga:
--      COMMIT na końcu procedury jest celowy (procedura „jednostki pracy”).
--      Jeśli integrujesz to w większej transakcji – usuń COMMIT.
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE TMYSZAK.P_WEATHER_SYNC_ONE_LOC (
    p_id_location IN NUMBER
) AS
    ----------------------------------------------------------------------------
    -- Sekcja stałych / parametrów konfiguracyjnych
    ----------------------------------------------------------------------------
    c_base_url       CONSTANT VARCHAR2(200) := 'http://127.0.0.1:5005/weather/latest';
    c_http_timeout_s CONSTANT PLS_INTEGER   := 10;       -- timeout w sekundach

    ----------------------------------------------------------------------------
    -- Dane lokalizacji
    ----------------------------------------------------------------------------
    l_lat            NUMBER;           -- szerokość geograficzna
    l_lon            NUMBER;           -- długość geograficzna

    ----------------------------------------------------------------------------
    -- Obsługa HTTP i JSON
    ----------------------------------------------------------------------------
    l_http_request   UTL_HTTP.req;
    l_http_response  UTL_HTTP.resp;
    l_chunk          VARCHAR2(32767);  -- bufor odczytu
    l_json           CLOB;             -- pełen JSON zwrócony przez serwis

    ----------------------------------------------------------------------------
    -- Dane pomiaru
    ----------------------------------------------------------------------------
    l_measured_at    TIMESTAMP;
    l_temp_c         NUMBER;
    l_humidity       NUMBER;
    l_wind_speed_ms  NUMBER;
    l_is_rain_raw    VARCHAR2(5);      -- „true” / „false” / „1” / „0”
    l_is_rain_char   CHAR(1);          -- 'Y' / 'N'

    ----------------------------------------------------------------------------
    -- Stałe / wyjątki
    ----------------------------------------------------------------------------
    e_no_location EXCEPTION;           -- brak aktywnej lokalizacji
    PRAGMA EXCEPTION_INIT(e_no_location, -01403);  -- NO_DATA_FOUND
BEGIN
    ----------------------------------------------------------------------------
    -- 1) Pobieramy współrzędne z tabeli lokalizacji
    ----------------------------------------------------------------------------
    BEGIN
        SELECT latitude,
               longitude
          INTO l_lat,
               l_lon
          FROM TMYSZAK.WEATHER_LOCATIONS
         WHERE id_location = p_id_location
           AND active_flag = 'Y';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- Brak aktywnej lokalizacji – kończymy bez błędu lub logujemy osobno
            RAISE_APPLICATION_ERROR(
                -20001,
                'Brak aktywnej lokalizacji o ID = ' || p_id_location
            );
    END;

    ----------------------------------------------------------------------------
    -- 2) Wołamy lokalny serwis Windows na localhost
    --    Query string budujemy z wymuszeniem kropki jako separatora dziesiętnego.
    ----------------------------------------------------------------------------
    -- Timeout na poziomie sesji UTL_HTTP (opcjonalnie)
    UTL_HTTP.set_transfer_timeout(c_http_timeout_s);

    l_http_request := UTL_HTTP.BEGIN_REQUEST(
        url    => c_base_url
                  || '?lat=' || TO_CHAR(
                                    l_lat,
                                    'FM9999990D999999',
                                    'NLS_NUMERIC_CHARACTERS=.,'
                                 )
                  || '&lon=' || TO_CHAR(
                                    l_lon,
                                    'FM9999990D999999',
                                    'NLS_NUMERIC_CHARACTERS=.,'
                                 ),
        method => 'GET'
    );

    l_http_response := UTL_HTTP.GET_RESPONSE(l_http_request);

    ----------------------------------------------------------------------------
    -- 2.1) Odbiór JSON do CLOB-a
    ----------------------------------------------------------------------------
    DBMS_LOB.CREATETEMPORARY(l_json, TRUE, DBMS_LOB.SESSION);

    LOOP
        BEGIN
            UTL_HTTP.READ_TEXT(l_http_response, l_chunk, 32767);

            EXIT WHEN l_chunk IS NULL;

            DBMS_LOB.WRITEAPPEND(l_json, LENGTH(l_chunk), l_chunk);
        EXCEPTION
            WHEN UTL_HTTP.END_OF_BODY THEN
                EXIT;
        END;
    END LOOP;

    UTL_HTTP.END_RESPONSE(l_http_response);

    ----------------------------------------------------------------------------
    -- 3) Parsujemy mały JSON zwrócony z .NET do poszczególnych zmiennych
    ----------------------------------------------------------------------------
    SELECT measuredAt,
           tempC,
           humidity,
           windSpeedMs,
           isRain
      INTO l_measured_at,
           l_temp_c,
           l_humidity,
           l_wind_speed_ms,
           l_is_rain_raw
      FROM JSON_TABLE(
               l_json,
               '$'
               COLUMNS (
                   measuredAt   TIMESTAMP   PATH '$.measuredAt',
                   tempC        NUMBER      PATH '$.tempC',
                   humidity     NUMBER      PATH '$.humidity',
                   windSpeedMs  NUMBER      PATH '$.windSpeedMs',
                   isRain       VARCHAR2(5) PATH '$.isRain'
               )
           );

    ----------------------------------------------------------------------------
    -- 4) Konwersja bool / 0/1 → 'Y' / 'N'
    ----------------------------------------------------------------------------
    l_is_rain_char :=
        CASE LOWER(l_is_rain_raw)
            WHEN 'true' THEN 'Y'
            WHEN '1'    THEN 'Y'
            ELSE 'N'
        END;

    ----------------------------------------------------------------------------
    -- 5) Insert do tabeli pomiarów
    ----------------------------------------------------------------------------
    INSERT INTO TMYSZAK.WEATHER_MEASUREMENTS
        (
            ID_LOCATION,
            MEASURED_AT,
            TEMP_C,
            HUMIDITY,
            WIND_SPEED_MS,
            IS_RAIN,
            RAW_JSON
        )
    VALUES
        (
            p_id_location,
            l_measured_at,
            l_temp_c,
            l_humidity,
            l_wind_speed_ms,
            l_is_rain_char,
            l_json
        );

    COMMIT;

    ----------------------------------------------------------------------------
    -- 6) Sprzątanie zasobów
    ----------------------------------------------------------------------------
    IF DBMS_LOB.ISTEMPORARY(l_json) = 1 THEN
        DBMS_LOB.FREETEMPORARY(l_json);
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        -- W przypadku błędu czyścimy tymczasowego CLOB-a
        IF DBMS_LOB.ISTEMPORARY(l_json) = 1 THEN
            DBMS_LOB.FREETEMPORARY(l_json);
        END IF;

        -- W środowisku produkcyjnym sensownie jest logować błąd do tabeli LOG
        -- zamiast DBMS_OUTPUT. Tu rzucamy dalej błąd z kontekstem.
        RAISE_APPLICATION_ERROR(
            -20002,
            'P_WEATHER_SYNC_ONE_LOC (ID=' || p_id_location || '): ' || SQLERRM
        );
END;
/
--------------------------------------------------------------------------------
--  KONIEC: P_WEATHER_SYNC_ONE_LOC
--------------------------------------------------------------------------------



--------------------------------------------------------------------------------
--  JOB_WEATHER_SYNC_ALL
--  Opis:
--      Job scheduler, który co 10 minut:
--        - iteruje po aktywnych lokalizacjach w WEATHER_LOCATIONS
--        - woła P_WEATHER_SYNC_ONE_LOC dla każdej z nich
--
--  Uwaga:
--      Skrypt tworzy job „od zera”. Jeśli job istnieje, można go wcześniej
--      usunąć (DROP_JOB) lub opakować w blok z obsługą wyjątku.
--------------------------------------------------------------------------------
BEGIN
  DBMS_SCHEDULER.CREATE_JOB (
    job_name        => 'JOB_WEATHER_SYNC_ALL',
    job_type        => 'PLSQL_BLOCK',
    job_action      => q'[
      DECLARE
        CURSOR c_loc IS
          SELECT id_location
            FROM TMYSZAK.WEATHER_LOCATIONS
           WHERE active_flag = 'Y';
      BEGIN
        FOR r IN c_loc LOOP
          TMYSZAK.P_WEATHER_SYNC_ONE_LOC(r.id_location);
        END LOOP;
      END;
    ]',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=MINUTELY;INTERVAL=10',  -- co 10 minut
    enabled         => TRUE,
    auto_drop       => FALSE,
    comments        => 'Cykliczna synchronizacja pogody z lokalnym serwisem Windows'
  );
END;
/
--------------------------------------------------------------------------------
--  KONIEC: JOB_WEATHER_SYNC_ALL
--------------------------------------------------------------------------------



--------------------------------------------------------------------------------
--  ACL: dostęp do 127.0.0.1 dla schematu TMYSZAK
--  Opis:
--      Pozwala schematowi TMYSZAK korzystać z UTL_HTTP (HTTP) na host 127.0.0.1
--------------------------------------------------------------------------------
BEGIN
  DBMS_NETWORK_ACL_ADMIN.append_host_ace(
    host => '127.0.0.1',
    ace  => xs$ace_type(
              privilege_list => xs$name_list('http'),
              principal_name => 'TMYSZAK',
              principal_type => xs_acl.ptype_db
           )
  );
END;
/
--------------------------------------------------------------------------------
--  KONIEC: ACL
--------------------------------------------------------------------------------



--------------------------------------------------------------------------------
--  P_TEST_WEATHER_PROXY
--  Opis:
--      Prosta procedura testowa do sprawdzenia, czy baza widzi serwis Windows
--      na endpoint /health.
--
--      Oczekiwany rezultat:
--        - wypisanie odpowiedzi serwisu (np. "OK") w DBMS_OUTPUT.
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE TMYSZAK.P_TEST_WEATHER_PROXY AS
    l_http_request  UTL_HTTP.req;
    l_http_response UTL_HTTP.resp;
    l_body          VARCHAR2(32767);
BEGIN
    -- (opcjonalnie) timeout
    UTL_HTTP.set_transfer_timeout(5);

    l_http_request  := UTL_HTTP.BEGIN_REQUEST('http://127.0.0.1:5005/health', 'GET');
    l_http_response := UTL_HTTP.GET_RESPONSE(l_http_request);

    UTL_HTTP.READ_TEXT(l_http_response, l_body, 32767);
    UTL_HTTP.END_RESPONSE(l_http_response);

    DBMS_OUTPUT.PUT_LINE('Odpowiedź serwisu: ' || l_body);
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Błąd wywołania serwisu: ' || SQLERRM);
        RAISE;
END;
/


-- uruchomienie procedury testowej
SET SERVEROUTPUT ON;

BEGIN
  TMYSZAK.P_TEST_WEATHER_PROXY;
END;
/

--------------------------------------------------------------------------------
--  KONIEC: P_TEST_WEATHER_PROXY
--------------------------------------------------------------------------------



--------------------------------------------------------------------------------
--  Wywołanie procedury testowej (do użycia w SQL*Plus / SQLcl / SQL Developer)
--
--  Uwaga:
--      SET SERVEROUTPUT i EXEC to komendy klienta (SQL*Plus), NIE PL/SQL.
--      Dlatego odpalamy je w konsoli, a nie wewnątrz bloku PL/SQL.
--------------------------------------------------------------------------------
-- Przykład użycia:
-- SET SERVEROUTPUT ON;
-- EXEC TMYSZAK.P_TEST_WEATHER_PROXY;
--------------------------------------------------------------------------------

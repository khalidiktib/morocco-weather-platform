WITH source AS (
    SELECT * FROM MOROCCO_WEATHER.RAW.WEATHER_RAW
),

staged AS (
    SELECT
        city,
        latitude,
        longitude,
        timestamp                                    AS weather_timestamp,
        DATE(timestamp)                              AS weather_date,
        HOUR(timestamp)                              AS weather_hour,
        temperature_2m,
        humidity,
        wind_speed,
        precipitation,
        weathercode,
        CASE
            WHEN weathercode = 0  THEN 'Clear sky'
            WHEN weathercode IN (1,2,3) THEN 'Partly cloudy'
            WHEN weathercode IN (45,48) THEN 'Foggy'
            WHEN weathercode IN (51,53,55) THEN 'Drizzle'
            WHEN weathercode IN (61,63,65) THEN 'Rain'
            WHEN weathercode IN (71,73,75) THEN 'Snow'
            WHEN weathercode IN (80,81,82) THEN 'Rain showers'
            WHEN weathercode IN (95,96,99) THEN 'Thunderstorm'
            ELSE 'Other'
        END                                          AS weather_description,
        ingested_at
    FROM source
)

SELECT * FROM staged

WITH staging AS (
    SELECT * FROM {{ ref('stg_weather') }}
)

SELECT
    city,
    weather_date,
    AVG(temperature_2m)                          AS avg_temp,
    MIN(temperature_2m)                          AS min_temp,
    MAX(temperature_2m)                          AS max_temp,
    AVG(humidity)                                AS avg_humidity,
    AVG(wind_speed)                              AS avg_wind_speed,
    SUM(precipitation)                           AS total_precipitation,
    MODE(weather_description)                    AS dominant_weather
FROM staging
GROUP BY city, weather_date
ORDER BY city, weather_date

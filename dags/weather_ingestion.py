from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import openmeteo_requests
import requests_cache
from retry_requests import retry
import pandas as pd
import snowflake.connector
import os

# Moroccan cities with coordinates
CITIES = [
    {"name": "Casablanca", "lat": 33.5731, "lon": -7.5898},
    {"name": "Rabat",      "lat": 34.0209, "lon": -6.8416},
    {"name": "Marrakech",  "lat": 31.6295, "lon": -7.9811},
    {"name": "Fes",        "lat": 34.0181, "lon": -5.0078},
    {"name": "Agadir",     "lat": 30.4278, "lon": -9.5981},
    {"name": "Tangier",    "lat": 35.7595, "lon": -5.8340},
]

default_args = {
    "owner": "khalid",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def fetch_and_load():
    # Setup Open-Meteo client with cache and retry
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    om = openmeteo_requests.Client(session=retry_session)

    all_records = []

    for city in CITIES:
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "hourly": [
                "temperature_2m",
                "relative_humidity_2m",
                "wind_speed_10m",
                "precipitation",
                "weathercode"
            ],
            "past_days": 7,
            "forecast_days": 1,
        }

        responses = om.weather_api("https://api.open-meteo.com/v1/forecast", params=params)
        response = responses[0]
        hourly = response.Hourly()

        times = pd.date_range(
            start=pd.Timestamp(hourly.Time(), unit="s", tz="UTC"),
            end=pd.Timestamp(hourly.TimeEnd(), unit="s", tz="UTC"),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        )

        for i, t in enumerate(times):
            all_records.append({
                "city":             city["name"],
                "latitude":         city["lat"],
                "longitude":        city["lon"],
                "timestamp":        t.strftime("%Y-%m-%d %H:%M:%S"),
                "temperature_2m":   round(float(hourly.Variables(0).ValuesAsNumpy()[i]), 2),
                "humidity":         round(float(hourly.Variables(1).ValuesAsNumpy()[i]), 2),
                "wind_speed":       round(float(hourly.Variables(2).ValuesAsNumpy()[i]), 2),
                "precipitation":    round(float(hourly.Variables(3).ValuesAsNumpy()[i]), 2),
                "weathercode":      int(hourly.Variables(4).ValuesAsNumpy()[i]),
                "ingested_at":      datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            })

    df = pd.DataFrame(all_records)
    print(f"Fetched {len(df)} records for {len(CITIES)} cities")

    # Connect to Snowflake and load
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database="MOROCCO_WEATHER",
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        schema="RAW",
    )
    cur = conn.cursor()
    cur.execute("USE DATABASE MOROCCO_WEATHER")
    cur.execute("USE SCHEMA RAW")
    cur.execute("USE WAREHOUSE MOROCCO_WH")

    # Create table if not exists	
    cur.execute("""
        CREATE TABLE IF NOT EXISTS MOROCCO_WEATHER.RAW.WEATHER_RAW (
            city             VARCHAR,
            latitude         FLOAT,
            longitude        FLOAT,
            timestamp        TIMESTAMP_NTZ,
            temperature_2m   FLOAT,
            humidity         FLOAT,
            wind_speed       FLOAT,
            precipitation    FLOAT,
            weathercode      INTEGER,
            ingested_at      TIMESTAMP_NTZ
        )
    """)
    cur.execute("""
        DELETE FROM MOROCCO_WEATHER.RAW.WEATHER_RAW
        WHERE ingested_at::DATE = CURRENT_DATE()
    """)

    # Insert records
    insert_sql = """
        INSERT INTO MOROCCO_WEATHER.RAW.WEATHER_RAW
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    rows = [
        (
            r["city"], r["latitude"], r["longitude"], r["timestamp"],
            r["temperature_2m"], r["humidity"], r["wind_speed"],
            r["precipitation"], r["weathercode"], r["ingested_at"]
        )
        for r in all_records
    ]
    cur.executemany(insert_sql, rows)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Loaded {len(rows)} rows into Snowflake RAW.WEATHER_RAW")


with DAG(
    dag_id="weather_ingestion",
    default_args=default_args,
    description="Fetch weather data for Moroccan cities and load into Snowflake",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["morocco", "weather", "ingestion"],
) as dag:

    ingest = PythonOperator(
        task_id="fetch_and_load_weather",
        python_callable=fetch_and_load,
    )

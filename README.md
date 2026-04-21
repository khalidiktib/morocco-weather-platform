# 🇲🇦 Morocco Weather Intelligence Platform
### End-to-End Automated Data & ML Pipeline

An automated production-grade pipeline that ingests real-time weather data for Moroccan cities, transforms it through a multi-layer Snowflake data warehouse, and generates daily temperature forecasts using a Prophet time-series model — all orchestrated with Apache Airflow running inside Docker.

---

## Architecture

```
Open-Meteo API (6 Moroccan Cities)
          │
          ▼
  [Airflow DAG — Daily]
          │
          ▼
  Snowflake — RAW Layer
  (raw hourly weather data)
          │
          ▼
      [dbt Run]
          │
          ▼
  Snowflake — STAGING Layer        Snowflake — MART Layer
  (cleaned, enriched view)         (daily aggregations table)
                                           │
                                           ▼
                                 [Prophet ML Model]
                                           │
                                           ▼
                               Snowflake — MART Layer
                               (7-day forecasts per city)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python, Open-Meteo API |
| Orchestration | Apache Airflow |
| Containerization | Docker, Docker Compose |
| Data Warehouse | Snowflake |
| Transformation | dbt (data build tool) |
| Forecasting | Prophet (Meta) |
| Language | Python, SQL |

---

## Data Pipeline

### RAW Layer
Raw hourly weather data landed directly from the Open-Meteo API for 6 Moroccan cities: **Casablanca, Rabat, Marrakech, Fes, Agadir, Tangier**.

Variables ingested per city per hour:
- Temperature (°C)
- Relative Humidity (%)
- Wind Speed (km/h)
- Precipitation (mm)
- Weather Code

### STAGING Layer
dbt view that cleans and enriches raw data:
- Extracts date and hour from timestamp
- Maps weather codes to human-readable descriptions (Clear sky, Rain, Thunderstorm, etc.)
- Renames and standardizes columns

### MART Layer
Two tables produced by dbt and the ML model:

**`MART_WEATHER_DAILY`** — Daily aggregations per city:
- Avg / Min / Max temperature
- Avg humidity and wind speed
- Total precipitation
- Dominant weather condition

**`WEATHER_FORECASTS`** — Prophet model output:
- 7-day temperature forecast per city
- Confidence intervals (lower/upper bound)
- Training timestamp

---

## Automation

A single master DAG `weather_pipeline` runs daily and chains all steps in sequence:

```
fetch_and_load_weather → dbt_transform → train_and_forecast
```

If any step fails, downstream steps are automatically skipped.

---

## Project Structure

```
morocco-weather-platform/
├── dags/
│   ├── weather_ingestion.py      # Ingestion DAG
│   ├── weather_forecast.py       # Forecast DAG
│   └── weather_pipeline.py       # Master pipeline DAG
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles/
│   │   └── profiles.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── models/
│       ├── staging/
│       │   └── stg_weather.sql
│       └── mart/
│           └── mart_weather_daily.sql
├── docker-compose.yml
└── .gitignore
```

---

## How to Run

### Prerequisites
- Docker & Docker Compose
- Snowflake account
- WSL2 (if on Windows)

### 1. Clone the repository
```bash
git clone https://github.com/khalidiktib/morocco-weather-platform.git
cd morocco-weather-platform
```

### 2. Configure environment variables
Create a `.env` file in the project root:
```env
AIRFLOW_UID=50000
SNOWFLAKE_ACCOUNT=your-account-identifier
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_DATABASE=MOROCCO_WEATHER
SNOWFLAKE_WAREHOUSE=MOROCCO_WH
SNOWFLAKE_SCHEMA=RAW
```

### 3. Set up Snowflake
Run the following in a Snowflake worksheet:
```sql
CREATE DATABASE IF NOT EXISTS MOROCCO_WEATHER;
CREATE SCHEMA IF NOT EXISTS MOROCCO_WEATHER.RAW;
CREATE SCHEMA IF NOT EXISTS MOROCCO_WEATHER.STAGING;
CREATE SCHEMA IF NOT EXISTS MOROCCO_WEATHER.MART;

CREATE WAREHOUSE IF NOT EXISTS MOROCCO_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;
```

### 4. Start Airflow
```bash
docker compose up airflow-init
docker compose up -d
```

### 5. Access the Airflow UI
Open [http://localhost:8080](http://localhost:8080) and log in with `admin / admin`.

Activate the `weather_pipeline` DAG and trigger it manually for the first run.

---

## Model Notes

The forecasting model is built with **Prophet** (Meta), chosen for its native handling of time-series seasonality and robustness to missing data — well suited for daily weather patterns.

The model improves automatically as data accumulates daily. With 2-3 months of data, weekly seasonality becomes reliable. With 1+ year, yearly cycles (Moroccan summer/winter) are captured.

---

## Cities Covered

| City | Latitude | Longitude |
|---|---|---|
| Casablanca | 33.5731 | -7.5898 |
| Rabat | 34.0209 | -6.8416 |
| Marrakech | 31.6295 | -7.9811 |
| Fes | 34.0181 | -5.0078 |
| Agadir | 30.4278 | -9.5981 |
| Tangier | 35.7595 | -5.8340 |

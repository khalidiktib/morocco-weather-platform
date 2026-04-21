from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import snowflake.connector
import pandas as pd
from prophet import Prophet
import os

default_args = {
    "owner": "khalid",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def train_and_forecast():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database="MOROCCO_WEATHER",
        warehouse="MOROCCO_WH",
        schema="STAGING",
    )
    cur = conn.cursor()
    cur.execute("USE DATABASE MOROCCO_WEATHER")
    cur.execute("USE WAREHOUSE MOROCCO_WH")

    # Load mart data
    cur.execute("SELECT city, weather_date, avg_temp FROM MOROCCO_WEATHER.STAGING_MART.MART_WEATHER_DAILY ORDER BY city, weather_date")
    rows = cur.fetchall()
    df = pd.DataFrame(rows, columns=["city", "ds", "y"])
    df["ds"] = pd.to_datetime(df["ds"])

    # Create predictions table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS MOROCCO_WEATHER.MART.WEATHER_FORECASTS (
            city          VARCHAR,
            forecast_date DATE,
            predicted_temp FLOAT,
            lower_bound   FLOAT,
            upper_bound   FLOAT,
            trained_at    TIMESTAMP_NTZ
        )
    """)
    cur.execute("DELETE FROM MOROCCO_WEATHER.MART.WEATHER_FORECASTS")

    all_forecasts = []
    cities = df["city"].unique()

    for city in cities:
        city_df = df[df["city"] == city][["ds", "y"]].reset_index(drop=True)

        if len(city_df) < 2:
            print(f"Skipping {city} — not enough data")
            continue

        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=False,
            changepoint_prior_scale=0.05,
        )
        model.fit(city_df)

        future = model.make_future_dataframe(periods=7)
        forecast = model.predict(future)

        forecast_tail = forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(7)
        trained_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        for _, row in forecast_tail.iterrows():
            all_forecasts.append((
                city,
                row["ds"].strftime("%Y-%m-%d"),
                round(row["yhat"], 2),
                round(row["yhat_lower"], 2),
                round(row["yhat_upper"], 2),
                trained_at,
            ))

        print(f"Forecasted 7 days for {city}")

    insert_sql = """
        INSERT INTO MOROCCO_WEATHER.MART.WEATHER_FORECASTS
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cur.executemany(insert_sql, all_forecasts)
    conn.commit()
    cur.close()
    conn.close()
    print(f"Stored {len(all_forecasts)} forecast rows in Snowflake MART.WEATHER_FORECASTS")


with DAG(
    dag_id="weather_forecast",
    default_args=default_args,
    description="Train Prophet model and forecast temperature for Moroccan cities",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["morocco", "weather", "ml", "forecast"],
) as dag:

    forecast = PythonOperator(
        task_id="train_and_forecast",
        python_callable=train_and_forecast,
    )

from datetime import datetime
from pathlib import Path
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator

BASE_DIR = Path.home() / "etl_hw_2"
INPUT_FILE = BASE_DIR / "data" / "IOT-temp.csv"
CLEANED_FILE = BASE_DIR / "output" / "cleaned_iot_temp.csv"
HOT_FILE = BASE_DIR / "output" / "top_5_hottest_days.csv"
COLD_FILE = BASE_DIR / "output" / "top_5_coldest_days.csv"


def process_iot_data():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(f"Не найден файл: {INPUT_FILE}")

    df = pd.read_csv(INPUT_FILE)


    df = df[df["out/in"] == "In"].copy()


    df["noted_date"] = pd.to_datetime(df["noted_date"], errors="coerce")
    df = df.dropna(subset=["noted_date"])
    df["noted_date"] = df["noted_date"].dt.date


    q05 = df["temp"].quantile(0.05)
    q95 = df["temp"].quantile(0.95)
    df = df[(df["temp"] >= q05) & (df["temp"] <= q95)].copy()

    CLEANED_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CLEANED_FILE, index=False)

    daily_temp = (
        df.groupby("noted_date", as_index=False)["temp"]
        .mean()
        .rename(columns={"temp": "avg_temp"})
    )

    hottest = daily_temp.sort_values("avg_temp", ascending=False).head(5)
    coldest = daily_temp.sort_values("avg_temp", ascending=True).head(5)

    hottest.to_csv(HOT_FILE, index=False)
    coldest.to_csv(COLD_FILE, index=False)

    print("Готово")
    print(CLEANED_FILE)
    print(HOT_FILE)
    print(COLD_FILE)


with DAG(
    dag_id="process_iot_temperature_data",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "csv", "temperature", "homework"],
) as dag:

    process_task = PythonOperator(
        task_id="process_iot_temp_csv",
        python_callable=process_iot_data,
    )

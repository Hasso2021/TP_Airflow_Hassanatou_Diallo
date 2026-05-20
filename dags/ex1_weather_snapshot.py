"""
Exercice 1 — Weather Snapshot (AIRFLOW_EXOS).
"""

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.sdk import Asset

# ici on importe les fonctions utiles pour le weather snapshot
from common.weather_utils import (
    SNAPSHOTS_DIR,
    compute_risk_level,
    fetch_forecast,
    geocode_city,
    hourly_metrics,
    utc_now_iso,
    write_json,
)

# ici on importe les assets utiles pour le weather snapshot
WEATHER_SNAPSHOT_ASSET = Asset("file:///opt/airflow/data/weather_snapshots")

# definition de la dag  qui va permettre de generer un snapshot du temps pour une ville donnee
@dag(
    dag_id="weather_snapshot",
    description="Daily weather snapshot for a city (Open-Meteo).",
    schedule="@daily", # ici on specifie que la dag se lance quotidiennement
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"), # date de debut de la dag est le 1er janvier 2026
    catchup=False, # ici on specifie que la dag ne se lance pas en catchup
    tags=["weather", "ex1"], # tags role est de permettre de filtrer les dags dans l'interface d'airflow
)

# declare deux taches dans la dag : resolve_city et fetch_weather
def weather_snapshot():
    # tache 1 : resolve_city qui permet de resoudre la ville pour laquelle on veut generer le snapshot
    @task
    def resolve_city() -> dict:
        return geocode_city()

    # tache 2 : fetch_weather qui permet de recuperer le forecast du temps pour la ville donnee
    @task
    def fetch_weather(location: dict) -> dict:
        forecast = fetch_forecast(
            location["latitude"],
            location["longitude"],
            hourly="temperature_2m,precipitation,windspeed_10m,weather_code",
            forecast_days=1, 
            timezone_name=location.get("timezone", "UTC"),
        )

        # calcul des metriques du temps pour la ville donnee (temperature, precipitation, vent, etc.)
        metrics = hourly_metrics(forecast)
        return {
            "city": location["city"],
            "coordinates": {
                "latitude": location["latitude"],
                "longitude": location["longitude"],
            },
            "timezone": location.get("timezone"),
            "generated_at": utc_now_iso(),
            "metrics": metrics,
            "risk_level": compute_risk_level(
                metrics["avg_temperature_c"],
                metrics["max_wind_speed_kmh"],
                metrics["total_precipitation_mm"],
            ),
            "source": "open-meteo",
        }



    # tache 3 : save_snapshot qui permet de sauvegarder le snapshot du temps pour la ville donnee
    @task(outlets=[WEATHER_SNAPSHOT_ASSET])
    def save_snapshot(snapshot: dict) -> str:
        timestamp = pendulum.now("UTC").format("YYYYMMDD_HHmmss")
        filename = f"{snapshot['city'].replace(' ', '_').lower()}_{timestamp}.json"
        path = SNAPSHOTS_DIR / filename
        return write_json(path, snapshot)

    save_snapshot(fetch_weather(resolve_city()))


weather_snapshot()



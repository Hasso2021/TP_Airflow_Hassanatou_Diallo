"""
Exercice 2 —  (extract → transform → load),
ici on ne garde que les heures de la période Airflow (data_interval).
Deux DAGs : un toutes les heures, un tous les jours.
"""

import json
import os
from collections import Counter
from datetime import datetime

import pendulum  # pour comparer les dates/heures de l'API météo
import requests
from airflow.sdk import dag, get_current_context, task

# Dossier où on écrit les rapports (dans le conteneur Docker)
OUTPUT_DIR = "/opt/airflow/data/weather_intervals"


def _geocode_and_forecast(city: str, past_hours: int) -> dict:
    """
    Petite fonction réutilisable (hors tâche Airflow) :
    1) trouve lat/lon de la ville
    2) récupère les weather_code heure par heure
    past_hours = combien d'heures d'historique on demande à l'API
    """
    geo_url = f"https://geocoding-api.open-meteo.com/v1/search?name={city}"
    geo = requests.get(geo_url, timeout=30).json()
    lat = geo["results"][0]["latitude"]
    lon = geo["results"][0]["longitude"]
    tz = geo["results"][0].get("timezone", "UTC")
    name = geo["results"][0].get("name", city)

    meteo_url = (
        "https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}"
        "&hourly=weather_code"
        f"&forecast_days=2&past_hours={past_hours}&timezone={tz}"
    )
    meteo = requests.get(meteo_url, timeout=30).json()

    return {
        "city": name,
        "latitude": lat,
        "longitude": lon,
        "timezone": tz,
        "hourly": meteo["hourly"],
    }


def _interval_bounds():
    """
    Airflow connaît l'intervalle de la run (ex. 10h→11h pour @hourly).
    On lit ces infos sur dag_run (compatible trigger manuel en Airflow 3).
    """
    dag_run = get_current_context()["dag_run"]
    return dag_run.data_interval_start, dag_run.data_interval_end


def _codes_in_airflow_interval(raw: dict) -> list[int]:
    """
    Parmi toutes les heures renvoyées par l'API, on ne garde que celles
    qui tombent dans [data_interval_start, data_interval_end[.
    """
    start, end = _interval_bounds()

    times = raw["hourly"]["time"]
    codes = raw["hourly"]["weather_code"]
    tz_name = raw["timezone"]

    selected = []
    for time_str, code in zip(times, codes):
        if code is None:
            continue
        hour_dt = pendulum.parse(time_str, tz=tz_name)
        if start <= hour_dt < end:
            selected.append(int(code))
    return selected


# --- DAG 1 : rapport toutes les heures ---
@dag(
    dag_id="weather_hourly",
    schedule="@hourly",  # une run par heure
    start_date=datetime(2026, 1, 1),
    catchup=False,  # pas de rattrapage des runs passées
    tags=["weather", "ex2"],
)
def weather_hourly(city="Paris"):
    @task()
    def extract(city):
        # Étape 1 : récupérer les données brutes (48 h d'historique pour être large)
        return _geocode_and_forecast(city, past_hours=48)

    @task()
    def transform(raw):
        # Étape 2 : filtrer sur l'intervalle Airflow + compter les codes météo
        codes = _codes_in_airflow_interval(raw)
        counts = Counter(codes)  # ex. {0: 3, 61: 1} = 3 fois "clair", 1 fois "pluie"
        dominant = counts.most_common(1)[0][0] if codes else None

        start, end = _interval_bounds()
        return {
            "city": raw["city"],
            "data_interval_start": start.isoformat(),
            "data_interval_end": end.isoformat(),
            "total_measurements": len(codes),
            "weather_code_distribution": {str(k): v for k, v in sorted(counts.items())},
            "dominant_weather_code": dominant,
            "generated_at": datetime.now().isoformat(),
        }

    @task()
    def load(report):
        # Étape 3 : écrire le rapport en JSON sur disque
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        start = report["data_interval_start"].replace(":", "").replace("-", "")
        fname = f"{report['city'].replace(' ', '_').lower()}_weather_hourly_{start}.json"
        path = os.path.join(OUTPUT_DIR, fname)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        return path

    # Enchaînement des tâches : extract → transform → load
    data = extract(city)
    report = transform(data)
    load(report)


# --- DAG 2 : même logique, mais une fois par jour ---
@dag(
    dag_id="weather_daily",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "ex2"],
)
def weather_daily(city="Paris"):
    @task()
    def extract(city):
        # 168 h = 7 jours d'historique (utile pour un rapport journalier)
        return _geocode_and_forecast(city, past_hours=168)

    @task()
    def transform(raw):
        codes = _codes_in_airflow_interval(raw)
        counts = Counter(codes)
        dominant = counts.most_common(1)[0][0] if codes else None

        start, end = _interval_bounds()
        return {
            "city": raw["city"],
            "data_interval_start": start.isoformat(),
            "data_interval_end": end.isoformat(),
            "total_measurements": len(codes),
            "weather_code_distribution": {str(k): v for k, v in sorted(counts.items())},
            "dominant_weather_code": dominant,
            "generated_at": datetime.now().isoformat(),
        }

    @task()
    def load(report):
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        start = report["data_interval_start"].replace(":", "").replace("-", "")
        fname = f"{report['city'].replace(' ', '_').lower()}_weather_daily_{start}.json"
        path = os.path.join(OUTPUT_DIR, fname)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        return path

    data = extract(city)
    report = transform(data)
    load(report)


# Enregistre les deux DAGs dans Airflow (obligatoire en fin de fichier)
weather_hourly()
weather_daily()

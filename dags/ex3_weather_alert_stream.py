"""
Exercice 3 — Même schéma extract → transform → load que l'exercice 1.
Ici "load" = envoyer un message Kafka si une alerte météo est détectée.
"""

import json
from datetime import datetime

import requests
from airflow.sdk import dag, task

# Nom du topic Kafka (doit exister côté broker)
KAFKA_TOPIC = "weather_alerts"


@dag(
    dag_id="weather_alert_stream",
    schedule="* * * * *",  # cron : toutes les minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # une seule run à la fois (évite les doublons)
    tags=["weather", "ex3", "kafka"],
)
def weather_alert_stream(city="Paris"):
    @task()
    def extract(city):
        """
        Tâche 1 — extract :
        géocodage de la ville + lecture de la météo ACTUELLE (pas hourly).
        """
        geo_url = f"https://geocoding-api.open-meteo.com/v1/search?name={city}"
        geo = requests.get(geo_url, timeout=30).json()
        lat = geo["results"][0]["latitude"]
        lon = geo["results"][0]["longitude"]
        tz = geo["results"][0].get("timezone", "UTC")
        name = geo["results"][0].get("name", city)

        # "current" = température / vent / code météo maintenant
        url = (
            "https://api.open-meteo.com/v1/forecast?"
            f"latitude={lat}&longitude={lon}"
            "&current=temperature_2m,windspeed_10m,weather_code"
            f"&timezone={tz}"
        )
        meteo = requests.get(url, timeout=30).json()
        cur = meteo.get("current") or {}

        return {
            "city": name,
            "temperature_c": float(cur["temperature_2m"]),
            "wind_speed_kmh": float(cur["windspeed_10m"]),
            "weather_code": int(cur["weather_code"]),
            "time": cur.get("time") or datetime.now().isoformat(),
        }

    @task()
    def transform(payload):
        """
        Tâche 2 — transform :
        on applique des règles simples pour créer 0, 1 ou 2 alertes.
        """
        alerts = []
        t = payload["temperature_c"]
        w = payload["wind_speed_kmh"]

        # Champs communs à toutes les alertes
        base = {
            "city": payload["city"],
            "timestamp": payload["time"],
            "temperature_c": t,
            "wind_speed_kmh": w,
            "weather_code": payload["weather_code"],
        }

        # Règle 1 : froid + vent fort
        if t < 5 and w > 20:
            alerts.append({**base, "alert_type": "cold_alert"})

        # Règle 2 : forte chaleur
        if t > 35:
            alerts.append({**base, "alert_type": "hot_alert"})

        return alerts  # liste vide = pas d'alerte, la tâche load ne fera rien

    @task()
    def load(alerts):
        """
        Tâche 3 — load :
        si la liste n'est pas vide, on publie chaque alerte dans Kafka.
        """
        if not alerts:
            return 0

        # Import ici (pas en haut du fichier) pour ne pas casser le parsing du DAG
        # si le provider Kafka n'est pas installé sur ta machine locale
        from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook

        hook = KafkaProducerHook(kafka_config_id="kafka_default")
        for alert in alerts:
            hook.produce(
                topic=KAFKA_TOPIC,
                value=json.dumps(alert).encode("utf-8"),
            )
        return len(alerts)  # nombre de messages envoyés

    # Chaîne des tâches (comme exercice 1)
    payload = extract(city)
    alerts = transform(payload)
    load(alerts)


weather_alert_stream()

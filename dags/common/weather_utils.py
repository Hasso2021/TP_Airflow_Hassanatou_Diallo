"""Shared helpers for Open-Meteo weather DAGs (AIRFLOW_EXOS)."""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

DEFAULT_CITY = "Paris"
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

DATA_ROOT = Path("/opt/airflow/data")
SNAPSHOTS_DIR = DATA_ROOT / "weather_snapshots"
INTERVALS_DIR = DATA_ROOT / "weather_intervals"
REPORTS_DIR = DATA_ROOT / "weather_reports"

KAFKA_TOPIC = "weather_alerts"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def geocode_city(city: str = DEFAULT_CITY) -> dict[str, Any]:
    response = requests.get(
        GEOCODING_URL,
        params={"name": city, "count": 1, "language": "fr", "format": "json"},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()
    results = payload.get("results") or []
    if not results:
        raise ValueError(f"No geocoding result for city: {city}")
    location = results[0]
    return {
        "city": location.get("name", city),
        "latitude": location["latitude"],
        "longitude": location["longitude"],
        "timezone": location.get("timezone", "UTC"),
        "country": location.get("country"),
    }


def fetch_forecast(
    latitude: float,
    longitude: float,
    *,
    hourly: str,
    forecast_days: int = 1,
    past_hours: int | None = None,
    timezone_name: str = "UTC",
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": hourly,
        "forecast_days": forecast_days,
        "timezone": timezone_name,
    }
    if past_hours is not None:
        params["past_hours"] = past_hours
    response = requests.get(FORECAST_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_current_weather(
    latitude: float,
    longitude: float,
    *,
    timezone_name: str = "UTC",
) -> dict[str, Any]:
    response = requests.get(
        FORECAST_URL,
        params={
            "latitude": latitude,
            "longitude": longitude,
            "current": "temperature_2m,windspeed_10m,weather_code",
            "timezone": timezone_name,
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def compute_risk_level(avg_temp: float, max_wind: float, precip_sum: float) -> str:
    if max_wind > 60 or precip_sum > 20 or avg_temp < 0 or avg_temp > 35:
        return "high"
    if max_wind > 30 or precip_sum > 5 or avg_temp < 5 or avg_temp > 30:
        return "medium"
    return "low"


def hourly_metrics(forecast: dict[str, Any]) -> dict[str, float]:
    hourly = forecast.get("hourly") or {}
    temperatures = hourly.get("temperature_2m") or []
    winds = hourly.get("windspeed_10m") or []
    precipitations = hourly.get("precipitation") or []
    if not temperatures:
        raise ValueError("No hourly temperature data returned by Open-Meteo")
    avg_temp = sum(temperatures) / len(temperatures)
    max_wind = max(winds) if winds else 0.0
    precip_sum = sum(precipitations) if precipitations else 0.0
    return {
        "avg_temperature_c": round(avg_temp, 2),
        "max_wind_speed_kmh": round(max_wind, 2),
        "total_precipitation_mm": round(precip_sum, 2),
    }


def filter_weather_codes_for_interval(
    forecast: dict[str, Any],
    interval_start,
    interval_end,
    timezone_name: str = "UTC",
) -> list[int]:
    import pendulum

    hourly = forecast.get("hourly") or {}
    times = hourly.get("time") or []
    codes = hourly.get("weather_code") or []
    selected: list[int] = []
    for time_str, code in zip(times, codes):
        if code is None:
            continue
        hour_dt = pendulum.parse(time_str, tz=timezone_name)
        if interval_start <= hour_dt < interval_end:
            selected.append(int(code))
    return selected


def weather_code_report(
    city: str,
    interval_start,
    interval_end,
    weather_codes: list[int],
) -> dict[str, Any]:
    distribution = dict(Counter(weather_codes))
    dominant = Counter(weather_codes).most_common(1)[0][0] if weather_codes else None
    return {
        "city": city,
        "data_interval_start": interval_start.isoformat(),
        "data_interval_end": interval_end.isoformat(),
        "total_measurements": len(weather_codes),
        "weather_code_distribution": {str(k): v for k, v in sorted(distribution.items())},
        "dominant_weather_code": dominant,
        "generated_at": utc_now_iso(),
    }


def write_json(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return str(path)


def load_snapshot_files() -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    if not SNAPSHOTS_DIR.exists():
        return snapshots
    for file_path in sorted(SNAPSHOTS_DIR.glob("*.json")):
        snapshots.append(json.loads(file_path.read_text(encoding="utf-8")))
    return snapshots


def build_aggregation_report(snapshots: list[dict[str, Any]]) -> dict[str, Any]:
    if not snapshots:
        return {
            "total_snapshots": 0,
            "global_avg_temperature_c": None,
            "hottest_city": None,
            "coldest_city": None,
            "risk_level_distribution": {},
            "cities_processed": [],
            "generated_at": utc_now_iso(),
        }

    temps: list[tuple[str, float]] = []
    risk_counter: Counter[str] = Counter()
    cities: list[str] = []

    for snapshot in snapshots:
        city = snapshot.get("city", "unknown")
        cities.append(city)
        metrics = snapshot.get("metrics", {})
        avg_temp = metrics.get("avg_temperature_c")
        if avg_temp is not None:
            temps.append((city, float(avg_temp)))
        risk_counter[snapshot.get("risk_level", "unknown")] += 1

    global_avg = round(sum(value for _, value in temps) / len(temps), 2) if temps else None
    if temps:
        hot_city, hot_temp = max(temps, key=lambda item: item[1])
        cold_city, cold_temp = min(temps, key=lambda item: item[1])
        hottest = {"city": hot_city, "temperature_c": hot_temp}
        coldest = {"city": cold_city, "temperature_c": cold_temp}
    else:
        hottest = None
        coldest = None

    return {
        "total_snapshots": len(snapshots),
        "global_avg_temperature_c": global_avg,
        "hottest_city": hottest,
        "coldest_city": coldest,
        "risk_level_distribution": dict(risk_counter),
        "cities_processed": sorted(set(cities)),
        "generated_at": utc_now_iso(),
    }

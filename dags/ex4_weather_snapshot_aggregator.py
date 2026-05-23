"""
Exercice 4 — Toujours extract → transform → load.
On lit les fichiers JSON créés par l'exercice 1, on calcule des stats,
puis on écrit un rapport d'agrégation.
"""

import glob
import json
import os
from datetime import datetime

from airflow.sdk import dag, task

# Même racine que exercice 1 (load écrit weather_Ville_date.json ici)
DATA_DIR = "/opt/airflow/data"
# Sous-dossier dédié aux rapports de cet exercice
REPORTS_DIR = os.path.join(DATA_DIR, "weather_reports")


@dag(
    dag_id="weather_snapshot_aggregator",
    schedule="@daily",  # une fois par jour (simple pour débuter)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "ex4"],
)
def weather_snapshot_aggregator():
    @task()
    def extract():
        """
        Tâche 1 — extract :
        lit tous les fichiers weather_*.json produits par l'exercice 1.
        """
        pattern = os.path.join(DATA_DIR, "weather_*.json")
        paths = sorted(glob.glob(pattern))

        snapshots = []
        for path in paths:
            with open(path, encoding="utf-8") as f:
                snapshots.append(json.load(f))
        return snapshots  # liste de dict (un dict = un snapshot)

    @task()
    def transform(snapshots):
        """
        Tâche 2 — transform :
        calcule moyenne globale, ville la plus chaude / la plus froide.
        """
        if not snapshots:
            return {
                "total_snapshots": 0,
                "message": "Aucun fichier weather_*.json trouvé.",
                "generated_at": datetime.now().isoformat(),
            }

        rows = []  # paires (ville, température moyenne)
        for s in snapshots:
            city = s.get("city", "inconnu")

            # Format exercice 1 simple (clé avg_temperature)
            avg = s.get("avg_temperature")
            if avg is None:
                # Ancien format possible (clé dans metrics)
                m = s.get("metrics") or {}
                avg = m.get("avg_temperature_c")

            if avg is not None:
                rows.append((city, float(avg)))

        if not rows:
            return {
                "total_snapshots": len(snapshots),
                "message": "Fichiers trouvés mais pas de température moyenne lisible.",
                "generated_at": datetime.now().isoformat(),
            }

        global_avg = round(sum(v for _, v in rows) / len(rows), 2)
        hottest_city, hot_temp = max(rows, key=lambda x: x[1])
        coldest_city, cold_temp = min(rows, key=lambda x: x[1])

        return {
            "total_snapshots": len(snapshots),
            "global_avg_temperature_c": global_avg,
            "hottest_city": {"city": hottest_city, "temperature_c": hot_temp},
            "coldest_city": {"city": coldest_city, "temperature_c": cold_temp},
            "cities_seen": sorted({c for c, _ in rows}),
            "generated_at": datetime.now().isoformat(),
        }

    @task()
    def load(report):
        """
        Tâche 3 — load :
        sauvegarde le rapport dans weather_reports/.
        """
        os.makedirs(REPORTS_DIR, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(REPORTS_DIR, f"aggregation_{stamp}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        return path

    # Enchaînement : extract → transform → load
    snaps = extract()
    summary = transform(snaps)
    load(summary)


weather_snapshot_aggregator()

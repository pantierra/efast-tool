import json
from pathlib import Path
from datetime import datetime

WINDOW_DAYS = 14
NDVI_THRESHOLD = 0.3
NDVI_DELTA = 0.15
MIN_WINDOW_SIZE = 3


def detect_clouds(season, site_name):
    output_file = Path(f"data/{site_name}/{season}/clouds.json")
    clouds = {"s2": [], "s3": []}

    for source in ["s2", "s3"]:
        timeseries_file = Path(
            f"data/{site_name}/{season}/raw/ndvi/{source}/timeseries.json"
        )
        if not timeseries_file.exists():
            print(f"[CLOUDS-{source.upper()}] No timeseries.json found")
            continue

        print(f"[CLOUDS-{source.upper()}] Processing {timeseries_file}...")

        with open(timeseries_file) as f:
            timeseries = json.load(f)

        entries = [
            (e, datetime.fromisoformat(e["date"].replace("Z", "+00:00")))
            for e in timeseries
            if e.get("ndvi") is not None
        ]

        for entry, entry_date in entries:
            window_ndvi = [
                e["ndvi"]
                for e, d in entries
                if abs((d - entry_date).days) <= WINDOW_DAYS
            ]

            if len(window_ndvi) < MIN_WINDOW_SIZE:
                continue

            max_ndvi = max(window_ndvi)
            threshold = max_ndvi - NDVI_DELTA

            if entry["ndvi"] < threshold and entry["ndvi"] < NDVI_THRESHOLD:
                clouds[source].append(entry["filename"])

        print(
            f"[CLOUDS-{source.upper()}] Found {len(clouds[source])} cloud-covered files"
        )

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(clouds, f, indent=2)

    print(f"[CLOUDS] Saved: {output_file}")

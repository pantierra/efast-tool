"""Pre-selection: NDVI-based cloud/flaw filtering for S2 and S3 data."""
import json
from pathlib import Path
from datetime import datetime

WINDOW_DAYS = 14
MIN_WINDOW_SIZE = 3
THRESHOLDS = {"aggressive": {"threshold": 0.3, "delta": 0.15}, "nonaggressive": {"threshold": 0.2, "delta": 0.25}}


def detect_clouds(season, site_name, cleaning_strategy="aggressive"):
    """Filter cloud-covered/flawed S2 and S3 files using NDVI thresholds."""
    output_file = Path(f"data/{site_name}/{season}/clouds_{cleaning_strategy}.json")
    clouds = {"s2": [], "s3": []}
    thresholds = THRESHOLDS[cleaning_strategy]

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

        # Flag entries with ndvi: None as outliers (bad/invalid data)
        for e in timeseries:
            if e.get("ndvi") is None:
                clouds[source].append(e["filename"])

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
            threshold = max_ndvi - thresholds["delta"]

            if entry["ndvi"] < threshold and entry["ndvi"] < thresholds["threshold"]:
                clouds[source].append(entry["filename"])

        print(
            f"[CLOUDS-{source.upper()}] Found {len(clouds[source])} cloud-covered files"
        )

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(clouds, f, indent=2)

    print(f"[CLOUDS] Saved: {output_file}")


# Alias for backward compatibility
preselect = detect_clouds

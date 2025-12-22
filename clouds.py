import json
from pathlib import Path
from datetime import datetime


def detect_clouds(year, site_name):
    output_file = Path(f"data/{site_name}/{year}/clouds.json")
    clouds = {"s2": [], "s3": []}
    
    for source in ["s2", "s3"]:
        timeseries_file = Path(f"data/{site_name}/{year}/ndvi/{source}/timeseries.json")
        if not timeseries_file.exists():
            print(f"[CLOUDS-{source.upper()}] No timeseries.json found")
            continue
        
        print(f"[CLOUDS-{source.upper()}] Processing {timeseries_file}...")
        
        with open(timeseries_file) as f:
            timeseries = json.load(f)
        
        entries = [(e, datetime.fromisoformat(e["date"].replace("Z", "+00:00"))) for e in timeseries if e["ndvi"] is not None]
        
        for entry, entry_date in entries:
            # Use 14-day window for seasonal context, require NDVI < 0.3 and >0.15 below max
            window_ndvi = [e["ndvi"] for e, d in entries if abs((d - entry_date).days) <= 14]
            
            if len(window_ndvi) < 3:
                continue
            
            max_ndvi = max(window_ndvi)
            threshold = max_ndvi - 0.15
            
            if entry["ndvi"] < threshold and entry["ndvi"] < 0.3:
                clouds[source].append(entry["filename"])
        
        print(f"[CLOUDS-{source.upper()}] Found {len(clouds[source])} cloud-covered files")
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(clouds, f, indent=2)
    
    print(f"[CLOUDS] Saved: {output_file}")


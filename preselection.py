"""Pre-selection: self-contained NDVI timeseries with cloud/dark-imagery exclusion markers."""
import csv
import json
import numpy as np
import rasterio
from rasterio.warp import transform as transform_coords
from pathlib import Path
from datetime import datetime

WINDOW_DAYS = 14
MIN_WINDOW_SIZE = 3
THRESHOLDS = {"aggressive": {"threshold": 0.3, "delta": 0.15}, "nonaggressive": {"threshold": 0.2, "delta": 0.25}}
# S2 uses reflectance * 10000, S3 uses 0-1
BLUE_MIN = {"s2": 100, "s3": 0.01}

GREEN_BAND = 2
RED_BAND = 3
NIR_BAND = 4
BLUE_BAND = 1
BAND_KEYS = ["b02", "b03", "b04", "b8a"]


def _sample_3x3(input_file, site_position):
    """Sample mean NDVI and all four bands (3x3 window) at site. Returns (ndvi, {b02,b03,b04,b8a}) or (None, None)."""
    try:
        with rasterio.open(input_file) as src:
            if src.count < 4:
                return None, None
            bands = [src.read(i).astype(np.float32) for i in range(1, 5)]
            lon, lat = site_position[1], site_position[0]
            x, y = transform_coords("EPSG:4326", src.crs, [lon], [lat])
            if not (
                src.bounds.left <= x[0] <= src.bounds.right
                and src.bounds.bottom <= y[0] <= src.bounds.top
            ):
                return None, None
            row, col = src.index(x[0], y[0])
            if row < 0 or row >= src.height or col < 0 or col >= src.width:
                return None, None
            r0, r1 = max(0, row - 1), min(src.height, row + 2)
            c0, c1 = max(0, col - 1), min(src.width, col + 2)
            windows = [b[r0:r1, c0:c1] for b in bands]
            red_w, nir_w = windows[RED_BAND - 1], windows[NIR_BAND - 1]
            mask = (red_w > 0) & (nir_w > 0) & ~np.isnan(red_w) & ~np.isnan(nir_w)
            if not np.any(mask):
                return None, None
            ndvi = float(np.mean((nir_w[mask] - red_w[mask]) / (nir_w[mask] + red_w[mask])))
            band_means = {k: round(float(np.mean(w[mask])), 6) for k, w in zip(BAND_KEYS, windows)}
            return ndvi, band_means
    except Exception:
        return None, None


def _extract_date(filename):
    for part in filename.replace(".geotiff", "").split("_"):
        if len(part) == 8 and part.isdigit():
            return part, datetime.strptime(part, "%Y%m%d").isoformat()
    return None, None


def _is_excluded(entry, entries, strategy, source="s2"):
    """True if entry is excluded by strategy (NDVI threshold/delta or dark blue)."""
    th = THRESHOLDS[strategy]
    if entry.get("ndvi") is None:
        return True
    blue_min = BLUE_MIN.get(source, BLUE_MIN["s2"])
    if entry.get("b02") is not None and entry["b02"] < blue_min:
        return True
    entry_date = datetime.fromisoformat(entry["date"].replace("Z", "+00:00"))
    window_ndvi = []
    for e in entries:
        if e.get("ndvi") is None:
            continue
        d = datetime.fromisoformat(e["date"].replace("Z", "+00:00"))
        if abs((d - entry_date).days) <= WINDOW_DAYS:
            window_ndvi.append(e["ndvi"])
    if len(window_ndvi) < MIN_WINDOW_SIZE:
        return False
    threshold = max(window_ndvi) - th["delta"]
    return entry["ndvi"] < threshold and entry["ndvi"] < th["threshold"]


def create_timeseries(season, site_position, site_name):
    """Build NDVI timeseries (3x3 window) for raw S2/S3, with exclusion markers for both strategies."""
    lat, lon = site_position
    base = Path(f"data/{site_name}/{season}")

    print(f"[PRESELECT] Creating NDVI timeseries: {site_name} ({lat:.6f}, {lon:.6f}), {season}")

    for source in ["s2", "s3"]:
        input_dir = base / "raw" / source
        out_dir = base / "raw" / "preselection"
        out_dir.mkdir(parents=True, exist_ok=True)
        output_file = out_dir / f"{source}_preselection.json"

        if not input_dir.exists():
            print(f"[PRESELECT] Skipping {source}: {input_dir} not found")
            continue

        timeseries = []
        for f in sorted(input_dir.glob("*.geotiff")):
            if "DIST_CLOUD" in f.name:
                continue
            date_str, date_iso = _extract_date(f.name)
            if not date_str:
                continue
            ndvi, band_means = _sample_3x3(f, site_position)
            entry = {"filename": f.name, "date": date_iso, "ndvi": ndvi}
            if band_means:
                entry.update(band_means)
            timeseries.append(entry)

        timeseries.sort(key=lambda e: e["date"])
        for e in timeseries:
            e["excluded_aggressive"] = _is_excluded(e, timeseries, "aggressive", source)
            e["excluded_nonaggressive"] = _is_excluded(e, timeseries, "nonaggressive", source)

        with open(output_file, "w") as out:
            json.dump(timeseries, out, indent=2)

        csv_file = out_dir / f"{source}_preselection.csv"
        fieldnames = ["filename", "date", "ndvi"] + BAND_KEYS + ["excluded_aggressive", "excluded_nonaggressive"]
        with open(csv_file, "w", newline="") as out:
            w = csv.DictWriter(out, fieldnames=fieldnames, extrasaction="ignore")
            w.writeheader()
            for e in timeseries:
                w.writerow({k: e.get(k) for k in fieldnames})

        n_excl_agg = sum(1 for e in timeseries if e["excluded_aggressive"])
        n_excl_non = sum(1 for e in timeseries if e["excluded_nonaggressive"])
        print(f"[PRESELECT] Saved {output_file} + {csv_file.name}: {len(timeseries)} entries ({n_excl_agg} aggressive, {n_excl_non} nonaggressive excluded)")

    print("[PRESELECT] Completed")


# Backward compatibility
def detect_clouds(season, site_position, site_name, cleaning_strategy="aggressive"):
    """Create timeseries with exclusion markers. Strategy is read from timeseries when preparing."""
    create_timeseries(season, site_position, site_name)


preselect = create_timeseries

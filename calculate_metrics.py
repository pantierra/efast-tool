"""Calculate metrics comparing fusion-derived GCC with phenocam GCC ground truth."""
import json
import numpy as np
from pathlib import Path
from datetime import datetime
from scipy.stats import pearsonr
import rasterio
from rasterio.warp import transform as transform_coords

from generate_indexes import BLUE_BAND, GREEN_BAND, RED_BAND


def load_timeseries(filepath):
    """Load JSON timeseries and return dict mapping date -> value."""
    if not Path(filepath).exists():
        return {}
    with open(filepath) as f:
        data = json.load(f)
    return {item["date"]: item.get("greenness_index") for item in data}


def match_dates(fusion_ts, phenocam_ts):
    """Match dates between timeseries, return aligned numpy arrays (filter None values)."""
    common_dates = set(fusion_ts.keys()) & set(phenocam_ts.keys())
    fusion_vals = []
    phenocam_vals = []
    dates = []
    
    for date in sorted(common_dates):
        fusion_val = fusion_ts[date]
        phenocam_val = phenocam_ts[date]
        if fusion_val is not None and phenocam_val is not None:
            fusion_vals.append(fusion_val)
            phenocam_vals.append(phenocam_val)
            dates.append(date)
    
    return np.array(fusion_vals), np.array(phenocam_vals), dates


def pearson_correlation(y_true, y_pred):
    """Calculate Pearson correlation coefficient r."""
    if len(y_true) < 2 or np.std(y_true) == 0 or np.std(y_pred) == 0:
        return None
    r, _ = pearsonr(y_true, y_pred)
    return float(r)


def r_squared(y_true, y_pred):
    """Calculate coefficient of determination R²."""
    if len(y_true) < 2 or np.std(y_true) == 0:
        return None
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
    if ss_tot == 0:
        return None
    return float(1 - (ss_res / ss_tot))


def rmse(y_true, y_pred):
    """Calculate Root Mean Square Error."""
    if len(y_true) == 0:
        return None
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def mae(y_true, y_pred):
    """Calculate Mean Absolute Error."""
    if len(y_true) == 0:
        return None
    return float(np.mean(np.abs(y_true - y_pred)))


def nrmse(y_true, y_pred):
    """Calculate normalized RMSE (RMSE / mean(y_true))."""
    if len(y_true) == 0:
        return None
    mean_val = np.mean(y_true)
    if mean_val == 0:
        return None
    rmse_val = rmse(y_true, y_pred)
    return float(rmse_val / mean_val) if rmse_val is not None else None


def nse(y_true, y_pred):
    """Calculate Nash-Sutcliffe Efficiency."""
    if len(y_true) < 2:
        return None
    numerator = np.sum((y_true - y_pred) ** 2)
    denominator = np.sum((y_true - np.mean(y_true)) ** 2)
    if denominator == 0:
        return None
    return float(1 - (numerator / denominator))


def calculate_temporal_metrics(fusion_ts, phenocam_ts):
    """Calculate all 6 temporal metrics."""
    fusion_vals, phenocam_vals, dates = match_dates(fusion_ts, phenocam_ts)
    
    if len(fusion_vals) < 2:
        return None
    
    metrics = {
        "pearson_r": pearson_correlation(phenocam_vals, fusion_vals),
        "r_squared": r_squared(phenocam_vals, fusion_vals),
        "rmse": rmse(phenocam_vals, fusion_vals),
        "mae": mae(phenocam_vals, fusion_vals),
        "nrmse": nrmse(phenocam_vals, fusion_vals),
        "nse": nse(phenocam_vals, fusion_vals),
        "n_samples": len(fusion_vals),
        "date_range": {"start": dates[0], "end": dates[-1]} if dates else None,
    }
    return metrics


def calculate_phenocam_stats(phenocam_ts):
    """Calculate phenocam summary statistics."""
    values = [v for v in phenocam_ts.values() if v is not None]
    if len(values) == 0:
        return None
    
    vals = np.array(values)
    return {
        "mean": float(np.mean(vals)),
        "std": float(np.std(vals)),
        "min": float(np.min(vals)),
        "max": float(np.max(vals)),
        "n_samples": len(vals),
    }


def _get_spatial_stats_from_raster(raster_file, site_position):
    """Extract spatial statistics (mean, std, min, max) from GCC raster in 3x3 window."""
    try:
        with rasterio.open(raster_file) as src:
            if src.count < 3:
                return None
            
            blue = src.read(BLUE_BAND).astype(np.float32)
            green = src.read(GREEN_BAND).astype(np.float32)
            red = src.read(RED_BAND).astype(np.float32)
            
            lon, lat = site_position[1], site_position[0]
            x, y = transform_coords("EPSG:4326", src.crs, [lon], [lat])
            
            if not (
                src.bounds.left <= x[0] <= src.bounds.right
                and src.bounds.bottom <= y[0] <= src.bounds.top
            ):
                return None
            
            row, col = src.index(x[0], y[0])
            if row < 0 or row >= src.height or col < 0 or col >= src.width:
                return None
            
            # Extract 3x3 window with boundary handling
            r0, r1 = max(0, row - 1), min(src.height, row + 2)
            c0, c1 = max(0, col - 1), min(src.width, col + 2)
            blue_window = blue[r0:r1, c0:c1]
            green_window = green[r0:r1, c0:c1]
            red_window = red[r0:r1, c0:c1]
            
            # Calculate GCC for each pixel in window
            total = red_window + green_window + blue_window
            mask = (total > 0) & ~np.isnan(total) & (blue_window >= 0) & (green_window >= 0) & (red_window >= 0)
            if not np.any(mask):
                return None
            
            gcc_window = np.zeros_like(green_window, dtype=np.float32)
            gcc_window[mask] = green_window[mask] / total[mask]
            valid_gcc = gcc_window[mask]
            
            if len(valid_gcc) == 0:
                return None
            
            return {
                "mean": float(np.mean(valid_gcc)),
                "std": float(np.std(valid_gcc)),
                "min": float(np.min(valid_gcc)),
                "max": float(np.max(valid_gcc)),
            }
    except Exception:
        return None


def calculate_spatial_metrics(fusion_raster_dir, phenocam_ts, site_position):
    """Calculate r and R² on spatial statistics."""
    fusion_raster_dir = Path(fusion_raster_dir)
    if not fusion_raster_dir.exists():
        return None
    
    spatial_means = []
    phenocam_vals = []
    
    # Process each fusion raster file
    for raster_file in sorted(fusion_raster_dir.glob("*.geotiff")):
        if "DIST_CLOUD" in raster_file.name:
            continue
        
        # Extract date from filename
        parts = raster_file.stem.split("_")
        date_str = None
        for part in parts:
            if len(part) == 8 and part.isdigit():
                date_str = part
                break
        
        if not date_str:
            continue
        
        # Convert to ISO format for matching
        try:
            date = datetime.strptime(date_str, "%Y%m%d").isoformat()
        except ValueError:
            continue
        
        # Get phenocam value for this date
        phenocam_val = phenocam_ts.get(date)
        if phenocam_val is None:
            continue
        
        # Extract spatial statistics
        stats = _get_spatial_stats_from_raster(raster_file, site_position)
        if stats is None:
            continue
        
        spatial_means.append(stats["mean"])
        phenocam_vals.append(phenocam_val)
    
    if len(spatial_means) < 2:
        return None
    
    spatial_means = np.array(spatial_means)
    phenocam_vals = np.array(phenocam_vals)
    
    return {
        "pearson_r": pearson_correlation(phenocam_vals, spatial_means),
        "r_squared": r_squared(phenocam_vals, spatial_means),
        "n_samples": len(spatial_means),
    }


def calculate_scenario_metrics(season, site_name, strategy, sigma, site_position):
    """Calculate metrics for one scenario."""
    base = Path(f"data/{site_name}/{season}")
    processed_dir = f"processed_{strategy}_sigma{sigma}"
    
    # Load timeseries
    fusion_ts_path = base / processed_dir / "gcc" / "fusion" / "timeseries.json"
    phenocam_ts_path = base / "raw" / "phenocam" / "timeseries.json"
    
    fusion_ts = load_timeseries(fusion_ts_path)
    phenocam_ts = load_timeseries(phenocam_ts_path)
    
    if not fusion_ts or not phenocam_ts:
        return None, None
    
    # Calculate temporal metrics
    temporal_metrics = calculate_temporal_metrics(fusion_ts, phenocam_ts)
    
    # Calculate spatial metrics
    fusion_raster_dir = base / processed_dir / "fusion"
    spatial_metrics = calculate_spatial_metrics(fusion_raster_dir, phenocam_ts, site_position)
    
    return temporal_metrics, spatial_metrics


def calculate_all_metrics(season, site_name, site_position):
    """Calculate metrics for all 4 scenarios and save to JSON."""
    results = {"temporal": {}, "spatial": {}}
    base = Path(f"data/{site_name}/{season}")
    
    # Load phenocam timeseries once (same for all scenarios)
    phenocam_ts_path = base / "raw" / "phenocam" / "timeseries.json"
    phenocam_ts = load_timeseries(phenocam_ts_path)
    
    if not phenocam_ts:
        print("[METRICS] Warning: No phenocam data found")
        return results
    
    # Calculate phenocam stats
    phenocam_stats = calculate_phenocam_stats(phenocam_ts)
    if phenocam_stats:
        results["phenocam_stats"] = phenocam_stats
    
    # Calculate S2 baseline metrics once (S2 data is identical across scenarios)
    s2_ts_path = base / "processed_aggressive_sigma20" / "gcc" / "s2" / "timeseries.json"
    s2_ts = load_timeseries(s2_ts_path)
    if s2_ts:
        s2_metrics = calculate_temporal_metrics(s2_ts, phenocam_ts)
        if s2_metrics:
            results["baseline"] = {"s2": s2_metrics}
    
    # Calculate fusion metrics for each scenario
    for strategy in ["aggressive", "nonaggressive"]:
        for sigma in [20, 30]:
            scenario_name = f"{strategy}_sigma{sigma}"
            print(f"[METRICS] Calculating metrics for {scenario_name}...")
            
            processed_dir = f"processed_{strategy}_sigma{sigma}"
            
            # Load fusion timeseries
            fusion_ts_path = base / processed_dir / "gcc" / "fusion" / "timeseries.json"
            fusion_ts = load_timeseries(fusion_ts_path)
            
            if not fusion_ts:
                print(f"[METRICS] Warning: Missing fusion data for {scenario_name}, skipping")
                continue
            
            # Calculate temporal metrics
            temporal_metrics = calculate_temporal_metrics(fusion_ts, phenocam_ts)
            if temporal_metrics:
                results["temporal"][scenario_name] = temporal_metrics
            
            # Calculate spatial metrics
            fusion_raster_dir = base / processed_dir / "fusion"
            spatial_metrics = calculate_spatial_metrics(fusion_raster_dir, phenocam_ts, site_position)
            if spatial_metrics:
                results["spatial"][scenario_name] = spatial_metrics
    
    # Add summary
    if results["temporal"]:
        best_temporal = max(
            results["temporal"].items(),
            key=lambda x: x[1].get("r_squared", -1) if x[1].get("r_squared") is not None else -1
        )
        results["summary"] = {"best_temporal_scenario": best_temporal[0]}
    
    if results["spatial"]:
        best_spatial = max(
            results["spatial"].items(),
            key=lambda x: x[1].get("r_squared", -1) if x[1].get("r_squared") is not None else -1
        )
        if "summary" not in results:
            results["summary"] = {}
        results["summary"]["best_spatial_scenario"] = best_spatial[0]
    
    # Save results
    output_path = Path(f"data/{site_name}/{season}/metrics.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"[METRICS] Saved results to {output_path}")
    
    return results


def main():
    """Standalone script entry point."""
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: calculate_metrics.py <season> <site_name> <lat> <lon>")
        print("Example: calculate_metrics.py 2024 innsbruck 47.116171 11.320308")
        sys.exit(1)
    
    season = int(sys.argv[1])
    site_name = sys.argv[2]
    site_position = (float(sys.argv[3]), float(sys.argv[4]))
    
    results = calculate_all_metrics(season, site_name, site_position)
    
    # Save results
    output_path = Path(f"data/{site_name}/{season}/metrics.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"[METRICS] Saved results to {output_path}")


if __name__ == "__main__":
    main()

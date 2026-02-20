"""Index generation: NDVI and GCC from S2/S3/fusion GeoTIFFs."""
import json
import numpy as np
import rasterio
from rasterio.warp import transform as transform_coords
from pathlib import Path
from datetime import datetime

from preselection import _sample_3x3

RED_BAND = 3
NIR_BAND = 4
BLUE_BAND = 1
GREEN_BAND = 2


def _calculate_and_write_ndvi(input_file, output_file):
    with rasterio.open(input_file) as src:
        red = src.read(RED_BAND).astype(np.float32)
        nir = src.read(NIR_BAND).astype(np.float32)

        mask = (red > 0) & (nir > 0)
        ndvi = np.zeros_like(red, dtype=np.float32)
        ndvi[mask] = (nir[mask] - red[mask]) / (nir[mask] + red[mask])

        profile = src.profile.copy()
        profile.update(
            {
                "count": 1,
                "dtype": "float32",
                "nodata": 0,
                "compress": "lzw",
            }
        )

        with rasterio.open(output_file, "w", **profile) as dst:
            dst.write(ndvi, 1)
            dst.set_band_description(1, "NDVI")


def _get_ndvi_value(ndvi_file, site_position):
    try:
        with rasterio.open(ndvi_file) as src:
            lon, lat = site_position[1], site_position[0]
            x, y = transform_coords("EPSG:4326", src.crs, [lon], [lat])

            # Check if point is within bounds
            if not (
                src.bounds.left <= x[0] <= src.bounds.right
                and src.bounds.bottom <= y[0] <= src.bounds.top
            ):
                return None  # Point is outside raster bounds

            samples = list(src.sample([(x[0], y[0])]))
            if samples:
                value = float(samples[0][0])
                # Check if it's actually nodata (using raster's nodata value)
                if src.nodata is not None and value == src.nodata:
                    return None  # This is nodata, not a valid 0 value
                if np.isnan(value):
                    return None  # NaN is invalid
                # 0 is a valid NDVI value (no vegetation), so return it
                return value
    except Exception as e:
        print(f"Error sampling {ndvi_file.name}: {e}")
        pass
    return None


def _create_timeseries_for_dir(input_dir, output_dir, site_position, source_name, pattern="*.geotiff"):
    print(f"[NDVI-{source_name}] Creating timeseries.json...")
    timeseries = []

    for input_file in sorted(input_dir.glob(pattern)):
        if "DIST_CLOUD" in input_file.name:
            continue

        filename = input_file.name
        parts = filename.replace(".geotiff", "").split("_")
        date_str = None

        for part in parts:
            if len(part) == 8 and part.isdigit():
                date_str = part
                break

        if date_str:
            try:
                date = datetime.strptime(date_str, "%Y%m%d").isoformat()
            except ValueError:
                date = date_str
        else:
            date_str = parts[0]
            date = date_str
            print(
                f"[NDVI-{source_name}] Warning: Could not extract date from {filename}, using '{date_str}'"
            )

        ndvi_value, band_means = _sample_3x3(input_file, site_position)
        blue_mean = band_means.get("b02") if band_means else None
        if ndvi_value is None:
            print(
                f"[NDVI-{source_name}] Warning: Could not sample {filename} (outside bounds or nodata)"
            )

        entry = {"date": date, "filename": filename, "ndvi": ndvi_value}
        if blue_mean is not None:
            entry["blue"] = blue_mean
        timeseries.append(entry)

    timeseries.sort(key=lambda x: x["date"])
    output_dir.mkdir(parents=True, exist_ok=True)
    timeseries_file = output_dir / "timeseries.json"
    with open(timeseries_file, "w") as f:
        json.dump(timeseries, f, indent=2)

    print(f"[NDVI-{source_name}] Saved: {timeseries_file} ({len(timeseries)} entries)")


def _process_ndvi_files(
    input_dir, output_dir, source_name, pattern="*.geotiff", output_namer=None
):
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"[NDVI-{source_name}] Processing {input_dir}...")

    geotiff_files = sorted(input_dir.glob(pattern))
    if not geotiff_files:
        print(f"[NDVI-{source_name}] No files found")
        return

    for geotiff_file in geotiff_files:
        # Skip DIST_CLOUD files silently (single-band distance-to-clouds, not suitable for NDVI)
        if "DIST_CLOUD" in geotiff_file.name:
            continue

        # Check if file has enough bands (need at least 4 for RED and NIR)
        try:
            with rasterio.open(geotiff_file) as src:
                if src.count < 4:
                    print(
                        f"[NDVI-{source_name}] Skipping {geotiff_file.name} (only {src.count} band(s), need 4+)"
                    )
                    continue
        except Exception as e:
            print(
                f"[NDVI-{source_name}] Skipping {geotiff_file.name} (error reading: {e})"
            )
            continue

        output_file = output_dir / (
            output_namer(geotiff_file) if output_namer else geotiff_file.name
        )

        _calculate_and_write_ndvi(geotiff_file, output_file)
        print(f"[NDVI-{source_name}] Saved: {output_file}")


def generate_ndvi_raw(season, site_position, site_name):
    # No longer creating NDVI GeoTIFF files, only timeseries
    pass


def _get_output_name_prepared(geotiff_file):
    if geotiff_file.suffix == ".tif":
        if "REFL" in geotiff_file.stem:
            # For S2: S2A_MSIL2A_20240101_REFL -> date is at index [2]
            # For S3: composite_20240101.tif -> date is at index [1] after removing .tif
            parts = geotiff_file.stem.split("_")
            if len(parts) >= 3 and parts[0].startswith("S2"):
                # S2 format: S2A_MSIL2A_YYYYMMDD_REFL
                date_str = parts[2]
            elif len(parts) >= 2 and parts[0] == "composite":
                # S3 format: composite_YYYYMMDD
                date_str = parts[1]
            else:
                # Fallback: try index [1] for other formats
                date_str = parts[1] if len(parts) > 1 else parts[0]
            return f"{date_str}_ndvi.geotiff"
        return geotiff_file.name.replace(".tif", ".geotiff")
    return geotiff_file.name


def _fusion_namer(f):
    date_str = f.stem.split("_")[1]
    return f"{date_str}_ndvi.geotiff"


def generate_ndvi_post_process(season, site_position, site_name):
    # No longer creating NDVI GeoTIFF files, only timeseries
    pass


def create_ndvi_timeseries_post_process(season, site_position, site_name):
    for strategy in ["aggressive", "nonaggressive"]:
        for sigma in [20, 30]:
            processed_dir = f"processed_{strategy}_sigma{sigma}"
            for source in ["s2", "s3"]:
                input_dir = Path(f"data/{site_name}/{season}/{processed_dir}/{source}/")
                output_dir = Path(f"data/{site_name}/{season}/{processed_dir}/ndvi/{source}/")
                _create_timeseries_for_dir(
                    input_dir, output_dir, site_position, f"POST-PROCESS-{source.upper()}-{strategy}-σ{sigma}"
                )
            input_dir = Path(f"data/{site_name}/{season}/{processed_dir}/fusion/")
            output_dir = Path(f"data/{site_name}/{season}/{processed_dir}/ndvi/fusion/")
            _create_timeseries_for_dir(input_dir, output_dir, site_position, f"POST-PROCESS-FUSION-{strategy}-σ{sigma}")


def _calculate_and_write_gcc(input_file, output_file):
    with rasterio.open(input_file) as src:
        blue = src.read(BLUE_BAND).astype(np.float32)
        green = src.read(GREEN_BAND).astype(np.float32)
        red = src.read(RED_BAND).astype(np.float32)

        total = red + green + blue
        mask = total > 0
        gcc = np.zeros_like(green, dtype=np.float32)
        gcc[mask] = green[mask] / total[mask]

        profile = src.profile.copy()
        profile.update(
            {
                "count": 1,
                "dtype": "float32",
                "nodata": 0,
                "compress": "lzw",
            }
        )

        with rasterio.open(output_file, "w", **profile) as dst:
            dst.write(gcc, 1)
            dst.set_band_description(1, "GCC")


def _get_gcc_value(gcc_file, site_position):
    try:
        with rasterio.open(gcc_file) as src:
            lon, lat = site_position[1], site_position[0]
            x, y = transform_coords("EPSG:4326", src.crs, [lon], [lat])

            if not (
                src.bounds.left <= x[0] <= src.bounds.right
                and src.bounds.bottom <= y[0] <= src.bounds.top
            ):
                return None

            samples = list(src.sample([(x[0], y[0])]))
            if samples:
                value = float(samples[0][0])
                if src.nodata is not None and value == src.nodata:
                    return None
                if np.isnan(value):
                    return None
                return value
    except Exception as e:
        print(f"Error sampling {gcc_file.name}: {e}")
        pass
    return None


def _get_gcc_from_original(input_file, site_position):
    """Calculate GCC directly from original file without creating GeoTIFF."""
    try:
        with rasterio.open(input_file) as src:
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
                negative_pixels = np.sum((blue_window < 0) | (green_window < 0) | (red_window < 0))
                if negative_pixels > 0:
                    print(f"Warning: {input_file.name} excluded - all pixels have negative band values ({negative_pixels} negative pixels in window)")
                return None

            gcc_window = np.zeros_like(green_window, dtype=np.float32)
            gcc_window[mask] = green_window[mask] / total[mask]

            # Return mean of valid GCC values
            valid_gcc = gcc_window[mask]
            return float(np.mean(valid_gcc)) if len(valid_gcc) > 0 else None
    except Exception as e:
        return None


def _create_gcc_timeseries_for_dir(input_dir, output_dir, site_position, source_name, pattern="*.geotiff"):
    print(f"[GCC-{source_name}] Creating timeseries.json...")
    timeseries = []

    for input_file in sorted(input_dir.glob(pattern)):
        if "DIST_CLOUD" in input_file.name:
            continue

        filename = input_file.name
        parts = filename.replace(".geotiff", "").split("_")
        date_str = None

        for part in parts:
            if len(part) == 8 and part.isdigit():
                date_str = part
                break

        if date_str:
            try:
                date = datetime.strptime(date_str, "%Y%m%d").isoformat()
            except ValueError:
                date = date_str
        else:
            date_str = parts[0]
            date = date_str
            print(
                f"[GCC-{source_name}] Warning: Could not extract date from {filename}, using '{date_str}'"
            )

        gcc_value = _get_gcc_from_original(input_file, site_position)
        if gcc_value is None:
            print(
                f"[GCC-{source_name}] Warning: Could not sample {filename} (outside bounds or nodata)"
            )

        timeseries.append({"date": date, "filename": filename, "greenness_index": gcc_value})

    timeseries.sort(key=lambda x: x["date"])
    output_dir.mkdir(parents=True, exist_ok=True)
    timeseries_file = output_dir / "timeseries.json"
    with open(timeseries_file, "w") as f:
        json.dump(timeseries, f, indent=2)

    print(f"[GCC-{source_name}] Saved: {timeseries_file} ({len(timeseries)} entries)")


def _process_gcc_files(
    input_dir, output_dir, source_name, pattern="*.geotiff", output_namer=None
):
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"[GCC-{source_name}] Processing {input_dir}...")

    geotiff_files = sorted(input_dir.glob(pattern))
    if not geotiff_files:
        print(f"[GCC-{source_name}] No files found")
        return

    for geotiff_file in geotiff_files:
        if "DIST_CLOUD" in geotiff_file.name:
            continue

        try:
            with rasterio.open(geotiff_file) as src:
                if src.count < 3:
                    print(
                        f"[GCC-{source_name}] Skipping {geotiff_file.name} (only {src.count} band(s), need 3+)"
                    )
                    continue
        except Exception as e:
            print(
                f"[GCC-{source_name}] Skipping {geotiff_file.name} (error reading: {e})"
            )
            continue

        output_file = output_dir / (
            output_namer(geotiff_file) if output_namer else geotiff_file.name
        )

        _calculate_and_write_gcc(geotiff_file, output_file)
        print(f"[GCC-{source_name}] Saved: {output_file}")


def generate_gcc_post_process(season, site_position, site_name):
    # No longer creating GCC GeoTIFF files, only timeseries
    pass


def create_gcc_timeseries_post_process(season, site_position, site_name):
    for strategy in ["aggressive", "nonaggressive"]:
        for sigma in [20, 30]:
            processed_dir = f"processed_{strategy}_sigma{sigma}"
            for source in ["s2", "s3"]:
                input_dir = Path(f"data/{site_name}/{season}/{processed_dir}/{source}/")
                output_dir = Path(f"data/{site_name}/{season}/{processed_dir}/gcc/{source}/")
                _create_gcc_timeseries_for_dir(
                    input_dir, output_dir, site_position, f"POST-PROCESS-{source.upper()}-{strategy}-σ{sigma}"
                )
            input_dir = Path(f"data/{site_name}/{season}/{processed_dir}/fusion/")
            output_dir = Path(f"data/{site_name}/{season}/{processed_dir}/gcc/fusion/")
            _create_gcc_timeseries_for_dir(input_dir, output_dir, site_position, f"POST-PROCESS-FUSION-{strategy}-σ{sigma}")


def _get_bands_from_original(input_file, site_position):
    """Extract mean B02, B03, B04, B8A from 3x3 window at site. Returns dict or None."""
    try:
        with rasterio.open(input_file) as src:
            if src.count < 4:
                return None
            lon, lat = site_position[1], site_position[0]
            x, y = transform_coords("EPSG:4326", src.crs, [lon], [lat])
            if not (
                src.bounds.left <= x[0] <= src.bounds.right
                and src.bounds.bottom <= y[0] <= src.bounds.top
            ):
                return None
            row, col = src.index(x[0], y[0])
            r0, r1 = max(0, row - 1), min(src.height, row + 2)
            c0, c1 = max(0, col - 1), min(src.width, col + 2)
            bands = [src.read(i + 1, window=((r0, r1), (c0, c1))).astype(np.float32) for i in range(4)]
            mask = ~np.any([np.isnan(b) for b in bands], axis=0)
            mask &= np.all([b > 0 for b in bands], axis=0)
            if not np.any(mask):
                return None
            return {
                "b02": float(np.mean(bands[0][mask])),
                "b03": float(np.mean(bands[1][mask])),
                "b04": float(np.mean(bands[2][mask])),
                "b8a": float(np.mean(bands[3][mask])),
            }
    except Exception:
        return None


def _create_s2_bands_timeseries_for_dir(input_dir, output_dir, site_position):
    print(f"[S2-BANDS] Creating timeseries.json...")
    timeseries = []
    for f in sorted(input_dir.glob("*.geotiff")):
        date_str = f.stem.split("_")[0]
        if len(date_str) != 8 or not date_str.isdigit():
            continue
        date = datetime.strptime(date_str, "%Y%m%d").isoformat()
        bands = _get_bands_from_original(f, site_position)
        timeseries.append({"date": date, "filename": f.name, **(bands or {})})
    timeseries.sort(key=lambda x: x["date"])
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "timeseries.json").write_text(json.dumps(timeseries, indent=2))
    print(f"[S2-BANDS] Saved: {output_dir / 'timeseries.json'} ({len(timeseries)} entries)")


def create_s2_bands_timeseries_post_process(season, site_position, site_name):
    for strategy in ["aggressive", "nonaggressive"]:
        for sigma in [20, 30]:
            processed_dir = f"processed_{strategy}_sigma{sigma}"
            input_dir = Path(f"data/{site_name}/{season}/{processed_dir}/s2/")
            output_dir = Path(f"data/{site_name}/{season}/{processed_dir}/s2_bands/")
            if input_dir.exists():
                _create_s2_bands_timeseries_for_dir(input_dir, output_dir, site_position)

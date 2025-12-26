import json
import numpy as np
import rasterio
from rasterio.warp import transform as transform_coords
from pathlib import Path
from datetime import datetime


def _calculate_and_write_ndvi(input_file, output_file):
    """Calculate NDVI from red and NIR bands and write to output file."""
    with rasterio.open(input_file) as src:
        red = src.read(3).astype(np.float32)
        nir = src.read(4).astype(np.float32)

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


def _create_timeseries_for_dir(output_dir, site_position, source_name):
    """Create timeseries.json for NDVI files in the given directory."""
    print(f"[NDVI-{source_name}] Creating timeseries.json...")
    timeseries = []

    ndvi_files = sorted(output_dir.glob("*.geotiff"))
    for ndvi_file in ndvi_files:
        filename = ndvi_file.name
        date_str = filename.split("_")[0]
        try:
            date = datetime.strptime(date_str, "%Y%m%d").isoformat()
        except ValueError:
            date = date_str

        ndvi_value = None
        try:
            with rasterio.open(ndvi_file) as src:
                lon, lat = site_position[1], site_position[0]
                x, y = transform_coords("EPSG:4326", src.crs, [lon], [lat])
                samples = list(src.sample([(x[0], y[0])]))
                if samples:
                    value = float(samples[0][0])
                    if value != 0 and not np.isnan(value):
                        ndvi_value = value
        except Exception as e:
            print(f"[NDVI-{source_name}] Warning: Could not sample {filename}: {e}")

        timeseries.append({"date": date, "filename": filename, "ndvi": ndvi_value})

    timeseries.sort(key=lambda x: x["date"])
    timeseries_file = output_dir / "timeseries.json"
    with open(timeseries_file, "w") as f:
        json.dump(timeseries, f, indent=2)

    print(f"[NDVI-{source_name}] Saved: {timeseries_file} ({len(timeseries)} entries)")


def generate_ndvi_raw(season, site_position, site_name):
    for source in ["s2", "s3"]:
        input_dir = Path(f"data/{site_name}/{season}/raw/{source}/")
        output_dir = Path(f"data/{site_name}/{season}/raw/ndvi/{source}/")
        output_dir.mkdir(parents=True, exist_ok=True)

        print(f"[NDVI-{source.upper()}] Processing {input_dir}...")

        geotiff_files = sorted(input_dir.glob("*.geotiff"))
        if not geotiff_files:
            print(f"[NDVI-{source.upper()}] No files found")
            continue

        for geotiff_file in geotiff_files:
            output_file = output_dir / geotiff_file.name

            if output_file.exists():
                print(f"[NDVI-{source.upper()}] Skipping {geotiff_file.name} (exists)")
                continue

            _calculate_and_write_ndvi(geotiff_file, output_file)
            print(f"[NDVI-{source.upper()}] Saved: {output_file}")

        print(f"[NDVI-{source.upper()}] Completed")


def create_ndvi_timeseries_raw(season, site_position, site_name):
    for source in ["s2", "s3"]:
        output_dir = Path(f"data/{site_name}/{season}/raw/ndvi/{source}/")
        _create_timeseries_for_dir(output_dir, site_position, source.upper())


def generate_ndvi_prepared(season, site_position, site_name):
    for source in ["s2", "s3"]:
        input_dir = Path(f"data/{site_name}/{season}/prepared/{source}/")
        output_dir = Path(f"data/{site_name}/{season}/prepared/ndvi/{source}/")
        output_dir.mkdir(parents=True, exist_ok=True)

        print(f"[NDVI-PREPARED-{source.upper()}] Processing {input_dir}...")

        geotiff_files = sorted(input_dir.glob("*.geotiff")) + sorted(input_dir.glob("*.tif"))
        if not geotiff_files:
            print(f"[NDVI-PREPARED-{source.upper()}] No files found")
            continue

        for geotiff_file in geotiff_files:
            if geotiff_file.suffix == ".tif":
                if "REFL" in geotiff_file.stem:
                    date_str = geotiff_file.stem.split("_")[1]
                    output_file = output_dir / f"{date_str}_ndvi.geotiff"
                else:
                    output_file = output_dir / geotiff_file.name.replace(".tif", ".geotiff")
            else:
                output_file = output_dir / geotiff_file.name

            if output_file.exists():
                print(f"[NDVI-PREPARED-{source.upper()}] Skipping {geotiff_file.name} (exists)")
                continue

            _calculate_and_write_ndvi(geotiff_file, output_file)
            print(f"[NDVI-PREPARED-{source.upper()}] Saved: {output_file}")

        print(f"[NDVI-PREPARED-{source.upper()}] Completed")

    input_dir = Path(f"data/{site_name}/{season}/prepared/fusion/")
    output_dir = Path(f"data/{site_name}/{season}/prepared/ndvi/fusion/")
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[NDVI-FUSION] Processing {input_dir}...")

    geotiff_files = sorted(input_dir.glob("REFL_*.tif"))
    if not geotiff_files:
        print(f"[NDVI-FUSION] No files found")
        return

    for geotiff_file in geotiff_files:
        date_str = geotiff_file.stem.split("_")[1]
        output_file = output_dir / f"{date_str}_ndvi.geotiff"

        if output_file.exists():
            print(f"[NDVI-FUSION] Skipping {geotiff_file.name} (exists)")
            continue

        _calculate_and_write_ndvi(geotiff_file, output_file)
        print(f"[NDVI-FUSION] Saved: {output_file}")

    print(f"[NDVI-FUSION] Completed")


def create_ndvi_timeseries_prepared(season, site_position, site_name):
    for source in ["s2", "s3"]:
        output_dir = Path(f"data/{site_name}/{season}/prepared/ndvi/{source}/")
        _create_timeseries_for_dir(output_dir, site_position, f"PREPARED-{source.upper()}")

    output_dir = Path(f"data/{site_name}/{season}/prepared/ndvi/fusion/")
    _create_timeseries_for_dir(output_dir, site_position, "FUSION")

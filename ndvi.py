import json
import numpy as np
import rasterio
from rasterio.warp import transform as transform_coords
from pathlib import Path
from datetime import datetime

RED_BAND = 3
NIR_BAND = 4


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
            samples = list(src.sample([(x[0], y[0])]))
            if samples:
                value = float(samples[0][0])
                if value != 0 and not np.isnan(value):
                    return value
                # Return the raw value even if 0 or NaN for diagnostic purposes
                return value
    except Exception:
        pass
    return None


def _create_timeseries_for_dir(output_dir, site_position, source_name):
    print(f"[NDVI-{source_name}] Creating timeseries.json...")
    timeseries = []

    for ndvi_file in sorted(output_dir.glob("*.geotiff")):
        filename = ndvi_file.name
        date_str = filename.split("_")[0]
        try:
            date = datetime.strptime(date_str, "%Y%m%d").isoformat()
        except ValueError:
            date = date_str

        ndvi_value = _get_ndvi_value(ndvi_file, site_position)
        if ndvi_value is None:
            print(f"[NDVI-{source_name}] Warning: Could not sample {filename}")
        elif ndvi_value == 0:
            print(f"[NDVI-{source_name}] Warning: Could not sample {filename} (NoData)")
            ndvi_value = None  # Set to None for timeseries
        elif np.isnan(ndvi_value):
            print(f"[NDVI-{source_name}] Warning: Could not sample {filename} (NaN)")
            ndvi_value = None  # Set to None for timeseries

        timeseries.append({"date": date, "filename": filename, "ndvi": ndvi_value})

    timeseries.sort(key=lambda x: x["date"])
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
        output_file = output_dir / (
            output_namer(geotiff_file) if output_namer else geotiff_file.name
        )

        if output_file.exists():
            print(f"[NDVI-{source_name}] Skipping {geotiff_file.name} (exists)")
            continue

        _calculate_and_write_ndvi(geotiff_file, output_file)
        print(f"[NDVI-{source_name}] Saved: {output_file}")


def generate_ndvi_raw(season, site_position, site_name):
    for source in ["s2", "s3"]:
        input_dir = Path(f"data/{site_name}/{season}/raw/{source}/")
        output_dir = Path(f"data/{site_name}/{season}/raw/ndvi/{source}/")
        _process_ndvi_files(input_dir, output_dir, source.upper())


def create_ndvi_timeseries_raw(season, site_position, site_name):
    for source in ["s2", "s3"]:
        output_dir = Path(f"data/{site_name}/{season}/raw/ndvi/{source}/")
        _create_timeseries_for_dir(output_dir, site_position, source.upper())


def _get_output_name_prepared(geotiff_file):
    if geotiff_file.suffix == ".tif":
        if "REFL" in geotiff_file.stem:
            date_str = geotiff_file.stem.split("_")[1]
            return f"{date_str}_ndvi.geotiff"
        return geotiff_file.name.replace(".tif", ".geotiff")
    return geotiff_file.name


def _fusion_namer(f):
    date_str = f.stem.split("_")[1]
    return f"{date_str}_ndvi.geotiff"


def generate_ndvi_prepared(season, site_position, site_name):
    for source in ["s2", "s3"]:
        input_dir = Path(f"data/{site_name}/{season}/prepared/{source}/")
        output_dir = Path(f"data/{site_name}/{season}/prepared/ndvi/{source}/")
        for pattern in ["*.geotiff", "*.tif"]:
            _process_ndvi_files(
                input_dir,
                output_dir,
                f"PREPARED-{source.upper()}",
                pattern=pattern,
                output_namer=_get_output_name_prepared,
            )

    input_dir = Path(f"data/{site_name}/{season}/prepared/fusion/")
    output_dir = Path(f"data/{site_name}/{season}/prepared/ndvi/fusion/")
    _process_ndvi_files(
        input_dir,
        output_dir,
        "FUSION",
        pattern="REFL_*.tif",
        output_namer=_fusion_namer,
    )


def create_ndvi_timeseries_prepared(season, site_position, site_name):
    for source in ["s2", "s3"]:
        output_dir = Path(f"data/{site_name}/{season}/prepared/ndvi/{source}/")
        _create_timeseries_for_dir(
            output_dir, site_position, f"PREPARED-{source.upper()}"
        )

    output_dir = Path(f"data/{site_name}/{season}/prepared/ndvi/fusion/")
    _create_timeseries_for_dir(output_dir, site_position, "FUSION")

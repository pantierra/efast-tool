import json
import numpy as np
import rasterio
from rasterio.warp import transform as transform_coords
from pathlib import Path
from datetime import datetime


def generate_ndvi(year, site_position, site_name):
    for source in ["s2", "s3"]:
        input_dir = Path(f"data/{site_name}/{year}/{source}/")
        output_dir = Path(f"data/{site_name}/{year}/ndvi/{source}/")
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

            with rasterio.open(geotiff_file) as src:
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

            print(f"[NDVI-{source.upper()}] Saved: {output_file}")

        print(f"[NDVI-{source.upper()}] Completed")


def create_ndvi_timeseries(year, site_position, site_name):
    for source in ["s2", "s3"]:
        output_dir = Path(f"data/{site_name}/{year}/ndvi/{source}/")

        print(f"[NDVI-{source.upper()}] Creating timeseries.json...")
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
                    if samples and len(samples) > 0:
                        value = float(samples[0][0])
                        if value != 0 and not np.isnan(value):
                            ndvi_value = value
            except Exception as e:
                print(
                    f"[NDVI-{source.upper()}] Warning: Could not sample {filename}: {e}"
                )

            timeseries.append({"date": date, "filename": filename, "ndvi": ndvi_value})

        timeseries.sort(key=lambda x: x["date"])
        timeseries_file = output_dir / "timeseries.json"
        with open(timeseries_file, "w") as f:
            json.dump(timeseries, f, indent=2)

        print(
            f"[NDVI-{source.upper()}] Saved: {timeseries_file} ({len(timeseries)} entries)"
        )

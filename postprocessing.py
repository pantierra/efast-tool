"""Post-processing: crop fusion/S2/S3 to valid pixels."""
from pathlib import Path
import numpy as np
import rasterio
from rasterio import windows
from rasterio.warp import reproject, Resampling
from rasterio.io import MemoryFile


def process_cropped(season, site_position, site_name, cleaning_strategy="aggressive", sigma=None):
    """Crop fusion to valid data, then crop S2/S3 to match."""
    base = Path(f"data/{site_name}/{season}")
    prepared = base / f"prepared_{cleaning_strategy}"
    processed_dir = f"processed_{cleaning_strategy}_sigma{sigma}" if sigma else f"processed_{cleaning_strategy}_sigma20"
    processed = base / processed_dir

    s2_prep = prepared / "s2"
    s3_prep = prepared / "s3"
    fusion_prep = prepared / (f"fusion_sigma{sigma}" if sigma else "fusion")

    for output_dir in [processed / "s2", processed / "s3", processed / "fusion"]:
        output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[PROCESS] Processing files: {site_name}, {season}, {cleaning_strategy}, sigma={sigma or 20}")

    # Crop fusion to valid data and get dimensions
    fusion_dims = {}
    for fusion_file in fusion_prep.glob("REFL_*.tif"):
        date_str = fusion_file.stem.split("_")[1]
        with rasterio.open(fusion_file) as src:
            data = src.read()
            valid = ~np.isnan(data) & (data > 0.001)
            rows = np.any(valid, axis=(0, 2))
            cols = np.any(valid, axis=(0, 1))
            row_idx = np.where(rows)[0]
            col_idx = np.where(cols)[0]
            if len(row_idx) == 0 or len(col_idx) == 0:
                print(f"[PROCESS] Skipping {fusion_file.name} (no valid pixels)")
                continue
            r0, r1 = row_idx[0], row_idx[-1]
            c0, c1 = col_idx[0], col_idx[-1]
            w, h = c1 - c0 + 1, r1 - r0 + 1
            window = windows.Window(c0, r0, w, h)
            data_crop = src.read(window=window)
            transform = rasterio.windows.transform(window, src.transform)
            p = src.profile.copy()
            p.update({"width": w, "height": h, "transform": transform})
            output_file = processed / "fusion" / f"{date_str}_0.geotiff"
            with rasterio.open(output_file, "w", **p) as dst:
                dst.write(data_crop)
            fusion_dims[date_str] = (c0, r0, w, h, transform, src.transform, src.crs, src.profile)
        print(f"[PROCESS] Cropped fusion: {output_file}")

    # Crop S2 and S3 to fusion size
    for date_str, (c0, r0, w, h, transform, fusion_transform, crs, fusion_profile) in fusion_dims.items():
        window = windows.Window(c0, r0, w, h)
        # S2
        for s2_file in s2_prep.glob("*REFL.tif"):
            if s2_file.stem.split("_")[2] == date_str:
                output_file = processed / "s2" / f"{date_str}_0.geotiff"
                with rasterio.open(s2_file) as src:
                    data = src.read(window=window)
                    p2 = src.profile.copy()
                    p2.update({"width": w, "height": h, "transform": transform, "crs": crs})
                    with rasterio.open(output_file, "w", **p2) as dst:
                        dst.write(data)
                print(f"[PROCESS] Cropped: {output_file}")
        # S3: resample to fusion pixel size, then crop
        s3_file = s3_prep / f"composite_{date_str}.tif"
        if s3_file.exists():
            output_file = processed / "s3" / f"{date_str}_0.geotiff"
            with rasterio.open(s3_file) as src:
                # Resample to fusion pixel size
                temp_profile = fusion_profile.copy()
                temp_profile.update({"dtype": src.profile["dtype"], "count": src.count})
                with rasterio.MemoryFile() as memfile:
                    with memfile.open(**temp_profile) as resampled:
                        for i in range(1, src.count + 1):
                            reproject(
                                source=rasterio.band(src, i),
                                destination=rasterio.band(resampled, i),
                                src_transform=src.transform,
                                src_crs=src.crs,
                                dst_transform=fusion_transform,
                                dst_crs=crs,
                                resampling=Resampling.nearest
                            )
                        # Crop using same window
                        data = resampled.read(window=window)
                        p2 = resampled.profile.copy()
                        p2.update({"width": w, "height": h, "transform": transform})
                        with rasterio.open(output_file, "w", **p2) as dst:
                            dst.write(data)
            print(f"[PROCESS] Cropped: {output_file}")

    print("[PROCESS] Completed")


def process_all_scenarios(season, site_position, site_name):
    """Process all 4 EFAST scenarios."""
    for strategy in ["aggressive", "nonaggressive"]:
        for sigma in [None, 30]:
            process_cropped(season, site_position, site_name, cleaning_strategy=strategy, sigma=sigma)


# Aliases
postprocess = process_cropped
postprocess_all_scenarios = process_all_scenarios

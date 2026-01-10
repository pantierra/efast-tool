import json
import shutil
import importlib.util
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import rasterio
from rasterio.warp import Resampling
from rasterio.vrt import WarpedVRT
from rasterio import shutil as rio_shutil
from scipy import ndimage

RESOLUTION_RATIO = 21

try:
    import efast as efast_fusion
except ImportError:
    import site

    efast_fusion = None
    for site_pkg in site.getsitepackages():
        candidate = Path(site_pkg) / "efast" / "efast.py"
        if candidate.exists():
            spec = importlib.util.spec_from_file_location(
                "efast_fusion_module", candidate
            )
            efast_fusion = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(efast_fusion)
            break
    if efast_fusion is None:
        raise ImportError(
            "efast package not found. Install with: pip install git+https://github.com/DHI-GRAS/efast.git"
        )


def _load_clouds(clouds_file):
    clouds = {"s2": set(), "s3": set()}
    if clouds_file.exists():
        clouds_data = json.loads(clouds_file.read_text())
        clouds["s2"] = set(clouds_data.get("s2", []))
        clouds["s3"] = set(clouds_data.get("s3", []))
    return clouds


def _reproject_to_target(
    data, src_transform, src_crs, target_bounds, target_crs, width, height, resampling
):
    dst_transform = rasterio.transform.from_bounds(
        target_bounds.left,
        target_bounds.bottom,
        target_bounds.right,
        target_bounds.top,
        width,
        height,
    )
    reprojected, _ = rasterio.warp.reproject(
        source=data,
        destination=np.zeros((data.shape[0], height, width), dtype=data.dtype),
        src_transform=src_transform,
        src_crs=src_crs,
        dst_transform=dst_transform,
        dst_crs=target_crs,
        resampling=resampling,
    )
    return reprojected, dst_transform


def prepare_s2(season, site_position, site_name, date_range=None):
    s2_dir = Path(f"data/{site_name}/{season}/raw/s2/")
    s3_dir = Path(f"data/{site_name}/{season}/raw/s3/")
    s2_output_dir = Path(f"data/{site_name}/{season}/prepared/s2/")
    clouds_file = Path(f"data/{site_name}/{season}/clouds.json")

    clouds = _load_clouds(clouds_file)

    s2_output_dir.mkdir(parents=True, exist_ok=True)

    s3_files = [f for f in s3_dir.glob("*.geotiff") if f.name not in clouds["s3"]]
    if not s3_files:
        raise ValueError("No non-cloud S3 files found for reference bounds")

    with rasterio.open(s3_files[0]) as s3_ref:
        target_bounds = s3_ref.bounds
        target_crs = s3_ref.crs
        s3_width = s3_ref.width
        s3_height = s3_ref.height
        s2_width = s3_width * RESOLUTION_RATIO
        s2_height = s3_height * RESOLUTION_RATIO

    for s2_file in s2_dir.glob("*.geotiff"):
        if s2_file.name in clouds["s2"]:
            continue
        date_str = s2_file.name.split("_")[0]

        refl_dst = s2_output_dir / f"S2A_MSIL2A_{date_str}_REFL.tif"
        if not refl_dst.exists():
            with rasterio.open(s2_file) as src:
                data = src.read().astype("float32") / 10000.0
                reprojected_data, dst_transform = _reproject_to_target(
                    data,
                    src.transform,
                    src.crs,
                    target_bounds,
                    target_crs,
                    s2_width,
                    s2_height,
                    Resampling.cubic,
                )
                profile = src.profile.copy()
                profile.update(
                    {
                        "dtype": "float32",
                        "nodata": 0,
                        "width": s2_width,
                        "height": s2_height,
                        "transform": dst_transform,
                        "crs": target_crs,
                    }
                )
                with rasterio.open(refl_dst, "w", **profile) as dst_file:
                    dst_file.write(reprojected_data)

        dist_cloud_dst = s2_output_dir / f"S2A_MSIL2A_{date_str}_DIST_CLOUD.tif"
        if not dist_cloud_dst.exists():
            with rasterio.open(refl_dst) as src:
                s2_hr = src.read(1)
                mask = s2_hr == 0
                distance_to_cloud_hr = np.clip(
                    ndimage.distance_transform_edt(~mask), 0, 255
                ).astype("float32")

                distance_to_cloud_lr, lr_transform = _reproject_to_target(
                    distance_to_cloud_hr[np.newaxis, :, :],
                    src.transform,
                    src.crs,
                    target_bounds,
                    target_crs,
                    s3_width,
                    s3_height,
                    Resampling.average,
                )
                distance_to_cloud_lr = distance_to_cloud_lr[0]

                profile = src.profile.copy()
                profile.update(
                    {
                        "count": 1,
                        "dtype": "float32",
                        "width": s3_width,
                        "height": s3_height,
                        "transform": lr_transform,
                    }
                )
                with rasterio.open(dist_cloud_dst, "w", **profile) as dst:
                    dst.write(distance_to_cloud_lr, 1)


def prepare_s3(season, site_position, site_name, date_range=None):
    s3_dir = Path(f"data/{site_name}/{season}/raw/s3/")
    s2_prepared_dir = Path(f"data/{site_name}/{season}/prepared/s2/")
    s3_preprocessed_dir = Path(f"data/{site_name}/{season}/prepared/s3/")
    clouds_file = Path(f"data/{site_name}/{season}/clouds.json")

    clouds = _load_clouds(clouds_file)
    s3_preprocessed_dir.mkdir(parents=True, exist_ok=True)

    # Get reference profile from S2 DIST_CLOUD file
    dist_cloud_files = list(s2_prepared_dir.glob("*DIST_CLOUD.tif"))
    if not dist_cloud_files:
        raise ValueError("No S2 DIST_CLOUD files found. Run prepare_s2 first.")
    
    with rasterio.open(dist_cloud_files[0]) as src:
        target_profile = src.profile

    # Group S3 files by date
    s3_by_date = {}
    for s3_file in s3_dir.glob("*.geotiff"):
        if s3_file.name in clouds["s3"]:
            continue
        date_str = s3_file.name.split("_")[0]
        if date_str not in s3_by_date:
            s3_by_date[date_str] = []
        s3_by_date[date_str].append(s3_file)

    # Process each date
    for date_str, s3_files in s3_by_date.items():
        output_path = s3_preprocessed_dir / f"composite_{date_str}.tif"
        if output_path.exists():
            continue

        if len(s3_files) == 1:
            # Single file: reproject directly
            with rasterio.open(s3_files[0]) as src:
                vrt_options = {
                    "transform": target_profile["transform"],
                    "height": target_profile["height"],
                    "width": target_profile["width"],
                    "crs": target_profile["crs"],
                    "resampling": Resampling.cubic,
                }
                with WarpedVRT(src, **vrt_options) as vrt:
                    rio_shutil.copy(vrt, output_path, driver="GTiff")
        else:
            # Multiple files: create weighted composite
            s3_stack = []
            for s3_file in s3_files:
                with rasterio.open(s3_file) as src:
                    vrt_options = {
                        "transform": target_profile["transform"],
                        "height": target_profile["height"],
                        "width": target_profile["width"],
                        "crs": target_profile["crs"],
                        "resampling": Resampling.cubic,
                    }
                    with WarpedVRT(src, **vrt_options) as vrt:
                        data = vrt.read()
                        # Remove abnormally high values (pixel-wise mean across bands)
                        pixel_means = np.abs(np.nanmean(data, axis=0))
                        mask = pixel_means >= 5
                        data[:, mask] = np.nan
                        s3_stack.append(data)
            
            s3_stack = np.array(s3_stack)
            # Simple mean composite (can be enhanced with temporal weighting)
            composite = np.nanmean(s3_stack, axis=0)
            composite = composite.astype("float32")
            
            profile = target_profile.copy()
            profile.update({"count": composite.shape[0], "dtype": "float32"})
            with rasterio.open(output_path, "w", **profile) as dst:
                dst.write(composite)


def run_efast(season, site_position, site_name, date_range=None):
    lat, lon = site_position
    datetime_range = date_range or f"{season}-01-01/{season}-12-31"

    efast_base_dir = Path(f"data/{site_name}/{season}/prepared/")
    s2_output_dir = efast_base_dir / "s2"
    s3_output_dir = efast_base_dir / "s3"
    fusion_output_dir = efast_base_dir / "fusion"

    fusion_output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[EFAST] Starting fusion: {site_name} ({lat:.6f}, {lon:.6f}), {season}")

    start_str, end_str = datetime_range.split("/")
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")

        output_file = fusion_output_dir / f"REFL_{date_str}.tif"
        if output_file.exists():
            print(f"[EFAST] Skipping {date_str} (exists)")
            current_date += timedelta(days=1)
            continue

        try:
            efast_fusion.fusion(
                current_date,
                s3_output_dir,
                s2_output_dir,
                fusion_output_dir,
                product="REFL",
                max_days=30,
                date_position=2,
                minimum_acquisition_importance=0.0,
                ratio=RESOLUTION_RATIO,
            )
            if output_file.exists():
                print(f"[EFAST] Saved: {output_file}")
            else:
                print(f"[EFAST] No output for {date_str} (insufficient nearby data)")
        except Exception as e:
            print(f"[EFAST] Error processing {date_str}: {e}")

        current_date += timedelta(days=1)

    print("[EFAST] Completed")

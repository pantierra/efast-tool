import json
import shutil
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np
import rasterio
from rasterio.warp import Resampling
from rasterio.vrt import WarpedVRT
from rasterio import shutil as rio_shutil


def _import_efast():
    """Lazy import of efast to avoid import errors when not using efast functions."""
    try:
        import efast
        from efast.s2_processing import distance_to_clouds
        from efast.s3_processing import reproject_and_crop_s3

        return efast, distance_to_clouds, reproject_and_crop_s3
    except ImportError:
        raise ImportError(
            "efast package not found. Install with: pip install git+https://github.com/DHI-GRAS/efast.git"
        )


RESOLUTION_RATIO = 21


def _load_clouds(clouds_file):
    clouds = {"s2": set(), "s3": set()}
    if clouds_file.exists():
        clouds_data = json.loads(clouds_file.read_text())
        clouds["s2"] = set(clouds_data.get("s2", []))
        clouds["s3"] = set(clouds_data.get("s3", []))
    return clouds


def _reproject_raster_to_target(
    src_path,
    dst_path,
    target_bounds,
    target_crs,
    width,
    height,
    resampling=Resampling.cubic,
):
    dst_transform = rasterio.transform.from_bounds(
        target_bounds.left,
        target_bounds.bottom,
        target_bounds.right,
        target_bounds.top,
        width,
        height,
    )
    with rasterio.open(src_path) as src:
        vrt_options = {
            "transform": dst_transform,
            "height": height,
            "width": width,
            "crs": target_crs,
            "resampling": resampling,
        }
        with WarpedVRT(src, **vrt_options) as vrt:
            profile = vrt.profile.copy()
            profile.update({"dtype": "float32", "nodata": 0, "driver": "GTiff"})
            rio_shutil.copy(vrt, dst_path, **profile)


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
        s2_width = s3_ref.width * RESOLUTION_RATIO
        s2_height = s3_ref.height * RESOLUTION_RATIO

    for s2_file in s2_dir.glob("*.geotiff"):
        if s2_file.name in clouds["s2"]:
            continue
        date_str = s2_file.name.split("_")[0]
        refl_dst = s2_output_dir / f"S2A_MSIL2A_{date_str}_REFL.tif"
        if refl_dst.exists():
            continue

        temp_normalized = s2_output_dir / f"temp_{s2_file.name}"
        with rasterio.open(s2_file) as src:
            data = src.read().astype("float32") / 10000.0
            profile = src.profile.copy()
            profile.update({"dtype": "float32", "nodata": 0})
            with rasterio.open(temp_normalized, "w", **profile) as dst:
                dst.write(data)

        _reproject_raster_to_target(
            temp_normalized, refl_dst, target_bounds, target_crs, s2_width, s2_height
        )
        temp_normalized.unlink()

    _, distance_to_clouds, _ = _import_efast()
    distance_to_clouds(s2_output_dir, ratio=RESOLUTION_RATIO)


def prepare_s3(season, site_position, site_name, date_range=None):
    s3_dir = Path(f"data/{site_name}/{season}/raw/s3/")
    s2_prepared_dir = Path(f"data/{site_name}/{season}/prepared/s2/")
    s3_preprocessed_dir = Path(f"data/{site_name}/{season}/prepared/s3/")
    clouds_file = Path(f"data/{site_name}/{season}/clouds.json")

    clouds = _load_clouds(clouds_file)
    s3_preprocessed_dir.mkdir(parents=True, exist_ok=True)

    s3_by_date = defaultdict(list)
    for s3_file in s3_dir.glob("*.geotiff"):
        if s3_file.name not in clouds["s3"]:
            s3_by_date[s3_file.name.split("_")[0]].append(s3_file)

    temp_composite_dir = s3_preprocessed_dir / "temp_composites"
    if temp_composite_dir.exists():
        shutil.rmtree(temp_composite_dir)
    temp_composite_dir.mkdir()

    for date_str, s3_files in s3_by_date.items():
        composite_path = temp_composite_dir / f"composite_{date_str}.tif"
        if len(s3_files) == 1:
            shutil.copy(s3_files[0], composite_path)
        else:
            s3_stack = []
            for s3_file in s3_files:
                with rasterio.open(s3_file) as src:
                    data = src.read()
                    data[:, np.abs(np.nanmean(data, axis=0)) >= 5] = np.nan
                    s3_stack.append(data)
            composite = np.nanmean(np.array(s3_stack), axis=0).astype("float32")
            with rasterio.open(s3_files[0]) as src:
                profile = src.profile.copy()
                profile.update({"count": composite.shape[0], "dtype": "float32"})
            with rasterio.open(composite_path, "w", **profile) as dst:
                dst.write(composite)

    # Reproject S3 to match S2 REFL bounds (full coverage) instead of DIST_CLOUD bounds
    # This ensures fusion covers the same area as S2 and dimensions match
    sen2_ref_paths = list(s2_prepared_dir.glob("*REFL.tif"))
    if len(sen2_ref_paths) == 0:
        raise ValueError(f"No REFL files found in {s2_prepared_dir}")

    # Get bounds from REFL file (full coverage, matches S2)
    with rasterio.open(sen2_ref_paths[0]) as s2_ref:
        target_bounds = s2_ref.bounds
        target_crs = s2_ref.crs
        s2_resolution = abs(s2_ref.transform[0])
        s3_resolution = s2_resolution * RESOLUTION_RATIO
        width = int((target_bounds.right - target_bounds.left) / s3_resolution)
        height = int((target_bounds.top - target_bounds.bottom) / s3_resolution)
        s3_transform = rasterio.transform.from_bounds(
            target_bounds.left,
            target_bounds.bottom,
            target_bounds.right,
            target_bounds.top,
            width,
            height,
        )

    # Reproject each S3 composite to match S2 REFL bounds
    sen3_paths = list(temp_composite_dir.glob("*.tif"))
    for sen3_path in sen3_paths:
        vrt_options = {
            "transform": s3_transform,
            "height": height,
            "width": width,
            "crs": target_crs,
            "resampling": Resampling.cubic,
        }
        with rasterio.open(sen3_path) as s3_src:
            with WarpedVRT(s3_src, **vrt_options) as vrt:
                name = sen3_path.name
                outfile = s3_preprocessed_dir / name
                profile = vrt.profile.copy()
                profile.update({"dtype": "float32", "nodata": 0, "driver": "GTiff"})
                rio_shutil.copy(vrt, outfile, **profile)

    shutil.rmtree(temp_composite_dir)


def run_efast(season, site_position, site_name, date_range=None):
    lat, lon = site_position
    datetime_range = date_range or f"{season}-01-01/{season}-12-31"

    efast_base_dir = Path(f"data/{site_name}/{season}/prepared/")
    s2_output_dir = efast_base_dir / "s2"
    s3_output_dir = efast_base_dir / "s3"
    fusion_output_dir = efast_base_dir / "fusion"

    fusion_output_dir.mkdir(parents=True, exist_ok=True)
    print(f"[EFAST] Starting fusion: {site_name} ({lat:.6f}, {lon:.6f}), {season}")

    efast, _, _ = _import_efast()

    start_str, end_str = datetime_range.split("/")
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        output_file = fusion_output_dir / f"REFL_{date_str}.tif"
        try:
            efast.fusion(
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
            print(
                f"[EFAST] Saved: {output_file}"
                if output_file.exists()
                else f"[EFAST] No output for {date_str} (insufficient nearby data)"
            )
        except Exception as e:
            print(f"[EFAST] Error processing {date_str}: {e}")
        current_date += timedelta(days=1)

    print("[EFAST] Completed")

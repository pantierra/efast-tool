import json
import shutil
import importlib.util
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np
import rasterio
from rasterio.warp import Resampling
from rasterio.vrt import WarpedVRT
from scipy import ndimage

_this_file = Path(__file__).resolve()
_venv_lib = _this_file.parent.parent / "venv" / "lib"
_efast_pkg_path = None
if _venv_lib.exists():
    for py_dir in _venv_lib.glob("python*"):
        candidate = py_dir / "site-packages" / "efast" / "efast.py"
        if candidate.exists():
            _efast_pkg_path = candidate
            break

if _efast_pkg_path and _efast_pkg_path.exists():
    spec = importlib.util.spec_from_file_location(
        "efast_fusion_module", _efast_pkg_path
    )
    efast_fusion = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(efast_fusion)
else:
    import site

    for site_pkg in site.getsitepackages():
        candidate = Path(site_pkg) / "efast" / "efast.py"
        if candidate.exists():
            spec = importlib.util.spec_from_file_location(
                "efast_fusion_module", candidate
            )
            efast_fusion = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(efast_fusion)
            break
    else:
        raise ImportError(
            "efast package not found. Install with: pip install git+https://github.com/DHI-GRAS/efast.git"
        )


def prepare_s2(season, site_position, site_name, date_range=None):
    s2_dir = Path(f"data/{site_name}/{season}/raw/s2/")
    s3_dir = Path(f"data/{site_name}/{season}/raw/s3/")
    s2_output_dir = Path(f"data/{site_name}/{season}/prepared/s2/")
    clouds_file = Path(f"data/{site_name}/{season}/clouds.json")

    clouds = {"s2": set(), "s3": set()}
    if clouds_file.exists():
        clouds_data = json.loads(clouds_file.read_text())
        clouds["s2"] = set(clouds_data.get("s2", []))
        clouds["s3"] = set(clouds_data.get("s3", []))

    s2_output_dir.mkdir(parents=True, exist_ok=True)

    s3_files = [f for f in s3_dir.glob("*.geotiff") if f.name not in clouds["s3"]]
    if not s3_files:
        raise ValueError("No non-cloud S3 files found for reference bounds")

    with rasterio.open(s3_files[0]) as s3_ref:
        target_bounds = s3_ref.bounds
        target_crs = s3_ref.crs
        s3_width = s3_ref.width
        s3_height = s3_ref.height
        ratio = 21
        s2_width = s3_width * ratio
        s2_height = s3_height * ratio

    for s2_file in s2_dir.glob("*.geotiff"):
        if s2_file.name in clouds["s2"]:
            continue
        date_str = s2_file.name.split("_")[0]

        refl_dst = s2_output_dir / f"S2A_MSIL2A_{date_str}_REFL.tif"
        if not refl_dst.exists():
            with rasterio.open(s2_file) as src:
                data = src.read().astype("float32") / 10000.0
                s2_res = (target_bounds.right - target_bounds.left) / s2_width
                dst_transform = rasterio.transform.from_bounds(
                    target_bounds.left,
                    target_bounds.bottom,
                    target_bounds.right,
                    target_bounds.top,
                    s2_width,
                    s2_height,
                )
                reprojected_data, _ = rasterio.warp.reproject(
                    source=data,
                    destination=np.zeros(
                        (src.count, s2_height, s2_width), dtype=data.dtype
                    ),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=dst_transform,
                    dst_crs=target_crs,
                    resampling=Resampling.cubic,
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
                distance_to_cloud_hr = ndimage.distance_transform_edt(~mask)
                distance_to_cloud_hr = np.clip(distance_to_cloud_hr, 0, 255).astype(
                    "float32"
                )

                s3_res = (target_bounds.right - target_bounds.left) / s3_width
                lr_transform = rasterio.transform.from_bounds(
                    target_bounds.left,
                    target_bounds.bottom,
                    target_bounds.right,
                    target_bounds.top,
                    s3_width,
                    s3_height,
                )
                distance_to_cloud_lr, _ = rasterio.warp.reproject(
                    source=distance_to_cloud_hr[np.newaxis, :, :],
                    destination=np.zeros(
                        (1, s3_height, s3_width), dtype=distance_to_cloud_hr.dtype
                    ),
                    src_transform=src.transform,
                    src_crs=target_crs,
                    dst_transform=lr_transform,
                    dst_crs=target_crs,
                    resampling=Resampling.average,
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
    s3_preprocessed_dir = Path(f"data/{site_name}/{season}/prepared/s3/")
    clouds_file = Path(f"data/{site_name}/{season}/clouds.json")

    clouds = {"s3": set()}
    if clouds_file.exists():
        clouds_data = json.loads(clouds_file.read_text())
        clouds["s3"] = set(clouds_data.get("s3", []))

    s3_preprocessed_dir.mkdir(parents=True, exist_ok=True)
    for s3_file in s3_dir.glob("*.geotiff"):
        if s3_file.name in clouds["s3"]:
            continue
        date_str = s3_file.name.split("_")[0]
        output_path = s3_preprocessed_dir / f"composite_{date_str}.tif"
        if output_path.exists():
            continue
        shutil.copy2(s3_file, output_path)


def run_efast(season, site_position, site_name, date_range=None):
    lat, lon = site_position
    datetime_range = date_range or f"{season}-01-01/{season}-12-31"

    efast_base_dir = Path(f"data/{site_name}/{season}/prepared/")
    s2_output_dir = efast_base_dir / "s2"
    s3_output_dir = efast_base_dir / "s3"
    fusion_output_dir = efast_base_dir / "fusion"

    fusion_output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[EFAST] Starting fusion: {site_name} ({lat:.6f}, {lon:.6f}), {season}")

    start_date = datetime.strptime(datetime_range.split("/")[0], "%Y-%m-%d")
    end_date = datetime.strptime(datetime_range.split("/")[1], "%Y-%m-%d")

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
                ratio=21,
            )
            if output_file.exists():
                print(f"[EFAST] Saved: {output_file}")
            else:
                print(f"[EFAST] No output for {date_str} (insufficient nearby data)")
        except Exception as e:
            print(f"[EFAST] Error processing {date_str}: {e}")

        current_date += timedelta(days=1)

    print("[EFAST] Completed")

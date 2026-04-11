"""Data preparation: S2/S3 preprocessing for fusion."""

import json
import shutil
from pathlib import Path
from collections import defaultdict
import numpy as np
import rasterio
from rasterio.warp import Resampling
from rasterio.vrt import WarpedVRT
from rasterio import shutil as rio_shutil

RESOLUTION_RATIO = 21
# Centred temporal MA on S3 LR stack (METHODOLOGY §5.4.3); odd ≥3, or 1 to disable.
S3_MOVING_AVERAGE_WINDOW_DAYS = 5


def _apply_s3_temporal_moving_average(s3_dir, window):
    """In-place smoothing of composite_*.tif along calendar order; nodata 0 → NaN for averaging."""
    if window <= 1:
        return
    paths = sorted(s3_dir.glob("composite_*.tif"), key=lambda p: p.stem.split("_")[1])
    if not paths:
        return
    k = (window - 1) // 2
    arrs = []
    profiles = []
    for p in paths:
        with rasterio.open(p) as src:
            d = src.read().astype(np.float32)
            d[d == 0] = np.nan
            arrs.append(d)
            profiles.append(src.profile.copy())
    stack = np.stack(arrs, axis=0)
    t, _, _, _ = stack.shape
    out = np.empty_like(stack)
    for i in range(t):
        lo, hi = max(0, i - k), min(t, i + k + 1)
        out[i] = np.nanmean(stack[lo:hi], axis=0)
    out = np.nan_to_num(out, nan=0.0, posinf=0.0, neginf=0.0).astype(np.float32)
    for p, prof, slc in zip(paths, profiles, out):
        prof.update({"dtype": "float32", "nodata": 0})
        with rasterio.open(p, "w", **prof) as dst:
            dst.write(slc)
    print(f"[S3-PREP] Applied {window}-day centred MA ({t} composites)")


def _import_distance_to_clouds():
    """Lazy import of efast.distance_to_clouds."""
    try:
        from efast.s2_processing import distance_to_clouds

        return distance_to_clouds
    except ImportError:
        raise ImportError(
            "efast package not found. Install with: pip install git+https://github.com/DHI-GRAS/efast.git"
        )


def _load_excluded(season, site_name, cleaning_strategy):
    """Load excluded filenames from NDVI timeseries (excluded_aggressive / excluded_nonaggressive)."""
    base = Path(f"data/{site_name}/{season}/raw/preselection")
    key = f"excluded_{cleaning_strategy}"
    clouds = {"s2": set(), "s3": set()}
    for source in ["s2", "s3"]:
        ts_file = base / f"{source}_preselection.json"
        if ts_file.exists():
            data = json.loads(ts_file.read_text())
            clouds[source] = {e["filename"] for e in data if e.get(key)}
    return clouds


def _get_base_dir(season, site_name, cleaning_strategy):
    return Path(f"data/{site_name}/{season}/prepared_{cleaning_strategy}/")


def _get_itb_base_dir(season, site_name, cleaning_strategy):
    return Path(f"data/{site_name}/{season}/prepared_{cleaning_strategy}_itb")


def _compute_gcc_from_refl_array(blue, green, red):
    total = red.astype(np.float32) + green.astype(np.float32) + red.astype(np.float32)
    mask = (total > 0) & np.isfinite(total)
    gcc = np.zeros_like(green, dtype=np.float32)
    gcc[mask] = green[mask].astype(np.float32) / total[mask]
    return gcc


def _link_dist_cloud_from_prepared(src_s2_dir, dst_s2_dir):
    dst_s2_dir.mkdir(parents=True, exist_ok=True)
    for src in src_s2_dir.glob("*DIST_CLOUD.tif"):
        dst = dst_s2_dir / src.name
        if dst.exists():
            continue
        try:
            dst.symlink_to(src.resolve())
        except OSError:
            shutil.copy2(src, dst)


def prepare_s2_gcc_for_itb(
    season, site_position, site_name, cleaning_strategy="aggressive"
):
    base = _get_base_dir(season, site_name, cleaning_strategy)
    itb_s2 = _get_itb_base_dir(season, site_name, cleaning_strategy) / "s2"
    s2_prep = base / "s2"
    itb_s2.mkdir(parents=True, exist_ok=True)
    for refl in sorted(s2_prep.glob("*REFL.tif")):
        out = itb_s2 / refl.name.replace("_REFL.tif", "_GCC.tif")
        if out.exists():
            continue
        with rasterio.open(refl) as src:
            if src.count < 4:
                continue
            b, g, r = (src.read(i).astype(np.float32) for i in range(1, 4))
            gcc = _compute_gcc_from_refl_array(b, g, r)
            profile = src.profile.copy()
            profile.update({"count": 1, "dtype": "float32", "nodata": 0})
            with rasterio.open(out, "w", **profile) as dst:
                dst.write(gcc, 1)
        print(f"[S2-ITB] Saved {out.name}")
    _link_dist_cloud_from_prepared(s2_prep, itb_s2)


def prepare_s3_gcc_for_itb(
    season, site_position, site_name, cleaning_strategy="aggressive"
):
    base = _get_base_dir(season, site_name, cleaning_strategy)
    itb_s3 = _get_itb_base_dir(season, site_name, cleaning_strategy) / "s3"
    itb_s3.mkdir(parents=True, exist_ok=True)
    for comp in sorted((base / "s3").glob("composite_*.tif")):
        out = itb_s3 / comp.name
        if out.exists():
            continue
        with rasterio.open(comp) as src:
            if src.count < 4:
                continue
            b, g, r = (src.read(i).astype(np.float32) for i in range(1, 4))
            gcc = _compute_gcc_from_refl_array(b, g, r)
            profile = src.profile.copy()
            profile.update({"count": 1, "dtype": "float32", "nodata": 0})
            with rasterio.open(out, "w", **profile) as dst:
                dst.write(gcc, 1)
        print(f"[S3-ITB] Saved {out.name}")


def _reproject_raster_to_target(
    src_path,
    dst_path,
    target_bounds,
    target_crs,
    width,
    height,
    resampling=Resampling.bilinear,
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


def _rescale_dist_cloud_for_small_roi(s2_output_dir):
    """Rescale DIST_CLOUD when max distance ≤1 so EFAST fusion gets valid weights.

    EFAST uses wo_i = (distance - 1) / D; values ≤1 yield zero/NaN weights. In small
    ROIs (e.g. PhenoCam sites, 7×4 LR grid), distance_transform_edt never exceeds 1.
    Scale non-zero values to ≥2 so fusion can produce non-NaN output.
    """
    for dc_path in s2_output_dir.glob("*DIST_CLOUD.tif"):
        with rasterio.open(dc_path, "r") as src:
            d = src.read(1)
        d_max = float(np.nanmax(d))
        if d_max <= 1:
            # Map (0, 1] -> (0, 2] so (d-1)/15 gives positive weight
            d_scaled = np.where(d > 0, 2.0, d).astype(np.float32)
            with rasterio.open(dc_path, "r+") as dst:
                dst.write(d_scaled, 1)
            print(f"[S2-PREP] Rescaled DIST_CLOUD for {dc_path.name} (max was {d_max})")


def prepare_s2(
    season, site_position, site_name, cleaning_strategy="aggressive", date_range=None
):
    lat, lon = site_position
    s2_dir = Path(f"data/{site_name}/{season}/raw/s2/")
    s3_dir = Path(f"data/{site_name}/{season}/raw/s3/")
    s2_output_dir = _get_base_dir(season, site_name, cleaning_strategy) / "s2"

    clouds = _load_excluded(season, site_name, cleaning_strategy)
    s2_output_dir.mkdir(parents=True, exist_ok=True)

    print(
        f"[S2-PREP] Starting preparation: {site_name} ({lat:.6f}, {lon:.6f}), {season}, strategy={cleaning_strategy}"
    )

    s3_files = [f for f in s3_dir.glob("*.geotiff") if f.name not in clouds["s3"]]
    if not s3_files:
        raise ValueError("No non-cloud S3 files found for reference bounds")

    with rasterio.open(s3_files[0]) as s3_ref:
        target_bounds = s3_ref.bounds
        target_crs = s3_ref.crs
        s2_width = s3_ref.width * RESOLUTION_RATIO
        s2_height = s3_ref.height * RESOLUTION_RATIO

    for s2_file in sorted(s2_dir.glob("*.geotiff")):
        if s2_file.name in clouds["s2"]:
            print(
                f"[S2-PREP] Skipping {s2_file.name} (excluded by {cleaning_strategy})"
            )
            continue
        date_str = s2_file.name.split("_")[0]
        refl_dst = s2_output_dir / f"S2A_MSIL2A_{date_str}_REFL.tif"
        if refl_dst.exists():
            print(f"[S2-PREP] Skipping {s2_file.name} (exists)")
            continue

        print(f"[S2-PREP] Processing {s2_file.name}...")
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
        print(f"[S2-PREP] Saved: {refl_dst}")

    print("[S2-PREP] Computing distance-to-clouds...")
    distance_to_clouds = _import_distance_to_clouds()
    distance_to_clouds(s2_output_dir, ratio=RESOLUTION_RATIO)
    _rescale_dist_cloud_for_small_roi(s2_output_dir)
    print("[S2-PREP] Completed")


def prepare_s3(
    season, site_position, site_name, cleaning_strategy="aggressive", date_range=None
):
    lat, lon = site_position
    s3_dir = Path(f"data/{site_name}/{season}/raw/s3/")
    base_dir = _get_base_dir(season, site_name, cleaning_strategy)
    s2_prepared_dir = base_dir / "s2"
    s3_preprocessed_dir = base_dir / "s3"

    clouds = _load_excluded(season, site_name, cleaning_strategy)
    s3_preprocessed_dir.mkdir(parents=True, exist_ok=True)

    print(
        f"[S3-PREP] Starting preparation: {site_name} ({lat:.6f}, {lon:.6f}), {season}, strategy={cleaning_strategy}"
    )

    s3_by_date = defaultdict(list)
    for s3_file in s3_dir.glob("*.geotiff"):
        if s3_file.name not in clouds["s3"]:
            s3_by_date[s3_file.name.split("_")[0]].append(s3_file)
        else:
            print(
                f"[S3-PREP] Skipping {s3_file.name} (excluded by {cleaning_strategy})"
            )

    print(
        f"[S3-PREP] Found {sum(len(v) for v in s3_by_date.values())} acquisitions across {len(s3_by_date)} dates"
    )

    temp_composite_dir = s3_preprocessed_dir / "temp_composites"
    if temp_composite_dir.exists():
        shutil.rmtree(temp_composite_dir)
    temp_composite_dir.mkdir()

    for date_str, s3_files in sorted(s3_by_date.items()):
        composite_path = temp_composite_dir / f"composite_{date_str}.tif"
        if len(s3_files) == 1:
            shutil.copy(s3_files[0], composite_path)
            print(f"[S3-PREP] Composite {date_str}: 1 acquisition")
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
            print(
                f"[S3-PREP] Composite {date_str}: {len(s3_files)} acquisitions merged"
            )

    # Reproject S3 to match S2 REFL bounds (full coverage) instead of DIST_CLOUD bounds
    # This ensures fusion covers the same area as S2 and dimensions match
    sen2_ref_paths = list(s2_prepared_dir.glob("*REFL.tif"))
    if len(sen2_ref_paths) == 0:
        raise ValueError(f"No REFL files found in {s2_prepared_dir}")

    # Get bounds from REFL file (full coverage, matches S2)
    # Use integer division to match distance_to_clouds logic exactly
    with rasterio.open(sen2_ref_paths[0]) as s2_ref:
        target_bounds = s2_ref.bounds
        target_crs = s2_ref.crs
        # Use integer division matching distance_to_clouds: s2_height // ratio, s2_width // ratio
        width = s2_ref.width // RESOLUTION_RATIO
        height = s2_ref.height // RESOLUTION_RATIO
        s3_transform = rasterio.transform.from_bounds(
            target_bounds.left,
            target_bounds.bottom,
            target_bounds.right,
            target_bounds.top,
            width,
            height,
        )

    print(
        f"[S3-PREP] Reprojecting {len(list(temp_composite_dir.glob('*.tif')))} composites to S2 grid ({width}×{height} px)..."
    )

    # Reproject each S3 composite to match S2 REFL bounds
    sen3_paths = sorted(temp_composite_dir.glob("*.tif"))
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
        print(f"[S3-PREP] Saved: {outfile}")

    _apply_s3_temporal_moving_average(
        s3_preprocessed_dir, S3_MOVING_AVERAGE_WINDOW_DAYS
    )
    shutil.rmtree(temp_composite_dir)
    print("[S3-PREP] Completed")

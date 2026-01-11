from pathlib import Path
from datetime import datetime
import numpy as np
import rasterio
from rasterio import windows


def _crop_to_bounds(src_file, bounds, output_file, row_based_height=None):
    """Crop a raster file to given bounds and save."""
    crop_left, crop_bottom, crop_right, crop_top, crop_crs = bounds
    
    with rasterio.open(src_file) as src:
        # Calculate window from bounds
        window = windows.from_bounds(crop_left, crop_bottom, crop_right, crop_top, src.transform)
        
        # Use row-based height if provided (for fusion), otherwise calculate from bounds
        if row_based_height is not None:
            col_off = int(round(window.col_off))
            window = windows.Window(col_off, 0, src.width, row_based_height)
            # Calculate bottom Y from row index
            bottom_y = src.transform[5] + row_based_height * src.transform[4]
        else:
            pixel_size = abs(src.transform[0])
            width = int(round((crop_right - crop_left) / pixel_size))
            height = int(round((crop_top - crop_bottom) / pixel_size))
            window = windows.Window(
                int(round(window.col_off)), int(round(window.row_off)), width, height
            )
            bottom_y = crop_bottom
        
        # Clip window to source bounds
        src_window = windows.Window(0, 0, src.width, src.height)
        window = window.intersection(src_window)
        if not window or window.height <= 0 or window.width <= 0:
            return False
        
        data = src.read(window=window)
        transform = rasterio.transform.from_bounds(
            crop_left, bottom_y, crop_right, crop_top, window.width, window.height
        )
        
        profile = src.profile.copy()
        profile.update({
            "height": window.height,
            "width": window.width,
            "transform": transform,
            "crs": crop_crs,
        })
    
    with rasterio.open(output_file, "w", **profile) as dst:
        dst.write(data)
    return True


def process_cropped(season, site_position, site_name):
    """Crop prepared S2, S3, and fusion files to fusion valid data bounds."""
    base = Path(f"data/{site_name}/{season}")
    prepared = base / "prepared"
    processed = base / "processed"
    
    s2_prep = prepared / "s2"
    s3_prep = prepared / "s3"
    fusion_prep = prepared / "fusion"
    
    for output_dir in [processed / "s2", processed / "s3", processed / "fusion"]:
        output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"[PROCESS] Cropping files to fusion valid data bounds: {site_name}, {season}")
    
    # Collect all available DIST_CLOUD files and their dates
    dist_cloud_files = {}
    for dist_cloud_file in s2_prep.glob("S2A_MSIL2A_*_DIST_CLOUD.tif"):
        date_str = dist_cloud_file.stem.split("_")[2]
        try:
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            dist_cloud_files[date_obj] = dist_cloud_file
        except ValueError:
            continue
    
    if not dist_cloud_files:
        print("[PROCESS] Warning: No DIST_CLOUD files found. Cannot process fusion files.")
        return
    
    dist_cloud_dates = sorted(dist_cloud_files.keys())
    
    def find_closest_dist_cloud(target_date_str):
        """Find the closest DIST_CLOUD file to the target date."""
        try:
            target_date = datetime.strptime(target_date_str, "%Y%m%d")
        except ValueError:
            return None
        
        # Find closest date
        closest_date = min(dist_cloud_dates, key=lambda d: abs((d - target_date).days))
        return dist_cloud_files[closest_date]
    
    # Determine valid bounds for each fusion file
    fusion_bounds = {}
    fusion_rows = {}
    
    for fusion_file in fusion_prep.glob("REFL_*.tif"):
        date_str = fusion_file.stem.split("_")[1]
        
        # Try exact date first, then find closest
        dist_cloud = s2_prep / f"S2A_MSIL2A_{date_str}_DIST_CLOUD.tif"
        if not dist_cloud.exists():
            dist_cloud = find_closest_dist_cloud(date_str)
            if dist_cloud is None:
                continue
        
        with rasterio.open(dist_cloud) as dist_src:
            dist_bounds = dist_src.bounds
            dist_crs = dist_src.crs
        
        # Find first valid row from bottom in fusion file
        with rasterio.open(fusion_file) as fusion_src:
            data = fusion_src.read()
            height = data.shape[1]
            
            first_valid_row = height
            for row_idx in range(height - 1, -1, -1):
                if np.any(~np.isnan(data[:, row_idx, :]) & (data[:, row_idx, :] > 0.001)):
                    first_valid_row = row_idx
                    break
            
            valid_bottom_y = (fusion_src.transform * (0, first_valid_row + 1))[1]
            crop_bottom = max(dist_bounds.bottom, valid_bottom_y)
            
            fusion_bounds[date_str] = (
                dist_bounds.left, crop_bottom, dist_bounds.right, dist_bounds.top, dist_crs
            )
            fusion_rows[date_str] = first_valid_row
    
    # Process S2 files
    for refl_file in s2_prep.glob("*REFL.tif"):
        date_str = refl_file.stem.split("_")[2]
        if date_str in fusion_bounds:
            output_file = processed / "s2" / f"{date_str}_0.geotiff"
            if _crop_to_bounds(refl_file, fusion_bounds[date_str], output_file):
                print(f"[PROCESS] Saved: {output_file}")
    
    # Process S3 files
    for s3_file in s3_prep.glob("composite_*.tif"):
        date_str = s3_file.stem.split("_")[1]
        if date_str in fusion_bounds:
            output_file = processed / "s3" / f"{date_str}_0.geotiff"
            if _crop_to_bounds(s3_file, fusion_bounds[date_str], output_file):
                print(f"[PROCESS] Saved: {output_file}")
    
    # Process fusion files (use row-based cropping)
    for date_str, bounds in fusion_bounds.items():
        fusion_file = fusion_prep / f"REFL_{date_str}.tif"
        if fusion_file.exists():
            output_file = processed / "fusion" / f"{date_str}_0.geotiff"
            if _crop_to_bounds(fusion_file, bounds, output_file, row_based_height=fusion_rows[date_str] + 1):
                print(f"[PROCESS] Saved: {output_file}")
    
    print("[PROCESS] Completed")

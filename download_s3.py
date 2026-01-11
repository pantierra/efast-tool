import os
import time
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
import openeo
import requests
import netCDF4
import numpy as np
import rasterio
from rasterio.transform import from_bounds

load_dotenv()

BBOX_SIZE = 0.016  # Larger than S2 to ensure full coverage including padded pixels


def _get_bbox(lon, lat):
    half = BBOX_SIZE / 2
    return [lon - half, lat - half, lon + half, lat + half]


def _process_netcdf(nc_file, output_dir, bands, openeo_bands):
    with netCDF4.Dataset(str(nc_file), "r") as nc:
        times = netCDF4.num2date(nc.variables["t"][:], nc.variables["t"].units)
        x_coords = nc.variables["x"][:]
        y_coords = nc.variables["y"][:]
        band_vars = sorted(
            [v for v in nc.variables.keys() if v.startswith("B") and v[1:].isdigit()]
        )
        band_names = [list(bands.keys())[openeo_bands.index(b)] for b in band_vars]

        transform = from_bounds(
            float(x_coords.min()),
            float(y_coords.min()),
            float(x_coords.max()),
            float(y_coords.max()),
            len(x_coords),
            len(y_coords),
        )

        print(f"[S3] Found {len(times)} time steps")
        date_counts = {}
        for t_idx, time_val in enumerate(times):
            dt = (
                time_val
                if isinstance(time_val, datetime)
                else netCDF4.num2date(nc.variables["t"][t_idx], nc.variables["t"].units)
            )
            date_str = dt.strftime("%Y%m%d")
            increment = date_counts.get(date_str, 0)
            date_counts[date_str] = increment + 1

            band_data = [nc.variables[b][t_idx, :, :] for b in band_vars]
            stacked = np.stack(band_data, axis=0)

            output_path = output_dir / f"{date_str}_{increment}.geotiff"
            with rasterio.open(
                output_path,
                "w",
                driver="GTiff",
                height=len(y_coords),
                width=len(x_coords),
                count=len(band_data),
                dtype=stacked.dtype,
                crs="EPSG:32632",
                transform=transform,
                compress="lzw",
            ) as dst:
                dst.write(stacked)
                for i, band_name in enumerate(band_names, 1):
                    dst.set_band_description(i, band_name)
            print(f"[S3] Saved: {output_path}")


def download_s3(season, site_position, site_name, date_range=None):
    lat, lon = site_position
    datetime_range = date_range or f"{season}-01-01/{season}-12-31"
    output_dir = Path(f"data/{site_name}/{season}/raw/s3/")

    print(f"[S3] Starting download: {site_name} ({lat:.6f}, {lon:.6f}), {season}")

    bbox = _get_bbox(lon, lat)
    bands = {
        "SDR_Oa04": "blue",
        "SDR_Oa06": "green",
        "SDR_Oa08": "red",
        "SDR_Oa17": "nir",
    }
    output_dir.mkdir(parents=True, exist_ok=True)

    band_map = {
        "SDR_Oa04": "B04",
        "SDR_Oa06": "B06",
        "SDR_Oa08": "B08",
        "SDR_Oa17": "B17",
    }
    openeo_bands = [band_map.get(b, b) for b in bands.keys()]

    start_date, end_date = datetime_range.split("/")
    spatial_extent = {
        "west": bbox[0],
        "east": bbox[2],
        "south": bbox[1],
        "north": bbox[3],
    }

    print("[S3] Authenticating...")
    token_response = requests.post(
        "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "username": os.getenv("CDSE_USER"),
            "password": os.getenv("CDSE_PASSWORD"),
            "client_id": "cdse-public",
        },
    )
    token_response.raise_for_status()
    tokens = token_response.json()
    access_token = tokens["access_token"]

    print("[S3] Connecting to OpenEO...")
    conn = openeo.connect("openeo.dataspace.copernicus.eu")
    conn.authenticate_oidc_access_token(access_token)

    print("[S3] Loading collection...")
    datacube = conn.load_collection(
        "SENTINEL3_OLCI_L1B",
        spatial_extent=spatial_extent,
        temporal_extent=[start_date, end_date],
        bands=openeo_bands,
    ).resample_spatial(projection=32632)

    output_file = output_dir / "s3_data.nc"
    print(f"[S3] Downloading NetCDF to {output_file}...")
    print(f"[S3] Temporal extent: {start_date} to {end_date}")
    print(f"[S3] Spatial extent: {spatial_extent}")
    print(f"[S3] Bands: {openeo_bands}")
    print("[S3] This may take several minutes depending on data volume...")

    start_time = time.time()
    try:
        datacube.download(str(output_file), format="NetCDF")
        elapsed = time.time() - start_time
        print(f"[S3] Download completed in {elapsed:.1f} seconds")
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"[S3] Download failed after {elapsed:.1f} seconds: {e}")
        raise

    print("[S3] Processing NetCDF...")
    process_start = time.time()
    _process_netcdf(output_file, output_dir, bands, openeo_bands)
    process_elapsed = time.time() - process_start
    print(f"[S3] Processing completed in {process_elapsed:.1f} seconds")

    print(f"[S3] Removing temporary NetCDF file...")
    os.remove(output_file)
    print("[S3] Completed")

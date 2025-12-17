import os
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

datetime_range = "2024-01-01/2024-01-03"
lon, lat = 11.320308, 47.116171
bbox_size = 0.009
bbox = [lon - bbox_size/2, lat - bbox_size/2, lon + bbox_size/2, lat + bbox_size/2]
bands = {"SDR_Oa04": "blue", "SDR_Oa06": "green", "SDR_Oa08": "red", "SDR_Oa17": "nir"}
output_dir = Path("data/innsbruck/2024/s3/")
output_dir.mkdir(parents=True, exist_ok=True)

band_map = {"SDR_Oa04": "B04", "SDR_Oa06": "B06", "SDR_Oa08": "B08", "SDR_Oa17": "B17"}
openeo_bands = [band_map.get(b, b) for b in bands.keys()]

start_date, end_date = datetime_range.split("/")
spatial_extent = {
    "west": bbox[0], "east": bbox[2],
    "south": bbox[1], "north": bbox[3]
}

token_response = requests.post(
    "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token",
    data={
        "grant_type": "password",
        "username": os.getenv("CDSE_USER"),
        "password": os.getenv("CDSE_PASSWORD"),
        "client_id": "cdse-public"
    }
)
token_response.raise_for_status()
tokens = token_response.json()
access_token = tokens["access_token"]

conn = openeo.connect("openeo.dataspace.copernicus.eu")
conn.authenticate_oidc_access_token(access_token)

datacube = conn.load_collection(
    "SENTINEL3_OLCI_L1B",
    spatial_extent=spatial_extent,
    temporal_extent=[start_date, end_date],
    bands=openeo_bands,
).resample_spatial(projection=32632)

output_file = output_dir / "s3_data.nc"
datacube.download(str(output_file), format="NetCDF")

nc = netCDF4.Dataset(str(output_file), 'r')
times = netCDF4.num2date(nc.variables['t'][:], nc.variables['t'].units)
x_coords = nc.variables['x'][:]
y_coords = nc.variables['y'][:]
band_vars = sorted([v for v in nc.variables.keys() if v.startswith('B') and v[1:].isdigit()])

transform = from_bounds(
    float(x_coords.min()), float(y_coords.min()),
    float(x_coords.max()), float(y_coords.max()),
    len(x_coords), len(y_coords)
)

for t_idx, time_val in enumerate(times):
    date_str = time_val.strftime("%Y%m%dT%H%M%S") if isinstance(time_val, datetime) else netCDF4.num2date(nc.variables['t'][t_idx], nc.variables['t'].units).strftime("%Y%m%dT%H%M%S")
    
    band_data = [nc.variables[b][t_idx, :, :] for b in band_vars]
    stacked = np.stack(band_data, axis=0)
    
    output_path = output_dir / f"S3_OLCI__{date_str}.tif"
    with rasterio.open(
        output_path, 'w',
        driver='GTiff', height=len(y_coords), width=len(x_coords),
        count=len(band_data), dtype=stacked.dtype, crs='EPSG:32632',
        transform=transform, compress='lzw'
    ) as dst:
        dst.write(stacked)
    print(f"Saved: {output_path}")

nc.close()
os.remove(output_file)

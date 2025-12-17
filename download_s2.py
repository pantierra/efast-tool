import os
import rasterio
from rasterio.warp import transform_geom
from rasterio.windows import from_bounds, transform as window_transform
from pystac_client import Client

datetime_range = "2024-01-01/2024-01-03"
lon, lat = 11.320308, 47.116171
bbox_size = 0.009
bbox = [lon - bbox_size/2, lat - bbox_size/2, lon + bbox_size/2, lat + bbox_size/2]
bands = {"B02": "blue", "B03": "green", "B04": "red", "B8A": "nir"}
output_dir = "data/innsbruck/2024/s2/"
os.makedirs(output_dir, exist_ok=True)

client = Client.open("https://earth-search.aws.element84.com/v1")
search = client.search(
    collections=["sentinel-2-l2a"],
    intersects={"type": "Point", "coordinates": [lon, lat]},
    datetime=datetime_range,
    max_items=1000,
)

items_by_key = {}
for item in search.items():
    date = item.datetime.strftime("%Y%m%d")
    parts = item.id.split("_")
    increment = parts[3] if len(parts) > 3 else "0"
    key = (date, increment)
    if key not in items_by_key:
        items_by_key[key] = item

for (date, increment), item in items_by_key.items():
    filepath = os.path.join(output_dir, f"{date}_{increment}.geotiff")
    if os.path.exists(filepath):
        continue
    
    band_data = {}
    profile = None
    
    for band_name, asset_name in bands.items():
        if asset_name in item.assets:
            asset = item.assets[asset_name]
            with rasterio.open(asset.href) as src:
                bbox_geom = {
                    "type": "Polygon",
                    "coordinates": [[
                        [bbox[0], bbox[1]], [bbox[2], bbox[1]],
                        [bbox[2], bbox[3]], [bbox[0], bbox[3]], [bbox[0], bbox[1]]
                    ]]
                }
                bbox_transformed = transform_geom("EPSG:4326", src.crs, bbox_geom)
                coords = bbox_transformed["coordinates"][0]
                x_coords = [c[0] for c in coords[:4]]
                y_coords = [c[1] for c in coords[:4]]
                bbox_crs = [min(x_coords), min(y_coords), max(x_coords), max(y_coords)]
                src_bounds = src.bounds
                intersect_bbox = [
                    max(bbox_crs[0], src_bounds.left), max(bbox_crs[1], src_bounds.bottom),
                    min(bbox_crs[2], src_bounds.right), min(bbox_crs[3], src_bounds.top),
                ]
                window = from_bounds(*intersect_bbox, src.transform)
                if window.height > 0 and window.width > 0:
                    data = src.read(window=window)
                    new_transform = window_transform(window, src.transform)
                    if profile is None:
                        profile = {
                            "driver": "GTiff", "height": window.height, "width": window.width,
                            "count": len(bands), "dtype": data.dtype, "crs": src.crs,
                            "transform": new_transform, "compress": "lzw"
                        }
                    band_idx = list(bands.keys()).index(band_name)
                    band_data[band_idx] = data[0]
    
    if profile and len(band_data) == len(bands):
        stacked = [band_data[i] for i in sorted(band_data.keys())]
        band_names = [list(bands.keys())[i] for i in sorted(band_data.keys())]
        with rasterio.open(filepath, "w", **profile) as dst:
            for i, data in enumerate(stacked, 1):
                dst.write(data, i)
                dst.set_band_description(i, band_names[i-1])
        print(f"Saved: {filepath}")


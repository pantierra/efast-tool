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

for item in search.items():
    for band_name, asset_name in bands.items():
        if asset_name in item.assets:
            asset = item.assets[asset_name]
            filepath = os.path.join(output_dir, f"{item.id}_{band_name}.tif")
            if not os.path.exists(filepath):
                with rasterio.open(asset.href) as src:
                    bbox_geom = {
                        "type": "Polygon",
                        "coordinates": [[
                            [bbox[0], bbox[1]],
                            [bbox[2], bbox[1]],
                            [bbox[2], bbox[3]],
                            [bbox[0], bbox[3]],
                            [bbox[0], bbox[1]]
                        ]]
                    }
                    bbox_transformed = transform_geom("EPSG:4326", src.crs, bbox_geom)
                    coords = bbox_transformed["coordinates"][0]
                    x_coords = [c[0] for c in coords[:4]]
                    y_coords = [c[1] for c in coords[:4]]
                    bbox_crs = [min(x_coords), min(y_coords), max(x_coords), max(y_coords)]
                    src_bounds = src.bounds
                    intersect_bbox = [
                        max(bbox_crs[0], src_bounds.left),
                        max(bbox_crs[1], src_bounds.bottom),
                        min(bbox_crs[2], src_bounds.right),
                        min(bbox_crs[3], src_bounds.top),
                    ]
                    window = from_bounds(*intersect_bbox, src.transform)
                    if window.height > 0 and window.width > 0:
                        data = src.read(window=window)
                        new_transform = window_transform(window, src.transform)
                        with rasterio.open(
                            filepath, "w",
                            driver="COG",
                            height=window.height,
                            width=window.width,
                            count=src.count,
                            dtype=data.dtype,
                            crs=src.crs,
                            transform=new_transform,
                            compress="lzw",
                        ) as dst:
                            dst.write(data)
                        print(f"Downloaded: {filepath}")


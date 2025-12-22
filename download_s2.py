import os
import rasterio
import xml.etree.ElementTree as ET
import requests
from rasterio.warp import transform_geom
from rasterio.windows import from_bounds, transform as window_transform
from pystac_client import Client


def download_s2(year, site_position, site_name, date_range=None):
    lat, lon = site_position
    datetime_range = date_range or f"{year}-01-01/{year}-12-31"
    output_dir = f"data/{site_name}/{year}/s2/"

    print(f"[S2] Starting download: {site_name} ({lat:.6f}, {lon:.6f}), {year}")

    bbox_size = 0.011
    bbox = [
        lon - bbox_size / 2,
        lat - bbox_size / 2,
        lon + bbox_size / 2,
        lat + bbox_size / 2,
    ]
    bands = {"B02": "blue", "B03": "green", "B04": "red", "B8A": "nir"}
    os.makedirs(output_dir, exist_ok=True)

    print("[S2] Connecting to STAC catalog...")
    client = Client.open("https://earth-search.aws.element84.com/v1")
    search = client.search(
        collections=["sentinel-2-l2a"],
        intersects={"type": "Point", "coordinates": [lon, lat]},
        datetime=datetime_range,
        max_items=1000,
    )

    print("[S2] Searching items...")
    items_by_key = {}
    for item in search.items():
        date = item.datetime.strftime("%Y%m%d")
        parts = item.id.split("_")
        increment = parts[3] if len(parts) > 3 else "0"
        key = (date, increment)
        if key not in items_by_key:
            items_by_key[key] = item

    print(f"[S2] Found {len(items_by_key)} unique items")

    for (date, increment), item in items_by_key.items():
        filepath = os.path.join(output_dir, f"{date}_{increment}.geotiff")
        if os.path.exists(filepath):
            print(f"[S2] Skipping {date}_{increment}.geotiff (exists)")
            continue

        print(f"[S2] Processing {date}_{increment}...")
        band_data = {}
        profile = None

        for band_name, asset_name in bands.items():
            if asset_name in item.assets:
                asset = item.assets[asset_name]
                with rasterio.open(asset.href) as src:
                    bbox_geom = {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [bbox[0], bbox[1]],
                                [bbox[2], bbox[1]],
                                [bbox[2], bbox[3]],
                                [bbox[0], bbox[3]],
                                [bbox[0], bbox[1]],
                            ]
                        ],
                    }
                    bbox_transformed = transform_geom("EPSG:4326", src.crs, bbox_geom)
                    coords = bbox_transformed["coordinates"][0]
                    x_coords = [c[0] for c in coords[:4]]
                    y_coords = [c[1] for c in coords[:4]]
                    bbox_crs = [
                        min(x_coords),
                        min(y_coords),
                        max(x_coords),
                        max(y_coords),
                    ]
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
                        if profile is None:
                            profile = {
                                "driver": "GTiff",
                                "height": window.height,
                                "width": window.width,
                                "count": len(bands),
                                "dtype": data.dtype,
                                "crs": src.crs,
                                "transform": new_transform,
                                "compress": "lzw",
                            }
                        band_idx = list(bands.keys()).index(band_name)
                        band_data[band_idx] = data[0]

        if profile and len(band_data) == len(bands):
            stacked = [band_data[i] for i in sorted(band_data.keys())]
            band_names = [list(bands.keys())[i] for i in sorted(band_data.keys())]

            # Extract viewing angle from granule metadata XML
            viewing_angle = None
            if "granule_metadata" in item.assets:
                try:
                    xml_url = item.assets["granule_metadata"].href
                    xml_resp = requests.get(xml_url, timeout=10)
                    xml_resp.raise_for_status()
                    root = ET.fromstring(xml_resp.content)
                    # Find Mean_Viewing_Incidence_Angle ZENITH_ANGLE
                    for angle_elem in root.findall(".//Mean_Viewing_Incidence_Angle"):
                        if angle_elem.get("bandId") == "0":  # Use first band or average
                            zenith_elem = angle_elem.find("ZENITH_ANGLE")
                            if zenith_elem is not None:
                                viewing_angle = abs(float(zenith_elem.text))
                                break
                    # If not found, try averaging all bands
                    if viewing_angle is None:
                        angles = []
                        for angle_elem in root.findall(
                            ".//Mean_Viewing_Incidence_Angle"
                        ):
                            zenith_elem = angle_elem.find("ZENITH_ANGLE")
                            if zenith_elem is not None:
                                angles.append(abs(float(zenith_elem.text)))
                        if angles:
                            viewing_angle = sum(angles) / len(angles)
                except Exception as e:
                    print(f"[S2] Warning: Could not extract viewing angle: {e}")

            with rasterio.open(filepath, "w", **profile) as dst:
                for i, data in enumerate(stacked, 1):
                    dst.write(data, i)
                    dst.set_band_description(i, band_names[i - 1])
                if viewing_angle is not None:
                    dst.update_tags(VIEWING_ZENITH_ANGLE=viewing_angle)

            print(
                f"[S2] Saved: {filepath} (viewing angle: {viewing_angle:.2f}°)"
                if viewing_angle
                else f"[S2] Saved: {filepath}"
            )
        else:
            print(f"[S2] Skipping {date}_{increment} (missing bands)")

    print("[S2] Completed")

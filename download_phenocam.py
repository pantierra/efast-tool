import requests
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

PHENOCAM_API = "https://phenocam.nau.edu/api"


def _find_start_offset(site_name, start_dt, total_count):
    """Binary search to find approximate offset for start date."""
    low, high = 0, total_count - 1
    limit = 1
    
    for _ in range(15):
        mid = (low + high) // 2
        response = requests.get(
            f"{PHENOCAM_API}/middayimages/",
            params={"site": site_name, "limit": limit, "offset": mid},
            timeout=30
        )
        response.raise_for_status()
        results = response.json().get("results", [])
        if not results:
            break
        
        mid_date_str = results[0].get("imgdate", "")
        if not mid_date_str:
            break
        
        try:
            mid_date = datetime.strptime(mid_date_str, "%Y-%m-%d")
            if mid_date < start_dt:
                low = mid + 1
            else:
                high = mid
        except ValueError:
            break
    
    return max(0, low - 100)


def download_phenocam(season, site_position, site_name, date_range=None):
    lat, lon = site_position
    datetime_range = date_range or f"{season}-01-01/{season}-12-31"
    output_dir = Path(f"data/{site_name}/{season}/raw/phenocam/")
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"[PhenoCam] Starting download: {site_name} ({lat:.6f}, {lon:.6f}), {season}")

    start_date, end_date = datetime_range.split("/")
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    try:
        response = requests.get(
            f"{PHENOCAM_API}/middayimages/",
            params={"site": site_name, "limit": 1},
            timeout=30
        )
        response.raise_for_status()
        total_count = response.json().get("count", 0)
        
        if total_count == 0:
            print(f"[PhenoCam] No images found for site '{site_name}'")
            return
        
        print(f"[PhenoCam] Found {total_count} total images, estimating start offset...")
        start_offset = _find_start_offset(site_name, start_dt, total_count)
        
        url = f"{PHENOCAM_API}/middayimages/"
        params = {"site": site_name, "offset": start_offset}
        
        print(f"[PhenoCam] Fetching image list from offset {start_offset}...")
        images = []
        page = 1
        max_pages = 500
        past_end_date = False
        
        while url and page <= max_pages and not past_end_date:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            results = data.get("results", [])
            
            if not results:
                break
            
            for img in results:
                img_date_str = img.get("imgdate", "")
                if not img_date_str:
                    continue
                try:
                    img_date = datetime.strptime(img_date_str, "%Y-%m-%d")
                    if img_date > end_dt:
                        past_end_date = True
                        break
                    if start_dt <= img_date <= end_dt:
                        images.append(img)
                except ValueError:
                    continue
            
            if url and not past_end_date:
                url = data.get("next")
                params = None
                page += 1
                if page % 50 == 0:
                    print(f"[PhenoCam] Processed {page} pages, found {len(images)} images in range...")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"[PhenoCam] Site '{site_name}' not found")
            return
        raise

    print(f"[PhenoCam] Found {len(images)} images")

    def _download_image(img):
        date_str = img.get("imgdate", "").replace("-", "")
        if not date_str:
            return None
        
        filepath = output_dir / f"{date_str}.jpg"
        if filepath.exists():
            return f"Skipped {date_str}.jpg (exists)"
        
        img_path = img.get("imgpath")
        if not img_path:
            return None
        
        img_url = f"https://phenocam.nau.edu{img_path}"
        try:
            img_response = requests.get(img_url, timeout=30)
            img_response.raise_for_status()
            filepath.write_bytes(img_response.content)
            return f"Saved {date_str}.jpg"
        except Exception as e:
            return f"Error downloading {date_str}: {e}"

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(_download_image, img) for img in images]
        for future in as_completed(futures):
            result = future.result()
            if result:
                print(f"[PhenoCam] {result}")

    print("[PhenoCam] Completed")


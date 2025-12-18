from download_s2 import download_s2
from download_s3 import download_s3

year = 2024
site_position = (47.116171, 11.320308)
site_name = "innsbruck"

print(f"Downloading data for {site_name}, {year}")
download_s2(year, site_position, site_name)
download_s3(year, site_position, site_name)
print("All downloads completed")

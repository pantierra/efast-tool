from efast import run_efast, prepare_s2, prepare_s3

year = 2024
site_position = (47.116171, 11.320308)
site_name = "innsbruck"

# print(f"Downloading data for {site_name}, {year}")
# download_s2(year, site_position, site_name)
# download_s3(year, site_position, site_name)
# print("All downloads completed")

# print(f"Generating NDVI for {site_name}, {year}")
# generate_ndvi(year, site_position, site_name)
# create_ndvi_timeseries(year, site_position, site_name)
# print("All NDVI generation completed")

# print(f"Detecting clouds for {site_name}, {year}")
# detect_clouds(year, site_name)
# print("Cloud detection completed")

print(f"Preparing data for EFAST fusion for {site_name}, {year}")
prepare_s2(year, site_position, site_name)
prepare_s3(year, site_position, site_name)
print("Data preparation completed")

print(f"Running EFAST fusion for {site_name}, {year}")
run_efast(year, site_position, site_name)
print("EFAST fusion completed")

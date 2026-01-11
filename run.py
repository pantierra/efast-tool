from call_efast import run_efast, prepare_s2, prepare_s3
from post_process import process_cropped
from ndvi import (
    generate_ndvi_raw,
    create_ndvi_timeseries_raw,
    generate_ndvi_post_process,
    create_ndvi_timeseries_post_process,
)
from download_s2 import download_s2
from download_s3 import download_s3
from download_phenocam import download_phenocam
from clouds import detect_clouds


def run_pipeline(season, site_position, site_name):
    try:
        # print(f"Downloading data for {site_name}, {season}")
        # download_s2(season, site_position, site_name)
        # download_s3(season, site_position, site_name)
        # download_phenocam(season, site_position, site_name)

        # print(f"Generating NDVI for raw data: {site_name}, {season}")
        # generate_ndvi_raw(season, site_position, site_name)
        # create_ndvi_timeseries_raw(season, site_position, site_name)

        # print(f"Detecting clouds for {site_name}, {season}")
        # detect_clouds(season, site_name)

        # print(f"Preparing data for EFAST fusion for {site_name}, {season}")
        # prepare_s2(season, site_position, site_name)
        # prepare_s3(season, site_position, site_name)

        # print(f"Running EFAST fusion for {site_name}, {season}")
        # run_efast(season, site_position, site_name)

        # print(f"Post-processing data: {site_name}, {season}")
        process_cropped(season, site_position, site_name)
        # print(f"Generating NDVI for final outputs: {site_name}, {season}")
        generate_ndvi_post_process(season, site_position, site_name)
        create_ndvi_timeseries_post_process(season, site_position, site_name)

    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    run_pipeline(2024, (47.116171, 11.320308), "innsbruck")

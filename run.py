from call_efast import run_all_efast_scenarios
from post_process import process_all_scenarios
from generate_indexes import (
    generate_ndvi_raw,
    create_ndvi_timeseries_raw,
    generate_ndvi_post_process,
    create_ndvi_timeseries_post_process,
    generate_gcc_post_process,
    create_gcc_timeseries_post_process,
)
from download_s2 import download_s2
from download_s3 import download_s3
from download_phenocam import download_phenocam, download_phenocam_greenness
from clouds import detect_clouds


def run_pipeline(season, site_position, site_name):
    try:
        # print(f"Downloading data for {site_name}, {season}")
        # download_s2(season, site_position, site_name)
        # download_s3(season, site_position, site_name)
        # download_phenocam(season, site_position, site_name)
        #download_phenocam_greenness(season, site_position, site_name)

        # print(f"Generating NDVI for raw data: {site_name}, {season}")
        # create_ndvi_timeseries_raw(season, site_position, site_name)

        # print(f"Running EFAST fusion for all scenarios: {site_name}, {season}")
        # run_all_efast_scenarios(season, site_position, site_name)

        # print(f"Post-processing data: {site_name}, {season}")
        # process_all_scenarios(season, site_position, site_name)  # Already done
        # print(f"Generating NDVI for final outputs: {site_name}, {season}")
        # create_ndvi_timeseries_post_process(season, site_position, site_name)  # Already done
        print(f"Generating GCC for final outputs: {site_name}, {season}")
        # generate_gcc_post_process(season, site_position, site_name)  # No-op function
        create_gcc_timeseries_post_process(season, site_position, site_name)

    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    run_pipeline(2024, (47.116171, 11.320308), "innsbruck")

from fusion import run_all_efast_scenarios
from postprocessing import process_all_scenarios
from metrics_indices import (
    create_ndvi_timeseries_raw,
    create_ndvi_timeseries_post_process,
    create_gcc_timeseries_post_process,
    create_s2_bands_timeseries_post_process,
)
from acquisition_s2 import download_s2
from acquisition_s3 import download_s3
from acquisition_phenocam import download_phenocam, download_phenocam_greenness
from metrics_stats import calculate_all_metrics


def run_pipeline(season, site_position, site_name):
    """Run pipeline (downloads + EFAST fusion + post-process + metrics)."""
    try:
        # Download steps (needed for new site/season)
        #download_s2(season, site_position, site_name)
        #download_s3(season, site_position, site_name)
        #download_phenocam(season, site_position, site_name)
        #download_phenocam_greenness(season, site_position, site_name)

        #print(f"Generating NDVI for raw data: {site_name}, {season}")
        #create_ndvi_timeseries_raw(season, site_position, site_name)

        print(f"Running EFAST fusion for all scenarios: {site_name}, {season}")
        run_all_efast_scenarios(season, site_position, site_name)

        print(f"Post-processing data: {site_name}, {season}")
        process_all_scenarios(season, site_position, site_name)

        print(f"Generating NDVI for final outputs: {site_name}, {season}")
        create_ndvi_timeseries_post_process(season, site_position, site_name)

        print(f"Generating GCC for final outputs: {site_name}, {season}")
        create_gcc_timeseries_post_process(season, site_position, site_name)

        print(f"Generating S2 band timeseries: {site_name}, {season}")
        create_s2_bands_timeseries_post_process(season, site_position, site_name)

        print(f"Calculating metrics: {site_name}, {season}")
        calculate_all_metrics(season, site_name, site_position)

    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":

    run_pipeline(2024, (47.116171, 11.320308), "innsbruck")
    # forthgr - FORTH Heraklion Greece, Agriculture, 2024
    # sites.geojson: lon=25.0743, lat=35.3045
    #run_pipeline(2024, (35.3045, 25.0743), "forthgr")

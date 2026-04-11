from fusion import run_all_efast_scenarios, run_all_efast_itb_scenarios
from postprocessing import (
    post_process_all_scenarios,
    post_process_all_itb_scenarios,
    post_process_timeseries,
)
from acquisition_s2 import download_s2
from acquisition_s3 import download_s3
from acquisition_phenocam import download_phenocam
from preselection import create_timeseries
from preparation import (
    prepare_s2,
    prepare_s3,
    prepare_s2_gcc_for_itb,
    prepare_s3_gcc_for_itb,
)
from metrics_indices import create_prepared_fusion_timeseries
from metrics_stats import calculate_all_metrics


def run_pipeline(season, site_position, site_name):
    """Run pipeline."""
    try:
        # print(f"Downloading S2, S3, and PhenoCam: {site_name}, {season}")
        # download_s2(season, site_position, site_name)
        # download_s3(season, site_position, site_name)
        # download_phenocam(season, site_position, site_name)

        # print(f"Creating preselection timeseries: {site_name}, {season}")
        # create_timeseries(season, site_position, site_name)

        # print(f"Preparing S2 and S3 for fusion: {site_name}, {season}")
        # for strategy in ["aggressive", "nonaggressive"]:
        #     prepare_s2(season, site_position, site_name, cleaning_strategy=strategy)
        #     prepare_s3(season, site_position, site_name, cleaning_strategy=strategy)

        # print(f"Running EFAST fusion for all scenarios: {site_name}, {season}")
        # run_all_efast_scenarios(season, site_position, site_name)

        # Index-then-Blend (ItB): GCC stacks, EFAST fusion with product=GCC
        # for strategy in ["aggressive", "nonaggressive"]:
        #     prepare_s2_gcc_for_itb(season, site_position, site_name, cleaning_strategy=strategy)
        #     prepare_s3_gcc_for_itb(season, site_position, site_name, cleaning_strategy=strategy)
        # run_all_efast_itb_scenarios(season, site_position, site_name)
        # post_process_all_itb_scenarios(season, site_position, site_name)

        print(f"Creating prepared/fusion timeseries: {site_name}, {season}")
        create_prepared_fusion_timeseries(season, site_position, site_name)

        print(f"Post-processing: {site_name}, {season}")
        # post_process_all_scenarios(season, site_position, site_name)
        post_process_timeseries(season, site_position, site_name)

        print(f"Calculating metrics: {site_name}, {season}")
        # calculate_all_metrics(season, site_name, site_position)

    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    run_pipeline(2024, (47.116171, 11.320308), "innsbruck")
    run_pipeline(2024, (35.3045, 25.0743), "forthgr")
    run_pipeline(2020, (47.116171, 11.320308), "innsbruck")
    run_pipeline(2024, (58.5633, 24.3688), "pitsalu")
    run_pipeline(2023, (64.2437, 19.7673), "vindeln2")
    run_pipeline(2024, (36.7455, -6.0033), "sunflowerjerez1")
    run_pipeline(2024, (42.6558, 26.9837), "institutekarnobat")

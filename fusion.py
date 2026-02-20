"""EFAST fusion: S2/S3 reflectance fusion for four scenarios."""
from pathlib import Path
from datetime import datetime, timedelta

from preselection import create_timeseries
from preparation import (
    prepare_s2,
    prepare_s3,
    _get_base_dir,
    RESOLUTION_RATIO,
)


def _import_efast():
    """Lazy import of efast to avoid import errors when not using efast functions."""
    try:
        import efast
        return efast
    except ImportError:
        raise ImportError(
            "efast package not found. Install with: pip install git+https://github.com/DHI-GRAS/efast.git"
        )


def run_efast(season, site_position, site_name, cleaning_strategy="aggressive", sigma=None, date_range=None):
    lat, lon = site_position
    datetime_range = date_range or f"{season}-01-01/{season}-12-31"

    efast_base_dir = _get_base_dir(season, site_name, cleaning_strategy)
    s2_output_dir = efast_base_dir / "s2"
    s3_output_dir = efast_base_dir / "s3"
    fusion_output_dir = efast_base_dir / (f"fusion_sigma{sigma}" if sigma else "fusion")

    fusion_output_dir.mkdir(parents=True, exist_ok=True)
    print(f"[EFAST] Starting fusion: {site_name} ({lat:.6f}, {lon:.6f}), {season}")

    efast = _import_efast()

    start_str, end_str = datetime_range.split("/")
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        output_file = fusion_output_dir / f"REFL_{date_str}.tif"
        try:
            kwargs = {
                "product": "REFL",
                "max_days": 30,
                "date_position": 2,
                "minimum_acquisition_importance": 0.0,
                "ratio": RESOLUTION_RATIO,
            }
            if sigma is not None:
                kwargs["sigma"] = sigma
            efast.fusion(current_date, s3_output_dir, s2_output_dir, fusion_output_dir, **kwargs)
            print(
                f"[EFAST] Saved: {output_file}"
                if output_file.exists()
                else f"[EFAST] No output for {date_str} (insufficient nearby data)"
            )
        except Exception as e:
            print(f"[EFAST] Error processing {date_str}: {e}")
        current_date += timedelta(days=1)

    print("[EFAST] Completed")


def run_all_efast_scenarios(season, site_position, site_name, sigma_value=30, date_range=None):
    create_timeseries(season, site_position, site_name)
    for strategy in ["aggressive", "nonaggressive"]:
        prepare_s2(season, site_position, site_name, cleaning_strategy=strategy, date_range=date_range)
        prepare_s3(season, site_position, site_name, cleaning_strategy=strategy, date_range=date_range)
        run_efast(season, site_position, site_name, cleaning_strategy=strategy, sigma=None, date_range=date_range)
        run_efast(season, site_position, site_name, cleaning_strategy=strategy, sigma=sigma_value, date_range=date_range)

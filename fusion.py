"""EFAST fusion: S2/S3 reflectance fusion for four scenarios."""

from datetime import datetime, timedelta

from preparation import _get_base_dir, _get_itb_base_dir, RESOLUTION_RATIO


def _import_efast():
    """Lazy import of efast to avoid import errors when not using efast functions."""
    try:
        import efast

        return efast
    except ImportError:
        raise ImportError(
            "efast package not found. Install with: pip install git+https://github.com/DHI-GRAS/efast.git"
        )


def run_efast(
    season,
    site_position,
    site_name,
    cleaning_strategy="aggressive",
    sigma=None,
    date_range=None,
):
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
            efast.fusion(
                current_date, s3_output_dir, s2_output_dir, fusion_output_dir, **kwargs
            )
            print(
                f"[EFAST] Saved: {output_file}"
                if output_file.exists()
                else f"[EFAST] No output for {date_str} (insufficient nearby data)"
            )
        except Exception as e:
            print(f"[EFAST] Error processing {date_str}: {e}")
        current_date += timedelta(days=1)

    print("[EFAST] Completed")


def run_all_efast_scenarios(
    season, site_position, site_name, sigma_value=30, date_range=None
):
    """Run EFAST fusion for all 4 scenarios. Expects prepared_*/s2 and prepared_*/s3 to exist."""
    for strategy in ["aggressive", "nonaggressive"]:
        run_efast(
            season,
            site_position,
            site_name,
            cleaning_strategy=strategy,
            sigma=None,
            date_range=date_range,
        )
        run_efast(
            season,
            site_position,
            site_name,
            cleaning_strategy=strategy,
            sigma=sigma_value,
            date_range=date_range,
        )


def run_efast_itb(
    season,
    site_position,
    site_name,
    cleaning_strategy="aggressive",
    sigma=None,
    date_range=None,
):
    lat, lon = site_position
    datetime_range = date_range or f"{season}-01-01/{season}-12-31"
    efast_base_dir = _get_itb_base_dir(season, site_name, cleaning_strategy)
    s2_output_dir = efast_base_dir / "s2"
    s3_output_dir = efast_base_dir / "s3"
    fusion_output_dir = efast_base_dir / (f"fusion_sigma{sigma}" if sigma else "fusion")
    fusion_output_dir.mkdir(parents=True, exist_ok=True)
    print(f"[EFAST-ITB] Fusion GCC: {site_name} ({lat:.6f}, {lon:.6f}), {season}")
    efast = _import_efast()
    start_str, end_str = datetime_range.split("/")
    start_date = datetime.strptime(start_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_str, "%Y-%m-%d")
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")
        output_file = fusion_output_dir / f"GCC_{date_str}.tif"
        try:
            kwargs = {
                "product": "GCC",
                "max_days": 30,
                "date_position": 2,
                "minimum_acquisition_importance": 0.0,
                "ratio": RESOLUTION_RATIO,
            }
            if sigma is not None:
                kwargs["sigma"] = sigma
            efast.fusion(
                current_date, s3_output_dir, s2_output_dir, fusion_output_dir, **kwargs
            )
            print(
                f"[EFAST-ITB] Saved: {output_file}"
                if output_file.exists()
                else f"[EFAST-ITB] No output for {date_str}"
            )
        except Exception as e:
            print(f"[EFAST-ITB] Error {date_str}: {e}")
        current_date += timedelta(days=1)
    print("[EFAST-ITB] Completed")


def run_all_efast_itb_scenarios(
    season, site_position, site_name, sigma_value=30, date_range=None
):
    for strategy in ["aggressive", "nonaggressive"]:
        run_efast_itb(
            season,
            site_position,
            site_name,
            cleaning_strategy=strategy,
            sigma=None,
            date_range=date_range,
        )
        run_efast_itb(
            season,
            site_position,
            site_name,
            cleaning_strategy=strategy,
            sigma=sigma_value,
            date_range=date_range,
        )

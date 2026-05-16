"""EFAST with symlinked S2 dir (withhold one acquisition); outputs under validation/."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from fusion import run_efast, run_efast_itb
from preparation import _get_base_dir, _get_itb_base_dir

from gap_validation.s2_mask_dir import build_masked_s2_dir_bti, build_masked_s2_dir_itb


def prepared_s3_dir(season: int, site_name: str, strategy: str) -> Path:
    return _get_base_dir(season, site_name, strategy) / "s3"


def validation_fusion_dir(
    site_name: str,
    season: int,
    gap_days: int,
    strategy: str,
    sigma: int | None,
    mode: str,
) -> Path:
    """``data/.../validation/fusion/gap_{n}/{strategy}_sigma{20|30}_{bti|itb}/``."""
    sig = 30 if sigma == 30 else 20
    return (
        Path(f"data/{site_name}/{season}/validation")
        / "fusion"
        / f"gap_{gap_days}"
        / f"{strategy}_sigma{sig}_{mode}"
    )


def run_masked_fusion_one_date(
    season: int,
    site_position: tuple[float, float],
    site_name: str,
    strategy: str,
    sigma: int | None,
    mode: str,
    prediction_date_iso: str,
    withheld_yyyymmdd: str,
    fusion_output_dir: Path,
) -> Path:
    """Build temp masked S2 dir, run EFAST for ``prediction_date_iso`` only; return output dir."""
    fusion_output_dir.mkdir(parents=True, exist_ok=True)
    date_range = f"{prediction_date_iso[:10]}/{prediction_date_iso[:10]}"
    s3_dir = prepared_s3_dir(season, site_name, strategy)

    with TemporaryDirectory(prefix="gapval_s2_") as tmp:
        tmp_s2 = Path(tmp) / "s2"
        if mode == "bti":
            prep_s2 = _get_base_dir(season, site_name, strategy) / "s2"
            build_masked_s2_dir_bti(prep_s2, withheld_yyyymmdd, tmp_s2)
            run_efast(
                season,
                site_position,
                site_name,
                cleaning_strategy=strategy,
                sigma=sigma,
                date_range=date_range,
                s2_output_dir=tmp_s2,
                s3_output_dir=s3_dir,
                fusion_output_dir=fusion_output_dir,
            )
        elif mode == "itb":
            prep_s2 = _get_itb_base_dir(season, site_name, strategy) / "s2"
            s3_itb = _get_itb_base_dir(season, site_name, strategy) / "s3"
            build_masked_s2_dir_itb(prep_s2, withheld_yyyymmdd, tmp_s2)
            run_efast_itb(
                season,
                site_position,
                site_name,
                cleaning_strategy=strategy,
                sigma=sigma,
                date_range=date_range,
                s2_output_dir=tmp_s2,
                s3_output_dir=s3_itb,
                fusion_output_dir=fusion_output_dir,
            )
        else:
            raise ValueError(f"mode must be bti or itb, got {mode!r}")

    return fusion_output_dir


def production_fusion_path(
    season: int,
    site_name: str,
    strategy: str,
    sigma: int | None,
    mode: str,
    yyyymmdd: str,
) -> Path:
    """Single-date fused raster from the normal prepared tree (no-gap baseline)."""
    if mode == "bti":
        base = _get_base_dir(season, site_name, strategy)
        sub = f"fusion_sigma{sigma}" if sigma else "fusion"
        return base / sub / f"REFL_{yyyymmdd}.tif"
    base = _get_itb_base_dir(season, site_name, strategy)
    sub = f"fusion_sigma{sigma}" if sigma else "fusion"
    return base / sub / f"GCC_{yyyymmdd}.tif"


def withheld_s2_refl_path(
    season: int, site_name: str, strategy: str, withheld_filename: str | None
) -> Path | None:
    if not withheld_filename:
        return None
    p = _get_base_dir(season, site_name, strategy) / "s2" / withheld_filename
    return p if p.is_file() else None

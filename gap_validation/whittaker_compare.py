"""Whittaker S2 GCC (λ=400 d²) as a spatial constant vs withheld S2 GCC; crossover vs fusion nse_s2."""

from __future__ import annotations

from pathlib import Path

from metrics_stats import (
    WHITTAKER_LAMBDA_DAYS_SQ,
    _norm_date_key,
    _s2_gcc_series_from_preselection,
    _whittaker_smooth_dict,
)


def whittaker_gcc_on_gap_masked_series(
    base: Path,
    strategy: str,
    prediction_iso: str,
    withheld_iso: str,
    lam: float = WHITTAKER_LAMBDA_DAYS_SQ,
) -> float | None:
    """Whittaker smooth on cloud-screened S2 GCC **excluding** the withheld acquisition day.

    Comparator aligned with ``baseline.s2_whittaker_lambda400`` in ``metrics_stats`` (same λ,
    same preselection GCC), but the withheld date is removed so the smoother does not see
    the target acquisition. Value at ``prediction_iso`` (YYYY-MM-DD) is returned.
    """
    pred_k = _norm_date_key(prediction_iso)
    wh_k = _norm_date_key(withheld_iso)
    if not pred_k or not wh_k:
        return None
    all_gcc, flags = _s2_gcc_series_from_preselection(base)
    if not all_gcc:
        return None
    idx = 0 if strategy == "aggressive" else 1
    kept = sorted(
        (d, g)
        for d, g in all_gcc.items()
        if d in flags and not flags[d][idx] and _norm_date_key(d) != wh_k
    )
    if len(kept) < 2:
        return None
    obs_d, obs_v = zip(*kept)
    smooth = _whittaker_smooth_dict(obs_d, obs_v, lam)
    return smooth.get(pred_k)


def first_gap_where_fusion_below_whittaker(
    rows: list[dict],
    *,
    fusion_key: str = "nse_s2",
    whittaker_key: str = "nse_s2",
) -> int | None:
    """Smallest ``gap_days`` where fusion[metric] < whittaker[metric] (strict)."""
    eligible = [
        r
        for r in rows
        if r.get(fusion_key) is not None and r.get(whittaker_key) is not None
    ]
    eligible.sort(key=lambda r: r["gap_days"])
    for r in eligible:
        if r[fusion_key] < r[whittaker_key]:
            return int(r["gap_days"])
    return None

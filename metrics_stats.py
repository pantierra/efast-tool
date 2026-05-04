"""Metrics and statistics: temporal metrics and PhenoCam stats."""

import json
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from scipy import sparse
from scipy.sparse.linalg import spsolve
from scipy.stats import pearsonr

WHITTAKER_LAMBDA_DAYS_SQ = 400.0


def _norm_date_key(s):
    if s is None:
        return None
    t = str(s).strip()
    return t.split("T")[0][:10] if "T" in t else t[:10]


def load_timeseries(filepath):
    """Load JSON timeseries and return dict mapping date -> value."""
    if not Path(filepath).exists():
        return {}
    with open(filepath) as f:
        data = json.load(f)
    return {item["date"]: item.get("greenness_index") for item in data}


def match_dates(fusion_ts, phenocam_ts):
    """Match dates between timeseries, return aligned numpy arrays (filter None values)."""

    def _bundle(m):
        out = {}
        for k, v in m.items():
            nk = _norm_date_key(k)
            if nk and nk not in out:
                out[nk] = v
        return out

    fa, pa = _bundle(fusion_ts), _bundle(phenocam_ts)
    common_dates = set(fa) & set(pa)
    fusion_vals = []
    phenocam_vals = []
    dates = []

    for date in sorted(common_dates):
        fusion_val = fa[date]
        phenocam_val = pa[date]
        if fusion_val is not None and phenocam_val is not None:
            fusion_vals.append(fusion_val)
            phenocam_vals.append(phenocam_val)
            dates.append(date)

    return np.array(fusion_vals), np.array(phenocam_vals), dates


def pearson_correlation(y_true, y_pred):
    """Calculate Pearson correlation coefficient r."""
    if len(y_true) < 2 or np.std(y_true) == 0 or np.std(y_pred) == 0:
        return None
    r, _ = pearsonr(y_true, y_pred)
    return float(r)


def r_squared(y_true, y_pred):
    """Calculate coefficient of determination R²."""
    if len(y_true) < 2 or np.std(y_true) == 0:
        return None
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
    if ss_tot == 0:
        return None
    return float(1 - (ss_res / ss_tot))


def rmse(y_true, y_pred):
    """Calculate Root Mean Square Error."""
    if len(y_true) == 0:
        return None
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def mae(y_true, y_pred):
    """Calculate Mean Absolute Error."""
    if len(y_true) == 0:
        return None
    return float(np.mean(np.abs(y_true - y_pred)))


def nrmse(y_true, y_pred):
    """Calculate normalized RMSE (RMSE / mean(y_true))."""
    if len(y_true) == 0:
        return None
    mean_val = np.mean(y_true)
    if mean_val == 0:
        return None
    rmse_val = rmse(y_true, y_pred)
    return float(rmse_val / mean_val) if rmse_val is not None else None


def nse(y_true, y_pred):
    """Calculate Nash-Sutcliffe Efficiency."""
    if len(y_true) < 2:
        return None
    numerator = np.sum((y_true - y_pred) ** 2)
    denominator = np.sum((y_true - np.mean(y_true)) ** 2)
    if denominator == 0:
        return None
    return float(1 - (numerator / denominator))


def residual_vs_phenocam(fusion_ts, phenocam_ts):
    """Stats of (fused_GCC − PhenoCam_GCC) on matched dates; None if too few points.

    Mean: positive → fusion systematically above PhenoCam; negative → below; ~0 → unbiased mean.
    Compare BtI vs ItB means at same strategy/σ (``derived.bti_vs_itb_mean_residual``): closer to 0 → less mean bias vs PhenoCam.
    """
    yf, yp, _dates = match_dates(fusion_ts, phenocam_ts)
    if len(yf) < 2:
        return None
    r = yf - yp
    return {
        "mean": float(np.mean(r)),
        "std": float(np.std(r)),
        "mae": float(np.mean(np.abs(r))),
        "rmse": float(np.sqrt(np.mean(r**2))),
        "n_samples": int(len(r)),
    }


def calculate_temporal_metrics(fusion_ts, phenocam_ts):
    """Temporal metrics vs PhenoCam (nse_pc; nse is the same value)."""
    fusion_vals, phenocam_vals, dates = match_dates(fusion_ts, phenocam_ts)

    if len(fusion_vals) < 2:
        return None

    n_pc = nse(phenocam_vals, fusion_vals)
    metrics = {
        "pearson_r": pearson_correlation(phenocam_vals, fusion_vals),
        "r_squared": r_squared(phenocam_vals, fusion_vals),
        "rmse": rmse(phenocam_vals, fusion_vals),
        "mae": mae(phenocam_vals, fusion_vals),
        "nrmse": nrmse(phenocam_vals, fusion_vals),
        "nse_pc": n_pc,
        "nse": n_pc,
        "n_samples": len(fusion_vals),
        "date_range": {"start": dates[0], "end": dates[-1]} if dates else None,
    }
    rv = residual_vs_phenocam(fusion_ts, phenocam_ts)
    if rv:
        metrics["residual_vs_phenocam"] = rv
    return metrics


def derived_tier1(temporal: dict) -> dict:
    """ΔNSE_PC (σ20 − σ30) and paired BtI vs ItB mean residual; needs temporal fusion keys.

    ΔNSE_PC > 0 → NSE_PC higher at σ=20 than σ=30 (tighter EFAST temporal kernel wins).
    ΔNSE_PC < 0 → σ=30 wins (broader smoothing matches PhenoCam better).
    """
    d_nse = {"bti": {}, "itb": {}}
    for strategy in ("aggressive", "nonaggressive"):
        for mode, suf in (("bti", ""), ("itb", "_itb")):
            k20 = f"{strategy}_sigma20{suf}"
            k30 = f"{strategy}_sigma30{suf}"
            n20 = (temporal.get(k20) or {}).get("nse_pc")
            n30 = (temporal.get(k30) or {}).get("nse_pc")
            if isinstance(n20, (int, float)) and isinstance(n30, (int, float)):
                d_nse[mode][strategy] = float(n20 - n30)
            else:
                d_nse[mode][strategy] = None

    paired = []
    for strategy in ("aggressive", "nonaggressive"):
        for sig in (20, 30):
            kb, ki = f"{strategy}_sigma{sig}", f"{strategy}_sigma{sig}_itb"
            mb = (temporal.get(kb) or {}).get("residual_vs_phenocam", {}).get("mean")
            mi = (temporal.get(ki) or {}).get("residual_vs_phenocam", {}).get("mean")
            paired.append(
                {
                    "strategy": strategy,
                    "sigma": sig,
                    "mean_residual_bti": float(mb)
                    if isinstance(mb, (int, float))
                    else None,
                    "mean_residual_itb": float(mi)
                    if isinstance(mi, (int, float))
                    else None,
                }
            )
    return {
        "delta_nse_pc_sigma20_minus_sigma30": d_nse,
        "bti_vs_itb_mean_residual": paired,
    }


def calculate_phenocam_stats(phenocam_ts):
    """Calculate phenocam summary statistics."""
    values = [v for v in phenocam_ts.values() if v is not None]
    if len(values) == 0:
        return None

    vals = np.array(values)
    return {
        "mean": float(np.mean(vals)),
        "std": float(np.std(vals)),
        "min": float(np.min(vals)),
        "max": float(np.max(vals)),
        "n_samples": len(vals),
    }


def _s2_gcc_series_from_preselection(base: Path):
    """Build the raw S2 GCC series from s2_preselection.json.

    Uses the 3x3 site-window band means stored per raw S2 acquisition and
    computes GCC = b03 / (b02 + b03 + b04). Scale cancels, so DN vs
    reflectance is irrelevant. Returns (all_gcc, flags) where all_gcc maps
    YYYY-MM-DD -> gcc for every row with a positive band sum, and flags maps
    the same date key -> (excluded_aggressive, excluded_nonaggressive).
    """
    path = base / "raw" / "preselection" / "s2_preselection.json"
    if not path.exists():
        return {}, {}
    with open(path) as f:
        rows = json.load(f)
    all_gcc: dict = {}
    flags: dict = {}
    for e in rows:
        nk = _norm_date_key(e.get("date"))
        if not nk:
            continue
        try:
            b02 = float(e.get("b02"))
            b03 = float(e.get("b03"))
            b04 = float(e.get("b04"))
        except (TypeError, ValueError):
            continue
        total = b02 + b03 + b04
        if not np.isfinite(total) or total <= 0:
            continue
        gcc = b03 / total
        if not np.isfinite(gcc):
            continue
        if nk in all_gcc:
            continue
        all_gcc[nk] = float(gcc)
        flags[nk] = (
            bool(e.get("excluded_aggressive")),
            bool(e.get("excluded_nonaggressive")),
        )
    return all_gcc, flags


def _whittaker_smooth_dict(obs_dates, obs_values, lam: float, n_min: int = 3):
    """Daily Whittaker (weights 1 at obs); returns {YYYY-MM-DD: z}."""
    pairs = [
        (_norm_date_key(d), float(v))
        for d, v in zip(obs_dates, obs_values)
        if v is not None and _norm_date_key(d)
    ]
    if len(pairs) < 2:
        return {}
    days = sorted({p[0] for p in pairs})
    t0 = datetime.strptime(days[0], "%Y-%m-%d").date()
    t1 = datetime.strptime(days[-1], "%Y-%m-%d").date()
    n = (t1 - t0).days + 1
    if n < n_min:
        return {}

    w = np.zeros(n)
    y = np.zeros(n)
    for dk, val in pairs:
        i = (datetime.strptime(dk, "%Y-%m-%d").date() - t0).days
        if 0 <= i < n:
            w[i] = 1.0
            y[i] = val

    D = sparse.diags(
        [1.0, -2.0, 1.0], [0, 1, 2], shape=(n - 2, n), format="csc", dtype=np.float64
    )
    H = D.T @ D
    Wm = sparse.diags(w.astype(np.float64), format="csc")
    z = spsolve(Wm + lam * H, w * y)
    out = {}
    for i in range(n):
        out[(t0 + timedelta(days=i)).isoformat()] = float(z[i])
    return out


def calculate_all_metrics(season, site_name, site_position):
    """Calculate metrics for all 4 scenarios and save to JSON."""
    del site_position
    results = {"temporal": {}}
    base = Path(f"data/{site_name}/{season}")

    # Load phenocam timeseries once (same for all scenarios)
    phenocam_ts_path = base / "raw" / "phenocam" / "phenocam_gcc.json"
    phenocam_ts = load_timeseries(phenocam_ts_path)

    if not phenocam_ts:
        print("[METRICS] Warning: No phenocam data found")
        return results

    # Calculate phenocam stats
    phenocam_stats = calculate_phenocam_stats(phenocam_ts)
    if phenocam_stats:
        results["phenocam_stats"] = phenocam_stats

    baseline = {}
    all_gcc, flags = _s2_gcc_series_from_preselection(base)
    if all_gcc:
        m0 = calculate_temporal_metrics(all_gcc, phenocam_ts)
        if m0:
            baseline["s2"] = m0
        for strategy, flag_idx in (("aggressive", 0), ("nonaggressive", 1)):
            kept_items = sorted(
                (
                    (d, g)
                    for d, g in all_gcc.items()
                    if d in flags and not flags[d][flag_idx]
                ),
                key=lambda x: x[0],
            )
            if not kept_items:
                continue
            kept_ts = dict(kept_items)
            mcf = calculate_temporal_metrics(kept_ts, phenocam_ts)
            if mcf:
                baseline.setdefault("s2_cloudfree", {})[strategy] = mcf
            obs_d, obs_v = zip(*kept_items)
            smooth = _whittaker_smooth_dict(obs_d, obs_v, WHITTAKER_LAMBDA_DAYS_SQ)
            if smooth:
                mw = calculate_temporal_metrics(smooth, phenocam_ts)
                if mw:
                    baseline.setdefault("s2_whittaker_lambda400", {})[strategy] = mw

    for strategy in ("aggressive", "nonaggressive"):
        p = base / f"processed_{strategy}_sigma20" / "gcc" / "s3" / "timeseries.json"
        if not p.exists():
            continue
        s3_ts = load_timeseries(p)
        if s3_ts:
            m3 = calculate_temporal_metrics(s3_ts, phenocam_ts)
            if m3:
                baseline.setdefault("s3", {})[strategy] = m3

    if baseline:
        results["baseline"] = baseline

    # Calculate fusion metrics for each scenario
    for strategy in ["aggressive", "nonaggressive"]:
        for sigma in [20, 30]:
            scenario_name = f"{strategy}_sigma{sigma}"
            print(f"[METRICS] Calculating metrics for {scenario_name}...")

            processed_dir = f"processed_{strategy}_sigma{sigma}"

            # Load fusion timeseries
            fusion_ts_path = base / processed_dir / "gcc" / "fusion" / "timeseries.json"
            fusion_ts = load_timeseries(fusion_ts_path)

            if not fusion_ts:
                print(
                    f"[METRICS] Warning: Missing fusion data for {scenario_name}, skipping"
                )
                continue

            temporal_metrics = calculate_temporal_metrics(fusion_ts, phenocam_ts)
            if temporal_metrics:
                results["temporal"][scenario_name] = temporal_metrics

    for strategy in ["aggressive", "nonaggressive"]:
        for sigma in [20, 30]:
            scenario_name = f"{strategy}_sigma{sigma}_itb"
            processed_dir = f"processed_{strategy}_itb_sigma{sigma}"
            fusion_ts_path = base / processed_dir / "gcc" / "fusion" / "timeseries.json"
            fusion_ts = load_timeseries(fusion_ts_path)
            if not fusion_ts:
                print(
                    f"[METRICS] Warning: Missing ItB fusion data for {scenario_name}, skipping"
                )
                continue
            temporal_metrics = calculate_temporal_metrics(fusion_ts, phenocam_ts)
            if temporal_metrics:
                results["temporal"][scenario_name] = temporal_metrics

    if results["temporal"]:
        results["derived"] = derived_tier1(results["temporal"])

    # Save results
    output_path = Path(f"data/{site_name}/{season}/metrics.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"[METRICS] Saved results to {output_path}")

    return results


def main():
    """Standalone script entry point."""
    import sys

    if len(sys.argv) < 4:
        print("Usage: metrics_stats.py <season> <site_name> <lat> <lon>")
        print("Example: metrics_stats.py 2024 innsbruck 47.116171 11.320308")
        sys.exit(1)

    season = int(sys.argv[1])
    site_name = sys.argv[2]
    site_position = (float(sys.argv[3]), float(sys.argv[4]))

    results = calculate_all_metrics(season, site_name, site_position)

    # Save results
    output_path = Path(f"data/{site_name}/{season}/metrics.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"[METRICS] Saved results to {output_path}")


if __name__ == "__main__":
    main()

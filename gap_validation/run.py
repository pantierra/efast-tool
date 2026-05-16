"""Tier-2 gap validation CLI: manifest, masked EFAST, spatial ``nse_s2``, Whittaker crossover."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from gap_validation.calendar import load_manifest, validation_dir, write_manifest
from gap_validation.fusion_masked import (
    production_fusion_path,
    run_masked_fusion_one_date,
    validation_fusion_dir,
    withheld_s2_refl_path,
)
from gap_validation.spatial_metrics import evaluate_gap_vs_withheld
from gap_validation.whittaker_compare import first_gap_where_fusion_below_whittaker


def _ymd_from_iso(iso_d: str) -> str:
    return datetime.strptime(iso_d[:10], "%Y-%m-%d").strftime("%Y%m%d")


def _yyyymmdd_from_withheld_filename(fn: str) -> str | None:
    for part in fn.replace(".tif", "").split("_"):
        if len(part) == 8 and part.isdigit():
            return part
    return None


def _withheld_iso(entry: dict) -> str | None:
    d = entry.get("withheld_s2_date")
    if isinstance(d, str) and len(d) >= 10:
        return d[:10]
    fn = entry.get("withheld_s2_filename")
    if not fn or not isinstance(fn, str):
        return None
    ymd = _yyyymmdd_from_withheld_filename(fn)
    if not ymd:
        return None
    return datetime.strptime(ymd, "%Y%m%d").date().isoformat()


def _fused_file(fusion_dir: Path, mode: str, ymd: str) -> Path:
    stem = "REFL" if mode == "bti" else "GCC"
    return fusion_dir / f"{stem}_{ymd}.tif"


def _scenario_key(strategy: str, sigma: int | None, mode: str) -> str:
    sig = 30 if sigma == 30 else 20
    return f"{strategy}_sigma{sig}_{mode}"


def _git_rev() -> str | None:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parent.parent,
            text=True,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return None


def run_validation(
    site_name: str,
    season: int,
    site_position: tuple[float, float],
    strategy: str,
    sigma: int | None,
    mode: str,
    *,
    skip_manifest: bool,
    skip_fusion: bool,
    write_manifest_only: bool,
    gap_days_filter: list[int] | None,
    s2_calendar_strategy: str,
) -> Path:
    base = Path(f"data/{site_name}/{season}")
    vdir = validation_dir(site_name, season)
    vdir.mkdir(parents=True, exist_ok=True)

    if not skip_manifest:
        write_manifest(
            site_name, season, site_position, s2_calendar_strategy=s2_calendar_strategy
        )
    if write_manifest_only:
        return vdir / "gap_manifest.json"

    manifest = load_manifest(site_name, season)
    entries = manifest["entries"]
    if gap_days_filter:
        entries = [e for e in entries if e.get("gap_days") in gap_days_filter]

    results: list[dict] = []
    for entry in entries:
        gap_days = entry["gap_days"]
        pred = entry["prediction_date"]
        fn = entry.get("withheld_s2_filename")
        if not fn:
            results.append(
                {
                    "gap_days": gap_days,
                    "error": "no_withheld_s2_filename",
                    "entry": entry,
                }
            )
            continue
        ymd = _ymd_from_iso(pred)
        wh_ymd = _yyyymmdd_from_withheld_filename(fn)
        if not wh_ymd:
            results.append(
                {
                    "gap_days": gap_days,
                    "error": "could_not_parse_withheld_yyyymmdd",
                    "withheld_s2_filename": fn,
                }
            )
            continue
        withheld_iso = (
            _withheld_iso(entry) or f"{wh_ymd[:4]}-{wh_ymd[4:6]}-{wh_ymd[6:8]}"
        )

        fusion_out = validation_fusion_dir(
            site_name, season, gap_days, strategy, sigma, mode
        )
        if not skip_fusion:
            run_masked_fusion_one_date(
                season,
                site_position,
                site_name,
                strategy,
                sigma,
                mode,
                pred,
                wh_ymd,
                fusion_out,
            )

        fused_gap = _fused_file(fusion_out, mode, ymd)
        prod = production_fusion_path(season, site_name, strategy, sigma, mode, ymd)
        wh_path = withheld_s2_refl_path(season, site_name, strategy, fn)
        if wh_path is None or not fused_gap.is_file():
            results.append(
                {
                    "gap_days": gap_days,
                    "prediction_date": pred,
                    "withheld_s2_filename": fn,
                    "scenario": {
                        "strategy": strategy,
                        "sigma": 30 if sigma == 30 else 20,
                        "mode": mode,
                    },
                    "error": "missing_withheld_refl_or_fused_gap",
                    "fused_gap_path": str(fused_gap),
                }
            )
            continue

        spatial = evaluate_gap_vs_withheld(
            wh_path,
            fused_gap,
            prod if prod.is_file() else None,
            mode,
            whittaker_context=(base, strategy, pred, withheld_iso),
        )
        fusion_nse = (spatial.get("gap") or {}).get("nse_s2")
        wh_nse = (spatial.get("whittaker") or {}).get("nse_s2")
        results.append(
            {
                "gap_days": gap_days,
                "prediction_date": pred,
                "withheld_s2_filename": fn,
                "scenario": {
                    "strategy": strategy,
                    "sigma": 30 if sigma == 30 else 20,
                    "mode": mode,
                },
                "paths": {
                    "fused_gap": str(fused_gap),
                    "fused_no_gap": str(prod) if prod.is_file() else None,
                    "withheld_s2_refl": str(wh_path),
                },
                "spatial": spatial,
                "whittaker_crossover_row": {
                    "gap_days": gap_days,
                    "nse_s2_fusion": fusion_nse,
                    "nse_s2_whittaker": wh_nse,
                },
            }
        )

    scenario = _scenario_key(strategy, sigma, mode)
    crossover_rows = [
        r["whittaker_crossover_row"]
        for r in results
        if isinstance(r.get("whittaker_crossover_row"), dict)
    ]
    summary = {
        "site_name": site_name,
        "season": season,
        "scenario": scenario,
        "command_line": sys.argv,
        "git_commit": _git_rev(),
        "manifest": str(vdir / "gap_manifest.json"),
        "results": results,
        "whittaker_crossover": {
            scenario: {
                "metric": "nse_s2_spatial_vs_withheld_s2_gcc",
                "whittaker_definition": (
                    "Whittaker λ=400 d² on cloud-screened S2 GCC from s2_preselection.json; "
                    "withheld acquisition removed from the fit; prediction is a spatially constant "
                    "field at the smoothed GCC(prediction_date), compared to withheld S2 GCC on the "
                    "same valid mask as fusion (aligned with baseline.s2_whittaker_lambda400 spirit)."
                ),
                "first_gap_days_fusion_nse_below_whittaker": first_gap_where_fusion_below_whittaker(
                    crossover_rows,
                    fusion_key="nse_s2_fusion",
                    whittaker_key="nse_s2_whittaker",
                ),
                "by_gap": crossover_rows,
            }
        },
    }
    out_path = vdir / "gap_validation_summary.json"
    out_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return out_path


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Tier-2 withheld-S2 gap validation (outputs under data/.../validation/)."
    )
    ap.add_argument("--site", required=True)
    ap.add_argument("--season", type=int, required=True)
    ap.add_argument("--lat", type=float, required=True)
    ap.add_argument("--lon", type=float, required=True)
    ap.add_argument(
        "--strategy", default="aggressive", choices=["aggressive", "nonaggressive"]
    )
    ap.add_argument("--sigma", type=int, default=20, choices=[20, 30])
    ap.add_argument("--mode", default="bti", choices=["bti", "itb"])
    ap.add_argument(
        "--gap-days",
        type=int,
        action="append",
        metavar="N",
        help="Restrict to gap length(s); repeatable (default: all manifest lengths).",
    )
    ap.add_argument("--skip-manifest", action="store_true")
    ap.add_argument(
        "--skip-fusion",
        action="store_true",
        help="Reuse existing validation fusion rasters.",
    )
    ap.add_argument(
        "--write-manifest-only",
        action="store_true",
        help="Write gap_manifest.json and exit (no EFAST).",
    )
    ap.add_argument(
        "--s2-calendar-strategy",
        default="aggressive",
        choices=["aggressive", "nonaggressive"],
        help="Which prepared_*/s2 tree is used to pick nearest S2 for withholding.",
    )
    args = ap.parse_args()
    sigma_kw = 30 if args.sigma == 30 else None
    site_position = (args.lat, args.lon)
    out = run_validation(
        args.site,
        args.season,
        site_position,
        args.strategy,
        sigma_kw,
        args.mode,
        skip_manifest=args.skip_manifest,
        skip_fusion=args.skip_fusion,
        write_manifest_only=args.write_manifest_only,
        gap_days_filter=args.gap_days,
        s2_calendar_strategy=args.s2_calendar_strategy,
    )
    print(out)


if __name__ == "__main__":
    main()

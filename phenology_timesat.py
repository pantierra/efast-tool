"""
PhenoCam GCC: green-up and green-down (50 % of seasonal amplitude) via TIMESAT.

Reads ``data/.../raw/phenocam/phenocam_gcc.json`` (or any path) and uses the
``timesat`` package (``timesat.tsfprocess``) with the same seasonal-threshold
meaning as the TIMESAT GUI: *startmethod* 1, *p_startcutoff* (0.5, 0.5) = 50 % of
the **per-season** amplitude above the local base. See the TIMESAT manual,
section 4.3 and row 37–38 (season start method = seasonal amplitude).

**License:** the ``timesat`` PyPI wheel is under the TIMESAT Research License
(non-commercial research; see package metadata on PyPI).

PhenoCam time series: single-year acquisition writes
  ``phenocam_gcc.json`` (and ``phenocam_gcc.csv``). The three-year series used
  for TIMESAT is stored separately as ``phenocam_gcc_3y.json`` in the same
  folder (created on first use from the one-day summary API, then reused).

Importable: ``write_phenocam_phenology_for_site`` is called from ``run.py``;
the CLI entry point remains optional for ad-hoc runs.

**Saving results:** use ``-o path.json`` or ``--sidecar`` to write a JSON file
(see ``--help``). Sidecar mode writes ``phenocam_phenology.json`` (two dates
only) next to ``phenocam_gcc.json``.

``run_pipeline`` in ``run.py`` writes the same ``phenocam_phenology.json`` by
default when ``timesat`` is installed. GCC for TIMESAT uses ``phenocam_gcc_3y.json``
if present, otherwise the PhenoCam API for that site (listed in
``data/sites.geojson``; not a site list from the API). One-year
``phenocam_gcc.json`` on disk can still fill gaps when merged.

Use ``python phenology_timesat.py --all`` to batch every
``(sitename, season)`` from ``data/sites.geojson`` (``properties.sitename`` and
``properties.seasons``).
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import requests

PHENOCAM_API = "https://phenocam.nau.edu/api"

try:
    import timesat as _timesat
except ImportError:
    _timesat = None

NODATA = -9999.0


def load_phenocam_gcc(path: Path) -> dict[str, float]:
    """Return map YYYY-MM-DD -> greenness index from PhenoCam JSON list."""
    with open(path) as f:
        rows = json.load(f)
    out: dict[str, float] = {}
    for row in rows:
        d = str(row.get("date", ""))[:10]
        v = row.get("greenness_index")
        if d and v is not None and np.isfinite(v):
            out[d] = float(v)
    return out


def _gcc_from_summary_row(row: dict, use_mean_fallback: bool) -> float | None:
    """Extract daily GCC from a one-day summary row (same rules as acquisition)."""
    if not use_mean_fallback:
        oflag = row.get("outlierflag_gcc_90")
        if oflag is not None and str(oflag).strip() in ("1", "1.0"):
            return None

    raw = row.get("gcc_mean" if use_mean_fallback else "gcc_90")
    if raw is None:
        return None
    text = str(raw).strip()
    if not text or text.upper() == "NA":
        return None
    try:
        val = float(text)
    except ValueError:
        return None
    if val <= -9998.0:
        return None
    return val


def _phenocam_one_day_summary_csv_url(site_name: str) -> str | None:
    """Return URL of the one-day summary CSV for *site_name*, or None on failure."""
    try:
        url = f"{PHENOCAM_API}/roilists/"
        params: dict | None = {"site": site_name}
        rois: list[dict] = []
        while url:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            rois.extend(
                [roi for roi in data.get("results", []) if roi["site"] == site_name]
            )
            url = data.get("next")
            params = None
            if rois:
                break
        if not rois:
            return None
        return rois[0].get("one_day_summary") or None
    except requests.RequestException:
        return None


def _parse_phenocam_gcc_from_csv_text(
    text: str, start_date: str, end_date: str
) -> dict[str, float]:
    """Map YYYY-MM-DD -> gcc for rows in [start_date, end_date] inclusive."""
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    lines = [line for line in text.split("\n") if line and not line.startswith("#")]
    reader = csv.DictReader(lines)
    fieldnames = reader.fieldnames or ()
    use_mean_fallback = "gcc_90" not in fieldnames
    out: dict[str, float] = {}
    for row in reader:
        try:
            date_str = row.get("date")
            if not date_str:
                continue
            date = datetime.strptime(date_str, "%Y-%m-%d")
            if not (start_dt <= date <= end_dt):
                continue
            gcc = _gcc_from_summary_row(row, use_mean_fallback)
            if gcc is not None:
                out[date.date().isoformat()] = gcc
        except (ValueError, KeyError):
            continue
    return out


def save_phenocam_gcc_json(path: Path, by_date: dict[str, float]) -> None:
    """Write the same list-of-objects format as :func:`acquisition_phenocam` GCC JSON."""
    rows = [
        {"date": d, "greenness_index": v}
        for d, v in sorted(by_date.items(), key=lambda x: x[0])
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)
        f.write("\n")


def fetch_phenocam_gcc_three_years_separately(
    site_name: str, season: int
) -> dict[str, float]:
    """
    Download PhenoCam one-day summary GCC for three **calendar** years
    (``season-1`` … ``season+1``), independently of :mod:`acquisition_phenocam`.

    Uses one HTTP GET of the full summary CSV, then **three** per-year
    extractions (same logic as the acquisition CSV filter, three date windows).
    """
    out: dict[str, float] = {}
    csv_url = _phenocam_one_day_summary_csv_url(site_name)
    if not csv_url:
        print(
            f"[PhenoCam phenology] No PhenoCam one-day summary URL for site {site_name!r}"
        )
        return out
    try:
        csv_r = requests.get(csv_url, timeout=30)
        csv_r.raise_for_status()
    except requests.RequestException as e:
        print(f"[PhenoCam phenology] API CSV fetch failed: {e}")
        return out
    text = csv_r.text
    for y in (season - 1, season, season + 1):
        part = _parse_phenocam_gcc_from_csv_text(text, f"{y}-01-01", f"{y}-12-31")
        out.update(part)
    return out


def load_or_fetch_phenocam_gcc_3y(
    site_name: str, season: int, gcc_3y_path: Path
) -> dict[str, float]:
    """
    Use ``phenocam_gcc_3y.json`` on disk if it exists and parses; else fetch
    three years from the PhenoCam one-day summary for *site_name* and save to
    *gcc_3y_path*.
    """
    if gcc_3y_path.is_file():
        try:
            cached = load_phenocam_gcc(gcc_3y_path)
        except (OSError, json.JSONDecodeError):
            cached = {}
        if cached:
            print(f"[PhenoCam phenology] Using {gcc_3y_path} ({len(cached)} values)")
            return cached
    out = fetch_phenocam_gcc_three_years_separately(site_name, season)
    if not out:
        return {}
    save_phenocam_gcc_json(gcc_3y_path, out)
    print(
        f"[PhenoCam phenology] Fetched and wrote {gcc_3y_path} "
        f"({len(out)} values for {season - 1}–{season + 1})"
    )
    return out


def resolve_phenocam_gcc_for_timesat(
    site_name: str, season: int, gcc_path: Path
) -> dict[str, float]:
    """
    Load three-year series from ``phenocam_gcc_3y.json`` (or fetch once and
    save there), merge with one-year ``gcc_path`` if present; three-year values
    win on duplicate dates.
    """
    gcc_3y = gcc_path.parent / "phenocam_gcc_3y.json"
    by_3y = load_or_fetch_phenocam_gcc_3y(site_name, season, gcc_3y)
    by_1y: dict[str, float] = {}
    if gcc_path.is_file():
        try:
            by_1y = load_phenocam_gcc(gcc_path)
        except (OSError, json.JSONDecodeError):
            pass
    if by_3y:
        return {**by_1y, **by_3y}
    return by_1y


def _day_count(calendar_year: int) -> int:
    a = datetime(calendar_year, 1, 1)
    b = datetime(calendar_year + 1, 1, 1)
    return (b - a).days


def daily_profile_for_year(by_date: dict[str, float], calendar_year: int) -> np.ndarray:
    """
    One value per day (length 365 or 366 for leap years). Gaps are filled by
    linear interpolation in time along the year; if only one valid point exists,
    that value is used for the whole year.
    """
    n = _day_count(calendar_year)
    raw = np.full(n, np.nan, dtype=np.float64)
    for d in range(1, n + 1):
        dt = datetime(calendar_year, 1, 1) + timedelta(days=d - 1)
        key = dt.strftime("%Y-%m-%d")
        if key in by_date:
            raw[d - 1] = by_date[key]
    valid = np.isfinite(raw) & (raw > 0.0)
    if not np.any(valid):
        raise ValueError(f"No valid GCC in JSON for calendar year {calendar_year}")
    if np.sum(valid) == 1:
        v = float(raw[valid][0])
        return np.full(n, v, dtype=np.float32)
    idx = np.arange(n, dtype=np.float64)
    raw = np.interp(idx, idx[valid], raw[valid])
    return raw.astype(np.float32)


def _gcc_profile_365_for_timesat(profile: np.ndarray) -> np.ndarray:
    """TIMESAT uses 365 days per season; drop Dec 31 on leap years."""
    p = np.asarray(profile, dtype=np.float32).ravel()
    if p.size == 366:
        return p[:365]
    if p.size == 365:
        return p
    raise ValueError(f"expected 365 or 366 daily values, got {p.size}")


def yyyydoy_to_iso(v: float) -> str:
    x = int(round(float(v)))
    y = x // 1000
    doy = x - y * 1000
    d = datetime(y, 1, 1) + timedelta(days=doy - 1)
    return d.date().isoformat()


def build_yraw_three_years(
    by_date: dict[str, float], y1: int, y2: int, y3: int
) -> tuple[np.ndarray, str]:
    """
    Stack three calendar years of daily GCC (365 pts/year) for TIMESAT.

    If each of *y1*, *y2*, *y3* has at least one valid GCC in *by_date* (after
    per-year gap filling), returns their concatenation — **three real years**.

    If any of those years cannot be built (e.g. single-year download only),
    falls back to **replicating** the profile for *y2* three times (legacy
    TIMESAT workaround).
    """
    try:
        p1 = _gcc_profile_365_for_timesat(daily_profile_for_year(by_date, y1))
        p2 = _gcc_profile_365_for_timesat(daily_profile_for_year(by_date, y2))
        p3 = _gcc_profile_365_for_timesat(daily_profile_for_year(by_date, y3))
        yraw = np.concatenate([p1, p2, p3]).astype(np.float32, copy=False)
        return yraw, "three_independent_years"
    except ValueError:
        p2 = _gcc_profile_365_for_timesat(daily_profile_for_year(by_date, y2))
        yraw = np.tile(p2, 3)
        return yraw, "single_year_replicated"


def run_timesat_phenology_from_yraw(
    yraw: np.ndarray,
    years_triplet: tuple[int, int, int],
    *,
    start_cutoff: tuple[float, float] = (0.5, 0.5),
    smooth_window: float = 2.0,
    p_ignoreday: int = 366,
) -> dict[str, str | float | None]:
    """
    Run TIMESAT on a length ``365 * 3`` daily VI stack and calendar *years_triplet*
    (YYYY, YYYY, YYYY) for the time vector. Middle year in the triplet is the
    season whose SOS/EOS we report.
    """
    yraw = np.asarray(yraw, dtype=np.float32).ravel()
    y1, y2, y3 = years_triplet
    nyear = 3
    npt = 365 * nyear
    if yraw.size != npt:
        raise ValueError(f"yraw must have length {npt}, got {yraw.size}")
    tlist: list[int] = []
    for y in (y1, y2, y3):
        t0 = datetime(y, 1, 1)
        for d in range(365):
            tlist.append(int((t0 + timedelta(days=d)).strftime("%Y%j")))
    tv = np.array(tlist, dtype=np.int32)
    if len(tv) != npt:
        raise RuntimeError("internal: length mismatch")

    vi = np.asfortranarray(yraw.reshape(1, 1, -1))
    qa = np.asfortranarray(np.ones((1, 1, npt), dtype=np.float32))
    lc = np.ones((1, 1), dtype=np.uint8)
    landuse = np.ones(255, dtype=np.uint8)
    p_out = np.arange(1, npt + 1, dtype=np.int32)
    p_ylu = np.asfortranarray(np.array([0.0, 1.0], dtype=np.float64))
    ci = 0
    p_fitmethod = np.zeros(255, dtype=np.int32)
    p_fitmethod[ci] = 1
    p_smooth = np.zeros(255, dtype=np.float64)
    p_smooth[ci] = float(smooth_window)
    p_nenvi = np.zeros(255, dtype=np.int32)
    p_nenvi[ci] = 1
    p_wfact = np.zeros(255, dtype=np.float64)
    p_wfact[ci] = 1.0
    p_startmethod = np.zeros(255, dtype=np.int32)
    p_startmethod[ci] = 1
    p_startcutoff = np.zeros((255, 2), dtype=np.float64, order="F")
    p_startcutoff[ci, :] = np.array(
        [start_cutoff[0], start_cutoff[1]], dtype=np.float64
    )
    p_low = np.zeros(255, dtype=np.float64)
    p_fillbase = np.zeros(255, dtype=np.int32)
    p_seasonmethod = np.zeros(255, dtype=np.int32)
    p_seasonmethod[ci] = 1
    p_seapar = np.zeros(255, dtype=np.float64)
    p_seapar[ci] = 1.0

    if _timesat is None:
        raise ImportError("Install the 'timesat' package: pip install timesat")
    vpp, _vppqa, nseason, yfit, _yfitqa, _seasonfit, _tseq = _timesat.tsfprocess(
        nyear,
        vi,
        qa,
        tv,
        lc,
        1,
        landuse,
        p_out,
        p_ignoreday,
        p_ylu,
        0,
        p_fitmethod,
        p_smooth,
        NODATA,
        45,
        0,
        p_nenvi,
        p_wfact,
        p_startmethod,
        p_startcutoff,
        p_low,
        p_fillbase,
        1,
        p_seasonmethod,
        p_seapar,
        1,
        1,
        1,
        npt,
        len(p_out),
    )
    a = vpp[0, 0, :]
    # three growing-season rows at indices 0, 13*2, 13*4 in the raw vector
    middle_block = 2
    off = 13 * middle_block
    sosd = a[off + 0] if a.size > off + 0 else np.nan
    sosv = a[off + 1] if a.size > off + 1 else np.nan
    eosd = a[off + 3] if a.size > off + 3 else np.nan
    eosv = a[off + 4] if a.size > off + 4 else np.nan
    yfit_max = float(np.max(yfit)) if yfit.size else float("nan")

    def pick(x: float) -> str | None:
        if not np.isfinite(x) or x < 1.0e5 or x < 0:
            return None
        try:
            return yyyydoy_to_iso(x)
        except (OverflowError, ValueError):
            return None

    return {
        "reference_calendar_year": y2,
        "green_up_50pct_date": pick(sosd),
        "green_up_50pct_fitted_gcc": float(sosv) if np.isfinite(sosv) else None,
        "green_down_50pct_date": pick(eosd),
        "green_down_50pct_fitted_gcc": float(eosv) if np.isfinite(eosv) else None,
        "nseason": nseason[0, 0].tolist() if nseason.ndim >= 2 else [],
        "yfit_max": yfit_max,
    }


def run_timesat_phenology(
    daily_profile: np.ndarray,
    years_triplet: tuple[int, int, int],
    *,
    start_cutoff: tuple[float, float] = (0.5, 0.5),
    smooth_window: float = 2.0,
    p_ignoreday: int = 366,
) -> dict[str, str | float | None]:
    """
    Back-compat: run TIMESAT on one year’s 365(–366) profile **replicated** three times.
    Prefer :func:`build_yraw_three_years` + :func:`run_timesat_phenology_from_yraw`.
    """
    prof = np.asarray(daily_profile, dtype=np.float32).ravel()
    if len(prof) not in (365, 366):
        raise ValueError("daily_profile must have length 365 or 366")
    if len(prof) == 366:
        prof = prof[:365]
    yraw = np.tile(prof, 3)
    return run_timesat_phenology_from_yraw(
        yraw,
        years_triplet,
        start_cutoff=start_cutoff,
        smooth_window=smooth_window,
        p_ignoreday=p_ignoreday,
    )


def phenocam_gcc_path(site_name: str, season: int) -> Path:
    return Path(f"data/{site_name}/{season}/raw/phenocam/phenocam_gcc.json")


def phenocam_gcc_3y_path(site_name: str, season: int) -> Path:
    return Path(f"data/{site_name}/{season}/raw/phenocam/phenocam_gcc_3y.json")


def iter_sites_seasons_with_phenocam(
    data_root: str | Path = "data",
) -> list[tuple[str, int]]:
    """``(site_name, season)`` for every ``phenocam_gcc.json`` under *data_root* (legacy)."""
    root = Path(data_root)
    if not root.is_dir():
        return []
    out: list[tuple[str, int]] = []
    seen: set[tuple[str, int]] = set()
    for p in sorted(root.glob("*/*/raw/phenocam/phenocam_gcc.json")):
        rel = p.relative_to(root)
        site, season_s = rel.parts[0], rel.parts[1]
        if not season_s.isdigit():
            continue
        season = int(season_s)
        key = (site, season)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def iter_sites_seasons_from_sites_geojson(
    path: str | Path = "data/sites.geojson",
) -> list[tuple[str, int]]:
    """
    ``(sitename, season)`` from a GeoJSON FeatureCollection: each feature’s
    ``properties.sitename`` and each key in ``properties.seasons`` (4-digit year).
    """
    path = Path(path)
    if not path.is_file():
        return []
    with open(path, encoding="utf-8") as f:
        fc = json.load(f)
    out: list[tuple[str, int]] = []
    for feat in fc.get("features", []):
        props = feat.get("properties") or {}
        name = props.get("sitename")
        seasons = props.get("seasons")
        if not name or not isinstance(seasons, dict):
            continue
        for skey in sorted(seasons.keys()):
            if skey.isdigit() and len(skey) == 4:
                out.append((str(name), int(skey)))
    return out


def write_phenocam_phenology_all(
    *,
    sites_geojson: str | Path | None = None,
    data_root: str | Path = "data",
    smooth_window: float = 2.0,
    p_ignoreday: int = 366,
) -> int:
    """
    Run :func:`write_phenocam_phenology_for_site` for every ``(site, season)`` in
    *sites_geojson* (default: :file:`<data_root>/sites.geojson`), not a glob over
    ``data/``.
    """
    geo = Path(
        sites_geojson
        if sites_geojson is not None
        else Path(data_root) / "sites.geojson"
    )
    pairs = iter_sites_seasons_from_sites_geojson(geo)
    if not pairs and geo.is_file():
        print(
            f"[PhenoCam phenology] No (sitename, season) entries in {geo} "
            "(check properties.sitename and properties.seasons)."
        )
    elif not pairs:
        print(f"[PhenoCam phenology] Missing or empty sites file: {geo}")
    n = 0
    for site, season in pairs:
        print(f"=== {site} {season} ===")
        write_phenocam_phenology_for_site(
            site, season, smooth_window=smooth_window, p_ignoreday=p_ignoreday
        )
        n += 1
    print(f"[PhenoCam phenology] Processed {n} site/season pair(s) from {geo}.")
    return n


def phenocam_phenology_path(site_name: str, season: int) -> Path:
    return Path(f"data/{site_name}/{season}/raw/phenocam/phenocam_phenology.json")


def write_phenocam_phenology_for_site(
    site_name: str,
    season: int,
    *,
    smooth_window: float = 2.0,
    p_ignoreday: int = 366,
) -> None:
    """
    If ``timesat`` is installed, build GCC from ``phenocam_gcc_3y.json`` (or fetch
    three years once and save there), with optional one-year ``phenocam_gcc.json``,
    then write
    ``phenocam_phenology.json`` in the same directory with
    ``green_up_50pct_date`` and ``green_down_50pct_date`` (ISO dates or null).
    """
    if _timesat is None:
        out = phenocam_phenology_path(site_name, season)
        print(
            f"[PhenoCam phenology] Skipped (no timesat); would write {out}. "
            "pip install timesat"
        )
        return
    gcc = phenocam_gcc_path(site_name, season)
    try:
        by_date = resolve_phenocam_gcc_for_timesat(site_name, season, gcc)
    except OSError as e:
        print(f"[PhenoCam phenology] Skipped: {e}")
        return
    if not by_date:
        g3 = gcc.parent / "phenocam_gcc_3y.json"
        print(
            f"[PhenoCam phenology] No GCC ({gcc} and no data in {g3} after API); "
            f"skipping {phenocam_phenology_path(site_name, season).name}."
        )
        return
    try:
        yraw, stack_mode = build_yraw_three_years(
            by_date, season - 1, season, season + 1
        )
    except (OSError, ValueError) as e:
        print(f"[PhenoCam phenology] Skipped: {e}")
        return
    out = run_timesat_phenology_from_yraw(
        yraw,
        (season - 1, season, season + 1),
        smooth_window=smooth_window,
        p_ignoreday=p_ignoreday,
    )
    record = {
        "green_up_50pct_date": out.get("green_up_50pct_date"),
        "green_down_50pct_date": out.get("green_down_50pct_date"),
    }
    out_path = phenocam_phenology_path(site_name, season)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(record, f, indent=2)
        f.write("\n")
    gup, gdn = record["green_up_50pct_date"], record["green_down_50pct_date"]
    print(
        f"[PhenoCam phenology] Wrote {out_path} (green-up {gup!r}, green-down {gdn!r}; "
        f"TIMESAT input={stack_mode})"
    )


def main() -> None:
    ap = argparse.ArgumentParser(
        description="TIMESAT 50 % seasonal-amplitude green-up / green-down for PhenoCam GCC JSON."
    )
    ap.add_argument(
        "--all",
        action="store_true",
        help="Write phenocam for every (sitename, season) in the sites GeoJSON (see --sites-geojson).",
    )
    ap.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Resolves default --sites-geojson to <data-root>/sites.geojson.",
    )
    ap.add_argument(
        "--sites-geojson",
        type=Path,
        default=None,
        help="For --all: path to data/sites.geojson (default: <data-root>/sites.geojson).",
    )
    ap.add_argument(
        "gcc_json",
        type=Path,
        nargs="?",
        default=Path("data/innsbruck/2024/raw/phenocam/phenocam_gcc.json"),
        help="Path to phenocam_gcc.json (default: Innsbruck 2024 if present).",
    )
    ap.add_argument(
        "--season",
        type=int,
        default=None,
        help="Calendar year to build the daily GCC profile (default: infer from file path .../<year>/...).",
    )
    ap.add_argument(
        "--savitzky-hw",
        type=float,
        default=2.0,
        help="Half-width for fitmethod 1 (Savitzky–Golay); default 2.",
    )
    ap.add_argument(
        "--p-ignoreday",
        type=int,
        default=366,
        help="TIMESAT p_ignoreday (default 366).",
    )
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Write results to this JSON file (same schema as stdout, plus metadata).",
    )
    ap.add_argument(
        "--sidecar",
        action="store_true",
        help="Save two-date JSON next to input as phenocam_phenology.json (implies -o).",
    )
    args = ap.parse_args()
    if _timesat is None:
        raise SystemExit(
            "The 'timesat' package is required. Install with: pip install timesat"
        )
    if args.all:
        write_phenocam_phenology_all(
            sites_geojson=args.sites_geojson,
            data_root=args.data_root,
            smooth_window=args.savitzky_hw,
            p_ignoreday=args.p_ignoreday,
        )
        return
    path: Path = args.gcc_json
    if not path.is_file():
        raise SystemExit(f"Not a file: {path}")

    season = args.season
    if season is None:
        for part in path.parts:
            if part.isdigit() and len(part) == 4:
                season = int(part)
                break
        if season is None:
            season = datetime.now().year

    by_date = load_phenocam_gcc(path)
    yraw, stack_mode = build_yraw_three_years(by_date, season - 1, season, season + 1)
    out = run_timesat_phenology_from_yraw(
        yraw,
        (season - 1, season, season + 1),
        smooth_window=args.savitzky_hw,
        p_ignoreday=args.p_ignoreday,
    )
    payload = {
        **out,
        "source_gcc_json": str(path.resolve()),
        "profile_year": season,
        "timesat_input": stack_mode,
        "method": "TIMESAT tsfprocess; startmethod=1; p_startcutoff=[0.5,0.5] (50% seasonal amplitude)",
    }
    out_path = args.output
    if args.sidecar:
        out_path = path.parent / "phenocam_phenology.json"
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        to_write = (
            {
                "green_up_50pct_date": out.get("green_up_50pct_date"),
                "green_down_50pct_date": out.get("green_down_50pct_date"),
            }
            if args.sidecar
            else payload
        )
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(to_write, f, indent=2)
            f.write("\n")
        print(f"Wrote {out_path}", file=sys.stderr)
    print(json.dumps(payload, indent=2))
    gup = out.get("green_up_50pct_date")
    gdn = out.get("green_down_50pct_date")
    if gup and gdn:
        print(
            f"Green-up (50 %): {gup}  |  Green-down (50 %): {gdn}  "
            f"(profile year {season}, TIMESAT reference year {out['reference_calendar_year']})"
        )


if __name__ == "__main__":
    main()

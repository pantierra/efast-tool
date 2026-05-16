"""Gap windows and nearest S2 acquisition (manifest inputs)."""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path

from phenology_timesat import phenocam_phenology_path

REFL_DATE_RE = re.compile(r"S2A_MSIL2A_(\d{8})_REFL\.tif$")


def validation_dir(site_name: str, season: int) -> Path:
    return Path(f"data/{site_name}/{season}/validation")


def phenology_midpoint(
    site_name: str, season: int, phenology_path: Path | None = None
) -> date:
    """Pick fusion gap midpoint: green-up if in season, else green-down, else July 1."""
    path = phenology_path or phenocam_phenology_path(site_name, season)
    y0, y1 = date(season, 1, 1), date(season, 12, 31)
    fallback = date(season, 7, 1)
    if not path.is_file():
        return fallback
    try:
        rec = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback
    up_s = rec.get("green_up_50pct_date")
    dn_s = rec.get("green_down_50pct_date")

    def _parse(s) -> date | None:
        if not s or not isinstance(s, str):
            return None
        try:
            d = datetime.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
        return d if y0 <= d <= y1 else None

    up, dn = _parse(up_s), _parse(dn_s)
    if up:
        return up
    if dn:
        return dn
    return fallback


def centered_window(mid: date, gap_days: int, season: int) -> tuple[date, date]:
    """[start, end] inclusive, gap_days wide, clamped to calendar year."""
    half = gap_days // 2
    start = mid - timedelta(days=half)
    end = mid + timedelta(days=gap_days - 1 - half)
    y0, y1 = date(season, 1, 1), date(season, 12, 31)
    if start < y0:
        end = min(y1, end + (y0 - start))
        start = y0
    if end > y1:
        start = max(y0, start - (end - y1))
        end = y1
    return start, end


def list_s2_refl_dates(prepared_s2: Path) -> list[tuple[date, str]]:
    """Return sorted (acquisition_date, filename) for *REFL.tif."""
    out: list[tuple[date, str]] = []
    if not prepared_s2.is_dir():
        return out
    for p in sorted(prepared_s2.glob("*REFL.tif")):
        m = REFL_DATE_RE.search(p.name)
        if not m:
            continue
        d = datetime.strptime(m.group(1), "%Y%m%d").date()
        out.append((d, p.name))
    out.sort(key=lambda x: x[0])
    return out


def nearest_s2_acquisition(
    prediction: date, pairs: list[tuple[date, str]]
) -> tuple[date, str] | None:
    if not pairs:
        return None
    best = min(pairs, key=lambda t: abs((t[0] - prediction).days))
    return best


def build_manifest_entries(
    site_name: str,
    season: int,
    gap_lengths: tuple[int, ...] = (15, 30, 60, 90),
    s2_calendar_strategy: str = "aggressive",
) -> list[dict]:
    """One entry per gap length: window, prediction=midpoint, withheld = nearest S2 to midpoint."""
    mid = phenology_midpoint(site_name, season)
    prepared_s2 = Path(f"data/{site_name}/{season}/prepared_{s2_calendar_strategy}/s2")
    pairs = list_s2_refl_dates(prepared_s2)
    entries = []
    for gap_days in gap_lengths:
        w0, w1 = centered_window(mid, gap_days, season)
        prediction = mid
        ns = nearest_s2_acquisition(prediction, pairs)
        if ns is None:
            withheld_date = None
            withheld_filename = None
        else:
            withheld_date, withheld_filename = ns[0].isoformat(), ns[1]
        entries.append(
            {
                "gap_days": gap_days,
                "midpoint_rule": "green_up_50pct else green_down_50pct else July01",
                "midpoint_date": mid.isoformat(),
                "window_start": w0.isoformat(),
                "window_end": w1.isoformat(),
                "prediction_date": prediction.isoformat(),
                "withheld_s2_date": withheld_date,
                "withheld_s2_filename": withheld_filename,
            }
        )
    return entries


def write_manifest(
    site_name: str,
    season: int,
    site_position: tuple[float, float],
    s2_calendar_strategy: str = "aggressive",
) -> Path:
    out_dir = validation_dir(site_name, season)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "gap_manifest.json"
    payload = {
        "site_name": site_name,
        "season": season,
        "site_position_lat_lon": list(site_position),
        "s2_calendar_strategy": s2_calendar_strategy,
        "entries": build_manifest_entries(
            site_name, season, s2_calendar_strategy=s2_calendar_strategy
        ),
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def load_manifest(site_name: str, season: int) -> dict:
    path = validation_dir(site_name, season) / "gap_manifest.json"
    if not path.is_file():
        raise FileNotFoundError(f"Missing manifest: {path}")
    return json.loads(path.read_text(encoding="utf-8"))

"""Symlink prepared S2 into a temp dir, omitting one acquisition (REFL + DIST_CLOUD)."""

from __future__ import annotations

import re
from pathlib import Path

# Acquisition calendar day in prepared S2 names (BtI REFL/DIST; ItB GCC/DIST).
S2_PREP_DATE_RE = re.compile(r"_(\d{8})_(?:REFL|GCC|DIST_CLOUD)\.tif$", re.IGNORECASE)


def yyyymmdd_in_name(name: str) -> str | None:
    m = S2_PREP_DATE_RE.search(name)
    return m.group(1) if m else None


def build_masked_s2_dir(
    prepared_s2: Path, withheld_yyyymmdd: str, dest: Path, patterns: tuple[str, ...]
) -> int:
    """Symlink all files matching ``patterns`` except the withheld acquisition day."""
    dest.mkdir(parents=True, exist_ok=True)
    n = 0
    for pattern in patterns:
        for src in sorted(prepared_s2.glob(pattern)):
            if not src.is_file() and not src.is_symlink():
                continue
            y = yyyymmdd_in_name(src.name)
            if y == withheld_yyyymmdd:
                continue
            link = dest / src.name
            if link.exists() or link.is_symlink():
                link.unlink()
            link.symlink_to(src.resolve())
            n += 1
    return n


def build_masked_s2_dir_bti(
    prepared_s2: Path, withheld_yyyymmdd: str, dest: Path
) -> int:
    return build_masked_s2_dir(
        prepared_s2, withheld_yyyymmdd, dest, ("*REFL.tif", "*DIST_CLOUD.tif")
    )


def build_masked_s2_dir_itb(
    prepared_s2: Path, withheld_yyyymmdd: str, dest: Path
) -> int:
    return build_masked_s2_dir(
        prepared_s2, withheld_yyyymmdd, dest, ("*GCC.tif", "*DIST_CLOUD.tif")
    )

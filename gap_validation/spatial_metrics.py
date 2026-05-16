"""Per-pixel GCC vs withheld S2; NSE (nse_s2); no-gap baseline; deltas."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import rasterio
from rasterio.warp import reproject, Resampling
from scipy.stats import pearsonr

# Match postprocessing valid mask on reflectance (METH / postprocessing.py).
VALID_REFL_THRESHOLD = 0.001


def _gcc_from_rgb(blue: np.ndarray, green: np.ndarray, red: np.ndarray) -> np.ndarray:
    t = red.astype(np.float64) + green.astype(np.float64) + blue.astype(np.float64)
    out = np.full_like(blue, np.nan, dtype=np.float64)
    m = (
        np.isfinite(t)
        & (t > 0)
        & np.isfinite(blue)
        & np.isfinite(green)
        & np.isfinite(red)
    )
    out[m] = green[m].astype(np.float64) / t[m]
    return out.astype(np.float32)


def read_fused_gcc(fusion_path: Path) -> tuple[np.ndarray, dict]:
    """Fused GCC: BtI from 4-band REFL or ItB single-band GCC."""
    with rasterio.open(fusion_path) as src:
        if src.count >= 4:
            b = src.read(1).astype(np.float32)
            g = src.read(2).astype(np.float32)
            r = src.read(3).astype(np.float32)
            gcc = _gcc_from_rgb(b, g, r)
        else:
            gcc = src.read(1).astype(np.float32)
        prof = src.profile.copy()
    return gcc, prof


def warp_refl_bands_to_grid(
    refl_path: Path,
    height: int,
    width: int,
    transform,
    crs,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Resample S2 REFL blue/green/red to fusion grid (bilinear)."""
    with rasterio.open(refl_path) as src:
        b = np.empty((height, width), dtype=np.float32)
        g = np.empty((height, width), dtype=np.float32)
        r = np.empty((height, width), dtype=np.float32)
        for i, dst in enumerate((b, g, r), start=1):
            reproject(
                source=rasterio.band(src, i),
                destination=dst,
                src_transform=src.transform,
                src_crs=src.crs,
                dst_transform=transform,
                dst_crs=crs,
                resampling=Resampling.bilinear,
            )
    return b, g, r


def valid_mask_fused(fusion_path: Path, mode: str) -> np.ndarray:
    """Valid pixels: BtI uses REFL-style mask; ItB uses single-band GCC (postprocessing ItB)."""
    with rasterio.open(fusion_path) as src:
        if mode == "itb" or src.count < 4:
            d = src.read(1).astype(np.float32)
            return np.isfinite(d) & (d > VALID_REFL_THRESHOLD)
        stacks = src.read().astype(np.float32)
        ok = np.isfinite(stacks).all(axis=0) & (
            np.nanmax(stacks, axis=0) > VALID_REFL_THRESHOLD
        )
        return ok


def spatial_scores(
    y_true_gcc: np.ndarray,
    y_pred_gcc: np.ndarray,
    mask: np.ndarray,
) -> dict:
    """RMSE, MAE, mean bias, Pearson r, nse_s2 (Nash–Sutcliffe vs spatial truth)."""
    yt = y_true_gcc[mask].astype(np.float64).ravel()
    yp = y_pred_gcc[mask].astype(np.float64).ravel()
    n = int(yt.size)
    if n < 2:
        return {"n_pixels": n}
    mean_t = float(np.mean(yt))
    rmse = float(np.sqrt(np.mean((yt - yp) ** 2)))
    mae = float(np.mean(np.abs(yt - yp)))
    bias = float(np.mean(yp - yt))
    den = float(np.sum((yt - mean_t) ** 2))
    nse_s2 = float(1.0 - np.sum((yt - yp) ** 2) / den) if den > 0 else None
    r = None
    if np.std(yt) > 0 and np.std(yp) > 0:
        r = float(pearsonr(yt, yp)[0])
    return {
        "n_pixels": n,
        "rmse": rmse,
        "mae": mae,
        "mean_bias": bias,
        "pearson_r": r,
        "nse_s2": nse_s2,
    }


def withheld_gcc_on_fusion_grid(
    withheld_refl_path: Path, fused_path: Path
) -> tuple[np.ndarray, np.ndarray, dict]:
    """``y_true`` GCC (withheld S2) and ``y_pred`` GCC from ``fused_path``, same grid."""
    yp, prof = read_fused_gcc(fused_path)
    h, w = yp.shape
    b, g, r = warp_refl_bands_to_grid(
        withheld_refl_path, h, w, prof["transform"], prof["crs"]
    )
    yt = _gcc_from_rgb(b, g, r)
    return yt, yp, prof


def common_valid_mask(
    yt: np.ndarray,
    y_gap: np.ndarray,
    y_nogap: np.ndarray | None,
    fused_gap_path: Path,
    mode: str,
) -> np.ndarray:
    """Shared finite mask: truth GCC, gap/nogap preds, and fusion valid-data rules."""
    m = (
        valid_mask_fused(fused_gap_path, mode)
        & np.isfinite(yt)
        & np.isfinite(y_gap)
        & (yt > VALID_REFL_THRESHOLD)
        & (y_gap > VALID_REFL_THRESHOLD)
    )
    if y_nogap is not None:
        m &= np.isfinite(y_nogap) & (y_nogap > VALID_REFL_THRESHOLD)
    return m


def evaluate_gap_vs_withheld(
    withheld_refl_path: Path,
    fused_gap_path: Path,
    fused_nogap_path: Path | None,
    mode: str,
    *,
    whittaker_context: tuple[Path, str, str, str] | None = None,
) -> dict:
    """Spatial metrics for gap and no-gap; deltas; optional Whittaker constant-field vs same mask.

    ``delta_rmse`` = RMSE_gap − RMSE_no_gap; ``delta_nse`` = NSE_no_gap − NSE_gap (higher gap loss → positive delta_nse).
    """
    yt, y_gap, _prof = withheld_gcc_on_fusion_grid(withheld_refl_path, fused_gap_path)
    y_nogap = None
    if fused_nogap_path is not None and fused_nogap_path.is_file():
        y_nogap, _ = read_fused_gcc(fused_nogap_path)
    mask = common_valid_mask(yt, y_gap, y_nogap, fused_gap_path, mode)
    out: dict = {"gap": spatial_scores(yt, y_gap, mask)}
    if y_nogap is not None:
        out["no_gap"] = spatial_scores(yt, y_nogap, mask)
        g, ng = out["gap"], out["no_gap"]
        if g.get("rmse") is not None and ng.get("rmse") is not None:
            out["delta_rmse"] = float(g["rmse"] - ng["rmse"])
        if g.get("nse_s2") is not None and ng.get("nse_s2") is not None:
            out["delta_nse"] = float(ng["nse_s2"] - g["nse_s2"])
    if whittaker_context is not None:
        from gap_validation.whittaker_compare import whittaker_gcc_on_gap_masked_series

        base, strategy, prediction_iso, withheld_iso = whittaker_context
        wgcc = whittaker_gcc_on_gap_masked_series(
            base, strategy, prediction_iso, withheld_iso
        )
        if wgcc is not None:
            out["whittaker"] = constant_field_scores(yt, float(wgcc), mask)
    return out


def constant_field_scores(
    y_true_gcc: np.ndarray, scalar: float, mask: np.ndarray
) -> dict:
    """NSE / RMSE when prediction is a spatially constant Whittaker value (same mask as fusion)."""
    yp = np.full_like(y_true_gcc, scalar, dtype=np.float32)
    return spatial_scores(y_true_gcc, yp, mask)

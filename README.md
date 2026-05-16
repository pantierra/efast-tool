# Satellite Data Fusion Pipeline

Python pipeline for downloading Sentinel-2 and Sentinel-3 imagery and PhenoCam ground truth, applying NDVI-based cloud pre-selection, fusing sensors with the [EFAST](https://github.com/DHI-GRAS/efast) algorithm, and evaluating fused **Green Chromatic Coordinate (GCC)** time series against PhenoCam `gcc_90`.

## Features

- **Acquisition** — S2 L2A (AWS Element84 STAC), S3 OLCI L1B (Copernicus OpenEO), PhenoCam midday images and GCC CSV
- **Pre-selection** — Aggressive and non-aggressive NDVI-based cloud screening (plus dark-scene rejection)
- **Preparation** — Harmonised reflectance/GCC rasters, distance-to-cloud weights, S3 compositing and optional temporal smoothing
- **Fusion** — EFAST under eight scenarios per site (BtI and ItB × two strategies × σ ∈ {20, 30} days)
- **Post-processing** — Crop to valid-data window; NDVI and GCC timeseries at the site
- **Metrics** — Temporal comparison vs PhenoCam (`metrics.json`); optional Tier-2 withheld-S2 gap validation
- **Web viewer** — Static HTML dashboard over pipeline outputs (`webapp/`)

## Installation

```bash
pip install -r requirements.txt
pip install git+https://github.com/DHI-GRAS/efast.git   # not on PyPI
```

Create `.env` with Copernicus Data Space credentials:

- `CDSE_USER`
- `CDSE_PASSWORD`

Python version is pinned in `.python-version` (use `.venv/` locally).

## Usage

```python
from run import run_pipeline

run_pipeline(season=2024, site_position=(47.116171, 11.320308), site_name="innsbruck")
```

`site_position` is always **`(lat, lon)`**. Study sites are listed at the bottom of `run.py`: `innsbruck`, `forthgr`, `pitsalu`, `vindeln2`, `sunflowerjerez1`, `institutekarnobat`.

By default, most stages in `run.py` are **commented out** (metrics-only). Uncomment acquisition → pre-selection → preparation → fusion → post-processing for a full run.

### Pipeline stages

1. Download S2, S3, and PhenoCam
2. Pre-selection (per-sensor NDVI screening → `raw/preselection/`)
3. Prepare S2/S3 for each strategy (`prepared_{aggressive|nonaggressive}/` and `_itb/` variants)
4. EFAST fusion (BtI reflectance and ItB GCC products)
5. Post-process crops and timeseries (`processed_*_sigma{20,30}/`)
6. Compute metrics vs PhenoCam → `metrics.json`

### Gap validation (optional)

With prepared data and EFAST installed:

```bash
python -m gap_validation.run --site innsbruck --season 2024 --lat 47.116171 --lon 11.320308
```

Writes `data/{site}/{season}/validation/gap_manifest.json`, `gap_validation_summary.json`, and masked fusion under `validation/fusion/`. See `python -m gap_validation.run --help`.

## Data layout

```
data/{site_name}/{season}/
  raw/
    s2/                    # {YYYYMMDD}_{n}.geotiff — B02, B03, B04, B8A
    s3/                    # {YYYYMMDD}_{n}.geotiff — Oa04, Oa06, Oa08, Oa17
    phenocam/              # JPEGs, GCC JSON, phenology sidecar
    preselection/          # {s2,s3}_preselection.{json,csv}
  prepared_{strategy}/
    s2/                    # REFL + DIST_CLOUD GeoTIFFs
    s3/                    # composite_{YYYYMMDD}.tif
    fusion/                # REFL_{YYYYMMDD}.tif (σ≈20)
    fusion_sigma30/        # REFL (σ=30)
  prepared_{strategy}_itb/
    s2/  s3/  fusion/      # GCC products (Index-then-Blend)
  processed_{strategy}_sigma{20,30}/
    s2/  s3/  fusion/      # cropped {YYYYMMDD}_0.geotiff
    gcc/  ndvi/            # timeseries.json per source
  processed_{strategy}_itb_sigma{20,30}/
    s2/  s3/  fusion/  gcc/
  validation/            # gap experiment (when run)
  metrics.json
```

Site metadata: `data/sites.geojson` (six thesis sites). `data/coweeta/` is local/legacy and not listed there.

### File formats

**Sentinel-2** — Multi-band GeoTIFF; bands `[blue, green, red, nir]`; `VIEWING_ZENITH_ANGLE` metadata; filename `{YYYYMMDD}_{increment}.geotiff`.

**Sentinel-3** — Multi-band GeoTIFF; same band order; filename `{YYYYMMDD}_{increment}.geotiff`.

**Prepared S2** — `S2A_MSIL2A_{YYYYMMDD}_REFL.tif` plus `*DIST_CLOUD.tif` (cloud-distance weights for EFAST).

## Web viewer

Static HTML/JS in `webapp/` — no build step. Shared GeoTIFF helpers: `webapp/common.js`. CDN: Leaflet, geotiff.js, proj4. Symlink: `webapp/data` → `../data`.

Serve from the **repository root** (not `webapp/`):

```bash
python3 -m http.server 8000
# http://localhost:8000/webapp/index.html
```

Or from the workspace root: `make serve`.

| Page | Purpose | Primary data paths |
|------|---------|-------------------|
| `index.html` | Post-processed maps, NDVI/GCC timeseries, PhenoCam | `processed_{strategy}_sigma{n}/`, `raw/phenocam/` |
| `preselection.html` | Cloud-screening diagnostics | `raw/preselection/{s2,s3}_preselection.json` |
| `prepared.html` | Prepared REFL/GCC before crop | `prepared_{strategy}/`, `prepared_{strategy}_itb/` |
| `fusion.html` | EFAST daily fusion rasters | `prepared_*/fusion/`, `fusion_sigma30/` |
| `postprocessed.html` | Cropped processed stacks | `processed_*_sigma*/` |
| `metrics.html` | Tabular `metrics.json` (thesis export source) | `{site}/{season}/metrics.json` under `webapp/data/` |
| `gap_validation.html` | Withheld-S2 gap experiment | `{site}/{season}/validation/gap_validation_summary.json` |
| `phenology.html` | TIMESAT on PhenoCam GCC | `raw/phenocam/phenocam_phenology.json` |

Site/season dropdowns use `data/sites.geojson`. Map pages: **BtI | ItB**; scenarios `aggressive` / `nonaggressive`, σ 20 / 30. Keep the shared nav consistent across all eight pages. QA only — thesis tables are exported from the workspace root (`make export` or `../scripts/export_thesis_tables.py`).

## Development

```bash
ruff check --fix . && ruff format .
```

Pre-commit hooks: `.pre-commit-config.yaml`.

## License

GNU Affero General Public License v3.0 (AGPL-3.0). See [LICENSE](LICENSE).

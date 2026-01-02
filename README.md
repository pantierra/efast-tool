# Satellite Data Fusion Pipeline

A Python pipeline for downloading, processing, and fusing Sentinel-2 and Sentinel-3 satellite imagery to generate high-resolution NDVI time series.

## Features

- **Data Download**: Downloads Sentinel-2 L2A (via AWS Earth Search) and Sentinel-3 OLCI (via OpenEO/Copernicus)
- **Cloud Detection**: Identifies cloud-covered images using NDVI analysis
- **EFAST Fusion**: Combines S2 and S3 data using the EFAST algorithm for enhanced temporal resolution
- **NDVI Calculation**: Generates Normalized Difference Vegetation Index from raw and fused data
- **Web Visualization**: Interactive web viewer for exploring NDVI time series and imagery

## Installation

```bash
pip install -r requirements.txt
pip install git+https://github.com/DHI-GRAS/efast.git
```

## Configuration

Set environment variables for Copernicus Data Space authentication:
- `CDSE_USER`: Copernicus Data Space username
- `CDSE_PASSWORD`: Copernicus Data Space password

## Usage

```python
from run import run_pipeline

run_pipeline(season=2024, site_position=(47.116171, 11.320308), site_name="innsbruck")
```

The pipeline processes data in stages:
1. Download S2/S3 imagery
2. Generate NDVI from raw data
3. Detect clouds
4. Prepare data for fusion
5. Run EFAST fusion
6. Generate NDVI from fused outputs

## Data Structure

```
data/
  {site_name}/
    {season}/
      raw/
        s2/          # Sentinel-2 GeoTIFFs
        s3/          # Sentinel-3 GeoTIFFs
        ndvi/        # NDVI from raw data
      prepared/
        s2/          # Prepared S2 data
        s3/          # Prepared S3 data
        fusion/      # EFAST fusion outputs
        ndvi/        # NDVI from prepared/fused data
      clouds.json    # Cloud detection results
```

## Web Viewer

Run a local HTTP server to view the web interface:

```bash
cd webapp
python3 -m http.server 8000
```

Then open `http://localhost:8000/webapp` in your browser to visualize NDVI time series and compare S2, S3, and fusion outputs.


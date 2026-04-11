/** Shared GeoTIFF → canvas for RGB (≥3 bands) or grayscale (1 band). Requires global GeoTIFF. */
async function geotiffToCanvasDataUrl(arrayBuffer) {
    const tiff = await GeoTIFF.fromArrayBuffer(arrayBuffer);
    const image = await tiff.getImage();
    const rasters = await image.readRasters();
    const width = image.getWidth();
    const height = image.getHeight();
    const bbox = image.getBoundingBox();
    const geoKeys = image.getGeoKeys();
    const crsCode = geoKeys.ProjectedCSTypeGeoKey
        ? `EPSG:${geoKeys.ProjectedCSTypeGeoKey}`
        : geoKeys.GeographicTypeGeoKey !== 4326
          ? `EPSG:${geoKeys.GeographicTypeGeoKey}`
          : "EPSG:4326";
    const normalize = (arr) => {
        let min = Infinity,
            max = -Infinity;
        for (const v of arr) if (!isNaN(v) && v > 0) {
            min = Math.min(min, v);
            max = Math.max(max, v);
        }
        return arr.map((v) => Math.max(0, Math.min(255, ((v - min) / (max - min || 1)) * 255)));
    };
    const spp = typeof image.getSamplesPerPixel === "function" ? image.getSamplesPerPixel() : 0;
    const multi = spp >= 3 || (rasters[1] != null && rasters[2] != null);
    const b0 = Array.from(rasters[0]);
    let rN, gN, bN;
    if (multi) {
        const blue = b0,
            green = Array.from(rasters[1]),
            red = Array.from(rasters[2]);
        rN = normalize(red);
        gN = normalize(green);
        bN = normalize(blue);
    } else {
        const g = normalize(b0);
        rN = g;
        gN = g;
        bN = g;
    }
    const canvas = Object.assign(document.createElement("canvas"), { width, height });
    const ctx = canvas.getContext("2d");
    ctx.imageSmoothingEnabled = false;
    const imgData = ctx.createImageData(width, height);
    for (let i = 0; i < rN.length; i++) {
        const idx = i * 4;
        if (rN[i] === 0 && gN[i] === 0 && bN[i] === 0) imgData.data[idx + 3] = 0;
        else {
            imgData.data[idx] = rN[i];
            imgData.data[idx + 1] = gN[i];
            imgData.data[idx + 2] = bN[i];
            imgData.data[idx + 3] = 255;
        }
    }
    ctx.putImageData(imgData, 0, 0);
    return { dataUrl: canvas.toDataURL(), bbox, crsCode };
}

"""Exercise GDAL raster IO and PROJ data from the installed conda package."""

import os
import subprocess
import tempfile
from pathlib import Path

from osgeo import gdal, osr

gdal.UseExceptions()
os.environ["PROJ_NETWORK"] = "OFF"
assert gdal.GetDriverByName("netCDF") is not None, "GDAL NetCDF plugin is missing"

with tempfile.TemporaryDirectory() as directory:
    source = Path(directory) / "input.tif"
    output = Path(directory) / "output.tif"
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    dataset = gdal.GetDriverByName("GTiff").Create(str(source), 32, 32, 1, gdal.GDT_Byte)
    dataset.SetGeoTransform((4.0, 0.01, 0.0, 51.0, 0.0, -0.01))
    dataset.SetProjection(srs.ExportToWkt())
    dataset.GetRasterBand(1).Fill(42)
    dataset = None

    subprocess.run(
        ["createcog", "--input", str(source), "--output", str(output), "--noprogress"],
        check=True,
    )
    dataset = gdal.Open(str(output))
    assert dataset.RasterCount == 1
    assert dataset.RasterXSize > 0 and dataset.RasterYSize > 0
    assert dataset.GetSpatialRef().GetAuthorityCode(None) == "3857"
    assert dataset.GetRasterBand(1).Checksum() != 0
    dataset = None

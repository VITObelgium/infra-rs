use std::{mem::MaybeUninit, path::Path};

#[cfg(feature = "gdal")]
use crate::Error;
use crate::{ArrayDataType, GeoReference, RasterSize, Result, raster::io::RasterFormat};

#[cfg(feature = "gdal")]
pub mod gdal;
pub mod geotiff;

/// Trait for reading raster data from various formats.
/// Meant to be implemented by different raster format readers and not be used directly.
pub trait RasterReader {
    fn open_read_only(path: impl AsRef<Path>) -> Result<Self>
    where
        Self: Sized;
    fn open_read_only_with_options(path: impl AsRef<Path>, open_options: &RasterOpenOptions) -> Result<Self>
    where
        Self: Sized;

    fn band_count(&self) -> Result<usize>;
    fn raster_size(&self) -> Result<RasterSize>;
    fn georeference(&mut self, band_index: usize) -> Result<GeoReference>;
    fn data_type(&self, band_index: usize) -> Result<ArrayDataType>;
    fn overview_count(&self, band_index: usize) -> Result<usize>;

    fn read_raster_band(&mut self, band_index: usize, data_type: ArrayDataType, dst_data: &mut [MaybeUninit<u8>]) -> Result<GeoReference>;
    fn read_raster_band_region(
        &mut self,
        band_index: usize,
        extent: &GeoReference,
        data_type: ArrayDataType,
        dst_data: &mut [MaybeUninit<u8>],
    ) -> Result<GeoReference>;
}

#[derive(Debug, Clone, Default)]
pub struct RasterOpenOptions {
    pub layer_name: Option<String>,
    pub driver_specific_options: Option<Vec<String>>,
}

/// Creates a `RasterReader` for the specified path based on the file extension.
pub fn create_raster_reader(path: impl AsRef<Path>) -> Result<Box<dyn RasterReader>> {
    match RasterFormat::guess_from_path(path.as_ref()) {
        #[cfg(feature = "gdal")]
        RasterFormat::ArcAscii
        | RasterFormat::GeoTiff
        | RasterFormat::Gif
        | RasterFormat::Png
        | RasterFormat::PcRaster
        | RasterFormat::Netcdf
        | RasterFormat::MBTiles
        | RasterFormat::GeoPackage
        | RasterFormat::Grib
        | RasterFormat::Postgis
        | RasterFormat::Vrt => Ok(Box::new(gdal::GdalRasterIO::open_read_only(path.as_ref())?)),
        _ => Err(Error::Runtime(format!("Unsupported raster file type: {}", path.as_ref().display()))),
    }
}

/// Creates a `RasterReader` for the specified path based on the file extension.
pub fn create_raster_reader_with_options(path: impl AsRef<Path>, options: &RasterOpenOptions) -> Result<Box<dyn RasterReader>> {
    match RasterFormat::guess_from_path(path.as_ref()) {
        #[cfg(feature = "gdal")]
        RasterFormat::ArcAscii
        | RasterFormat::GeoTiff
        | RasterFormat::Gif
        | RasterFormat::Png
        | RasterFormat::PcRaster
        | RasterFormat::Netcdf
        | RasterFormat::MBTiles
        | RasterFormat::GeoPackage
        | RasterFormat::Grib
        | RasterFormat::Postgis
        | RasterFormat::Vrt => Ok(Box::new(gdal::GdalRasterIO::open_read_only_with_options(path.as_ref(), options)?)),
        _ => Err(Error::Runtime(format!("Unsupported raster file type: {}", path.as_ref().display()))),
    }
}

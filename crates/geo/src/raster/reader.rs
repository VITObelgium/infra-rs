use std::{mem::MaybeUninit, path::Path};

use inf::allocate::{AlignedVec, AlignedVecUnderConstruction};

#[cfg(feature = "gdal")]
use crate::Error;
use crate::{ArrayDataType, ArrayNum, GeoReference, RasterSize, Result, raster::io::RasterFormat};

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

pub struct RasterAccess {
    reader: Box<dyn RasterReader>,
}

impl RasterAccess {
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            reader: create_raster_reader(path)?,
        })
    }

    pub fn open_read_only_with_options(path: impl AsRef<Path>, open_options: &RasterOpenOptions) -> Result<Self> {
        Ok(Self {
            reader: create_raster_reader_with_options(path, open_options)?,
        })
    }

    pub fn band_count(&self) -> Result<usize> {
        self.reader.band_count()
    }

    pub fn raster_size(&self) -> Result<RasterSize> {
        self.reader.raster_size()
    }

    pub fn georeference(&mut self, band_index: usize) -> Result<GeoReference> {
        self.reader.georeference(band_index)
    }

    pub fn data_type(&self, band_index: usize) -> Result<ArrayDataType> {
        self.reader.data_type(band_index)
    }

    pub fn overview_count(&self, band_index: usize) -> Result<usize> {
        self.reader.overview_count(band_index)
    }

    pub fn read_raster_band<T: ArrayNum>(&mut self, band_index: usize) -> Result<(GeoReference, AlignedVec<T>)> {
        let raster_size = self.reader.raster_size()?;
        let mut dst_data = AlignedVecUnderConstruction::<T>::new(raster_size.cell_count());
        let georef = self
            .reader
            .read_raster_band(band_index, T::TYPE, dst_data.as_uninit_byte_slice_mut())?;
        Ok((georef, unsafe { dst_data.assume_init() }))
    }

    pub fn read_raster_band_region<T: ArrayNum>(
        &mut self,
        band_index: usize,
        bounds: &GeoReference,
    ) -> Result<(GeoReference, AlignedVec<T>)> {
        let mut dst_data = AlignedVecUnderConstruction::<T>::new(bounds.raster_size().cell_count());
        let georef = self
            .reader
            .read_raster_band_region(band_index, bounds, T::TYPE, dst_data.as_uninit_byte_slice_mut())?;
        Ok((georef, unsafe { dst_data.assume_init() }))
    }

    pub fn read_raster_band_into_buffer<T: ArrayNum>(&mut self, band_index: usize, buffer: &mut [MaybeUninit<T>]) -> Result<GeoReference> {
        self.reader.read_raster_band(band_index, T::TYPE, unsafe {
            std::slice::from_raw_parts_mut(
                buffer.as_mut_ptr().cast::<MaybeUninit<u8>>(),
                buffer.len() * std::mem::size_of::<T>(),
            )
        })
    }

    pub fn read_raster_band_region_into_buffer<T: ArrayNum>(
        &mut self,
        band_index: usize,
        bounds: &GeoReference,
        buffer: &mut [MaybeUninit<T>],
    ) -> Result<GeoReference> {
        self.reader.read_raster_band_region(band_index, bounds, T::TYPE, unsafe {
            std::slice::from_raw_parts_mut(
                buffer.as_mut_ptr().cast::<MaybeUninit<u8>>(),
                buffer.len() * std::mem::size_of::<T>(),
            )
        })
    }
}

/// Creates a `RasterReader` for the specified path based on the file extension.
fn create_raster_reader(path: impl AsRef<Path>) -> Result<Box<dyn RasterReader>> {
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
fn create_raster_reader_with_options(path: impl AsRef<Path>, options: &RasterOpenOptions) -> Result<Box<dyn RasterReader>> {
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

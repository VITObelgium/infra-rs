use std::{mem::MaybeUninit, path::Path};

use crate::{ArrayDataType, GeoReference, RasterSize, Result, raster::utils::cast_uninit_slice_to_byte};

#[cfg(feature = "gdal")]
pub mod gdal;
#[cfg(feature = "raster-io-geotiff")]
pub mod geotiff;

pub enum FormatProvider {
    GeoTiff,
    Gdal,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RasterFileFormat {
    Memory,
    ArcAscii,
    GeoTiff,
    Gif,
    Png,
    PcRaster,
    Netcdf,
    MBTiles,
    GeoPackage,
    Grib,
    Postgis,
    Vrt,
    Unknown,
}

impl RasterFileFormat {
    /// Given a file path, guess the raster type based on the file extension
    pub fn guess_from_path(file_path: impl AsRef<Path>) -> RasterFileFormat {
        let file_path = file_path.as_ref();
        let ext = file_path.extension().map(|ext| ext.to_string_lossy().to_lowercase());

        if let Some(ext) = ext {
            match ext.as_ref() {
                "asc" => return RasterFileFormat::ArcAscii,
                "tiff" | "tif" => return RasterFileFormat::GeoTiff,
                "gif" => return RasterFileFormat::Gif,
                "png" => return RasterFileFormat::Png,
                "map" => return RasterFileFormat::PcRaster,
                "nc" => return RasterFileFormat::Netcdf,
                "mbtiles" | "db" => return RasterFileFormat::MBTiles,
                "gpkg" => return RasterFileFormat::GeoPackage,
                "grib" => return RasterFileFormat::Grib,
                _ => {}
            }
        }

        let path = file_path.to_string_lossy();
        if path.starts_with("postgresql://") || path.starts_with("pg:") {
            RasterFileFormat::Postgis
        } else {
            RasterFileFormat::Unknown
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RasterOpenOptions {
    pub layer_name: Option<String>,
    pub driver_specific_options: Option<Vec<String>>,
}

// Dyn compatible methods for raster formats
// Dyn compatibility is obtained by passing the databuffer as a byte slice whuch is then reinterpreted to the desired type in the concrete implementation
pub trait RasterFormatDyn {
    fn band_count(&self) -> Result<usize>;
    fn raster_size(&self) -> Result<RasterSize>;
    fn georeference(&mut self, band_index: usize) -> Result<GeoReference>;
    fn data_type(&self, band_index: usize) -> Result<ArrayDataType>;
    fn overview_count(&self, band_index: usize) -> Result<usize>;

    /// Reads a full raster band into the provided data buffer.
    /// The buffer should be allocated and have the correct size. (bytecount = raster rows * raster cols * `data_type` bytes)
    fn read_band_into_byte_buffer(
        &mut self,
        band_index: usize,
        data_type: ArrayDataType,
        dst_data: &mut [MaybeUninit<u8>],
    ) -> Result<GeoReference>;

    /// Reads a subregion of a raster band into the provided data buffer.
    /// The buffer should be allocated and have the correct size. (bytecount = region rows * region cols * `data_type` bytes)
    fn read_band_region_into_byte_buffer(
        &mut self,
        band: usize,
        region: &GeoReference,
        data_type: ArrayDataType,
        data: &mut [MaybeUninit<u8>],
    ) -> Result<GeoReference>;
}

/// Trait for reading raster data from various formats.
/// Meant to be implemented by different raster format readers and not be used directly.
/// Clients should use the `crate::raster::io::RasterIO` struct to open and read rasters.
pub trait RasterFormat: RasterFormatDyn + Sized {
    fn open_read_only(path: impl AsRef<Path>) -> Result<Self>;
    fn open_read_only_with_options(path: impl AsRef<Path>, open_options: &RasterOpenOptions) -> Result<Self>;
}

pub trait RasterFormatGeneric: RasterFormatDyn {
    // Read a complete raster band into the provided buffer
    // implemented here to avoid code duplication in implementations
    fn read_band<T: crate::ArrayNum>(&mut self, band: usize, dst: &mut [MaybeUninit<T>]) -> Result<GeoReference> {
        assert_eq!(T::TYPE, self.data_type(band)?);
        match T::TYPE {
            ArrayDataType::Uint8 => self.read_band_into_byte_buffer(band, ArrayDataType::Uint8, cast_uninit_slice_to_byte(dst)),
            ArrayDataType::Uint16 => self.read_band_into_byte_buffer(band, ArrayDataType::Uint16, cast_uninit_slice_to_byte(dst)),
            ArrayDataType::Uint32 => self.read_band_into_byte_buffer(band, ArrayDataType::Uint32, cast_uninit_slice_to_byte(dst)),
            ArrayDataType::Uint64 => self.read_band_into_byte_buffer(band, ArrayDataType::Uint64, cast_uninit_slice_to_byte(dst)),
            ArrayDataType::Int8 => self.read_band_into_byte_buffer(band, ArrayDataType::Int8, cast_uninit_slice_to_byte(dst)),
            ArrayDataType::Int16 => self.read_band_into_byte_buffer(band, ArrayDataType::Int16, cast_uninit_slice_to_byte(dst)),
            ArrayDataType::Int32 => self.read_band_into_byte_buffer(band, ArrayDataType::Int32, cast_uninit_slice_to_byte(dst)),
            ArrayDataType::Int64 => self.read_band_into_byte_buffer(band, ArrayDataType::Int64, cast_uninit_slice_to_byte(dst)),
            ArrayDataType::Float32 => self.read_band_into_byte_buffer(band, ArrayDataType::Float32, cast_uninit_slice_to_byte(dst)),
            ArrayDataType::Float64 => self.read_band_into_byte_buffer(band, ArrayDataType::Float64, cast_uninit_slice_to_byte(dst)),
        }
    }

    // Read a raster band region into the provided buffer
    // implemented here to avoid code duplication in implementations
    fn read_band_region<T: crate::ArrayNum>(
        &mut self,
        band: usize,
        region: &GeoReference,
        dst: &mut [MaybeUninit<T>],
    ) -> Result<GeoReference> {
        assert_eq!(T::TYPE, self.data_type(band)?);

        match T::TYPE {
            ArrayDataType::Uint8 => {
                self.read_band_region_into_byte_buffer(band, region, ArrayDataType::Uint8, cast_uninit_slice_to_byte(dst))
            }
            ArrayDataType::Uint16 => {
                self.read_band_region_into_byte_buffer(band, region, ArrayDataType::Uint16, cast_uninit_slice_to_byte(dst))
            }
            ArrayDataType::Uint32 => {
                self.read_band_region_into_byte_buffer(band, region, ArrayDataType::Uint32, cast_uninit_slice_to_byte(dst))
            }
            ArrayDataType::Uint64 => {
                self.read_band_region_into_byte_buffer(band, region, ArrayDataType::Uint64, cast_uninit_slice_to_byte(dst))
            }
            ArrayDataType::Int8 => {
                self.read_band_region_into_byte_buffer(band, region, ArrayDataType::Int8, cast_uninit_slice_to_byte(dst))
            }
            ArrayDataType::Int16 => {
                self.read_band_region_into_byte_buffer(band, region, ArrayDataType::Int16, cast_uninit_slice_to_byte(dst))
            }
            ArrayDataType::Int32 => {
                self.read_band_region_into_byte_buffer(band, region, ArrayDataType::Int32, cast_uninit_slice_to_byte(dst))
            }
            ArrayDataType::Int64 => {
                self.read_band_region_into_byte_buffer(band, region, ArrayDataType::Int64, cast_uninit_slice_to_byte(dst))
            }
            ArrayDataType::Float32 => {
                self.read_band_region_into_byte_buffer(band, region, ArrayDataType::Float32, cast_uninit_slice_to_byte(dst))
            }
            ArrayDataType::Float64 => {
                self.read_band_region_into_byte_buffer(band, region, ArrayDataType::Float64, cast_uninit_slice_to_byte(dst))
            }
        }
    }
}

// Make sure boxed trait objects are also RasterFormatGeneric
impl<R: RasterFormatDyn + ?Sized> RasterFormatGeneric for R {}

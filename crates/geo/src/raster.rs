pub mod algo;
mod denseraster;
mod denserasterconversions;
pub mod intersection;
#[cfg(feature = "gdal")]
pub mod io;

use std::path::Path;

use crate::GeoReference;

// pub mod warp;
use super::Result;

#[cfg(all(feature = "python", feature = "arrow"))]
pub mod arrow {
    // pub(super) mod arrowraster;
    // #[cfg(feature = "gdal")]
    // mod arrowrasterio;
    // mod arrowrasterops;
    pub(super) mod arrowutil;
}

// #[cfg(feature = "arrow")]
// pub use arrow::arrowraster::ArrowRaster;
// #[cfg(feature = "arrow")]
// pub use arrow::arrowraster::ArrowRasterNum;
#[doc(inline)]
pub use denseraster::DenseRaster;

#[cfg(all(feature = "python", feature = "arrow"))]
mod python;

#[cfg(all(feature = "python", feature = "arrow"))]
pub use python::pyraster::PyRaster;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum TiffChunkType {
    #[default]
    Striped,
    Tiled,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Compression {
    Lzw,
    Zstd,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Predictor {
    Horizontal,
    FloatingPoint,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GeoTiffWriteOptions {
    /// data layout of the raster (in tiles or strips)
    pub chunk_type: TiffChunkType,
    /// The compression type to use for writing the raster
    pub compression: Option<Compression>,
    /// The predictor selection to use for writing the raster (only relevant when compression is used)
    pub predictor: Option<Predictor>,
    /// Sparse files have 0 tile/strip offsets for blocks that contain only nodata  and save space
    pub sparse_ok: bool,
}

pub enum WriteRasterOptions {
    /// Write the raster with the default options
    Default,
    /// Write the raster with the provided options
    GeoTiff(GeoTiffWriteOptions),
}

/// Raster IO operations trait
pub trait RasterIO
where
    Self: Sized,
{
    /// Reads the full raster from disk
    /// No processing (cutting, resampling) is done on the raster data, the original data is returned
    fn read_band(path: impl AsRef<Path>, band_index: usize) -> Result<Self>;
    /// Same as `read_band_from_disk` but reads the first band
    fn read(path: impl AsRef<Path>) -> Result<Self>;

    /// Reads a subset of the raster from disk
    /// The provided extent does not have to be contained within the raster
    /// Areas outside of the original raster will be filled with the nodata value
    fn read_bounds(path: impl AsRef<Path>, region: &GeoReference, band_index: usize) -> Result<Self>;

    /// Write the full raster to disk (raster type is detected based on the file extension, default options are used)
    fn write(&mut self, path: impl AsRef<Path>) -> Result;

    /// Write the full raster to disk
    fn write_with_options(&mut self, path: impl AsRef<Path>, options: WriteRasterOptions) -> Result;
}

/// Trait for raster types that can handle nodata values and need te exchanged with external code
/// that does not use the default nodata value for the type.
pub trait RasterNodataCompatibility {
    /// Initialize the value with the nodata value if it matches the nodata condition
    fn init_nodata();
    /// Restore the original metadata nodata value from the metadata
    fn restore_nodata();
}

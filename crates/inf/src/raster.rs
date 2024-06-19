use num::NumCast;
use num::{Bounded, ToPrimitive};

use crate::{Error, GeoMetadata, Nodata, Result};

pub trait RasterNum<T: ToPrimitive>: Copy + num::NumCast + num::Zero + PartialEq + Bounded + Nodata<T> {}

/// A trait representing a raster.
/// A raster implementation provides access to the pixel data and the geographic metadata associated with the raster.
pub trait Raster<T: RasterNum<T>> {
    /// Create a new raster with the given metadata and data buffer.
    fn new(metadata: GeoMetadata, data: Vec<T>) -> Self
    where
        Self: Sized;

    fn from_iter<Iter>(metadata: GeoMetadata, iter: Iter) -> Self
    where
        Self: Sized,
        Iter: Iterator<Item = Option<T>>;

    /// Create a new raster with the given metadata and filled with zeros.
    fn zeros(metadata: GeoMetadata) -> Self
    where
        Self: Sized;

    /// Create a new raster with the given metadata and filled with the provided value.
    fn filled_with(val: T, metadata: GeoMetadata) -> Self
    where
        Self: Sized;

    /// Returns a reference to the geographic metadata associated with the raster.
    fn geo_metadata(&self) -> &GeoMetadata
    where
        Self: Sized;

    /// Returns the width of the raster.
    fn width(&self) -> usize;

    /// Returns the height of the raster.
    fn height(&self) -> usize;

    fn len(&self) -> usize {
        self.width() * self.height()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a mutable reference to the raster data.
    fn as_mut_slice(&mut self) -> &mut [T];

    /// Returns a reference to the raster data.
    fn as_slice(&self) -> &[T];

    /// Returns a copy of the data as a vector of optional values where None represents nodata values.
    fn masked_data(&self) -> Vec<Option<T>>;

    /// Returns the optional nodata value that is used in the raster to identify missing data.
    fn nodata_value(&self) -> Option<T>;

    /// Returns the number of nodata values in the raster
    fn nodata_count(&self) -> usize;

    /// Return true if the cell at the given index contains valid data
    fn index_has_data(&self, index: usize) -> bool;

    /// Return the value at the given index or None if the index contains nodata
    fn value(&self, index: usize) -> Option<T>;

    /// Return the sum of all the data values
    fn sum(&self) -> f64;
}

pub fn cast<TDest: RasterNum<TDest>, TSrc: RasterNum<TSrc>, RDest, RSrc>(src: &RSrc) -> RDest
where
    RDest: Raster<TDest>,
    RSrc: Raster<TSrc>,
    for<'a> &'a RSrc: IntoIterator<Item = Option<TSrc>>,
{
    RDest::from_iter(
        src.geo_metadata().copy_with_nodata(Some(TDest::nodata_value())),
        src.into_iter().map(|x| x.and_then(|x| NumCast::from(x))),
    )
}

/// A trait representing a raster io operations
pub trait RasterIO<T: RasterNum<T>, TRas: Raster<T>> {
    /// Reads the full raster from disk
    /// No processing (cutting, resampling) is done on the raster data, the original data is returned
    fn read_band(path: &std::path::Path, band_index: usize) -> Result<TRas>;
    /// Same as read_band_from_disk but reads the first band
    fn read(path: &std::path::Path) -> Result<TRas>;

    /// Reads a subset of the raster from disk
    /// The provided extent does not have to be contained within the raster
    /// Areas outside of the original raster will be filled with the nodata value
    fn read_bounds(path: &std::path::Path, region: &GeoMetadata, band_index: usize) -> Result<TRas>;

    /// Write the full raster to disk
    fn write(&mut self, path: &std::path::Path) -> Result;
}

impl RasterNum<i8> for i8 {}
impl RasterNum<u8> for u8 {}
impl RasterNum<i16> for i16 {}
impl RasterNum<u16> for u16 {}
impl RasterNum<i32> for i32 {}
impl RasterNum<u32> for u32 {}
impl RasterNum<i64> for i64 {}
impl RasterNum<u64> for u64 {}
impl RasterNum<f32> for f32 {}
impl RasterNum<f64> for f64 {}

pub fn check_dimensions<R1, R2, T1, T2>(r1: &R1, r2: &R2) -> Result<()>
where
    R1: Raster<T1>,
    R2: Raster<T2>,
    T1: RasterNum<T1>,
    T2: RasterNum<T2>,
{
    if r1.width() != r2.width() || r1.height() != r2.height() {
        return Err(Error::SizeMismatch {
            size1: (r1.width(), r1.height()),
            size2: (r2.width(), r2.height()),
        });
    }

    Ok(())
}

pub fn assert_dimensions<R1, R2, T1, T2>(r1: &R1, r2: &R2)
where
    R1: Raster<T1>,
    R2: Raster<T2>,
    T1: RasterNum<T1>,
    T2: RasterNum<T2>,
{
    assert_eq!(r1.width(), r2.width(), "Raster widths do not match");
    assert_eq!(r1.height(), r2.height(), "Raster heights do not match");
}

// Submodule configuration
mod denseraster;
#[cfg(feature = "gdal")]
pub mod denserasterio;
mod denserasterops;

#[cfg(feature = "arrow")]
pub mod arrow {
    pub(super) mod arrowraster;
    #[cfg(feature = "gdal")]
    mod arrowrasterio;
    mod arrowrasterops;
    mod arrowutil;
}

#[cfg(feature = "arrow")]
pub use arrow::arrowraster::ArrowRaster;
#[cfg(feature = "arrow")]
pub use arrow::arrowraster::ArrowRasterNum;
pub use denseraster::DenseRaster;

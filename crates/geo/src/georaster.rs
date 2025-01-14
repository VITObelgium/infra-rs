#[cfg(feature = "gdal")]
pub mod algo;
mod cell;
mod densegeoraster;
mod densegeorasterops;
mod denserasterconversions;
#[cfg(feature = "gdal")]
mod denserasterio;
#[cfg(feature = "gdal")]
pub mod io;
#[cfg(feature = "gdal")]
mod rasteriotests;
mod rastertests;
#[cfg(test)]
mod testutils;
use crate::GeoReference;

// pub mod warp;
use super::Result;

#[cfg(feature = "arrow")]
pub mod arrow {
    pub(super) mod arrowraster;
    #[cfg(feature = "gdal")]
    mod arrowrasterio;
    mod arrowrasterops;
    pub(super) mod arrowutil;
}

#[cfg(feature = "arrow")]
pub use arrow::arrowraster::ArrowRaster;
#[cfg(feature = "arrow")]
pub use arrow::arrowraster::ArrowRasterNum;
pub use cell::Cell;
pub use cell::CellIterator;
#[doc(inline)]
pub use densegeoraster::DenseGeoRaster;

#[cfg(all(feature = "python", feature = "arrow"))]
mod python;
use num::NumCast;
#[cfg(all(feature = "python", feature = "arrow"))]
pub use python::pyraster::PyRaster;
use raster::Raster;
use raster::RasterNum;

pub fn cast<TDest: RasterNum<TDest>, TSrc: RasterNum<TSrc>, RDest, RSrc>(src: &RSrc) -> RDest
where
    RDest: GeoRaster<TDest> + GeoRasterCreation<TDest>,
    RSrc: GeoRaster<TSrc>,
    for<'a> &'a RSrc: IntoIterator<Item = Option<TSrc>>,
{
    RDest::from_iter(
        src.geo_metadata().copy_with_nodata(Some(TDest::nodata_value())),
        src.into_iter().map(|x| x.and_then(|x| NumCast::from(x))),
    )
}

pub trait GeoRaster<T: RasterNum<T>>: Raster<T> {
    fn geo_metadata(&self) -> &GeoReference;
}

pub trait GeoRasterCreation<T: RasterNum<T>> {
    /// Create a new raster with the given metadata and data buffer.
    fn new(meta: GeoReference, data: Vec<T>) -> Self;

    fn from_iter<Iter>(meta: GeoReference, iter: Iter) -> Self
    where
        Iter: Iterator<Item = Option<T>>;

    /// Create a new raster with the given metadata and filled with zeros.
    fn zeros(meta: GeoReference) -> Self;

    /// Create a new raster with the given metadata and filled with the provided value.
    fn filled_with(val: T, meta: GeoReference) -> Self;

    /// Create a new raster filled with nodata.
    fn filled_with_nodata(meta: GeoReference) -> Self;
}

/// A trait representing a raster io operations
pub trait RasterIO<T: RasterNum<T>, TRas: Raster<T>> {
    /// Reads the full raster from disk
    /// No processing (cutting, resampling) is done on the raster data, the original data is returned
    fn read_band(path: &std::path::Path, band_index: usize) -> Result<TRas>;
    /// Same as `read_band_from_disk` but reads the first band
    fn read(path: &std::path::Path) -> Result<TRas>;

    /// Reads a subset of the raster from disk
    /// The provided extent does not have to be contained within the raster
    /// Areas outside of the original raster will be filled with the nodata value
    fn read_bounds(path: &std::path::Path, region: &GeoReference, band_index: usize) -> Result<TRas>;

    /// Write the full raster to disk
    fn write(&mut self, path: &std::path::Path) -> Result;
}

// pub fn check_dimensions<R1, R2, T1, T2>(r1: &R1, r2: &R2) -> Result<()>
// where
//     R1: Raster<T1>,
//     R2: Raster<T2>,
//     T1: RasterNum<T1>,
//     T2: RasterNum<T2>,
// {
//     if r1.width() != r2.width() || r1.height() != r2.height() {
//         return Err(Error::SizeMismatch {
//             size1: (r1.width(), r1.height()),
//             size2: (r2.width(), r2.height()),
//         });
//     }

//     Ok(())
// }

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

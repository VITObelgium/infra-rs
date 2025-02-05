pub mod algo;
mod denseraster;
mod denserasterconversions;
#[cfg(feature = "gdal")]
pub mod io;

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

// pub fn cast<TDest: RasterNum<TDest>, TSrc: RasterNum<TSrc>, RDest, RSrc>(src: &RSrc) -> RDest
// where
//     RDest: GeoRaster<TDest> + GeoRasterCreation<TDest>,
//     RSrc: GeoRaster<TSrc>,
//     for<'a> &'a RSrc: IntoIterator<Item = Option<TSrc>>,
// {
//     RDest::from_iter(
//         src.geo_reference().copy_with_nodata(Some(TDest::nodata_value())),
//         src.into_iter().map(|x| x.and_then(|x| NumCast::from(x))),
//     )
// }

/// A trait representing a raster io operations
pub trait RasterIO
where
    Self: Sized,
{
    /// Reads the full raster from disk
    /// No processing (cutting, resampling) is done on the raster data, the original data is returned
    fn read_band(path: &std::path::Path, band_index: usize) -> Result<Self>;
    /// Same as `read_band_from_disk` but reads the first band
    fn read(path: &std::path::Path) -> Result<Self>;

    /// Reads a subset of the raster from disk
    /// The provided extent does not have to be contained within the raster
    /// Areas outside of the original raster will be filled with the nodata value
    fn read_bounds(path: &std::path::Path, region: &GeoReference, band_index: usize) -> Result<Self>;

    /// Write the full raster to disk
    fn write(&mut self, path: &std::path::Path) -> Result;
}

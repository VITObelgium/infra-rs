#![warn(clippy::unwrap_used)]
extern crate approx;

pub type Error = inf::Error;
pub type Result<T = ()> = std::result::Result<T, Error>;

pub mod algo;
mod denseraster;
mod denserasterio;
mod denserasterops;
pub mod io;
mod nodata;
pub mod raster;
mod rasteriotests;
mod rastertests;
#[cfg(test)]
mod testutils;

#[cfg(feature = "arrow")]
pub mod arrow {
    pub(super) mod arrowraster;
    mod arrowrasterio;
    mod arrowrasterops;
    mod arrowutil;
}

#[cfg(feature = "arrow")]
pub use arrow::arrowraster::ArrowRaster;
#[cfg(feature = "arrow")]
pub use arrow::arrowraster::ArrowRasterNum;
#[doc(inline)]
pub use denseraster::DenseRaster;
pub use nodata::Nodata;
#[doc(inline)]
pub use raster::Raster;
#[doc(inline)]
pub use raster::RasterIO;
#[doc(inline)]
pub use raster::RasterNum;

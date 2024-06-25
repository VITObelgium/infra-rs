#![warn(clippy::unwrap_used)]
extern crate approx;

pub type Error = inf::Error;
pub type Result<T = ()> = inf::Result<T>;
pub trait RasterNum<T: num::ToPrimitive>:
    Copy + PartialEq + num::NumCast + num::Zero + num::Bounded + Nodata<T>
{
}

pub mod algo;
mod denseraster;
mod denserasterio;
mod denserasterops;
pub mod io;
mod nodata;
mod raster;
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
pub use raster::cast;
#[doc(inline)]
pub use raster::Raster;
#[doc(inline)]
pub use raster::RasterIO;

#[cfg(all(feature = "python", feature = "arrow"))]
mod python;
#[cfg(all(feature = "python", feature = "arrow"))]
pub use python::pyraster::PyRaster;

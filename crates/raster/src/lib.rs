pub mod algo;
mod anydenseraster;
mod anydenserasterops;
mod datatype;
mod denseraster;
mod denserasterops;
mod nodata;
pub mod ops;
mod raster;
mod rasternum;

#[cfg(test)]
mod rastertests;
#[cfg(test)]
mod testutils;

use thiserror::Error;

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
}

pub use anydenseraster::AnyDenseRaster;
pub use datatype::RasterDataType;
pub use denseraster::DenseRaster;
pub use nodata::Nodata;
pub use raster::Raster;
pub use raster::RasterCreation;
pub use raster::RasterSize;
pub use rasternum::RasterNum;

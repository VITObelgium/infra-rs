extern crate approx;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[cfg(feature = "gdal")]
    #[error("GDAL error: {0}")]
    GdalError(#[from] gdal::errors::GdalError),
    #[error("Raster dimensions do not match ({}x{}) <-> ({}x{})", .size1.0, .size1.1, .size2.0, .size2.1)]
    SizeMismatch {
        size1: (usize, usize),
        size2: (usize, usize),
    },
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Runtime error: {0}")]
    Runtime(String),
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("Invalid string: {0}")]
    InvalidString(#[from] std::ffi::NulError),
}

pub type Result<T = ()> = std::result::Result<T, Error>;

#[cfg(feature = "arrow")]
mod arrowraster;
#[cfg(feature = "arrow")]
pub mod arrowrasterio;
#[cfg(feature = "arrow")]
mod arrowutil;
pub mod cast;
mod cell;
pub mod color;
pub mod colormap;
mod coordinate;
#[cfg(feature = "gdal")]
mod coordinatetransformer;
pub mod crs;
mod denseraster;
#[cfg(feature = "gdal")]
pub mod denserasterio;
pub mod fs;
#[cfg(feature = "gdal")]
pub mod gdalinterop;
pub mod geoconstants;
pub mod geometadata;
pub mod interpolate;
pub mod latlonbounds;
pub mod legend;
pub mod legendscaletype;
mod nodata;
pub mod raster;
#[cfg(feature = "gdal")]
pub mod rasteralgo;
#[cfg(feature = "gdal")]
pub mod rasterio;
mod rasteriotests;
mod rastertests;
pub mod rect;
#[cfg(feature = "gdal")]
pub mod spatialreference;
#[cfg(feature = "sqlite3")]
pub mod sqliteconnection;
#[cfg(test)]
mod testutils;
pub mod tile;

#[cfg(feature = "arrow")]
pub use arrowraster::ArrowRaster;
#[cfg(feature = "arrow")]
pub use arrowraster::ArrowRasterNum;
pub use cell::Cell;
#[doc(inline)]
pub use color::Color;
pub use coordinate::Coordinate;
#[cfg(feature = "gdal")]
pub use coordinatetransformer::CoordinateTransformer;
pub use denseraster::DenseRaster;
#[doc(inline)]
pub use geometadata::CellSize;
#[doc(inline)]
pub use geometadata::GeoMetadata;
#[doc(inline)]
pub use geometadata::RasterSize;
#[doc(inline)]
pub use latlonbounds::LatLonBounds;
#[doc(inline)]
pub use legend::Legend;
#[doc(inline)]
pub use legend::MappedLegend;
pub use nodata::Nodata;
#[doc(inline)]
pub use raster::Raster;
#[doc(inline)]
pub use raster::RasterIO;
#[doc(inline)]
pub use raster::RasterNum;
#[doc(inline)]
pub use rect::Rect;
#[cfg(feature = "sqlite3")]
pub use sqliteconnection::SqliteConnection;
#[doc(inline)]
pub use tile::Tile;

pub type Point<T = f64> = geo_types::Point<T>;

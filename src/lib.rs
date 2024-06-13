extern crate approx;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[cfg(feature = "gdal")]
    #[error("GDAL error")]
    GdalError(#[from] gdal::errors::GdalError),
    #[error("Raster dimensions do not match ({}x{}) <-> ({}x{})", .size1.0, .size1.1, .size2.0, .size2.1)]
    SizeMismatch { size1: (usize, usize), size2: (usize, usize) },
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Runtime error: {0}")]
    Runtime(String),
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("Invalid string: {0}")]
    InvalidString(#[from] std::ffi::NulError),
}

pub type Result<T> = std::result::Result<T, Error>;

pub mod cell;
pub mod color;
pub mod colormap;
pub mod coordinate;
#[cfg(feature = "gdal")]
mod coordinatetransformer;
pub mod crs;
pub mod denseraster;
#[cfg(feature = "gdal")]
pub mod denserasterio;
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
pub mod rect;
#[cfg(feature = "gdal")]
pub mod spatialreference;
#[cfg(feature = "sqlite3")]
pub mod sqliteconnection;
pub mod tile;

pub use cell::Cell;
pub use color::Color;
pub use coordinate::Coordinate;
#[cfg(feature = "gdal")]
pub use coordinatetransformer::CoordinateTransformer;
pub use denseraster::DenseRaster;
pub use geo_types::Point;
pub use geometadata::CellSize;
pub use geometadata::GeoMetadata;
pub use geometadata::RasterSize;
pub use latlonbounds::LatLonBounds;
pub use legend::Legend;
pub use legend::MappedLegend;
pub use nodata::Nodata;
pub use raster::Raster;
pub use raster::RasterNum;
pub use rect::Rect;
#[cfg(feature = "sqlite3")]
pub use sqliteconnection::SqliteConnection;
pub use tile::Tile;

pub fn to_coordinate(p: Point<f64>) -> Coordinate {
    Coordinate::latlon(p.y(), p.x())
}

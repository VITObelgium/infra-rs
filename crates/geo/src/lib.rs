#![warn(clippy::unwrap_used)]

pub type Result<T = ()> = std::result::Result<T, Error>;
pub mod constants;
mod coordinate;
mod coordinatetransformer;
pub mod crs;
mod error;
mod gdalinterop;
mod latlonbounds;
mod metadata;
pub mod rect;
mod runtimeconfiguration;
mod spatialreference;
mod tile;

pub mod raster;
pub mod vector;

pub use coordinate::Coordinate;
#[cfg(feature = "gdal")]
pub use coordinatetransformer::CoordinateTransformer;
#[doc(inline)]
pub use error::Error;
#[doc(inline)]
pub use latlonbounds::LatLonBounds;
#[doc(inline)]
pub use metadata::CellSize;
#[doc(inline)]
pub use metadata::GeoReference;
#[doc(inline)]
pub use metadata::RasterSize;
#[doc(inline)]
pub use rect::Rect;
pub use runtimeconfiguration::RuntimeConfiguration;
#[cfg(feature = "gdal")]
#[doc(inline)]
pub use spatialreference::SpatialReference;
use thiserror::Error;
#[doc(inline)]
pub use tile::Tile;

pub type Point<T = f64> = geo_types::Point<T>;

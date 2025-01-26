#![warn(clippy::unwrap_used)]

pub type Result<T = ()> = std::result::Result<T, Error>;
pub mod constants;
mod coordinate;
#[cfg(feature = "gdal")]
mod coordinatetransformer;
pub mod crs;
mod error;
#[cfg(feature = "gdal")]
pub mod gdalinterop;
mod georeference;
mod latlonbounds;
pub mod rect;
#[cfg(feature = "gdal")]
mod runtimeconfiguration;
#[cfg(feature = "gdal")]
mod spatialreference;
mod tile;

pub mod georaster;

#[cfg(feature = "vector")]
pub mod vector;

pub use coordinate::Coordinate;
#[cfg(feature = "gdal")]
pub use coordinatetransformer::CoordinateTransformer;
#[doc(inline)]
pub use error::Error;
#[doc(inline)]
pub use georeference::CellSize;
#[doc(inline)]
pub use georeference::GeoReference;
#[doc(inline)]
pub use latlonbounds::LatLonBounds;
#[doc(inline)]
pub use rect::Rect;
#[cfg(feature = "gdal")]
pub use runtimeconfiguration::RuntimeConfiguration;
#[cfg(feature = "gdal")]
#[doc(inline)]
pub use spatialreference::SpatialReference;
use thiserror::Error;
#[doc(inline)]
pub use tile::Tile;
#[doc(inline)]
pub use tile::ZoomLevelStrategy;

pub type Point<T = f64> = geo_types::Point<T>;

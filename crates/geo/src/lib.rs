#![warn(clippy::unwrap_used)]

pub type Error = inf::Error;
pub type Result<T = ()> = inf::Result<T>;
pub mod constants;
mod coordinate;
mod coordinatetransformer;
pub mod crs;
mod gdalinterop;
mod latlonbounds;
mod metadata;
mod runtimeconfiguration;
mod spatialreference;
mod tile;

pub mod raster;
pub mod vector;

pub use coordinate::Coordinate;
#[cfg(feature = "gdal")]
pub use coordinatetransformer::CoordinateTransformer;
#[doc(inline)]
pub use latlonbounds::LatLonBounds;
#[doc(inline)]
pub use metadata::CellSize;
#[doc(inline)]
pub use metadata::GeoMetadata;
#[doc(inline)]
pub use metadata::RasterSize;
pub use runtimeconfiguration::RuntimeConfiguration;
#[cfg(feature = "gdal")]
#[doc(inline)]
pub use spatialreference::SpatialReference;
#[doc(inline)]
pub use tile::Tile;

pub type Point<T = f64> = geo_types::Point<T>;

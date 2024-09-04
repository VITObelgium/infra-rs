#![warn(clippy::unwrap_used)]
extern crate approx;

pub use error::Error;
pub type Result<T = ()> = std::result::Result<T, Error>;

pub mod cast;
mod cell;
pub mod color;
pub mod colormap;
mod coordinate;
#[cfg(feature = "gdal")]
mod coordinatetransformer;
pub mod crs;
pub mod duration;
mod error;
pub mod fs;
#[cfg(feature = "gdal")]
pub mod gdalinterop;
pub mod geoconstants;
pub mod geometadata;
pub mod interpolate;
pub mod latlonbounds;
pub mod legend;
pub mod legendscaletype;
pub mod rect;
#[cfg(feature = "gdal")]
pub mod spatialreference;
pub mod tile;
pub use cell::Cell;
pub use cell::CellIterator;
#[doc(inline)]
pub use color::Color;
pub use coordinate::Coordinate;
#[cfg(feature = "gdal")]
pub use coordinatetransformer::CoordinateTransformer;
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
#[doc(inline)]
pub use rect::Rect;
#[cfg(feature = "gdal")]
#[doc(inline)]
pub use spatialreference::SpatialReference;
#[doc(inline)]
pub use tile::Tile;

pub type Point<T = f64> = geo_types::Point<T>;

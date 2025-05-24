#![warn(clippy::unwrap_used)]

pub type Result<T = ()> = std::result::Result<T, Error>;
mod anydensearray;
mod anydensearrayops;
mod array;
mod arraydatatype;
mod arraynum;
pub mod arrayops;
mod cell;
pub mod constants;
mod coordinate;
#[cfg(feature = "gdal")]
mod coordinatetransformer;
pub mod crs;
mod densearray;
#[cfg(feature = "gdal")]
mod densearrayio;
mod densearrayiterators;
mod densearrayops;
pub(crate) mod densearrayutil;
mod error;
#[cfg(feature = "gdal")]
pub mod gdalinterop;
mod georeference;
mod latlonbounds;
mod nodata;
pub mod raster;
#[cfg(feature = "gdal")]
mod rasteriotests;
mod rastersize;
pub mod rect;
#[cfg(feature = "gdal")]
mod runtimeconfiguration;
#[cfg(feature = "gdal")]
mod spatialreference;
mod tile;
pub mod tileutils;
#[cfg(feature = "vector")]
pub mod vector;

#[cfg(test)]
mod arraytests;
#[cfg(test)]
pub mod testutils;

use thiserror::Error;

#[cfg(feature = "gdal")]
#[doc(inline)]
pub use {coordinatetransformer::CoordinateTransformer, runtimeconfiguration::RuntimeConfiguration, spatialreference::SpatialReference};

#[doc(inline)]
pub use {
    anydensearray::AnyDenseArray, array::Array, array::ArrayCopy, array::ArrayMetadata, array::Columns, array::Rows, array::Window,
    arraydatatype::ArrayDataType, arraynum::ArrayNum, cell::Cell, cell::CellIterator, coordinate::Coordinate, densearray::DenseArray,
    error::Error, georeference::CellSize, georeference::GeoReference, latlonbounds::LatLonBounds, nodata::Nodata, rastersize::RasterSize,
    rect::Rect, tile::Tile, tile::ZoomLevelStrategy,
};

pub type Point<T = f64> = geo_types::Point<T>;

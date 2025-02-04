#![warn(clippy::unwrap_used)]

pub type Result<T = ()> = std::result::Result<T, Error>;
mod anydensearray;
mod anydensearrayops;
mod array;
mod arrayops;
mod cell;
pub mod constants;
mod coordinate;
#[cfg(feature = "gdal")]
mod coordinatetransformer;
pub mod crs;
mod datatype;
mod densearray;
mod densearrayops;
mod error;
#[cfg(feature = "gdal")]
pub mod gdalinterop;
mod georeference;
mod latlonbounds;
mod nodata;
pub mod raster;
mod rasternum;
mod rastersize;
pub mod rect;
#[cfg(feature = "gdal")]
mod runtimeconfiguration;
#[cfg(feature = "gdal")]
mod spatialreference;
mod tile;
#[cfg(feature = "vector")]
pub mod vector;

#[cfg(test)]
mod arraytests;
#[cfg(test)]
mod testutils;

use thiserror::Error;

#[cfg(feature = "gdal")]
#[doc(inline)]
pub use {coordinatetransformer::CoordinateTransformer, runtimeconfiguration::RuntimeConfiguration, spatialreference::SpatialReference};

#[doc(inline)]
pub use {
    anydensearray::AnyDenseArray, array::Array, array::ArrayCopy, array::ArrayCreation, array::ArrayMetadata, cell::Cell,
    cell::CellIterator, coordinate::Coordinate, datatype::RasterDataType, densearray::DenseArray, error::Error, georeference::CellSize,
    georeference::GeoReference, latlonbounds::LatLonBounds, nodata::Nodata, rasternum::RasterNum, rastersize::RasterSize, rect::Rect,
    tile::Tile, tile::ZoomLevelStrategy,
};

pub type Point<T = f64> = geo_types::Point<T>;

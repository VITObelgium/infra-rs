#![cfg_attr(feature = "allocate", feature(allocator_api))]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub type Result<T = ()> = std::result::Result<T, Error>;
#[macro_use]
mod anydensearray_macros;
mod anydensearray;
pub mod anydensearrayalgo;
mod anydensearrayops;
mod array;
mod arraydatatype;
mod arraynum;
pub mod arrayops;
pub mod bandindex;
mod cell;
pub mod cog;
pub mod constants;
mod coordinate;
pub mod crs;
mod densearray;
mod densearrayio;
mod densearrayiterators;
mod densearrayops;
pub(crate) mod densearrayutil;
mod error;
#[cfg(feature = "gdal")]
#[cfg_attr(docsrs, doc(cfg(feature = "gdal")))]
pub mod gdalinterop;
mod georeference;
#[cfg(feature = "raster-io-geotiff")]
#[cfg_attr(docsrs, doc(cfg(feature = "raster-io-geotiff")))]
pub mod geotiff;
mod geotransform;
mod latlonbounds;
mod nodata;
mod point;
pub mod raster;
#[cfg(feature = "gdal")]
#[cfg_attr(docsrs, doc(cfg(feature = "gdal")))]
mod rasteriotests;
mod rastermetadata;
mod rasterscale;
mod rastersize;
mod rect;
#[cfg(feature = "gdal")]
#[cfg_attr(docsrs, doc(cfg(feature = "gdal")))]
mod runtimeconfiguration;
pub mod simd;
pub mod srs;
mod tile;
pub mod tileutils;
pub mod vector;

#[cfg(test)]
mod arraytests;
#[cfg(test)]
pub mod testutils;

use thiserror::Error;

#[cfg(feature = "gdal")]
#[cfg_attr(docsrs, doc(cfg(feature = "gdal")))]
#[doc(inline)]
pub use runtimeconfiguration::RuntimeConfiguration;

#[doc(inline)]
pub use bandindex::{BandIndex, FIRST_BAND};

#[doc(inline)]
pub use {
    anydensearray::AnyDenseArray, array::Array, array::ArrayCopy, array::ArrayInterop, array::ArrayMetadata, array::Columns,
    array::RasterWindow, array::Rows, arraydatatype::ArrayDataType, arraynum::ArrayNum, cell::Cell, cell::CellIterator,
    coordinate::Coordinate, densearray::DenseArray, error::Error, georeference::CellSize, georeference::GeoReference,
    geotransform::GeoTransform, latlonbounds::LatLonBounds, nodata::Nodata, raster::RasterNodataCompatibility,
    rastermetadata::RasterMetadata, rasterscale::RasterScale, rastersize::RasterSize, rect::Rect, tile::Tile, tile::ZoomLevelStrategy,
};

#[doc(inline)]
pub use point::Point;

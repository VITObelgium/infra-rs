#![cfg_attr(feature = "simd", feature(portable_simd, allocator_api))]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub type Result<T = ()> = std::result::Result<T, Error>;
mod anydensearray;
pub mod anydensearrayalgo;
mod anydensearrayops;
mod array;
mod arraydatatype;
mod arraynum;
pub mod arrayops;
mod cell;
#[cfg(feature = "raster-io-geotiff")]
#[cfg_attr(docsrs, doc(cfg(feature = "raster-io-geotiff")))]
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
mod rastersize;
mod rect;
#[cfg(feature = "gdal")]
#[cfg_attr(docsrs, doc(cfg(feature = "gdal")))]
mod runtimeconfiguration;
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
pub use {
    anydensearray::AnyDenseArray, array::Array, array::ArrayCopy, array::ArrayInterop, array::ArrayMetadata, array::Columns,
    array::RasterWindow, array::Rows, arraydatatype::ArrayDataType, arraynum::ArrayNum, arraynum::ArrayNumScalar, cell::Cell,
    cell::CellIterator, coordinate::Coordinate, densearray::DenseArray, error::Error, georeference::CellSize, georeference::GeoReference,
    geotransform::GeoTransform, latlonbounds::LatLonBounds, nodata::Nodata, raster::RasterNodataCompatibility,
    rastermetadata::RasterMetadata, rastersize::RasterSize, rect::Rect, tile::Tile, tile::ZoomLevelStrategy,
};

#[doc(inline)]
#[cfg(feature = "simd")]
#[cfg_attr(docsrs, doc(cfg(feature = "simd")))]
pub use arraynum::ArrayNumSimd;

#[doc(inline)]
#[cfg(feature = "simd")]
#[cfg_attr(docsrs, doc(cfg(feature = "simd")))]
pub use nodata::simd::NodataSimd;

pub use simd_macro::geo_simd_bounds as simd_bounds;

#[doc(inline)]
pub use point::Point;

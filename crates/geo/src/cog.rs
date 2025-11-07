//! Cloud Optimized GeoTIFF (COG) support and web tile utilities.

#[cfg(all(feature = "gdal", feature = "raster-io-geotiff"))]
#[cfg_attr(docsrs, doc(cfg(all(feature = "gdal", feature = "raster-io-geotiff"))))]
pub mod debug;

#[cfg(feature = "gdal")]
mod creation;

#[cfg(feature = "gdal")]
#[cfg_attr(docsrs, doc(cfg(feature = "gdal")))]
pub use creation::{CogCreationOptions, PredictorSelection, create_cog_tiles, create_gdal_args, create_multiband_cog_tiles};

#[cfg(feature = "raster-io-geotiff")]
#[cfg_attr(docsrs, doc(cfg(feature = "raster-io-geotiff")))]
mod webtiles;

#[cfg(feature = "raster-io-geotiff")]
#[cfg_attr(docsrs, doc(cfg(feature = "raster-io-geotiff")))]
pub use webtiles::{TileSource, WebTileInfo, WebTilesReader};

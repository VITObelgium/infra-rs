//! Cloud Optimized GeoTIFF (COG) support and web tile utilities.

#[cfg(feature = "gdal")]
pub mod debug;

#[cfg(feature = "gdal")]
mod creation;
mod webtiles;

pub use webtiles::{TileSource, WebTileInfo, WebTilesReader};

#[cfg(feature = "gdal")]
pub use creation::{CogCreationOptions, PredictorSelection, create_cog_tiles};

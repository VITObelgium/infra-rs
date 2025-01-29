#![warn(clippy::unwrap_used)]

mod layermetadata;
mod pixelformat;
mod tiledata;
mod tilediff;
mod tileformat;
mod tileio;
mod tileprovider;
pub mod tileproviderfactory;

mod directorytileprovider;
mod dynamictileprovider;
mod imageprocessing;
mod mbtilestileprovider;
mod rasterprocessing;
mod warpingtileprovider;

pub use directorytileprovider::DirectoryTileProvider;
pub use dynamictileprovider::DynamicTileProvider;
pub use layermetadata::LayerId;
pub use layermetadata::LayerMetadata;
pub use layermetadata::LayerSourceType;
pub use layermetadata::TileJson;
pub use pixelformat::PixelFormat;
use thiserror::Error;
pub use tiledata::TileData;
pub use tileformat::TileFormat;
pub use tileprovider::ColorMappedTileRequest;
pub use tileprovider::TileProvider;
pub use tileprovider::TileRequest;

pub use warpingtileprovider::WarpingTileProvider;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Runtime error: {0}")]
    Runtime(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Geo error: {0}")]
    GeoError(#[from] geo::Error),
    #[error("Error: {0}")]
    InfError(#[from] inf::Error),
    #[error("Sqlite error: {0}")]
    SqliteError(#[from] sqlite::Error),
    #[error("GDAL error: {0}")]
    GdalError(#[from] gdal::errors::GdalError),
    #[error("System time error")]
    TimeError(#[from] std::time::SystemTimeError),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[cfg(feature = "vector-tiles")]
    #[error("MVT error: {0}")]
    MvtError(#[from] mvt::Error),
    #[error("Raster tile error: {0}")]
    RasterTileError(#[from] raster_tile::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

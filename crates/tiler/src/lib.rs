#![warn(clippy::unwrap_used)]

mod layermetadata;
mod tiledata;
mod tileformat;
mod tileprovider;
pub mod tileproviderfactory;

mod directorytileprovider;
mod imageprocessing;
mod mbtilestileprovider;
mod rasterprocessing;
mod warpingtileprovider;

pub use directorytileprovider::DirectoryTileProvider;
pub use layermetadata::LayerId;
pub use layermetadata::LayerMetadata;
pub use layermetadata::TileJson;
use thiserror::Error;
pub use tiledata::TileData;
pub use tileformat::TileFormat;
pub use tileprovider::TileProvider;

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
}

pub type Result<T> = std::result::Result<T, Error>;

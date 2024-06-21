#![warn(clippy::unwrap_used)]

use std::{num::ParseIntError, time::SystemTimeError};

pub use layermetadata::LayerId;
use thiserror::Error;

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
#[cfg(feature = "rest")]
use layermetadata::layer_metadata_to_tile_json;
pub use layermetadata::LayerMetadata;
pub use layermetadata::TileJson;
pub use tiledata::TileData;
pub use tileformat::TileFormat;
pub use tileprovider::TileProvider;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Error")]
    InfError(#[from] inf::Error),
    #[error("GDAL error")]
    GdalError(#[from] gdal::errors::GdalError),
    #[error("System time error")]
    TimeError(#[from] SystemTimeError),
    #[error("IO error")]
    IOError(#[from] std::io::Error),
    #[error("Raster dimensions do not match ({}x{}) <-> ({}x{})", .size1.0, .size1.1, .size2.0, .size2.1)]
    SizeMismatch { size1: (usize, usize), size2: (usize, usize) },
    #[error("Invalid layer id: {0}")]
    InvalidLayer(LayerId),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Invalid number")]
    InvalidNumber(#[from] ParseIntError),
    #[error("Runtime error: {0}")]
    Runtime(String),
}

pub type Result<T> = std::result::Result<T, Error>;

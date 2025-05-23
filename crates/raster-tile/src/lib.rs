mod lz4;
mod rastertile;
mod tileheader;
pub mod utils;
use thiserror::Error;

pub use rastertile::RasterTileCastIO;
pub use rastertile::RasterTileIO;
pub use tileheader::CompressionAlgorithm;
pub use tileheader::RASTER_TILE_SIGNATURE;
pub use tileheader::TileHeader;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Error: {0}")]
    Runtime(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Error: {0}")]
    InfError(#[from] inf::Error),
    #[error("Error: {0}")]
    GeoError(#[from] geo::Error),
}

pub type Result<T = ()> = std::result::Result<T, Error>;

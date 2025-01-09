mod datatype;
mod lz4;
mod rastertile;
mod tileheader;
use thiserror::Error;

pub use datatype::RasterTileDataType;
pub use datatype::TileDataType;
pub use rastertile::AnyRasterTile;
pub use rastertile::RasterTile;
pub use tileheader::CompressionAlgorithm;
pub use tileheader::TileHeader;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
}

pub type Result<T = ()> = std::result::Result<T, Error>;

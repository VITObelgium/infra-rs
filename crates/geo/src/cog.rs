#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Compression {
    Lzw,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Predictor {
    Horizontal,
    FloatingPoint,
}

#[cfg(feature = "gdal")]
mod creation;
pub mod io;
mod reader;
mod stats;
mod utils;

pub use reader::{CogAccessor, CogMetadata, CogTileLocation, TileOffsets};
pub use stats::CogStats;
pub use utils::HorizontalUnpredictable;

#[cfg(feature = "gdal")]
pub use creation::{CogCreationOptions, create_cog_tiles};

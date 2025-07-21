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
pub mod debug;

#[cfg(feature = "gdal")]
mod creation;
mod decoder;
pub mod io;
mod projectioninfo;
mod reader;
mod stats;
mod utils;
mod webtiles;

use projectioninfo::ProjectionInfo;

pub use reader::{CogAccessor, CogMetadata, CogTileLocation};
pub use stats::CogStats;
pub use utils::HorizontalUnpredictable;
pub use webtiles::WebTilesReader;

#[cfg(feature = "gdal")]
pub use creation::{CogCreationOptions, PredictorSelection, create_cog_tiles};

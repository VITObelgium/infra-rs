//! GeoTIFF format reading, writing, and processing capabilities.

mod decoder;
mod gdalghostdata;
pub mod io;
mod metadata;
mod projectioninfo;
mod reader;
mod stats;
pub mod tileio;
pub(crate) mod utils;

use projectioninfo::ProjectionInfo;

pub use metadata::GeoTiffMetadata;
pub use reader::{ChunkDataLayout, GeoTiffReader, TiffChunkLocation, TiffOverview};
pub use stats::TiffStats;

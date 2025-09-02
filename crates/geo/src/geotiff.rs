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
mod writer;

use projectioninfo::ProjectionInfo;

pub use metadata::GeoTiffMetadata;
pub use reader::{ChunkDataLayout, GeoTiffReader, TiffChunkLocation, TiffOverview};
pub use stats::TiffStats;
pub use writer::write_geotiff_band;

//! GeoTIFF format reading, writing, and processing capabilities.

mod decoder;
mod gdalghostdata;
mod gdalmetadata;
pub mod io;
mod metadata;
mod projectioninfo;
mod reader;
pub mod tileio;
pub(crate) mod utils;

use projectioninfo::ProjectionInfo;

pub use gdalmetadata::{BandMetadata, GdalMetadata, TiffStats};
pub use metadata::{GeoTiffMetadata, ParseFromBufferError};
pub use reader::{ChunkDataLayout, GeoTiffReader, TiffChunkLocation, TiffOverview};

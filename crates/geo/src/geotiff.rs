//! GeoTIFF format reading, writing, and processing capabilities.

mod cog_assembler;
mod decoder;
mod gdalghostdata;
mod gdalmetadata;
pub mod io;
mod metadata;
mod projectioninfo;
mod reader;
pub mod tileio;
pub(crate) mod utils;

pub use crate::bandindex::{BandIndex, FIRST_BAND};

use projectioninfo::ProjectionInfo;

pub use cog_assembler::assemble_band_cogs;
pub use gdalmetadata::{BandMetadata, GdalMetadata, TiffStats};
pub use metadata::{GeoTiffMetadata, ParseFromBufferError};
pub use reader::{ChunkDataLayout, GeoTiffReader, TiffChunkLocation, TiffOverview};

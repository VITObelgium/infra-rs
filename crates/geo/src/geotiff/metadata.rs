use std::fs::File;
use std::path::Path;

use crate::geotiff::reader::PyramidInfo;
use crate::geotiff::{ChunkDataLayout, decoder::TiffDecoder, io::CogHeaderReader};
use crate::geotiff::{Compression, Predictor, TiffStats, io};
use crate::{ArrayDataType, Error, GeoReference, Result};

#[derive(Debug, Clone)]
pub struct GeoTiffMetadata {
    pub data_layout: ChunkDataLayout,
    pub band_count: u32,
    pub data_type: ArrayDataType,
    pub compression: Option<Compression>,
    pub predictor: Option<Predictor>,
    pub statistics: Option<TiffStats>,
    pub geo_reference: GeoReference,
    pub pyramids: Vec<PyramidInfo>,
}

impl GeoTiffMetadata {
    pub fn from_file(path: &Path) -> Result<Self> {
        let mut buffer_factor = 1;
        // This could be improved to reuse the existing buffer and append to it when the buffer is not large enough
        loop {
            let reader = CogHeaderReader::from_stream(File::open(path)?, io::COG_HEADER_SIZE * buffer_factor)?;
            let mut decoder = TiffDecoder::new(reader)?;

            let res = decoder.parse_cog_header();
            match res {
                Err(Error::IOError(io_err) | Error::TiffError(tiff::TiffError::IoError(io_err)))
                    if io_err.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    // If the error is an EOF, we need more data to parse the header
                    buffer_factor *= 2;
                    log::debug!("Cog header dit not fit in default header size, retry with header size factor {buffer_factor}");
                }
                Ok(meta) => return Ok(meta),
                Err(e) => return Err(e),
            }
        }
    }

    pub fn from_buffer(buf: Vec<u8>) -> Result<Self> {
        let reader = CogHeaderReader::from_buffer(buf)?;
        let mut decoder = TiffDecoder::new(reader)?;

        decoder.parse_cog_header()
    }

    pub fn chunk_row_length(&self) -> u32 {
        match self.data_layout {
            ChunkDataLayout::Tiled(size) => size,
            ChunkDataLayout::Striped(_) => self.geo_reference.columns().count() as u32,
        }
    }

    pub fn is_tiled(&self) -> bool {
        matches!(self.data_layout, ChunkDataLayout::Tiled(_))
    }
}

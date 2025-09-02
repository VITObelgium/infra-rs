/// Low level tiff chunk reading functions
/// Should only be needed for specific use cases.
/// Normal clients should use higher-level functions in `cog` module for reading COGs.
use crate::{
    ArrayDataType, ArrayNum,
    geotiff::{
        TiffChunkLocation,
        gdalghostdata::GdalGhostData,
        utils::{self},
    },
    raster::{Compression, Predictor},
};
use inf::cast;
use ruzstd::decoding::StreamingDecoder;
use simd_macro::simd_bounds;
use weezl::{BitOrder, decode::Decoder};

use crate::{Error, Result};
use std::{
    fs::File,
    io::{BufWriter, Read, Seek, SeekFrom},
    path::Path,
};

pub const COG_HEADER_SIZE: usize = 16 * 1024; // 16 KiB, which is usually sufficient for the COG header

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

// Detect if the file at the given path is a Cloud Optimized GeoTIFF (COG).
/// Detection is done based on the presence of the Gdal Ghost Data in the TIFF header,
pub fn file_is_cog(path: &Path) -> bool {
    File::open(path).is_ok_and(|mut f| stream_is_cog(&mut f))
}

pub fn stream_is_cog(stream: &mut impl Read) -> bool {
    let mut header = vec![0u8; COG_HEADER_SIZE];
    if stream.read_exact(&mut header).is_err() {
        return false;
    }

    GdalGhostData::from_tiff_header_buffer(&header).is_some_and(|ghost| ghost.is_cog())
}

/// This reader buffers the first x bytes of the source stream, which is usually sufficient for reading the COG header.
/// This way multiple io calls are avoided when reading the header.
/// Read operations outside of the header will cause `UnexpectedEof` error.
pub struct CogHeaderReader {
    buffer: Vec<u8>,
    pos: usize,
}

impl CogHeaderReader {
    pub fn from_stream(stream: &mut impl Read, header_size: usize) -> Result<Self> {
        // Read up to header_size bytes, handling partial reads
        let mut buffer = Vec::default();
        Self::append_from_stream_to_buffer(&mut buffer, stream, header_size)?;
        Ok(Self::from_buffer(buffer))
    }

    fn append_from_stream_to_buffer(buffer: &mut Vec<u8>, stream: &mut impl Read, size: usize) -> Result<()> {
        let initial_buf_len = buffer.len();
        buffer.resize(initial_buf_len + size, 0);

        let mut total_bytes_read = 0;

        while total_bytes_read < size {
            let bytes_read = stream.read(&mut buffer[initial_buf_len + total_bytes_read..])?;
            if bytes_read == 0 {
                // EOF reached before filling the buffer
                break;
            }

            total_bytes_read += bytes_read;
        }

        buffer.truncate(initial_buf_len + total_bytes_read);

        Ok(())
    }

    /// Increases the current buffer size by a factor of 2, resets the read position to 0,
    pub fn increase_buffer_size(&mut self, stream: &mut (impl Read + Seek)) -> Result<()> {
        let current_size = self.buffer.len();
        stream.seek(SeekFrom::Start(current_size as u64))?;
        Self::append_from_stream_to_buffer(&mut self.buffer, stream, current_size)?;

        // Reset position to the start of the buffer
        self.pos = 0;

        Ok(())
    }

    pub fn from_buffer(buffer: Vec<u8>) -> Self {
        Self { buffer, pos: 0 }
    }

    pub fn into_buffer(self) -> Vec<u8> {
        self.buffer
    }

    pub fn cog_header(&self) -> &[u8] {
        &self.buffer
    }
}

impl Read for CogHeaderReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos + buf.len() > self.buffer.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "Read outside of the COG header buffer (current position: {}, buffer size: {})",
                    self.pos,
                    self.buffer.len()
                ),
            ));
        }

        buf.copy_from_slice(&self.buffer[self.pos..self.pos + buf.len()]);
        self.pos += buf.len();
        Ok(buf.len())
    }
}

impl Seek for CogHeaderReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let seek_pos = match pos {
            SeekFrom::Start(offset) => offset as usize,
            SeekFrom::End(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Seek from end is not supported for BufferedReader",
                ));
            }
            SeekFrom::Current(offset) => {
                let new_pos = self.pos as i64 + offset;
                if new_pos < 0 {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Seek before start of buffer"));
                }

                new_pos as usize
            }
        };

        if seek_pos >= self.buffer.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Seek outside of the COG header buffer",
            ));
        }

        self.pos = seek_pos;
        Ok(seek_pos as u64)
    }
}

pub fn read_chunk(cog_location: &TiffChunkLocation, reader: &mut (impl Read + Seek), buf: &mut [u8]) -> Result<()> {
    let chunk_range = cog_location.range_to_fetch();
    debug_assert!(chunk_range.start != chunk_range.end, "Empty chunk passed to read_cog_chunk");
    debug_assert!(
        buf.len() == (chunk_range.end - chunk_range.start) as usize,
        "Buffer size does not match chunk size {} <-> {}",
        buf.len(),
        chunk_range.end - chunk_range.start
    );

    reader.seek(SeekFrom::Start(chunk_range.start))?;
    reader.read_exact(buf)?;

    Ok(())
}

#[simd_bounds]
pub fn read_chunk_data_into_buffer<T: ArrayNum>(
    chunk: &TiffChunkLocation,
    row_length: u32,
    nodata: Option<f64>,
    compression: Option<Compression>,
    predictor: Option<Predictor>,
    reader: &mut (impl Read + Seek),
    tile_data: &mut [T],
) -> Result<()> {
    if chunk.is_sparse() {
        tile_data.fill(cast::option(nodata).ok_or_else(|| Error::Runtime("Invalid nodata value".into()))?);
    }

    let mut cog_chunk = vec![0; chunk.size as usize];
    read_chunk(chunk, reader, &mut cog_chunk)?;
    parse_chunk_data_into_buffer(row_length, compression, predictor, &cog_chunk, tile_data)?;

    Ok(())
}

#[simd_bounds]
pub fn parse_chunk_data_into_buffer<T: ArrayNum>(
    row_length: u32,
    compression: Option<Compression>,
    predictor: Option<Predictor>,
    chunk_data: &[u8],
    decoded_chunk_data: &mut [T],
) -> Result<()> {
    debug_assert!(chunk_data.len() > 4);

    match compression {
        Some(Compression::Lzw) => lzw_decompress_to::<T>(chunk_data, decoded_chunk_data)?,
        Some(Compression::Zstd) => zstd_decompress_to::<T>(chunk_data, decoded_chunk_data)?,
        None => {
            if chunk_data.len() != std::mem::size_of_val(decoded_chunk_data) {
                return Err(Error::Runtime(format!(
                    "Uncompressed tile data size ({}) does not match the expected size {}",
                    chunk_data.len(),
                    std::mem::size_of_val(decoded_chunk_data)
                )));
            }

            decoded_chunk_data.copy_from_slice(bytemuck::cast_slice(chunk_data));
        }
    };

    match predictor {
        None => {}
        Some(Predictor::Horizontal) => {
            utils::unpredict_horizontal(decoded_chunk_data, row_length);
        }
        Some(Predictor::FloatingPoint) => match T::TYPE {
            ArrayDataType::Float32 => {
                utils::unpredict_fp32(bytemuck::cast_slice_mut(decoded_chunk_data), row_length);
            }
            ArrayDataType::Float64 => {
                utils::unpredict_fp64(bytemuck::cast_slice_mut(decoded_chunk_data), row_length);
            }
            _ => return Err(Error::Runtime("Floating point predictor only supported for f32 and f64".into())),
        },
    }

    Ok(())
}

fn lzw_decompress_to<T: ArrayNum>(data: &[u8], decode_buf: &mut [T]) -> Result<()> {
    let decode_buf_byte_length = std::mem::size_of_val(decode_buf);

    {
        let mut stream = BufWriter::new(bytemuck::cast_slice_mut(decode_buf));

        // Use MSB bit order and 8 as the initial code size, which is standard for TIFF LZW
        let decode_result = Decoder::with_tiff_size_switch(BitOrder::Msb, 8)
            .into_stream(&mut stream)
            .decode(data);

        if decode_result.bytes_read != data.len() {
            return Err(Error::Runtime("LZW decompression did not read all input bytes".into()));
        }

        if decode_result.bytes_written != decode_buf_byte_length {
            return Err(Error::Runtime("LZW decompression did not write all tile pixels".into()));
        }

        decode_result.status?;
    };

    Ok(())
}

fn zstd_decompress_to<T: ArrayNum>(data: &[u8], decode_buf: &mut [T]) -> Result<()> {
    let decode_buf_byte: &mut [u8] = bytemuck::cast_slice_mut(decode_buf);

    if let Ok(mut decoder) = StreamingDecoder::new(data) {
        decoder.read_exact(decode_buf_byte)?;
    } else {
        return Err(Error::Runtime("Failed to create Zstd decoder".into()));
    }

    Ok(())
}

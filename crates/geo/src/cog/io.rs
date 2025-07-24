use crate::{
    Array, ArrayDataType, ArrayInterop, ArrayMetadata as _, ArrayNum, Cell, Columns, DenseArray, RasterMetadata, RasterSize, Rows, Window,
    cog::{
        Compression, Predictor, TiffChunkLocation,
        utils::{self, HorizontalUnpredictable},
    },
    raster::intersection::CutOut,
};
use inf::allocate::{self, AlignedVec, AlignedVecUnderConstruction, aligned_vec_from_slice};
use simd_macro::simd_bounds;
use weezl::{BitOrder, decode::Decoder};

use crate::{Error, Result};
use std::io::{BufWriter, Read, Seek, SeekFrom};

pub const COG_HEADER_SIZE: usize = 64 * 1024; // 64 KiB, which is usually sufficient for the COG header

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

/// This reader buffers the first 64 KiB of the source stream, which is usually sufficient for reading the COG header.
/// This way multiple io calls are avoided when reading the header.
/// Read operations outside of the header will be redirected to the underlying file stream.
pub struct CogHeaderReader {
    buffer: Vec<u8>,
    pos: usize,
}

impl CogHeaderReader {
    pub fn from_stream(mut stream: impl Read, header_size: usize) -> Result<Self> {
        // Read up to header_size bytes, handling partial reads
        let mut buffer = vec![0; header_size];
        let mut total_bytes_read = 0;

        while total_bytes_read < header_size {
            let bytes_read = stream.read(&mut buffer[total_bytes_read..])?;
            if bytes_read == 0 {
                // EOF reached before filling the buffer
                break;
            }

            total_bytes_read += bytes_read;
        }

        buffer.truncate(total_bytes_read);
        Self::from_buffer(buffer)
    }

    pub fn from_buffer(buffer: Vec<u8>) -> Result<Self> {
        Ok(Self { buffer, pos: 0 })
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

pub fn read_cog_chunk(cog_location: &TiffChunkLocation, mut reader: impl Read + Seek) -> Result<Vec<u8>> {
    let chunk_range = cog_location.range_to_fetch();
    if chunk_range.start == chunk_range.end {
        return Ok(Vec::default());
    }

    reader.seek(SeekFrom::Start(chunk_range.start))?;

    let mut buf = vec![0; (chunk_range.end - chunk_range.start) as usize];
    reader.read_exact(&mut buf)?;

    Ok(buf)
}

#[simd_bounds]
pub fn read_tile_data<T: ArrayNum + HorizontalUnpredictable>(
    cog_location: &TiffChunkLocation,
    tile_size: u32,
    nodata: Option<f64>,
    compression: Option<Compression>,
    predictor: Option<Predictor>,
    mut reader: impl Read + Seek,
) -> Result<DenseArray<T>> {
    if cog_location.size == 0 {
        return Ok(DenseArray::empty());
    }

    let cog_chunk = read_cog_chunk(cog_location, &mut reader)?;
    parse_tile_data(cog_location, tile_size, nodata, compression, predictor, None, &cog_chunk)
}

#[simd_bounds]
pub fn parse_tile_data<T: ArrayNum + HorizontalUnpredictable>(
    cog_location: &TiffChunkLocation,
    tile_size: u32,
    nodata: Option<f64>,
    compression: Option<Compression>,
    predictor: Option<Predictor>,
    cutout: Option<&CutOut>,
    cog_chunk: &[u8],
) -> Result<DenseArray<T>> {
    debug_assert!(cog_chunk.len() > 4);
    // cog_chunk contains the tile data with the first 4 bytes being the size of the tile as cross-check
    let size_bytes: [u8; 4] = <[u8; 4]>::try_from(&cog_chunk[0..4]).unwrap();
    if cog_location.size != u32::from_le_bytes(size_bytes) as u64 {
        return Err(Error::Runtime("Tile size does not match the expected size".into()));
    }

    let mut tile_data = match compression {
        Some(Compression::Lzw) => lzw_decompress_to::<T>(&cog_chunk[4..], tile_size)?,
        None => {
            let tile_size = tile_size as usize;
            if cog_chunk[4..].len() != (tile_size * tile_size * std::mem::size_of::<T>()) {
                return Err(Error::Runtime(
                    "Uncompressed tile data size does not match the expected size".into(),
                ));
            }

            let byte_slice = &cog_chunk[4..];
            aligned_vec_from_slice::<T>(bytemuck::cast_slice(byte_slice))
        }
    };

    match predictor {
        None => {}
        Some(Predictor::Horizontal) => {
            utils::unpredict_horizontal(&mut tile_data, tile_size);
        }
        Some(Predictor::FloatingPoint) => match T::TYPE {
            ArrayDataType::Float32 => {
                let mut fp32_data = allocate::cast_aligned_vec::<T, f32>(tile_data);
                fp32_data = utils::unpredict_fp32(&mut fp32_data, tile_size);
                tile_data = allocate::cast_aligned_vec::<f32, T>(fp32_data);
            }
            ArrayDataType::Float64 => {
                let mut fp64_data = allocate::cast_aligned_vec::<T, f64>(tile_data);
                fp64_data = utils::unpredict_fp64(&mut fp64_data, tile_size);
                tile_data = allocate::cast_aligned_vec::<f64, T>(fp64_data);
            }
            _ => return Err(Error::Runtime("Floating point predictor only supported for f32 and f64".into())),
        },
    }

    let mut meta = RasterMetadata::sized_with_nodata(RasterSize::square(tile_size as i32), nodata);
    let mut arr = DenseArray::<T>::new_init_nodata(meta, tile_data)?;

    if let Some(cutout) = cutout {
        let size = RasterSize::with_rows_cols(Rows(cutout.rows), Columns(cutout.cols));
        let window = Window::new(Cell::from_row_col(cutout.src_row_offset, cutout.src_col_offset), size);

        let cutout_data = allocate::aligned_vec_from_iter(arr.iter_window(window));

        meta = RasterMetadata::sized_with_nodata(size, nodata);
        arr = DenseArray::<T>::new(meta, cutout_data)?;
    }

    Ok(arr)
}

fn lzw_decompress_to<T: ArrayNum>(data: &[u8], tile_size: u32) -> Result<AlignedVec<T>> {
    let decoded_len = tile_size as usize * tile_size as usize;
    let mut decode_buf = AlignedVecUnderConstruction::new(decoded_len);

    {
        // Safety: The buffer is allocated with enough capacity to hold the decoded data
        let mut stream = BufWriter::new(unsafe { decode_buf.as_byte_slice_mut() });

        // Use MSB bit order and 8 as the initial code size, which is standard for TIFF LZW
        let decode_result = Decoder::with_tiff_size_switch(BitOrder::Msb, 8)
            .into_stream(&mut stream)
            .decode(data);

        if decode_result.bytes_read != data.len() {
            return Err(Error::Runtime("LZW decompression did not read all input bytes".into()));
        }

        if decode_result.bytes_written != decoded_len * std::mem::size_of::<T>() {
            return Err(Error::Runtime("LZW decompression did not write all tile pixels".into()));
        }

        decode_result.status?;
    };

    let decode_buf = unsafe {
        // Safety: We verified the decoded length matches the expected size
        decode_buf.assume_init()
    };

    Ok(decode_buf)
}

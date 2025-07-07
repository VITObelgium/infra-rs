use geo::{ArrayInterop, ArrayMetadata as _, ArrayNum, DenseArray, RasterMetadata, RasterSize};
use inf::allocate::{self, AlignedVec, aligned_vec_from_slice};
use weezl::{BitOrder, decode::Decoder};

use crate::{
    Error, Result,
    cog::{CogTileLocation, Compression},
};
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
    pub fn from_stream(mut stream: impl Read) -> Result<Self> {
        // Immediately read the cog header into the buffer
        let mut buffer = vec![0; COG_HEADER_SIZE];
        stream.read_exact(&mut buffer)?;
        Self::from_buffer(buffer)
    }

    pub fn from_buffer(buffer: Vec<u8>) -> Result<Self> {
        if buffer.len() != COG_HEADER_SIZE {
            return Err(Error::InvalidArgument("Provided buffer should match the COG_HEADER_SIZE".into()));
        }

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
                std::io::ErrorKind::InvalidInput,
                "Read outside of the COG header buffer",
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
                    std::io::ErrorKind::InvalidInput,
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
                std::io::ErrorKind::InvalidInput,
                "Seek outside of the COG header buffer",
            ));
        }

        self.pos = seek_pos;
        Ok(seek_pos as u64)
    }
}

#[geo::simd_bounds]
pub fn read_tile_data<T: ArrayNum>(
    tile: &CogTileLocation,
    tile_size: i32,
    nodata: Option<f64>,
    compression: Compression,
    mut reader: impl Read + Seek,
) -> Result<DenseArray<T>> {
    let chunk_range = tile.range_to_fetch();
    reader.seek(SeekFrom::Start(chunk_range.start))?;

    let mut buf = vec![0; (chunk_range.end - chunk_range.start) as usize];
    reader.read_exact(&mut buf)?;

    parse_tile_data(tile, tile_size, nodata, compression, &buf)
}

#[geo::simd_bounds]
pub fn parse_tile_data<T: ArrayNum>(
    tile: &CogTileLocation,
    tile_size: i32,
    nodata: Option<f64>,
    compression: Compression,
    cog_chunk: &[u8],
) -> Result<DenseArray<T>> {
    // cog_chunk contains the tile data with the first 4 bytes being the size of the tile as cross-check
    let size_bytes: [u8; 4] = <[u8; 4]>::try_from(&cog_chunk[0..4]).unwrap();
    if tile.size != u32::from_le_bytes(size_bytes) as u64 {
        return Err(Error::Runtime("Tile size does not match the expected size".into()));
    }

    let tile_data = match compression {
        Compression::Lzw => lzw_decompress_to::<T>(&cog_chunk[4..], tile_size)?,
        Compression::None => {
            if cog_chunk[4..].len() != ((tile_size * tile_size) as usize * std::mem::size_of::<T>()) {
                return Err(Error::Runtime(
                    "Uncompressed tile data size does not match the expected size".into(),
                ));
            }

            unsafe {
                let byte_slice = &cog_chunk[4..];
                let ptr = byte_slice.as_ptr() as *const T;
                let len = byte_slice.len() / std::mem::size_of::<T>();
                let t_slice = std::slice::from_raw_parts(ptr, len);
                aligned_vec_from_slice::<T>(t_slice)
            }
        }
    };

    let meta = RasterMetadata::sized_with_nodata(RasterSize::square(tile_size), nodata);
    let arr = DenseArray::<T>::new_init_nodata(meta, tile_data)?;
    Ok(arr)
}

fn lzw_decompress_to<T: ArrayNum>(data: &[u8], tile_size: i32) -> Result<AlignedVec<T>> {
    let decoded_len = (tile_size * tile_size) as usize * std::mem::size_of::<T>();
    let mut decode_buffer = allocate::aligned_vec_with_capacity::<u8>(decoded_len);

    {
        let mut stream = BufWriter::new(&mut decode_buffer);

        // Use MSB bit order and 8 as the initial code size, which is standard for TIFF LZW
        let decode_result = Decoder::with_tiff_size_switch(BitOrder::Msb, 8)
            .into_stream(&mut stream)
            .decode(data);

        if decode_result.bytes_read != data.len() {
            return Err(Error::Runtime("LZW decompression did not read all input bytes".into()));
        }

        if decode_result.bytes_written != decoded_len {
            return Err(Error::Runtime("LZW decompression did not write all tile pixels".into()));
        }

        decode_result.status?;
    }

    Ok(unsafe { allocate::reinterpret_aligned_vec::<_, T>(decode_buffer) })
}

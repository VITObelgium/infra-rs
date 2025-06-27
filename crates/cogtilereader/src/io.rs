use geo::{Array, ArrayNum, DenseArray, RasterSize};
use inf::allocate;
use weezl::{BitOrder, decode::Decoder};

use crate::{Error, Result, cog::CogTileLocation};
use std::io::{Read, Seek, SeekFrom};

pub const COG_HEADER_SIZE: usize = 64 * 1024; // 64 KiB, which is usually sufficient for the COG header

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

pub fn read_tile_data<T: ArrayNum>(tile: CogTileLocation, mut reader: impl Read + Seek) -> Result<DenseArray<T>> {
    let start_pos = tile.offset - 4;
    reader.seek(SeekFrom::Start(start_pos))?;

    let mut buf = vec![0; (tile.size + 4) as usize];
    reader.read_exact(&mut buf)?;

    // Buf now contains the tile data with the first 4 bytes being the size of the tile
    let size_bytes: [u8; 4] = <[u8; 4]>::try_from(&buf[0..4]).unwrap();
    if tile.size != u32::from_le_bytes(size_bytes) as u64 {
        return Err(Error::Runtime("Tile size does not match the expected size".into()));
    }

    let tile_data = lzw_decompress_to::<T>(&buf[4..], 256)?;
    Ok(DenseArray::<T>::new(RasterSize::square(256), tile_data)?)
}

fn lzw_decompress_to<T: ArrayNum>(data: &[u8], tile_size: usize) -> Result<Vec<T>> {
    let mut decode_buffer = allocate::aligned_vec_with_capacity::<u8>(tile_size * tile_size * std::mem::size_of::<T>());

    // Use MSB bit order and 8 as the initial code size, which is standard for TIFF LZW
    Decoder::with_tiff_size_switch(BitOrder::Msb, 8)
        .into_vec(&mut decode_buffer)
        .decode(data)
        .status
        .map_err(|e| Error::Runtime(format!("LZW decompression failed: {}", e)))?;

    Ok(unsafe { allocate::reinterpret_aligned_vec::<_, T>(decode_buffer) })
}

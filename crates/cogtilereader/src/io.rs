use crate::{Error, Result};
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

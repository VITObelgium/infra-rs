use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

use crate::Result;

const COG_HEADER_SIZE: usize = 64 * 1024; // 64 KiB, which is usually sufficient for the COG header

pub trait CogStreamReader: Read + Seek {
    /// Returns the COG header as a byte slice.
    fn cog_header(&self) -> &[u8];
}

/// This reader buffers the first 64 KiB of the file, which is usually sufficient for reading the COG header.
/// This way multiple io calls are avoided when reading the header.
/// Read operations outside of the header will be redirected to the underlying file stream.
pub struct FileBasedReader {
    stream: std::fs::File,
    buffer: Vec<u8>,
    pos: usize,
}

impl FileBasedReader {
    pub fn new(path: &Path) -> Result<Self> {
        let mut buffer = vec![0; COG_HEADER_SIZE];
        let mut stream = File::open(path)?;
        stream.read_exact(&mut buffer)?;
        Ok(Self { stream, buffer, pos: 0 })
    }
}

impl CogStreamReader for FileBasedReader {
    fn cog_header(&self) -> &[u8] {
        &self.buffer
    }
}

impl Read for FileBasedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos + buf.len() > self.buffer.len() {
            log::debug!("Read outside of the header");
            self.stream.seek(SeekFrom::Start(self.pos as u64))?;
            return self.stream.read(&mut self.buffer);
        }

        buf.copy_from_slice(&self.buffer[self.pos..self.pos + buf.len()]);
        self.pos += buf.len();
        Ok(buf.len())
    }
}

impl Seek for FileBasedReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let seek_pos = match pos {
            SeekFrom::Start(offset) => offset as usize,
            SeekFrom::End(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Seek from end is not supported for FileBasedReader",
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
            log::debug!("Seek outside of the header, resetting buffer");
            self.buffer.clear();
            return self.stream.seek(SeekFrom::Start(seek_pos as u64));
        }

        self.pos = seek_pos;
        Ok(seek_pos as u64)
    }
}

use bytemuck::bytes_of;

use crate::datatype::TileDataType;
use crate::lz4;
use crate::{CompressionAlgorithm, Error, Result, TileHeader};

/// Struct containing the tile dimensions and pixel data
pub struct RasterTile<T> {
    pub width: usize,
    pub height: usize,
    pub data: Vec<T>,
}

impl<T: TileDataType> RasterTile<T> {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < std::mem::size_of::<TileHeader>() {
            return Err(Error::InvalidArgument("Tile data is too short".into()));
        }

        let header = TileHeader::from_bytes(data)?;
        if data.len() != std::mem::size_of::<TileHeader>() + header.data_size as usize {
            return Err(Error::InvalidArgument("Tile data size mismatch".into()));
        }

        let data = match header.compression {
            CompressionAlgorithm::Lz4 => lz4::decompress_tile_data(
                (header.tile_width * header.tile_height) as usize * std::mem::size_of::<T>(),
                &data[std::mem::size_of::<TileHeader>()..],
            )?,
        };

        Ok(Self {
            width: header.tile_width as usize,
            height: header.tile_height as usize,
            data,
        })
    }

    // Create an encoded tile from the data
    pub fn encode(&self, algorithm: CompressionAlgorithm) -> Result<Vec<u8>> {
        let compressed_data = match algorithm {
            CompressionAlgorithm::Lz4 => lz4::compress_tile_data(&self.data)?,
        };

        let header = TileHeader::new(
            T::TYPE,
            algorithm,
            self.width as u16,
            self.height as u16,
            compressed_data.len() as u32,
        );

        let mut data = Vec::with_capacity(std::mem::size_of::<TileHeader>() + compressed_data.len());
        data.extend_from_slice(bytes_of(&header));
        data.extend_from_slice(&compressed_data);

        Ok(data)
    }
}

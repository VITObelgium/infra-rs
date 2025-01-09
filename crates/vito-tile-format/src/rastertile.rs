use bytemuck::bytes_of;

use crate::datatype::TileDataType;
use crate::{lz4, RasterTileDataType};
use crate::{CompressionAlgorithm, Error, Result, TileHeader};

/// Struct containing the tile dimensions and pixel data
pub struct RasterTile<T> {
    pub width: usize,
    pub height: usize,
    pub data: Vec<T>,
}

pub enum AnyRasterTile {
    U8(RasterTile<u8>),
    U16(RasterTile<u16>),
    U32(RasterTile<u32>),
    U64(RasterTile<u64>),
    F32(RasterTile<f32>),
    F64(RasterTile<f64>),
    I8(RasterTile<i8>),
    I16(RasterTile<i16>),
    I32(RasterTile<i32>),
    I64(RasterTile<i64>),
}

impl AnyRasterTile {
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < std::mem::size_of::<TileHeader>() {
            return Err(Error::InvalidArgument("Tile data is too short".into()));
        }

        let header = TileHeader::from_bytes(data)?;
        let data_slice = &data[std::mem::size_of::<TileHeader>()..];

        Ok(match header.data_type {
            RasterTileDataType::Int8 => AnyRasterTile::I8(RasterTile::<i8>::from_header_and_data(&header, data_slice)?),
            RasterTileDataType::Uint8 => {
                AnyRasterTile::U8(RasterTile::<u8>::from_header_and_data(&header, data_slice)?)
            }
            RasterTileDataType::Int16 => {
                AnyRasterTile::I16(RasterTile::<i16>::from_header_and_data(&header, data_slice)?)
            }
            RasterTileDataType::Uint16 => {
                AnyRasterTile::U16(RasterTile::<u16>::from_header_and_data(&header, data_slice)?)
            }
            RasterTileDataType::Int32 => {
                AnyRasterTile::I32(RasterTile::<i32>::from_header_and_data(&header, data_slice)?)
            }
            RasterTileDataType::Uint32 => {
                AnyRasterTile::U32(RasterTile::<u32>::from_header_and_data(&header, data_slice)?)
            }
            RasterTileDataType::Int64 => {
                AnyRasterTile::I64(RasterTile::<i64>::from_header_and_data(&header, data_slice)?)
            }
            RasterTileDataType::Uint64 => {
                AnyRasterTile::U64(RasterTile::<u64>::from_header_and_data(&header, data_slice)?)
            }
            RasterTileDataType::Float32 => {
                AnyRasterTile::F32(RasterTile::<f32>::from_header_and_data(&header, data_slice)?)
            }
            RasterTileDataType::Float64 => {
                AnyRasterTile::F64(RasterTile::<f64>::from_header_and_data(&header, data_slice)?)
            }
        })
    }
}

impl<T: TileDataType> RasterTile<T> {
    pub fn from_header_and_data(header: &TileHeader, data: &[u8]) -> Result<Self> {
        if data.len() != header.data_size as usize {
            return Err(Error::InvalidArgument("Tile data size mismatch".into()));
        }

        let data = match header.compression {
            CompressionAlgorithm::Lz4 => {
                lz4::decompress_tile_data(header.tile_width as usize * header.tile_height as usize, data)?
            }
        };

        Ok(Self {
            width: header.tile_width as usize,
            height: header.tile_height as usize,
            data,
        })
    }

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
                header.tile_width as usize * header.tile_height as usize,
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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn encode_decode_u32() {
        // fill data with iota
        const TILE_WIDTH: usize = 256;
        const TILE_HEIGHT: usize = 256;

        let tile = RasterTile {
            width: TILE_WIDTH,
            height: TILE_HEIGHT,
            data: (0..(TILE_WIDTH * TILE_HEIGHT) as u32).collect::<Vec<u32>>(),
        };

        let encoded = tile.encode(CompressionAlgorithm::Lz4).unwrap();

        let decoded = AnyRasterTile::from_bytes(&encoded).unwrap();
        assert!(matches!(decoded, AnyRasterTile::U32(_)));

        let AnyRasterTile::U32(decoded_tile) = decoded else {
            panic!("Expected U32 tile");
        };

        assert_eq!(tile.width, decoded_tile.width);
        assert_eq!(tile.height, decoded_tile.height);
        assert_eq!(tile.data, decoded_tile.data);
    }

    #[test]
    fn encode_decode_u8() {
        // fill data with iota
        const TILE_WIDTH: usize = 10;
        const TILE_HEIGHT: usize = 10;

        let tile = RasterTile {
            width: TILE_WIDTH,
            height: TILE_HEIGHT,
            data: (0..(TILE_WIDTH * TILE_HEIGHT) as u8).collect::<Vec<u8>>(),
        };

        let encoded = tile.encode(CompressionAlgorithm::Lz4).unwrap();
        let decoded = RasterTile::<u8>::from_bytes(&encoded).unwrap();

        assert_eq!(tile.width, decoded.width);
        assert_eq!(tile.height, decoded.height);
        assert_eq!(tile.data, decoded.data);
    }
}

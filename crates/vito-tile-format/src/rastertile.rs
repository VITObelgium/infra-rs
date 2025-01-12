use bytemuck::bytes_of;

use crate::datatype::TileDataType;
use crate::{lz4, RasterTileDataType};
use crate::{CompressionAlgorithm, Error, Result, TileHeader};

/// Struct containing the tile dimensions and pixel data
#[derive(Clone)]
pub struct RasterTile<T> {
    pub width: usize,
    pub height: usize,
    pub data: Vec<T>,
}

/// Type erased `RasterTile`
#[derive(Clone)]
pub enum AnyRasterTile {
    U8(RasterTile<u8>),
    U16(RasterTile<u16>),
    U32(RasterTile<u32>),
    I8(RasterTile<i8>),
    I16(RasterTile<i16>),
    I32(RasterTile<i32>),
    F32(RasterTile<f32>),
    F64(RasterTile<f64>),
}

impl AnyRasterTile {
    /// Create an untyped raster tile from the raw data
    /// The data is expected to be in the format of a `TileHeader` followed by the compressed tile data
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
            RasterTileDataType::Float32 => {
                AnyRasterTile::F32(RasterTile::<f32>::from_header_and_data(&header, data_slice)?)
            }
            RasterTileDataType::Float64 => {
                AnyRasterTile::F64(RasterTile::<f64>::from_header_and_data(&header, data_slice)?)
            }
        })
    }

    pub fn width(&self) -> usize {
        match self {
            AnyRasterTile::U8(tile) => tile.width,
            AnyRasterTile::U16(tile) => tile.width,
            AnyRasterTile::U32(tile) => tile.width,
            AnyRasterTile::I8(tile) => tile.width,
            AnyRasterTile::I16(tile) => tile.width,
            AnyRasterTile::I32(tile) => tile.width,
            AnyRasterTile::F32(tile) => tile.width,
            AnyRasterTile::F64(tile) => tile.width,
        }
    }

    pub fn height(&self) -> usize {
        match self {
            AnyRasterTile::U8(tile) => tile.height,
            AnyRasterTile::U16(tile) => tile.height,
            AnyRasterTile::U32(tile) => tile.height,
            AnyRasterTile::I8(tile) => tile.height,
            AnyRasterTile::I16(tile) => tile.height,
            AnyRasterTile::I32(tile) => tile.height,
            AnyRasterTile::F32(tile) => tile.height,
            AnyRasterTile::F64(tile) => tile.height,
        }
    }

    pub fn data_type(&self) -> RasterTileDataType {
        match self {
            AnyRasterTile::U8(_) => RasterTileDataType::Uint8,
            AnyRasterTile::U16(_) => RasterTileDataType::Uint16,
            AnyRasterTile::U32(_) => RasterTileDataType::Uint32,
            AnyRasterTile::I8(_) => RasterTileDataType::Int8,
            AnyRasterTile::I16(_) => RasterTileDataType::Int16,
            AnyRasterTile::I32(_) => RasterTileDataType::Int32,
            AnyRasterTile::F32(_) => RasterTileDataType::Float32,
            AnyRasterTile::F64(_) => RasterTileDataType::Float64,
        }
    }

    #[cfg(target_arch = "wasm32")]
    pub fn from_array_buffer(array_buffer: &js_sys::ArrayBuffer) -> Result<Self> {
        if array_buffer.byte_length() == 0 {
            return Err(Error::InvalidArgument("Empty tile data buffer provided".into()));
        }

        let u8_array = js_sys::Uint8Array::new(array_buffer);
        Self::from_bytes(&u8_array.to_vec())
    }
}

impl<T: TileDataType> RasterTile<T> {
    // Create a raster tile from the header data structure and the raw compressed data
    pub fn from_header_and_data(header: &TileHeader, data: &[u8]) -> Result<Self> {
        if data.len() != header.data_size as usize {
            return Err(Error::InvalidArgument("Tile data size mismatch".into()));
        }

        let data = match header.compression {
            CompressionAlgorithm::Lz4Block => {
                lz4::decompress_tile_data(header.tile_width as usize * header.tile_height as usize, data)?
            }
        };

        Ok(Self {
            width: header.tile_width as usize,
            height: header.tile_height as usize,
            data,
        })
    }

    /// Create a raster tile from the raw data
    /// The data is expected to be in the format of a `TileHeader` followed by the compressed tile data
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < std::mem::size_of::<TileHeader>() {
            return Err(Error::InvalidArgument("Tile data is too short".into()));
        }

        let header = TileHeader::from_bytes(data)?;
        if data.len() != std::mem::size_of::<TileHeader>() + header.data_size as usize {
            return Err(Error::InvalidArgument("Tile data size mismatch".into()));
        }

        Self::from_header_and_data(&header, &data[std::mem::size_of::<TileHeader>()..])
    }

    // Encode this tile, the output will be a byte vector containing the `TileHeader` followed by the compressed tile data
    pub fn encode(&self, algorithm: CompressionAlgorithm) -> Result<Vec<u8>> {
        let compressed_data = match algorithm {
            CompressionAlgorithm::Lz4Block => crate::lz4::compress_tile_data(&self.data)?,
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

impl<T: TileDataType> From<RasterTile<T>> for Vec<T> {
    fn from(val: RasterTile<T>) -> Self {
        val.data
    }
}

#[macro_export]
macro_rules! impl_try_from_raster_tile {
    ( $tile_type:path, $tile_type_enum:ident ) => {
        impl TryFrom<AnyRasterTile> for RasterTile<$tile_type> {
            type Error = Error;

            fn try_from(value: AnyRasterTile) -> Result<Self> {
                match value {
                    AnyRasterTile::$tile_type_enum(tile) => Ok(tile),
                    _ => Err(Error::InvalidArgument(format!(
                        "Expected {} tile",
                        stringify!($tile_type),
                    ))),
                }
            }
        }
    };
}

impl_try_from_raster_tile!(u8, U8);
impl_try_from_raster_tile!(i8, I8);
impl_try_from_raster_tile!(u16, U16);
impl_try_from_raster_tile!(i16, I16);
impl_try_from_raster_tile!(u32, U32);
impl_try_from_raster_tile!(i32, I32);
impl_try_from_raster_tile!(f32, F32);
impl_try_from_raster_tile!(f64, F64);

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

        let encoded = tile.encode(CompressionAlgorithm::Lz4Block).unwrap();

        let decoded = AnyRasterTile::from_bytes(&encoded).unwrap();
        assert!(matches!(decoded, AnyRasterTile::U32(_)));

        let decoded_tile: RasterTile<u32> = decoded.try_into().expect("Expected U32 tile");
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

        let encoded = tile.encode(CompressionAlgorithm::Lz4Block).unwrap();
        let decoded = RasterTile::<u8>::from_bytes(&encoded).unwrap();

        assert_eq!(tile.width, decoded.width);
        assert_eq!(tile.height, decoded.height);
        assert_eq!(tile.data, decoded.data);
    }

    #[test]
    fn try_from() {
        let tile = RasterTile {
            width: 10,
            height: 10,
            data: (0..100).collect::<Vec<u32>>(),
        };

        let encoded = tile.encode(CompressionAlgorithm::Lz4Block).unwrap();
        let decoded = AnyRasterTile::from_bytes(&encoded).unwrap();

        let _: RasterTile<u32> = decoded.clone().try_into().expect("Cast failed");

        assert!(TryInto::<RasterTile<u8>>::try_into(decoded.clone()).is_err());
        assert!(TryInto::<RasterTile<i8>>::try_into(decoded.clone()).is_err());
        assert!(TryInto::<RasterTile<u16>>::try_into(decoded.clone()).is_err());
        assert!(TryInto::<RasterTile<i16>>::try_into(decoded.clone()).is_err());
        assert!(TryInto::<RasterTile<u32>>::try_into(decoded.clone()).is_ok());
        assert!(TryInto::<RasterTile<i32>>::try_into(decoded.clone()).is_err());
        assert!(TryInto::<RasterTile<f32>>::try_into(decoded.clone()).is_err());
        assert!(TryInto::<RasterTile<f64>>::try_into(decoded.clone()).is_err());
    }
}

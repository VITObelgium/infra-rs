use raster::{AnyDenseRaster, DenseRaster, Raster, RasterCreation, RasterDataType, RasterNum, RasterSize};

use crate::lz4;
use crate::{CompressionAlgorithm, Error, Result, TileHeader};

pub trait RasterTileIO {
    /// Create a raster tile from the raw data
    /// The data is expected to be in the format of a `TileHeader` followed by the compressed tile data
    fn from_tile_bytes(data: &[u8]) -> Result<Self>
    where
        Self: std::marker::Sized;

    // Create a raster tile from the header data structure and the raw compressed data
    fn from_tile_header_and_data(header: &TileHeader, data: &[u8]) -> Result<Self>
    where
        Self: std::marker::Sized;

    /// Encode this tile, the output will be a byte vector containing the `TileHeader` followed by the compressed tile data
    fn encode_raster_tile(&self, algorithm: CompressionAlgorithm) -> Result<Vec<u8>>;

    #[cfg(target_arch = "wasm32")]
    fn from_array_buffer(array_buffer: &js_sys::ArrayBuffer) -> Result<Self>
    where
        Self: std::marker::Sized;
}

pub trait RasterTileCastIO {
    /// Create a raster tile from the raw data
    /// The data is expected to be in the format of a `TileHeader` followed by the compressed tile data
    fn from_tile_bytes_with_cast(data: &[u8]) -> Result<Self>
    where
        Self: std::marker::Sized;

    // Create a raster tile from the header data structure and the raw compressed data
    // The dataype will be cast to the correct raster type if it doesnt match
    fn from_tile_header_and_data_with_cast(header: &TileHeader, data: &[u8]) -> Result<Self>
    where
        Self: std::marker::Sized;
}

impl<T: RasterNum<T>> RasterTileIO for DenseRaster<T> {
    /// Create a raster tile from the raw data
    /// The data is expected to be in the format of a `TileHeader` followed by the compressed tile data
    fn from_tile_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < std::mem::size_of::<TileHeader>() {
            return Err(Error::InvalidArgument("Tile data is too short".into()));
        }

        let header = TileHeader::from_bytes(data)?;
        if data.len() != std::mem::size_of::<TileHeader>() + header.data_size as usize {
            return Err(Error::InvalidArgument("Tile data size mismatch".into()));
        }

        Self::from_tile_header_and_data(&header, &data[std::mem::size_of::<TileHeader>()..])
    }

    fn encode_raster_tile(&self, algorithm: CompressionAlgorithm) -> Result<Vec<u8>> {
        let compressed_data = match algorithm {
            CompressionAlgorithm::Lz4Block => crate::lz4::compress_tile_data(self.as_slice())?,
        };

        let header = TileHeader::new(
            T::TYPE,
            algorithm,
            self.width() as u16,
            self.height() as u16,
            compressed_data.len() as u32,
        );

        let mut data = Vec::with_capacity(std::mem::size_of::<TileHeader>() + compressed_data.len());

        // Safety: The TileHeader struct is a plain old data struct so it is safe to transmute it to a byte slice
        let header_bytes =
            unsafe { ::core::slice::from_raw_parts((&header as *const TileHeader).cast::<u8>(), ::core::mem::size_of::<TileHeader>()) };

        data.extend_from_slice(header_bytes);
        data.extend_from_slice(&compressed_data);

        Ok(data)
    }

    /// Create a raster tile from the header data structure and the raw compressed data
    /// The data is expected to be in the format of a `TileHeader` followed by the compressed tile data
    fn from_tile_header_and_data(header: &TileHeader, data: &[u8]) -> Result<Self> {
        assert!(header.data_type == T::TYPE, "Tile data type mismatch");
        if data.len() != header.data_size as usize {
            return Err(Error::InvalidArgument("Tile data size mismatch".into()));
        }

        let data = match header.compression {
            CompressionAlgorithm::Lz4Block => lz4::decompress_tile_data(header.tile_width as usize * header.tile_height as usize, data)?,
        };

        Ok(DenseRaster::new(
            RasterSize::with_rows_cols(header.tile_height as usize, header.tile_width as usize),
            data,
        ))
    }

    #[cfg(target_arch = "wasm32")]
    fn from_array_buffer(array_buffer: &js_sys::ArrayBuffer) -> Result<Self>
    where
        Self: std::marker::Sized,
    {
        if array_buffer.byte_length() == 0 {
            return Err(Error::InvalidArgument("Empty tile data buffer provided".into()));
        }

        let u8_array = js_sys::Uint8Array::new(array_buffer);
        Self::from_tile_bytes(&u8_array.to_vec())
    }
}

impl<T: RasterNum<T>> RasterTileCastIO for DenseRaster<T> {
    /// Create a raster tile from the raw data
    /// The data is expected to be in the format of a `TileHeader` followed by the compressed tile data
    /// The data will be cast to the correct raster type if it doesnt match
    fn from_tile_bytes_with_cast(data: &[u8]) -> Result<Self> {
        if data.len() < std::mem::size_of::<TileHeader>() {
            return Err(Error::InvalidArgument("Tile data is too short".into()));
        }

        let header = TileHeader::from_bytes(data)?;
        if data.len() != std::mem::size_of::<TileHeader>() + header.data_size as usize {
            return Err(Error::InvalidArgument("Tile data size mismatch".into()));
        }

        Self::from_tile_header_and_data_with_cast(&header, &data[std::mem::size_of::<TileHeader>()..])
    }

    /// Create a raster tile from the header data structure and the raw compressed data
    /// The data will be cast to the correct raster type if it doesnt match
    fn from_tile_header_and_data_with_cast(header: &TileHeader, data: &[u8]) -> Result<Self> {
        if data.len() != header.data_size as usize {
            return Err(Error::InvalidArgument("Tile data size mismatch".into()));
        }

        match header.data_type {
            RasterDataType::Int8 => DenseRaster::<i8>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            RasterDataType::Uint8 => DenseRaster::<u8>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            RasterDataType::Int16 => DenseRaster::<i16>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            RasterDataType::Uint16 => DenseRaster::<u16>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            RasterDataType::Int32 => DenseRaster::<i32>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            RasterDataType::Uint32 => DenseRaster::<u32>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            RasterDataType::Int64 => DenseRaster::<i64>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            RasterDataType::Uint64 => DenseRaster::<u64>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            RasterDataType::Float32 => DenseRaster::<f32>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            RasterDataType::Float64 => DenseRaster::<f64>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
        }
    }
}

impl RasterTileIO for AnyDenseRaster {
    /// Create an untyped raster tile from the raw data
    /// The data is expected to be in the format of a `TileHeader` followed by the compressed tile data
    fn from_tile_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < std::mem::size_of::<TileHeader>() {
            return Err(Error::InvalidArgument("Tile data is too short".into()));
        }

        let header = TileHeader::from_bytes(data)?;
        if data.len() != std::mem::size_of::<TileHeader>() + header.data_size as usize {
            return Err(Error::InvalidArgument("Tile data size mismatch".into()));
        }

        Self::from_tile_header_and_data(&header, &data[std::mem::size_of::<TileHeader>()..])
    }

    fn from_tile_header_and_data(header: &TileHeader, data: &[u8]) -> Result<Self>
    where
        Self: std::marker::Sized,
    {
        Ok(match header.data_type {
            RasterDataType::Int8 => AnyDenseRaster::I8(DenseRaster::<i8>::from_tile_header_and_data(header, data)?),
            RasterDataType::Uint8 => AnyDenseRaster::U8(DenseRaster::<u8>::from_tile_header_and_data(header, data)?),
            RasterDataType::Int16 => AnyDenseRaster::I16(DenseRaster::<i16>::from_tile_header_and_data(header, data)?),
            RasterDataType::Uint16 => AnyDenseRaster::U16(DenseRaster::<u16>::from_tile_header_and_data(header, data)?),
            RasterDataType::Int32 => AnyDenseRaster::I32(DenseRaster::<i32>::from_tile_header_and_data(header, data)?),
            RasterDataType::Uint32 => AnyDenseRaster::U32(DenseRaster::<u32>::from_tile_header_and_data(header, data)?),
            RasterDataType::Int64 => AnyDenseRaster::I64(DenseRaster::<i64>::from_tile_header_and_data(header, data)?),
            RasterDataType::Uint64 => AnyDenseRaster::U64(DenseRaster::<u64>::from_tile_header_and_data(header, data)?),
            RasterDataType::Float32 => AnyDenseRaster::F32(DenseRaster::<f32>::from_tile_header_and_data(header, data)?),
            RasterDataType::Float64 => AnyDenseRaster::F64(DenseRaster::<f64>::from_tile_header_and_data(header, data)?),
        })
    }

    fn encode_raster_tile(&self, algorithm: CompressionAlgorithm) -> Result<Vec<u8>> {
        match self {
            AnyDenseRaster::I8(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseRaster::U8(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseRaster::I16(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseRaster::U16(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseRaster::I32(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseRaster::U32(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseRaster::I64(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseRaster::U64(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseRaster::F32(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseRaster::F64(raster) => raster.encode_raster_tile(algorithm),
        }
    }

    #[cfg(target_arch = "wasm32")]
    fn from_array_buffer(array_buffer: &js_sys::ArrayBuffer) -> Result<Self> {
        if array_buffer.byte_length() == 0 {
            return Err(Error::InvalidArgument("Empty tile data buffer provided".into()));
        }

        let u8_array = js_sys::Uint8Array::new(array_buffer);
        Self::from_tile_bytes(&u8_array.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_u32() {
        const TILE_WIDTH: usize = 256;
        const TILE_HEIGHT: usize = 256;

        let tile = DenseRaster::new(
            RasterSize::with_rows_cols(TILE_HEIGHT, TILE_WIDTH),
            (0..(TILE_WIDTH * TILE_HEIGHT) as u32).collect::<Vec<u32>>(),
        );

        let encoded = tile.encode_raster_tile(CompressionAlgorithm::Lz4Block).unwrap();

        let decoded = AnyDenseRaster::from_tile_bytes(&encoded).unwrap();
        assert!(matches!(decoded, AnyDenseRaster::U32(_)));

        let decoded_tile: DenseRaster<u32> = decoded.try_into().expect("Expected U32 tile");
        assert_eq!(tile.width(), decoded_tile.width());
        assert_eq!(tile.height(), decoded_tile.height());
        assert_eq!(tile.as_slice(), decoded_tile.as_slice());
    }

    #[test]
    fn encode_decode_u8() {
        const TILE_WIDTH: usize = 10;
        const TILE_HEIGHT: usize = 10;

        let tile = DenseRaster::new(
            RasterSize::with_rows_cols(TILE_HEIGHT, TILE_WIDTH),
            (0..(TILE_WIDTH * TILE_HEIGHT) as u8).collect::<Vec<u8>>(),
        );

        let encoded = tile.encode_raster_tile(CompressionAlgorithm::Lz4Block).unwrap();
        let decoded = DenseRaster::<u8>::from_tile_bytes(&encoded).unwrap();

        assert_eq!(tile.width(), decoded.width());
        assert_eq!(tile.height(), decoded.height());
        assert_eq!(tile.as_slice(), decoded.as_slice());
    }
}

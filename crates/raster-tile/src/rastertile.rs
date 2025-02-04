use geo::{raster, Columns, Rows};
use geo::{AnyDenseArray, Array, ArrayDataType, ArrayNum, DenseArray, RasterSize};

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

impl<T: ArrayNum<T>> RasterTileIO for DenseArray<T> {
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
            self.columns().count() as u16,
            self.rows().count() as u16,
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

        Ok(DenseArray::new(
            RasterSize::with_rows_cols(Rows(header.tile_height as i32), Columns(header.tile_width as i32)),
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

impl<T: ArrayNum<T>> RasterTileCastIO for DenseArray<T> {
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
            ArrayDataType::Int8 => DenseArray::<i8>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            ArrayDataType::Uint8 => DenseArray::<u8>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            ArrayDataType::Int16 => DenseArray::<i16>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            ArrayDataType::Uint16 => DenseArray::<u16>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            ArrayDataType::Int32 => DenseArray::<i32>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            ArrayDataType::Uint32 => DenseArray::<u32>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            ArrayDataType::Int64 => DenseArray::<i64>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            ArrayDataType::Uint64 => DenseArray::<u64>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            ArrayDataType::Float32 => DenseArray::<f32>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            ArrayDataType::Float64 => DenseArray::<f64>::from_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
        }
    }
}

impl RasterTileIO for AnyDenseArray {
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
            ArrayDataType::Int8 => AnyDenseArray::I8(DenseArray::<i8>::from_tile_header_and_data(header, data)?),
            ArrayDataType::Uint8 => AnyDenseArray::U8(DenseArray::<u8>::from_tile_header_and_data(header, data)?),
            ArrayDataType::Int16 => AnyDenseArray::I16(DenseArray::<i16>::from_tile_header_and_data(header, data)?),
            ArrayDataType::Uint16 => AnyDenseArray::U16(DenseArray::<u16>::from_tile_header_and_data(header, data)?),
            ArrayDataType::Int32 => AnyDenseArray::I32(DenseArray::<i32>::from_tile_header_and_data(header, data)?),
            ArrayDataType::Uint32 => AnyDenseArray::U32(DenseArray::<u32>::from_tile_header_and_data(header, data)?),
            ArrayDataType::Int64 => AnyDenseArray::I64(DenseArray::<i64>::from_tile_header_and_data(header, data)?),
            ArrayDataType::Uint64 => AnyDenseArray::U64(DenseArray::<u64>::from_tile_header_and_data(header, data)?),
            ArrayDataType::Float32 => AnyDenseArray::F32(DenseArray::<f32>::from_tile_header_and_data(header, data)?),
            ArrayDataType::Float64 => AnyDenseArray::F64(DenseArray::<f64>::from_tile_header_and_data(header, data)?),
        })
    }

    fn encode_raster_tile(&self, algorithm: CompressionAlgorithm) -> Result<Vec<u8>> {
        match self {
            AnyDenseArray::I8(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseArray::U8(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseArray::I16(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseArray::U16(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseArray::I32(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseArray::U32(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseArray::I64(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseArray::U64(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseArray::F32(raster) => raster.encode_raster_tile(algorithm),
            AnyDenseArray::F64(raster) => raster.encode_raster_tile(algorithm),
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

    const TILE_WIDTH: usize = 256;
    const TILE_HEIGHT: usize = 256;
    const TILE_SIZE: RasterSize = RasterSize::with_rows_cols(Rows(256), Columns(256));

    #[test]
    fn encode_decode_u32() {
        let tile = DenseArray::new(TILE_SIZE, (0..(TILE_WIDTH * TILE_HEIGHT) as u32).collect::<Vec<u32>>());

        let encoded = tile.encode_raster_tile(CompressionAlgorithm::Lz4Block).unwrap();

        let decoded = AnyDenseArray::from_tile_bytes(&encoded).unwrap();
        assert!(matches!(decoded, AnyDenseArray::U32(_)));

        let decoded_tile: DenseArray<u32> = decoded.try_into().expect("Expected U32 tile");
        assert_eq!(tile.columns(), decoded_tile.columns());
        assert_eq!(tile.rows(), decoded_tile.rows());
        assert_eq!(tile.as_slice(), decoded_tile.as_slice());
    }

    #[test]
    fn encode_decode_u8() {
        const TILE_WIDTH: usize = 10;
        const TILE_HEIGHT: usize = 10;
        const TILE_SIZE: RasterSize = RasterSize::with_rows_cols(Rows(10), Columns(10));

        let tile = DenseArray::new(TILE_SIZE, (0..(TILE_WIDTH * TILE_HEIGHT) as u8).collect::<Vec<u8>>());

        let encoded = tile.encode_raster_tile(CompressionAlgorithm::Lz4Block).unwrap();
        let decoded = DenseArray::<u8>::from_tile_bytes(&encoded).unwrap();

        assert_eq!(tile.columns(), decoded.columns());
        assert_eq!(tile.rows(), decoded.rows());
        assert_eq!(tile.as_slice(), decoded.as_slice());
    }
}

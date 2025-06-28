use geo::{AnyDenseArray, Array, ArrayDataType, ArrayMetadata, ArrayNum, DenseArray, RasterSize};
use geo::{Columns, Rows, raster};

use crate::{CompressionAlgorithm, Error, Result, TileHeader};
use crate::{RASTER_TILE_SIGNATURE, lz4};

enum TileFormat {
    #[cfg(feature = "float_png")]
    FloatPng,
    RasterTile,
}

fn detect_tile_format(header: &[u8]) -> Result<TileFormat> {
    #[cfg(feature = "float_png")]
    const PNG_SIGNATURE: u32 = u32::from_le_bytes([0x89, b'P', b'N', b'G']);

    let header: [u8; 4] = header[..4]
        .try_into()
        .map_err(|_| Error::InvalidArgument(format!("Invalid tile data, size is only {} bytes", header.len())))?;
    let header = u32::from_le_bytes(header);

    match header {
        #[cfg(feature = "float_png")]
        PNG_SIGNATURE => Ok(TileFormat::FloatPng),
        RASTER_TILE_SIGNATURE => Ok(TileFormat::RasterTile),
        _ => Err(Error::InvalidArgument("Could not recognize tile data format".into())),
    }
}

pub trait RasterTileIO {
    /// Create a raster tile from the raw data
    /// The data is expected to be in the format of a `TileHeader` followed by the compressed tile data
    fn from_raster_tile_bytes(data: &[u8]) -> Result<Self>
    where
        Self: std::marker::Sized;

    /// Create a raster tile from a float encoded PNG tile
    /// The data is expected to be in the format of a regular PNG file
    /// Each RGBA pixel is actually a float value after swapping the byte order
    #[cfg(feature = "float_png")]
    fn from_png_bytes(buffer: &[u8]) -> Result<Self>
    where
        Self: std::marker::Sized;

    /// Create a raster tile from the raw data
    /// This verssion also supports 'float encoded' PNG tiles where each RGBA pixel is actually a float value
    /// If no PNG header is detected it will fallback to the normal tile format
    /// In that case, the data is expected to be in the format of a `TileHeader` followed by the compressed tile data
    fn from_tile_bytes_autodetect_format(buffer: &[u8]) -> Result<Self>
    where
        Self: std::marker::Sized;

    // Create a raster tile from the header data structure and the raw compressed data
    fn from_raster_tile_header_and_data(header: &TileHeader, data: &[u8]) -> Result<Self>
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
    fn from_raster_tile_bytes_with_cast(data: &[u8]) -> Result<Self>
    where
        Self: std::marker::Sized;

    // Create a raster tile from the header data structure and the raw compressed data
    // The dataype will be cast to the correct raster type if it doesnt match
    fn from_raster_tile_header_and_data_with_cast(header: &TileHeader, data: &[u8]) -> Result<Self>
    where
        Self: std::marker::Sized;

    // Create a raster tile from the raw data
    // Analyzes the header to detect the format so supports both PNG and raster tile formats
    // The dataype will be cast to the correct raster type if it doesnt match
    fn from_tile_bytes_autodetect_format_with_cast(buffer: &[u8]) -> Result<Self>
    where
        Self: std::marker::Sized;
}

impl<T: ArrayNum, Meta: ArrayMetadata> RasterTileIO for DenseArray<T, Meta> {
    fn from_raster_tile_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < std::mem::size_of::<TileHeader>() {
            return Err(Error::InvalidArgument("Tile data is too short".into()));
        }

        let header = TileHeader::from_bytes(data)?;
        if data.len() != std::mem::size_of::<TileHeader>() + header.data_size as usize {
            return Err(Error::InvalidArgument("Tile data size mismatch".into()));
        }

        Self::from_raster_tile_header_and_data(&header, &data[std::mem::size_of::<TileHeader>()..])
    }

    #[cfg(feature = "float_png")]
    fn from_png_bytes(buffer: &[u8]) -> Result<Self> {
        use num::NumCast;

        let (data, raster_size, pixel_format) = crate::imageprocessing::decode_png(buffer)?;

        if pixel_format != png::ColorType::Rgba {
            return Err(Error::InvalidArgument("Only RGBA png data is supported".into()));
        }

        if data.len() != raster_size.cell_count() {
            return Err(Error::InvalidArgument("Invalid png tile data length".into()));
        }

        if T::TYPE != ArrayDataType::Float32 {
            // Safety: The data is a valid slice of f32 values, so it is safe to reinterpret it
            let float_vec = unsafe { inf::allocate::reinterpret_aligned_vec::<f32, T>(data) };
            Ok(Self::new(Meta::sized(raster_size, T::TYPE), float_vec).expect("Raster size bug"))
        } else {
            Ok(Self::from_iter_opt(
                Meta::sized(raster_size, T::TYPE),
                data.iter().map(|f| if f.is_nan() { None } else { NumCast::from(*f) }),
            )
            .expect("Raster size bug"))
        }
    }

    fn from_tile_bytes_autodetect_format(buffer: &[u8]) -> Result<Self> {
        match detect_tile_format(buffer)? {
            #[cfg(feature = "float_png")]
            TileFormat::FloatPng => Self::from_png_bytes(buffer),
            TileFormat::RasterTile => Self::from_raster_tile_bytes(buffer),
        }
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

    fn from_raster_tile_header_and_data(header: &TileHeader, data: &[u8]) -> Result<Self> {
        assert!(header.data_type == T::TYPE, "Tile data type mismatch");
        if data.len() != header.data_size as usize {
            return Err(Error::InvalidArgument("Tile data size mismatch".into()));
        }

        let data = match header.compression {
            CompressionAlgorithm::Lz4Block => lz4::decompress_tile_data(header.tile_width as usize * header.tile_height as usize, data)?,
        };

        Ok(DenseArray::new(
            Meta::sized_for_type::<T>(RasterSize::with_rows_cols(
                Rows(header.tile_height as i32),
                Columns(header.tile_width as i32),
            )),
            data,
        )
        .expect("Raster size calculation mistake"))
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
        Self::from_tile_bytes_autodetect_format(&u8_array.to_vec())
    }
}

impl<T: ArrayNum, Meta: ArrayMetadata> RasterTileCastIO for DenseArray<T, Meta> {
    fn from_raster_tile_bytes_with_cast(data: &[u8]) -> Result<Self> {
        if data.len() < std::mem::size_of::<TileHeader>() {
            return Err(Error::InvalidArgument("Tile data is too short".into()));
        }

        let header = TileHeader::from_bytes(data)?;
        if data.len() != std::mem::size_of::<TileHeader>() + header.data_size as usize {
            return Err(Error::InvalidArgument("Tile data size mismatch".into()));
        }

        Self::from_raster_tile_header_and_data_with_cast(&header, &data[std::mem::size_of::<TileHeader>()..])
    }

    fn from_raster_tile_header_and_data_with_cast(header: &TileHeader, data: &[u8]) -> Result<Self> {
        if data.len() != header.data_size as usize {
            return Err(Error::InvalidArgument("Tile data size mismatch".into()));
        }

        match header.data_type {
            ArrayDataType::Int8 => DenseArray::<i8, Meta>::from_raster_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            ArrayDataType::Uint8 => DenseArray::<u8, Meta>::from_raster_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            ArrayDataType::Int16 => DenseArray::<i16, Meta>::from_raster_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            ArrayDataType::Uint16 => {
                DenseArray::<u16, Meta>::from_raster_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r))
            }
            ArrayDataType::Int32 => DenseArray::<i32, Meta>::from_raster_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            ArrayDataType::Uint32 => {
                DenseArray::<u32, Meta>::from_raster_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r))
            }
            ArrayDataType::Int64 => DenseArray::<i64, Meta>::from_raster_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r)),
            ArrayDataType::Uint64 => {
                DenseArray::<u64, Meta>::from_raster_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r))
            }
            ArrayDataType::Float32 => {
                DenseArray::<f32, Meta>::from_raster_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r))
            }
            ArrayDataType::Float64 => {
                DenseArray::<f64, Meta>::from_raster_tile_header_and_data(header, data).map(|r| raster::algo::cast(&r))
            }
        }
    }

    fn from_tile_bytes_autodetect_format_with_cast(buffer: &[u8]) -> Result<Self>
    where
        Self: std::marker::Sized,
    {
        match detect_tile_format(buffer)? {
            #[cfg(feature = "float_png")]
            TileFormat::FloatPng => Self::from_png_bytes(buffer),
            TileFormat::RasterTile => Self::from_raster_tile_bytes_with_cast(buffer),
        }
    }
}

impl<T: ArrayMetadata> RasterTileIO for AnyDenseArray<T> {
    fn from_raster_tile_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < std::mem::size_of::<TileHeader>() {
            return Err(Error::InvalidArgument("Tile data is too short".into()));
        }

        let header = TileHeader::from_bytes(data)?;
        if data.len() != std::mem::size_of::<TileHeader>() + header.data_size as usize {
            return Err(Error::InvalidArgument("Tile data size mismatch".into()));
        }

        Self::from_raster_tile_header_and_data(&header, &data[std::mem::size_of::<TileHeader>()..])
    }

    fn from_raster_tile_header_and_data(header: &TileHeader, data: &[u8]) -> Result<Self>
    where
        Self: std::marker::Sized,
    {
        Ok(match header.data_type {
            ArrayDataType::Int8 => AnyDenseArray::I8(DenseArray::<i8, T>::from_raster_tile_header_and_data(header, data)?),
            ArrayDataType::Uint8 => AnyDenseArray::U8(DenseArray::<u8, T>::from_raster_tile_header_and_data(header, data)?),
            ArrayDataType::Int16 => AnyDenseArray::I16(DenseArray::<i16, T>::from_raster_tile_header_and_data(header, data)?),
            ArrayDataType::Uint16 => AnyDenseArray::U16(DenseArray::<u16, T>::from_raster_tile_header_and_data(header, data)?),
            ArrayDataType::Int32 => AnyDenseArray::I32(DenseArray::<i32, T>::from_raster_tile_header_and_data(header, data)?),
            ArrayDataType::Uint32 => AnyDenseArray::U32(DenseArray::<u32, T>::from_raster_tile_header_and_data(header, data)?),
            ArrayDataType::Int64 => AnyDenseArray::I64(DenseArray::<i64, T>::from_raster_tile_header_and_data(header, data)?),
            ArrayDataType::Uint64 => AnyDenseArray::U64(DenseArray::<u64, T>::from_raster_tile_header_and_data(header, data)?),
            ArrayDataType::Float32 => AnyDenseArray::F32(DenseArray::<f32, T>::from_raster_tile_header_and_data(header, data)?),
            ArrayDataType::Float64 => AnyDenseArray::F64(DenseArray::<f64, T>::from_raster_tile_header_and_data(header, data)?),
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
        Self::from_tile_bytes_autodetect_format(&u8_array.to_vec())
    }

    #[cfg(feature = "float_png")]
    fn from_png_bytes(buffer: &[u8]) -> Result<Self>
    where
        Self: std::marker::Sized,
    {
        Ok(AnyDenseArray::F32(DenseArray::<f32, T>::from_png_bytes(buffer)?))
    }

    fn from_tile_bytes_autodetect_format(buffer: &[u8]) -> Result<Self>
    where
        Self: std::marker::Sized,
    {
        match detect_tile_format(buffer)? {
            #[cfg(feature = "float_png")]
            TileFormat::FloatPng => Self::from_png_bytes(buffer),
            TileFormat::RasterTile => Self::from_raster_tile_bytes(buffer),
        }
    }
}

#[cfg(test)]
mod tests {
    use geo::{RasterMetadata, RasterSize};
    use inf::allocate;

    use super::*;

    const TILE_WIDTH: usize = 256;
    const TILE_HEIGHT: usize = 256;
    const TILE_SIZE: RasterSize = RasterSize::with_rows_cols(Rows(256), Columns(256));

    #[test]
    fn encode_decode_u32() {
        let meta = RasterMetadata::sized_for_type::<u32>(TILE_SIZE);
        let tile = DenseArray::new(meta, allocate::aligned_vec_from_iter(0..(TILE_WIDTH * TILE_HEIGHT) as u32)).unwrap();

        let encoded = tile.encode_raster_tile(CompressionAlgorithm::Lz4Block).unwrap();

        let decoded = AnyDenseArray::from_raster_tile_bytes(&encoded).unwrap();
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

        let meta = RasterMetadata::sized_for_type::<u8>(TILE_SIZE);
        let tile = DenseArray::new(meta, allocate::aligned_vec_from_iter(0..(TILE_WIDTH * TILE_HEIGHT) as u8)).unwrap();

        let encoded = tile.encode_raster_tile(CompressionAlgorithm::Lz4Block).unwrap();
        let decoded = DenseArray::<u8>::from_raster_tile_bytes(&encoded).unwrap();

        assert_eq!(tile.columns(), decoded.columns());
        assert_eq!(tile.rows(), decoded.rows());
        assert_eq!(tile.as_slice(), decoded.as_slice());
    }
}

use bytemuck::NoUninit;

use crate::{Error, RasterTileDataType, Result};

const SIGNATURE: u32 = u32::from_le_bytes([b'T', b'I', b'L', b'E']);

#[derive(Debug, Clone, Copy, PartialEq, Eq, NoUninit)]
#[repr(u8)]
pub enum CompressionAlgorithm {
    Lz4Block = 0,
}

/// Binary header format for the tile data.
/// The header is followed by the tile data
/// All the fields are stored in little endian
/// The data blob following the header is stored in the format specified by the `data_type` field
/// and is compressed using the zstd data compression algorithm. The decompressed data should always be
/// `tile_width` * `tile_height` * sizeof(`data_type`) bytes long.
#[derive(Clone, Copy, NoUninit)]
#[repr(packed, C)]
pub struct TileHeader {
    /// signature to recognize the file format (always ['T', 'I', 'L', 'E'"] or 0x454C4954)
    pub signature: u32,
    /// The version of the file format (currently 1)
    pub version: u16,
    /// The data type of the tile data represented by a `TileDataType` as u8
    pub data_type: RasterTileDataType,
    /// The compression algorithm used for the tile data
    pub compression: CompressionAlgorithm,
    /// The width of the tile in pixels
    pub tile_width: u16,
    /// The height of the tile in pixels
    pub tile_height: u16,
    /// The data size in bytes of the tile data that follows the header
    pub data_size: u32,
}

impl TileHeader {
    pub fn new(
        data_type: RasterTileDataType,
        compression: CompressionAlgorithm,
        tile_width: u16,
        tile_height: u16,
        data_size: u32,
    ) -> Self {
        Self {
            signature: SIGNATURE,
            version: 1,
            data_type,
            compression,
            tile_width,
            tile_height,
            data_size,
        }
    }

    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        let header_size = std::mem::size_of::<TileHeader>();
        if data.len() < header_size {
            return Err(Error::InvalidArgument("Tile data is too short".into()));
        }

        let header: TileHeader = unsafe { std::ptr::read(data.as_ptr().cast()) };
        if header.signature != SIGNATURE {
            return Err(Error::InvalidArgument("Invalid tile signature".into()));
        }

        if header.version != 1 {
            return Err(Error::InvalidArgument("Unsupported tile version".into()));
        }

        if header.data_type as u8 > RasterTileDataType::Float64 as u8 {
            return Err(Error::InvalidArgument("Invalid tile data type".into()));
        }

        if header.compression as u8 > CompressionAlgorithm::Lz4Block as u8 {
            return Err(Error::InvalidArgument("Invalid compression algorithm".into()));
        }

        Ok(header)
    }
}

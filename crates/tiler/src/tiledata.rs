use crate::{tileformat::TileFormat, PixelFormat};

#[derive(Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct TileData {
    pub format: TileFormat,
    pub pixel_format: PixelFormat,
    pub data: Vec<u8>,
}

impl TileData {
    pub fn new(format: TileFormat, pixel_format: PixelFormat, data: Vec<u8>) -> TileData {
        TileData {
            format,
            pixel_format,
            data,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

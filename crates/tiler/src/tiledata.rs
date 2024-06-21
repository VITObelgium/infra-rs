use serde::Serialize;

use crate::tileformat::TileFormat;

#[derive(Default, Serialize)]
pub struct TileData {
    pub format: TileFormat,
    pub data: Vec<u8>,
}

impl TileData {
    pub fn new(format: TileFormat, data: Vec<u8>) -> TileData {
        TileData { format, data }
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

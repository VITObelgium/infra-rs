use bytemuck::{AnyBitPattern, NoUninit};
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::prelude::*;

pub trait TileDataType: NoUninit + AnyBitPattern {
    const TYPE: RasterTileDataType;
    fn is_nodata(&self) -> bool;
}

impl TileDataType for i8 {
    const TYPE: RasterTileDataType = RasterTileDataType::Int8;

    fn is_nodata(&self) -> bool {
        *self == i8::MIN
    }
}

impl TileDataType for u8 {
    const TYPE: RasterTileDataType = RasterTileDataType::Uint8;

    fn is_nodata(&self) -> bool {
        *self == u8::MAX
    }
}

impl TileDataType for i16 {
    const TYPE: RasterTileDataType = RasterTileDataType::Int16;

    fn is_nodata(&self) -> bool {
        *self == i16::MIN
    }
}

impl TileDataType for u16 {
    const TYPE: RasterTileDataType = RasterTileDataType::Uint16;

    fn is_nodata(&self) -> bool {
        *self == u16::MAX
    }
}

impl TileDataType for i32 {
    const TYPE: RasterTileDataType = RasterTileDataType::Int32;

    fn is_nodata(&self) -> bool {
        *self == i32::MIN
    }
}

impl TileDataType for u32 {
    const TYPE: RasterTileDataType = RasterTileDataType::Uint32;

    fn is_nodata(&self) -> bool {
        *self == u32::MAX
    }
}

impl TileDataType for f32 {
    const TYPE: RasterTileDataType = RasterTileDataType::Float32;

    fn is_nodata(&self) -> bool {
        self.is_nan()
    }
}

impl TileDataType for f64 {
    const TYPE: RasterTileDataType = RasterTileDataType::Float64;

    fn is_nodata(&self) -> bool {
        self.is_nan()
    }
}

#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, NoUninit)]
#[repr(u8)]
pub enum RasterTileDataType {
    Int8 = 0,
    Uint8 = 1,
    Int16 = 2,
    Uint16 = 3,
    Int32 = 4,
    Uint32 = 5,
    Float32 = 6,
    Float64 = 7,
}

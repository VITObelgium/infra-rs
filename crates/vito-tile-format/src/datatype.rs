use bytemuck::{AnyBitPattern, NoUninit};

pub trait TileDataType: NoUninit + AnyBitPattern {
    const TYPE: RasterTileDataType;
}

impl TileDataType for i8 {
    const TYPE: RasterTileDataType = RasterTileDataType::Int8;
}

impl TileDataType for u8 {
    const TYPE: RasterTileDataType = RasterTileDataType::Uint8;
}

impl TileDataType for i16 {
    const TYPE: RasterTileDataType = RasterTileDataType::Int16;
}

impl TileDataType for u16 {
    const TYPE: RasterTileDataType = RasterTileDataType::Uint16;
}

impl TileDataType for i32 {
    const TYPE: RasterTileDataType = RasterTileDataType::Int32;
}

impl TileDataType for u32 {
    const TYPE: RasterTileDataType = RasterTileDataType::Uint32;
}

impl TileDataType for i64 {
    const TYPE: RasterTileDataType = RasterTileDataType::Int64;
}

impl TileDataType for u64 {
    const TYPE: RasterTileDataType = RasterTileDataType::Uint64;
}

impl TileDataType for f32 {
    const TYPE: RasterTileDataType = RasterTileDataType::Float32;
}

impl TileDataType for f64 {
    const TYPE: RasterTileDataType = RasterTileDataType::Float64;
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
    Int64 = 6,
    Uint64 = 7,
    Float32 = 8,
    Float64 = 9,
}

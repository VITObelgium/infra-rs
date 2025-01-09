use bytemuck::{AnyBitPattern, NoUninit};

use crate::RasterTile;

pub trait TileDataType: NoUninit + AnyBitPattern {
    const TYPE: RasterTileDataType;
    type TileType;
}

impl TileDataType for i8 {
    const TYPE: RasterTileDataType = RasterTileDataType::Int8;
    type TileType = RasterTile<i8>;
}

impl TileDataType for u8 {
    const TYPE: RasterTileDataType = RasterTileDataType::Uint8;
    type TileType = RasterTile<u8>;
}

impl TileDataType for i16 {
    const TYPE: RasterTileDataType = RasterTileDataType::Int16;
    type TileType = RasterTile<i16>;
}

impl TileDataType for u16 {
    const TYPE: RasterTileDataType = RasterTileDataType::Uint16;
    type TileType = RasterTile<u16>;
}

impl TileDataType for i32 {
    const TYPE: RasterTileDataType = RasterTileDataType::Int32;
    type TileType = RasterTile<i32>;
}

impl TileDataType for u32 {
    const TYPE: RasterTileDataType = RasterTileDataType::Uint32;
    type TileType = RasterTile<u32>;
}

impl TileDataType for i64 {
    const TYPE: RasterTileDataType = RasterTileDataType::Int64;
    type TileType = RasterTile<i64>;
}

impl TileDataType for u64 {
    const TYPE: RasterTileDataType = RasterTileDataType::Uint64;
    type TileType = RasterTile<u64>;
}

impl TileDataType for f32 {
    const TYPE: RasterTileDataType = RasterTileDataType::Float32;
    type TileType = RasterTile<f32>;
}

impl TileDataType for f64 {
    const TYPE: RasterTileDataType = RasterTileDataType::Float64;
    type TileType = RasterTile<f64>;
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

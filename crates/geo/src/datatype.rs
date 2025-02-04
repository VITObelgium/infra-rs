#[cfg_attr(target_arch = "wasm32", wasm_bindgen::prelude::wasm_bindgen)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "specta", derive(specta::Type))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RasterDataType {
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

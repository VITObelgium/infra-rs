#[cfg_attr(target_arch = "wasm32", wasm_bindgen::prelude::wasm_bindgen)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "specta", derive(specta::Type))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ArrayDataType {
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

impl ArrayDataType {
    pub fn to_str(&self) -> &'static str {
        match self {
            Self::Int8 => "int8",
            Self::Uint8 => "uint8",
            Self::Int16 => "int16",
            Self::Uint16 => "uint16",
            Self::Int32 => "int32",
            Self::Uint32 => "uint32",
            Self::Int64 => "int64",
            Self::Uint64 => "uint64",
            Self::Float32 => "float32",
            Self::Float64 => "float64",
        }
    }
}

impl std::fmt::Display for ArrayDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.to_str())
    }
}

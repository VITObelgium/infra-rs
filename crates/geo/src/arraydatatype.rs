use num::NumCast;

use crate::nodata::Nodata as _;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(target_arch = "wasm32", derive(tsify::Tsify), tsify(from_wasm_abi, into_wasm_abi))]
#[cfg_attr(any(feature = "serde", target_arch = "wasm32"), derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "specta", derive(specta::Type))]
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

    pub fn default_nodata_value(&self) -> f64 {
        match self {
            Self::Int8 => NumCast::from(i8::NODATA).unwrap_or(f64::NAN),
            Self::Uint8 => NumCast::from(u8::NODATA).unwrap_or(f64::NAN),
            Self::Int16 => NumCast::from(i16::NODATA).unwrap_or(f64::NAN),
            Self::Uint16 => NumCast::from(u16::NODATA).unwrap_or(f64::NAN),
            Self::Int32 => NumCast::from(i32::NODATA).unwrap_or(f64::NAN),
            Self::Uint32 => NumCast::from(u32::NODATA).unwrap_or(f64::NAN),
            Self::Int64 => NumCast::from(i64::NODATA).unwrap_or(f64::NAN),
            Self::Uint64 => NumCast::from(u64::NODATA).unwrap_or(f64::NAN),
            Self::Float32 => NumCast::from(f32::NODATA).unwrap_or(f64::NAN),
            Self::Float64 => NumCast::from(f64::NODATA).unwrap_or(f64::NAN),
        }
    }
}

impl std::fmt::Display for ArrayDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.to_str())
    }
}

//! Algorithms that operate on `AnyDenseArray`.
//!
//! These functions provide type-erased wrappers around the generic algorithms in `raster::algo`.
//! They use the `apply_to_anydensearray!` macro to automatically handle all variants of `AnyDenseArray`.
//!

use std::ops::RangeInclusive;

use crate::{AnyDenseArray, ArrayMetadata, DenseArray, Error, RasterScale, Result, apply_to_anydensearray, raster::algo};
use algo::Scale;

fn cast_range_f64_to_f32(range: Option<RangeInclusive<f64>>) -> Option<RangeInclusive<f32>> {
    range.map(|r| *r.start() as f32..=*r.end() as f32)
}

/// Crops an `AnyDenseArray` by removing nodata edges.
///
/// This is a type-erased wrapper around `algo::crop`.
pub fn crop(array: AnyDenseArray) -> AnyDenseArray {
    apply_to_anydensearray!(array, arr, algo::crop(arr))
}

impl<Meta: ArrayMetadata> Scale<f64> for AnyDenseArray<Meta> {
    type Meta = Meta;

    fn scale_to_u8(&self, input_range: Option<RangeInclusive<f64>>) -> Result<DenseArray<u8, Meta>> {
        match self {
            AnyDenseArray::F32(arr) => arr.scale_to_u8(cast_range_f64_to_f32(input_range)),
            AnyDenseArray::F64(arr) => arr.scale_to_u8(input_range),
            _ => Err(Error::InvalidArgument(
                "Scale is only supported for floating point rasters (f32 and f64)".to_string(),
            )),
        }
    }

    fn scale_to_u16(&self, input_range: Option<RangeInclusive<f64>>) -> Result<DenseArray<u16, Meta>> {
        match self {
            AnyDenseArray::F32(arr) => arr.scale_to_u16(cast_range_f64_to_f32(input_range)),
            AnyDenseArray::F64(arr) => arr.scale_to_u16(input_range),
            _ => Err(Error::InvalidArgument(
                "Scale is only supported for floating point rasters (f32 and f64)".to_string(),
            )),
        }
    }

    fn scale_to_u8_slice(&self, input_range: Option<RangeInclusive<f64>>, output: &mut [u8]) -> Result<RasterScale> {
        match self {
            AnyDenseArray::F32(arr) => arr.scale_to_u8_slice(cast_range_f64_to_f32(input_range), output),
            AnyDenseArray::F64(arr) => arr.scale_to_u8_slice(input_range, output),
            _ => Err(Error::InvalidArgument(
                "Scale is only supported for floating point rasters (f32 and f64)".to_string(),
            )),
        }
    }

    fn scale_to_u16_slice(&self, input_range: Option<RangeInclusive<f64>>, output: &mut [u16]) -> Result<RasterScale> {
        match self {
            AnyDenseArray::F32(arr) => arr.scale_to_u16_slice(cast_range_f64_to_f32(input_range), output),
            AnyDenseArray::F64(arr) => arr.scale_to_u16_slice(input_range, output),
            _ => Err(Error::InvalidArgument(
                "Scale is only supported for floating point rasters (f32 and f64)".to_string(),
            )),
        }
    }
}

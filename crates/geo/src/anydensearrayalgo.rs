//! Algorithms that operate on `AnyDenseArray`.
//!
//! These functions provide type-erased wrappers around the generic algorithms in `raster::algo`.
//! They use the `apply_to_anydensearray!` macro to automatically handle all variants of `AnyDenseArray`.
//!

use std::ops::RangeInclusive;

use crate::{AnyDenseArray, ArrayMetadata, DenseArray, Error, Result, apply_to_anydensearray, raster::algo};
use algo::Scale;

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
            AnyDenseArray::F32(arr) => {
                let range = input_range.map(|r| *r.start() as f32..=*r.end() as f32);
                arr.scale_to_u8(range)
            }
            AnyDenseArray::F64(arr) => arr.scale_to_u8(input_range),
            _ => Err(Error::InvalidArgument(
                "Scale is only supported for floating point rasters (f32 and f64)".to_string(),
            )),
        }
    }

    fn scale_to_u16(&self, input_range: Option<RangeInclusive<f64>>) -> Result<DenseArray<u16, Meta>> {
        match self {
            AnyDenseArray::F32(arr) => {
                let range = input_range.map(|r| *r.start() as f32..=*r.end() as f32);
                arr.scale_to_u16(range)
            }
            AnyDenseArray::F64(arr) => arr.scale_to_u16(input_range),
            _ => Err(Error::InvalidArgument(
                "Scale is only supported for floating point rasters (f32 and f64)".to_string(),
            )),
        }
    }
}

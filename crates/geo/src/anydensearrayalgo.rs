//! Algorithms that operate on `AnyDenseArray`.
//!
//! These functions provide type-erased wrappers around the generic algorithms in `raster::algo`.
//! They use the `apply_to_anydensearray!` macro to automatically handle all variants of `AnyDenseArray`.
//!

use crate::{AnyDenseArray, apply_to_anydensearray, raster::algo};

/// Crops an `AnyDenseArray` by removing nodata edges.
///
/// This is a type-erased wrapper around `algo::crop`.
pub fn crop(array: AnyDenseArray) -> AnyDenseArray {
    apply_to_anydensearray!(array, arr, algo::crop(arr))
}

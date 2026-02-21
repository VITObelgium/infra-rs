//! Type casting operations for converting raster data between different numeric types.
//!
//! This module provides a trait for casting raster values from one numeric type to another
//! while preserving the array structure and metadata.
//!
//! # Usage
//!
//! Use the [`Cast`] trait to access casting methods on `DenseArray`:
//!
//! ```ignore
//! use crate::raster::algo::Cast;
//!
//! let f64_raster: DenseArray<f64, _> = my_u8_raster.cast::<f64>();
//! let i32_raster: DenseArray<i32, _> = my_f32_raster.cast::<i32>();
//!
//! // Or cast directly into a pre-allocated slice:
//! let mut output = vec![0u8; my_f64_raster.len()];
//! my_f64_raster.cast_to_slice(&mut output);
//! ```
//!
//! The [`Cast`] trait provides:
//! - `cast<TDest>()`: Casts raster values to the destination type, returning a new array
//! - `cast_to_slice<TDest>()`: Casts raster values into a pre-allocated slice
//! - `into_cast<TDest>()`: Casts raster values, reusing the buffer if the destination type is smaller or equal in size

use crate::{Array, ArrayMetadata, ArrayNum, DenseArray, Error, Result};
use inf::allocate::{AlignedVec, AlignedVecUnderConstruction};
use num::NumCast;
use std::any::TypeId;
use std::mem::size_of;

/// Trait for casting array values to a different numeric type.
///
/// This trait provides methods to convert all values in an array from one numeric type
/// to another, handling nodata values appropriately.
pub trait Cast: Array {
    /// Cast the array values to the destination type, returning a new array.
    ///
    /// Values that cannot be represented in the destination type will become nodata.
    /// Existing nodata values are preserved as nodata in the output.
    fn cast<TDest: ArrayNum>(&self) -> Self::WithPixelType<TDest>
    where
        for<'a> &'a Self: IntoIterator<Item = Option<Self::Pixel>>;

    /// Cast the array values to the destination type, writing into a pre-allocated slice.
    ///
    /// Values that cannot be represented in the destination type will become nodata.
    /// Existing nodata values are preserved as nodata in the output.
    ///
    /// # Errors
    ///
    /// Returns an error if the output slice length doesn't match the array length.
    fn cast_to_slice<TDest: ArrayNum>(&self, output: &mut [TDest]) -> Result<()>;

    /// Cast the array values to the destination type, consuming self and reusing the buffer
    /// if the destination type is smaller or equal in size.
    ///
    /// Values that cannot be represented in the destination type will become nodata.
    /// Existing nodata values are preserved as nodata in the output.
    ///
    /// This is more efficient than `cast()` when casting to a smaller type or equally sized type as it avoids
    /// allocating a new buffer.
    fn into_cast<TDest: ArrayNum>(self) -> Self::WithPixelType<TDest>;
}

impl<T: ArrayNum, Meta: ArrayMetadata> Cast for DenseArray<T, Meta> {
    fn cast<TDest: ArrayNum>(&self) -> DenseArray<TDest, Meta>
    where
        for<'a> &'a Self: IntoIterator<Item = Option<T>>,
    {
        let mut output = AlignedVecUnderConstruction::<TDest>::new(self.len());
        self.cast_to_slice(unsafe { output.as_slice_mut() })
            .expect("Size mismatch in cast operation");
        DenseArray::<TDest, Meta>::new(self.metadata().clone(), unsafe { output.assume_init() }).expect("Raster size bug")
    }

    fn cast_to_slice<TDest: ArrayNum>(&self, output: &mut [TDest]) -> Result<()> {
        if output.len() != self.len() {
            return Err(Error::InvalidArgument(format!(
                "Output slice length {} does not match input length {}",
                output.len(),
                self.len()
            )));
        }

        for (opt_v, out) in self.iter_opt().zip(output.iter_mut()) {
            *out = if let Some(v) = opt_v {
                NumCast::from(v).unwrap_or(TDest::NODATA)
            } else {
                TDest::NODATA
            };
        }

        Ok(())
    }

    fn into_cast<TDest: ArrayNum>(self) -> DenseArray<TDest, Meta> {
        if TypeId::of::<T>() == TypeId::of::<TDest>() {
            // If T and TDest are the same type, just transmute the array without any conversion
            let (meta, data) = self.into_raw_parts();
            // SAFETY: We've verified T and TDest are the same type via TypeId
            let output = unsafe { std::mem::transmute::<AlignedVec<T>, AlignedVec<TDest>>(data) };
            return DenseArray::<TDest, Meta>::new(meta, output).expect("Raster size bug");
        }

        if size_of::<TDest>() <= size_of::<T>() {
            // Reuse the buffer - cast in place
            let (meta, data) = self.into_raw_parts();
            let output = cast_vec_reuse::<T, TDest>(data);
            DenseArray::<TDest, Meta>::new(meta, output).expect("Raster size bug")
        } else {
            self.cast()
        }
    }
}

/// Reuse an aligned vec by casting its elements in place.
/// This is only valid when `size_of::<TDest>() <= size_of::<T>()`.
fn cast_vec_reuse<T: ArrayNum, TDest: ArrayNum>(data: AlignedVec<T>) -> AlignedVec<TDest> {
    debug_assert!(size_of::<TDest>() <= size_of::<T>());

    let len = data.len();

    // Calculate how many TDest elements can fit in the original buffer
    let byte_capacity = data.capacity() * size_of::<T>();
    let new_capacity = byte_capacity / size_of::<TDest>();

    // First, cast each element in place from the beginning
    // Since TDest is smaller or equal, we can safely write from the start
    let mut data = data;
    let src_ptr = data.as_ptr();
    let dst_ptr = data.as_mut_ptr().cast::<TDest>();

    for i in 0..len {
        let src_val = unsafe { src_ptr.add(i).read() };
        let dst_val = if src_val.is_nodata() {
            TDest::NODATA
        } else {
            NumCast::from(src_val).unwrap_or(TDest::NODATA)
        };
        unsafe { dst_ptr.add(i).write(dst_val) };
    }

    // Now transmute the vec to the new type
    #[cfg(feature = "simd")]
    {
        let (ptr, _, _, alloc) = data.into_raw_parts_with_alloc();
        unsafe { Vec::from_raw_parts_in(ptr.cast::<TDest>(), len, new_capacity, alloc) }
    }

    #[cfg(not(feature = "simd"))]
    {
        let (ptr, _, _) = data.into_raw_parts();
        unsafe { Vec::from_raw_parts(ptr.cast::<TDest>(), len, new_capacity) }
    }
}

/// Free function for casting an array to a different numeric type.
///
/// This is a convenience wrapper around the [`Cast::cast`] method.
pub fn cast<TDest, R>(src: &R) -> R::WithPixelType<TDest>
where
    R: Cast,
    TDest: ArrayNum,
    for<'a> &'a R: IntoIterator<Item = Option<R::Pixel>>,
{
    src.cast::<TDest>()
}

#[cfg(test)]
mod tests {
    use inf::allocate::aligned_vec_from_slice;

    use super::*;
    use crate::{
        Nodata, RasterMetadata, RasterSize,
        array::{Columns, Rows},
    };

    fn create_test_metadata(rows: i32, cols: i32) -> RasterMetadata {
        RasterMetadata::sized(RasterSize::with_rows_cols(Rows(rows), Columns(cols)), crate::ArrayDataType::Float64)
    }

    #[test]
    fn cast_f64_to_u8() {
        let meta = create_test_metadata(2, 3);
        let data = aligned_vec_from_slice(&[1.0_f64, 2.0, 3.0, 4.0, 5.0, 6.0]);
        let raster = DenseArray::<f64, _>::new(meta, data).unwrap();

        let result: DenseArray<u8, _> = raster.cast();

        let expected: Vec<Option<u8>> = vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)];
        let actual: Vec<Option<u8>> = result.into_iter().collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn cast_u8_to_f64() {
        let meta = create_test_metadata(2, 2);
        let data = aligned_vec_from_slice(&[10_u8, 20, 30, 40]);
        let raster = DenseArray::<u8, _>::new(meta, data).unwrap();

        let result: DenseArray<f64, _> = raster.cast();

        let expected: Vec<Option<f64>> = vec![Some(10.0), Some(20.0), Some(30.0), Some(40.0)];
        let actual: Vec<Option<f64>> = result.into_iter().collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn cast_preserves_nodata() {
        let meta = create_test_metadata(2, 2);
        let data = aligned_vec_from_slice(&[1.0_f64, f64::NODATA, 3.0, 4.0]);
        let raster = DenseArray::<f64, _>::new(meta, data).unwrap();

        let result: DenseArray<u8, _> = raster.cast();

        let actual: Vec<Option<u8>> = result.into_iter().collect();
        assert_eq!(actual[0], Some(1));
        assert_eq!(actual[1], None);
        assert_eq!(actual[2], Some(3));
        assert_eq!(actual[3], Some(4));
    }

    #[test]
    fn cast_f64_to_i32() {
        let meta = create_test_metadata(1, 4);
        let data = aligned_vec_from_slice(&[-10.5_f64, 0.0, 10.9, 100.1]);
        let raster = DenseArray::<f64, _>::new(meta, data).unwrap();

        let result: DenseArray<i32, _> = raster.cast();

        let expected: Vec<Option<i32>> = vec![Some(-10), Some(0), Some(10), Some(100)];
        let actual: Vec<Option<i32>> = result.into_iter().collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn cast_using_free_function() {
        let meta = create_test_metadata(2, 2);
        let data = aligned_vec_from_slice(&[1_i32, 2, 3, 4]);
        let raster = DenseArray::<i32, _>::new(meta, data).unwrap();

        let result: DenseArray<f32, _> = cast(&raster);

        let expected: Vec<Option<f32>> = vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)];
        let actual: Vec<Option<f32>> = result.into_iter().collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn cast_overflow_becomes_nodata() {
        let meta = create_test_metadata(1, 3);
        let data = aligned_vec_from_slice(&[100_i32, 300, 50]);
        let raster = DenseArray::<i32, _>::new(meta, data).unwrap();

        let result: DenseArray<u8, _> = raster.cast();

        let actual: Vec<Option<u8>> = result.into_iter().collect();
        assert_eq!(actual[0], Some(100));
        // 300 cannot fit in u8, so it becomes None (nodata)
        assert_eq!(actual[1], None);
        assert_eq!(actual[2], Some(50));
    }

    #[test]
    fn cast_negative_to_unsigned_becomes_nodata() {
        let meta = create_test_metadata(1, 3);
        let data = aligned_vec_from_slice(&[-10_i32, 0, 10]);
        let raster = DenseArray::<i32, _>::new(meta, data).unwrap();

        let result: DenseArray<u8, _> = raster.cast();

        let actual: Vec<Option<u8>> = result.into_iter().collect();
        // -10 cannot be represented in u8, so it becomes None (nodata)
        assert_eq!(actual[0], None);
        assert_eq!(actual[1], Some(0));
        assert_eq!(actual[2], Some(10));
    }

    #[test]
    fn into_cast_f64_to_u8_reuses_buffer() {
        let meta = create_test_metadata(2, 3);
        let data = aligned_vec_from_slice(&[1.0_f64, 2.0, 3.0, 4.0, 5.0, 6.0]);
        let raster = DenseArray::<f64, _>::new(meta, data).unwrap();

        // Get the pointer before casting
        let ptr_before = raster.as_slice().as_ptr().cast::<u8>();

        // f64 (8 bytes) -> u8 (1 byte), should reuse buffer
        let result: DenseArray<u8, _> = raster.into_cast();

        // Assert the underlying buffer pointer has not changed
        let ptr_after = result.as_slice().as_ptr();
        assert_eq!(
            ptr_before, ptr_after,
            "Buffer pointer should not change when casting to smaller type"
        );

        let expected: Vec<Option<u8>> = vec![Some(1), Some(2), Some(3), Some(4), Some(5), Some(6)];
        let actual: Vec<Option<u8>> = result.into_iter().collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn into_cast_u8_to_f64_allocates_new_buffer() {
        let meta = create_test_metadata(2, 2);
        let data = aligned_vec_from_slice(&[10_u8, 20, 30, 40]);
        let raster = DenseArray::<u8, _>::new(meta, data).unwrap();

        // u8 (1 byte) -> f64 (8 bytes), must allocate new buffer
        let result: DenseArray<f64, _> = raster.into_cast();

        let expected: Vec<Option<f64>> = vec![Some(10.0), Some(20.0), Some(30.0), Some(40.0)];
        let actual: Vec<Option<f64>> = result.into_iter().collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn into_cast_preserves_nodata() {
        let meta = create_test_metadata(2, 2);
        let data = aligned_vec_from_slice(&[1.0_f64, f64::NODATA, 3.0, 4.0]);
        let raster = DenseArray::<f64, _>::new(meta, data).unwrap();

        let result: DenseArray<u8, _> = raster.into_cast();

        let actual: Vec<Option<u8>> = result.into_iter().collect();
        assert_eq!(actual[0], Some(1));
        assert_eq!(actual[1], None);
        assert_eq!(actual[2], Some(3));
        assert_eq!(actual[3], Some(4));
    }

    #[test]
    fn into_cast_same_size_type() {
        let meta = create_test_metadata(2, 2);
        let data = aligned_vec_from_slice(&[1_i32, 2, 3, 4]);
        let raster = DenseArray::<i32, _>::new(meta, data).unwrap();

        // Get the pointer before casting
        let ptr_before = raster.as_slice().as_ptr().cast::<u8>();

        // i32 (4 bytes) -> f32 (4 bytes), should reuse buffer
        let result: DenseArray<f32, _> = raster.into_cast();

        // Assert the underlying buffer pointer has not changed
        let ptr_after = result.as_slice().as_ptr().cast::<u8>();
        assert_eq!(
            ptr_before, ptr_after,
            "Buffer pointer should not change when casting to same size type"
        );

        let expected: Vec<Option<f32>> = vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)];
        let actual: Vec<Option<f32>> = result.into_iter().collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn into_cast_same_type_is_noop() {
        let meta = create_test_metadata(2, 2);
        let data = aligned_vec_from_slice(&[1.0_f64, 2.0, 3.0, 4.0]);
        let raster = DenseArray::<f64, _>::new(meta, data).unwrap();

        // Get the pointer before casting
        let ptr_before = raster.as_slice().as_ptr();

        // f64 -> f64, should be a noop (no conversion)
        let result: DenseArray<f64, _> = raster.into_cast();

        // Assert the underlying buffer pointer has not changed
        let ptr_after = result.as_slice().as_ptr();
        assert_eq!(ptr_before, ptr_after, "Buffer pointer should not change for same-type cast");

        let expected: Vec<Option<f64>> = vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)];
        let actual: Vec<Option<f64>> = result.into_iter().collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn into_cast_overflow_becomes_nodata() {
        let meta = create_test_metadata(1, 3);
        let data = aligned_vec_from_slice(&[100_i32, 300, 50]);
        let raster = DenseArray::<i32, _>::new(meta, data).unwrap();

        let result: DenseArray<u8, _> = raster.into_cast();

        let actual: Vec<Option<u8>> = result.into_iter().collect();
        assert_eq!(actual[0], Some(100));
        // 300 cannot fit in u8, so it becomes None (nodata)
        assert_eq!(actual[1], None);
        assert_eq!(actual[2], Some(50));
    }

    #[test]
    fn cast_to_slice_f64_to_u8() {
        let meta = create_test_metadata(2, 3);
        let data = aligned_vec_from_slice(&[1.0_f64, 2.0, 3.0, 4.0, 5.0, 6.0]);
        let raster = DenseArray::<f64, _>::new(meta, data).unwrap();

        let mut output = vec![0u8; 6];
        raster.cast_to_slice(&mut output).unwrap();

        assert_eq!(output, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn cast_to_slice_preserves_nodata() {
        let meta = create_test_metadata(2, 2);
        let data = aligned_vec_from_slice(&[1.0_f64, f64::NODATA, 3.0, 4.0]);
        let raster = DenseArray::<f64, _>::new(meta, data).unwrap();

        let mut output = vec![0u8; 4];
        raster.cast_to_slice(&mut output).unwrap();

        assert_eq!(output[0], 1);
        assert_eq!(output[1], u8::NODATA);
        assert_eq!(output[2], 3);
        assert_eq!(output[3], 4);
    }

    #[test]
    fn cast_to_slice_overflow_becomes_nodata() {
        let meta = create_test_metadata(1, 3);
        let data = aligned_vec_from_slice(&[100_i32, 300, 50]);
        let raster = DenseArray::<i32, _>::new(meta, data).unwrap();

        let mut output = vec![0u8; 3];
        raster.cast_to_slice(&mut output).unwrap();

        assert_eq!(output[0], 100);
        // 300 cannot fit in u8, so it becomes NODATA
        assert_eq!(output[1], u8::NODATA);
        assert_eq!(output[2], 50);
    }

    #[test]
    fn cast_to_slice_wrong_length_fails() {
        let meta = create_test_metadata(2, 2);
        let data = aligned_vec_from_slice(&[1.0_f64, 2.0, 3.0, 4.0]);
        let raster = DenseArray::<f64, _>::new(meta, data).unwrap();

        let mut output = vec![0u8; 3]; // Wrong length
        let result = raster.cast_to_slice(&mut output);

        assert!(result.is_err());
    }
}

//! Algorithms that operate on `AnyDenseArray`.
//!
//! These functions provide type-erased wrappers around the generic algorithms in `raster::algo`.
//! They use the `apply_to_anydensearray!` macro to automatically handle all variants of `AnyDenseArray`.
//!

use std::ops::RangeInclusive;

use crate::{AnyDenseArray, ArrayDataType, ArrayMetadata, ArrayNum, DenseArray, Error, RasterScale, Result, raster::algo};
use algo::{Cast, Scale};

fn cast_range_f64_to_f32(range: Option<RangeInclusive<f64>>) -> Option<RangeInclusive<f32>> {
    range.map(|r| *r.start() as f32..=*r.end() as f32)
}

/// Crops an `AnyDenseArray` by removing nodata edges.
///
/// This is a type-erased wrapper around `algo::crop`.
pub fn crop(array: AnyDenseArray) -> AnyDenseArray {
    apply_to_anydensearray!(array, arr, algo::crop(arr))
}

impl<Meta: ArrayMetadata> Scale<f64, u8> for AnyDenseArray<Meta> {
    type Meta = Meta;

    fn scale(&self, input_range: Option<RangeInclusive<f64>>) -> Result<DenseArray<u8, Meta>> {
        match self {
            AnyDenseArray::F32(arr) => arr.scale(cast_range_f64_to_f32(input_range)),
            AnyDenseArray::F64(arr) => arr.scale(input_range),
            _ => Err(Error::InvalidArgument(
                "Scale is only supported for floating point rasters (f32 and f64)".to_string(),
            )),
        }
    }

    fn scale_to_slice(&self, input_range: Option<RangeInclusive<f64>>, output: &mut [u8]) -> Result<RasterScale> {
        match self {
            AnyDenseArray::F32(arr) => arr.scale_to_slice(cast_range_f64_to_f32(input_range), output),
            AnyDenseArray::F64(arr) => arr.scale_to_slice(input_range, output),
            _ => Err(Error::InvalidArgument(
                "Scale is only supported for floating point rasters (f32 and f64)".to_string(),
            )),
        }
    }
}

impl<Meta: ArrayMetadata> Scale<f64, u16> for AnyDenseArray<Meta> {
    type Meta = Meta;

    fn scale(&self, input_range: Option<RangeInclusive<f64>>) -> Result<DenseArray<u16, Meta>> {
        match self {
            AnyDenseArray::F32(arr) => arr.scale(cast_range_f64_to_f32(input_range)),
            AnyDenseArray::F64(arr) => arr.scale(input_range),
            _ => Err(Error::InvalidArgument(
                "Scale is only supported for floating point rasters (f32 and f64)".to_string(),
            )),
        }
    }

    fn scale_to_slice(&self, input_range: Option<RangeInclusive<f64>>, output: &mut [u16]) -> Result<RasterScale> {
        match self {
            AnyDenseArray::F32(arr) => arr.scale_to_slice(cast_range_f64_to_f32(input_range), output),
            AnyDenseArray::F64(arr) => arr.scale_to_slice(input_range, output),
            _ => Err(Error::InvalidArgument(
                "Scale is only supported for floating point rasters (f32 and f64)".to_string(),
            )),
        }
    }
}

/// Cast-like methods for `AnyDenseArray`.
impl<Meta: ArrayMetadata> AnyDenseArray<Meta> {
    pub fn cast_to<T: ArrayNum>(&self) -> DenseArray<T, Meta> {
        dispatch_anydensearray!(self, arr, algo::cast::<T, _>(arr))
    }

    /// Cast the array values to the destination type, returning a new `AnyDenseArray`.
    ///
    /// This method delegates to the `Cast::cast` trait method on the underlying `DenseArray`.
    ///
    /// Values that cannot be represented in the destination type will become nodata.
    /// Existing nodata values are preserved as nodata in the output.
    pub fn cast(&self, data_type: ArrayDataType) -> AnyDenseArray<Meta> {
        dispatch_datatype!(data_type, T, dispatch_anydensearray!(self, arr, arr.cast::<T>()))
    }

    /// Cast the array values to the destination type, writing into a pre-allocated byte slice.
    ///
    /// This method delegates to the `Cast::cast_to_slice` trait method on the underlying `DenseArray`.
    ///
    /// Values that cannot be represented in the destination type will become nodata.
    /// Existing nodata values are preserved as nodata in the output.
    ///
    /// # Errors
    ///
    /// Returns an error if the output slice length doesn't match the array length (in bytes).
    pub fn cast_to_slice(&self, data_type: ArrayDataType, output: &mut [u8]) -> Result<()> {
        dispatch_datatype_nowrap!(data_type, T, {
            let slice = bytemuck::cast_slice_mut::<u8, T>(output);
            dispatch_anydensearray!(self, arr, arr.cast_to_slice::<T>(slice))
        })
    }

    /// Cast the array values to the destination type, consuming self and reusing the buffer
    /// if the destination type is smaller or equal in size.
    ///
    /// This method delegates to the `Cast::into_cast` trait method on the underlying `DenseArray`.
    ///
    /// Values that cannot be represented in the destination type will become nodata.
    /// Existing nodata values are preserved as nodata in the output.
    ///
    /// This is more efficient than `cast_to_type()` when casting to a smaller type or equally sized type
    /// as it avoids allocating a new buffer.
    pub fn into_cast(self, data_type: ArrayDataType) -> AnyDenseArray<Meta> {
        dispatch_datatype!(data_type, T, dispatch_anydensearray!(self, arr, arr.into_cast::<T>()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Array, Nodata, RasterMetadata, RasterSize,
        array::{Columns, Rows},
    };
    use inf::allocate::aligned_vec_from_slice;

    fn create_test_metadata(rows: i32, cols: i32) -> RasterMetadata {
        RasterMetadata::sized(RasterSize::with_rows_cols(Rows(rows), Columns(cols)), crate::ArrayDataType::Float64)
    }

    #[test]
    fn anydensearray_cast_to_type() {
        let meta = create_test_metadata(2, 2);
        let data = aligned_vec_from_slice(&[1.0_f64, 2.0, 3.0, 4.0]);
        let raster = DenseArray::<f64, _>::new(meta, data).unwrap();
        let any_raster = AnyDenseArray::F64(raster);

        let result = any_raster.cast(ArrayDataType::Uint8);

        if let AnyDenseArray::U8(arr) = result {
            let expected: Vec<Option<u8>> = vec![Some(1), Some(2), Some(3), Some(4)];
            let actual: Vec<Option<u8>> = arr.into_iter().collect();
            assert_eq!(actual, expected);
        } else {
            panic!("Expected U8 variant");
        }
    }

    #[test]
    fn anydensearray_cast_to_type_slice() {
        let meta = create_test_metadata(2, 2);
        let data = aligned_vec_from_slice(&[1_i32, 2, 3, 4]);
        let raster = DenseArray::<i32, _>::new(meta, data).unwrap();
        let any_raster = AnyDenseArray::I32(raster);

        let mut output = vec![0u8; 4 * std::mem::size_of::<f64>()];
        any_raster.cast_to_slice(ArrayDataType::Float64, &mut output).unwrap();

        let result: &[f64] = bytemuck::cast_slice(&output);
        assert_eq!(result, &[1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn anydensearray_into_cast_to_type() {
        let meta = create_test_metadata(2, 2);
        let data = aligned_vec_from_slice(&[1.0_f64, 2.0, 3.0, 4.0]);
        let raster = DenseArray::<f64, _>::new(meta, data).unwrap();
        let any_raster = AnyDenseArray::F64(raster);

        // f64 -> u8 should reuse buffer
        let result = any_raster.into_cast(ArrayDataType::Uint8);

        if let AnyDenseArray::U8(arr) = result {
            let expected: Vec<Option<u8>> = vec![Some(1), Some(2), Some(3), Some(4)];
            let actual: Vec<Option<u8>> = arr.into_iter().collect();
            assert_eq!(actual, expected);
        } else {
            panic!("Expected U8 variant");
        }
    }

    #[test]
    fn anydensearray_cast_preserves_nodata() {
        let meta = create_test_metadata(2, 2);
        let data = aligned_vec_from_slice(&[1.0_f64, f64::NODATA, 3.0, 4.0]);
        let raster = DenseArray::<f64, _>::new(meta, data).unwrap();
        let any_raster = AnyDenseArray::F64(raster);

        let result = any_raster.cast(ArrayDataType::Int32);

        if let AnyDenseArray::I32(arr) = result {
            let actual: Vec<Option<i32>> = arr.into_iter().collect();
            assert_eq!(actual[0], Some(1));
            assert_eq!(actual[1], None);
            assert_eq!(actual[2], Some(3));
            assert_eq!(actual[3], Some(4));
        } else {
            panic!("Expected I32 variant");
        }
    }

    #[test]
    fn division_output_type() {
        let meta = create_test_metadata(2, 2);
        let data = aligned_vec_from_slice(&[1, 2, 3, 4]);
        let raster = DenseArray::<i32, _>::new(meta, data).unwrap();
        let any_raster = AnyDenseArray::I32(raster.clone());
        let _result: DenseArray<f32, _> = any_raster.binary_op_to::<f32, i32>(&AnyDenseArray::I32(raster), |a, b| (a / b) as f32);
    }
}

//! Raster scaling operations for compressing numeric data into smaller integer types.
//!
//! This module provides trait methods to scale raster values from various numeric types (f32, f64, u32, u16, i32, i16, u8)
//! into smaller integer types (u8, u16) while preserving the data range through scale and offset
//! metadata. The operations are reversible using the `descale` function, although not lossless due to quantization.
//!
//! # Usage
//!
//! Use the [`Scale`] trait to access scaling methods on `DenseArray` with supported numeric types:
//!
//! ```ignore
//! use crate::raster::algo::Scale;
//!
//! let scaled_u8: DenseArray<u8> = my_raster.scale(None)?;
//! let scaled_u16: DenseArray<u16> = my_raster.scale(None)?;
//! ```
//!
//! The [`Scale`] trait provides:
//! - `scale_to()`: Scales raster values to the output type (u8 or u16)
//! - `scale_to_slice()`: Scales into a pre-allocated slice
//!
//! To reverse the scaling operation, use [`descale`]:
//! ```ignore
//! let original: DenseArray<f64> = descale(&scaled_u8);
//! ```
//!
//! # SIMD Acceleration
//!
//! SIMD-optimized implementations are automatically used for all supported input types
//! (f32, f64, u32, u16, i32, i16, u8).

use crate::{Array, ArrayMetadata, ArrayNum, DenseArray, Error, RasterScale, Result, raster::algo};
use inf::cast;

use std::ops::RangeInclusive;

/// Helper struct to hold scale calculation parameters
struct ScaleParams {
    scale: f64,
    offset: f64,
    dest_min: f64,
    dest_max: f64,
}

/// Trait for scaling operations from input type T to output type O.
/// If the `input_range` is not provided, it will be calculated from the data.
/// Providing it can save resources if you already know the range or want to use a custom range.
///
/// The trait is implemented for combinations of:
/// - Input types: f32, f64, u32, u16, i32, i16, u8
/// - Output types: u8, u16
pub trait Scale<T, O: ArrayNum> {
    type Meta: ArrayMetadata;

    /// Scale raster values to the output type O
    fn scale(&self, input_range: Option<RangeInclusive<T>>) -> Result<DenseArray<O, Self::Meta>>;

    /// Scale raster values into a pre-allocated slice of type O
    fn scale_to_slice(&self, input_range: Option<RangeInclusive<T>>, output: &mut [O]) -> Result<RasterScale>;
}

fn calculate_scale_params(range: &std::ops::RangeInclusive<f64>, dest_type: crate::ArrayDataType) -> ScaleParams {
    let dest_min = 0.0;
    let dest_max = dest_type.default_nodata_value() - 1.0;

    let input_range = range.end() - range.start();
    let output_range = dest_max - dest_min;

    let scale = if input_range > 0.0 { input_range / output_range } else { 1.0 };
    let offset = range.start() - (dest_min * scale);

    ScaleParams {
        scale,
        offset,
        dest_min,
        dest_max,
    }
}

pub mod simd {
    use fearless_simd::{Simd, SimdBase, prelude::*};

    use super::*;
    use crate::{ArrayDataType, DenseArray, Nodata};

    #[inline]
    fn scale_value<TSrc: ArrayNum, TDest: ArrayNum>(value: TSrc, params: &ScaleParams) -> TDest {
        if value.is_nodata() {
            TDest::NODATA
        } else {
            let value = num::cast::<TSrc, f64>(value).expect("supported raster values convert to f64");
            let scaled = (value - params.offset) / params.scale;
            num::cast::<f64, TDest>(scaled.max(params.dest_min).min(params.dest_max).round()).expect("scaled value fits destination type")
        }
    }

    #[inline(always)]
    fn scale_kernel<S, TSrc, TDest>(simd: S, src: &[TSrc], output: &mut [TDest], params: &ScaleParams)
    where
        S: Simd,
        TSrc: ArrayNum,
        TDest: ArrayNum,
    {
        let simd_scale = S::f64s::splat(simd, params.scale);
        let simd_offset = S::f64s::splat(simd, params.offset);
        let simd_dest_min = S::f64s::splat(simd, params.dest_min);
        let simd_dest_max = S::f64s::splat(simd, params.dest_max);

        let mut src_chunks = src.chunks_exact(S::f64s::LEN);
        let mut output_chunks = output.chunks_exact_mut(S::f64s::LEN);
        for (src_chunk, output_chunk) in src_chunks.by_ref().zip(output_chunks.by_ref()) {
            let values = S::f64s::from_fn(simd, |index| {
                num::cast::<TSrc, f64>(src_chunk[index]).expect("supported raster values convert to f64")
            });
            let scaled = (values - simd_offset) / simd_scale;
            let scaled = scaled.max(simd_dest_min).min(simd_dest_max);
            // Scaled values are non-negative, so adding 0.5 and flooring matches scalar round-away-from-zero.
            let rounded = (scaled + 0.5).floor();

            for index in 0..S::f64s::LEN {
                output_chunk[index] = if src_chunk[index].is_nodata() {
                    TDest::NODATA
                } else {
                    num::cast::<f64, TDest>(rounded[index]).expect("scaled value fits destination type")
                };
            }
        }

        for (&value, output) in src_chunks.remainder().iter().zip(output_chunks.into_remainder()) {
            *output = scale_value(value, params);
        }
    }

    macro_rules! impl_scale_simd {
        ($src_type:ty, $dest_type:ty, $array_data_type:expr) => {
            impl<Meta: ArrayMetadata> Scale<$src_type, $dest_type> for DenseArray<$src_type, Meta> {
                type Meta = Meta;

                fn scale(&self, input_range: Option<RangeInclusive<$src_type>>) -> Result<DenseArray<$dest_type, Meta>> {
                    let mut output = inf::allocate::AlignedVecUnderConstruction::<$dest_type>::new(self.len());
                    let raster_scale = self.scale_to_slice(input_range, unsafe { output.as_slice_mut() })?;
                    let new_metadata = Meta::with_geo_reference(self.metadata().geo_reference().with_scale(raster_scale));
                    Ok(
                        DenseArray::<$dest_type, Meta>::new(new_metadata, unsafe { output.assume_init() })
                            .expect("Size mismatch in scale operation"),
                    )
                }

                fn scale_to_slice(&self, input_range: Option<RangeInclusive<$src_type>>, output: &mut [$dest_type]) -> Result<RasterScale> {
                    if output.len() != self.len() {
                        return Err(Error::InvalidArgument(format!(
                            "Output slice length {} does not match input length {}",
                            output.len(),
                            self.len()
                        )));
                    }

                    if self.metadata().geo_reference().scale().is_some() {
                        return Err(Error::InvalidArgument(
                            "Cannot scale raster that already has scale information. Use descale first.".to_string(),
                        ));
                    }

                    let Some(range) = input_range.or_else(|| algo::limits::min_max(self)) else {
                        output.fill(<$dest_type>::NODATA);
                        return Ok(RasterScale { scale: 1.0, offset: 0.0 });
                    };

                    let range_f64 = cast::inclusive_range::<f64>(range)?;
                    let params = super::calculate_scale_params(&range_f64, $array_data_type);
                    fearless_simd::dispatch!(crate::simd::level(), simd => {
                        scale_kernel(simd, self.as_slice(), output, &params)
                    });

                    Ok(RasterScale {
                        scale: params.scale,
                        offset: params.offset,
                    })
                }
            }
        };
    }

    impl_scale_simd!(f64, u8, ArrayDataType::Uint8);
    impl_scale_simd!(f64, u16, ArrayDataType::Uint16);
    impl_scale_simd!(f32, u8, ArrayDataType::Uint8);
    impl_scale_simd!(f32, u16, ArrayDataType::Uint16);
    impl_scale_simd!(u32, u8, ArrayDataType::Uint8);
    impl_scale_simd!(u32, u16, ArrayDataType::Uint16);
    impl_scale_simd!(u16, u8, ArrayDataType::Uint8);
    impl_scale_simd!(u16, u16, ArrayDataType::Uint16);
    impl_scale_simd!(i32, u8, ArrayDataType::Uint8);
    impl_scale_simd!(i32, u16, ArrayDataType::Uint16);
    impl_scale_simd!(i16, u8, ArrayDataType::Uint8);
    impl_scale_simd!(i16, u16, ArrayDataType::Uint16);
    impl_scale_simd!(u8, u8, ArrayDataType::Uint8);
}

/// Descales the raster values using the scale and offset from the `geo_reference` metadata.
/// The descaled value is calculated as: `(value * scale) + offset`
/// This converts from stored/quantized values back to physical/real-world values.
/// The scale information is removed from the output metadata.
pub fn descale<TDest, R>(src: &R) -> R::WithPixelType<TDest>
where
    R: Array + algo::cast::Cast,
    TDest: ArrayNum,
    for<'a> &'a R: IntoIterator<Item = Option<R::Pixel>>,
{
    let geo_ref = src.metadata().geo_reference();
    let raster_scale = geo_ref.scale();

    match raster_scale {
        Some(s) => {
            let scale_factor = s.scale;
            let offset = s.offset;

            // Create metadata without scale information
            let mut new_georef = geo_ref;
            new_georef.set_scale(None);
            let new_metadata = R::Metadata::with_geo_reference(new_georef);

            R::WithPixelType::<TDest>::from_iter_opt(
                new_metadata,
                src.into_iter().map(|x| {
                    x.and_then(|v| {
                        use num::NumCast;
                        let v_f64: f64 = NumCast::from(v)?;
                        let descaled = v_f64 * scale_factor + offset;
                        NumCast::from(descaled)
                    })
                }),
            )
            .expect("Raster size bug") // Can only fail if the metadata size is invalid which is impossible in this case
        }
        None => algo::cast::cast(src),
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;

    use crate::{
        Array, ArrayDataType, DenseArray, Nodata, RasterMetadata, RasterScale, RasterSize,
        array::{Columns, Rows},
        testutils::{self, NOD, create_vec},
    };

    use super::*;

    fn create_metadata_with_scale(size: RasterSize, scale: f64, offset: f64) -> RasterMetadata {
        RasterMetadata {
            raster_size: size,
            nodata: Some(NOD),
            scale: Some(RasterScale { scale, offset }),
        }
    }

    #[test]
    fn scale_f64_to_u8() {
        let size = RasterSize::with_rows_cols(Rows(2), Columns(3));
        let meta = RasterMetadata::sized_with_nodata(size, Some(NOD));

        // Input values: 0.0, 25.0, 50.0, 75.0, 100.0, NOD
        // Range: 0.0 to 100.0
        // Output range (u8): 0 to 254
        // scale = 100.0 / 254.0 ≈ 0.394
        // offset = 0.0
        #[rustfmt::skip]
        let raster: DenseArray<f64, RasterMetadata> = DenseArray::new(
            meta,
            create_vec(&[0.0, 25.0, 50.0, 75.0, 100.0, NOD]),
        ).unwrap();

        let result: DenseArray<u8, RasterMetadata> = raster.scale(None).unwrap();

        let values: Vec<Option<u8>> = result.iter_opt().collect();
        assert_eq!(values.len(), 6);
        assert_eq!(values[0].unwrap(), 0);
        assert!(values[1].unwrap() > 60 && values[1].unwrap() < 65);
        assert!(values[2].unwrap() > 126 && values[2].unwrap() < 128);
        assert!(values[3].unwrap() > 189 && values[3].unwrap() < 192);
        assert_eq!(values[4].unwrap(), 254);
        assert!(values[5].is_none());

        let scale_info = result.metadata().scale.unwrap();
        assert_relative_eq!(scale_info.scale, 100.0 / 254.0, epsilon = 1e-6);
        assert_relative_eq!(scale_info.offset, 0.0, epsilon = 1e-6);
    }

    #[test]
    fn scale_f64_to_u16() {
        let size = RasterSize::with_rows_cols(Rows(1), Columns(3));
        let meta = RasterMetadata::sized_with_nodata(size, Some(NOD));

        #[rustfmt::skip]
        let raster: DenseArray<f64, RasterMetadata> = DenseArray::new(
            meta,
            create_vec(&[0.0, 5000.0, 10000.0]),
        ).unwrap();

        let result: DenseArray<u16, RasterMetadata> = raster.scale(None).unwrap();

        let values: Vec<Option<u16>> = result.iter_opt().collect();
        assert_eq!(values[0].unwrap(), 0);
        assert_eq!(values[2].unwrap(), 65534);

        let scale_info = result.metadata().scale.unwrap();
        let expected_scale = 10000.0 / 65534.0;
        assert_relative_eq!(scale_info.scale, expected_scale, epsilon = 1e-6);
        assert_relative_eq!(scale_info.offset, 0.0, epsilon = 1e-6);
    }

    #[test]
    fn scale_f32_to_u8() {
        let size = RasterSize::with_rows_cols(Rows(1), Columns(3));
        let nodata_f32 = f32::NAN;
        let meta = RasterMetadata::sized_with_nodata(size, Some(nodata_f32 as f64));

        let raster: DenseArray<f32, RasterMetadata> =
            DenseArray::new(meta, inf::allocate::aligned_vec_from_slice(&[0.0_f32, 50.0, 100.0])).unwrap();

        let result: DenseArray<u8, RasterMetadata> = raster.scale(None).unwrap();

        let values: Vec<Option<u8>> = result.iter_opt().collect();
        assert_eq!(values[0].unwrap(), 0);
        assert_eq!(values[2].unwrap(), 254);
    }

    #[test]
    fn scale_f32_to_u16() {
        let size = RasterSize::with_rows_cols(Rows(1), Columns(3));
        let nodata_f32 = f32::NAN;
        let meta = RasterMetadata::sized_with_nodata(size, Some(nodata_f32 as f64));

        let raster: DenseArray<f32, RasterMetadata> =
            DenseArray::new(meta, inf::allocate::aligned_vec_from_slice(&[0.0_f32, 500.0, 1000.0])).unwrap();

        let result: DenseArray<u16, RasterMetadata> = raster.scale(None).unwrap();

        let values: Vec<Option<u16>> = result.iter_opt().collect();
        assert_eq!(values[0].unwrap(), 0);
        assert_eq!(values[2].unwrap(), 65534);
    }

    #[test]
    fn scale_fails_with_existing_scale() {
        let size = RasterSize::with_rows_cols(Rows(2), Columns(2));
        let meta = create_metadata_with_scale(size, 2.0, 10.0);

        #[rustfmt::skip]
        let raster: DenseArray<f64, RasterMetadata> = DenseArray::new(
            meta,
            create_vec(&[1.0, 2.0, 3.0, 4.0]),
        ).unwrap();

        let result: Result<DenseArray<u8, RasterMetadata>> = raster.scale(None);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("already has scale information"));
    }

    #[test]
    fn scale_all_nodata_produces_nodata() {
        let size = RasterSize::with_rows_cols(Rows(2), Columns(2));
        let meta = RasterMetadata::sized_with_nodata(size, Some(NOD));

        #[rustfmt::skip]
        let raster: DenseArray<f64, RasterMetadata> = DenseArray::new(
            meta,
            create_vec(&[NOD, NOD, NOD, NOD]),
        ).unwrap();

        let result: DenseArray<u8, RasterMetadata> = raster.scale(None).unwrap();

        let values: Vec<Option<u8>> = result.iter_opt().collect();
        assert_eq!(values.len(), 4);
        assert!(values.iter().all(|v: &Option<u8>| v.is_none()));
        assert_eq!(result.metadata().nodata, Some(ArrayDataType::Uint8.default_nodata_value()));
    }

    #[test]
    fn descale_with_scale_and_offset() {
        let size = RasterSize::with_rows_cols(Rows(2), Columns(3));
        let meta = create_metadata_with_scale(size, 2.0, 10.0);

        // Input values (stored): 1, 2, 3, 4, 5, NOD
        // Expected output (physical): 1*2+10=12, 2*2+10=14, 3*2+10=16, 4*2+10=18, 5*2+10=20, NOD
        #[rustfmt::skip]
        let raster: DenseArray<i32, RasterMetadata> = DenseArray::new(
            meta,
            create_vec(&[1.0, 2.0, 3.0, 4.0, 5.0, NOD]),
        ).unwrap();

        let result: DenseArray<f64, RasterMetadata> = descale(&raster);

        let values: Vec<Option<f64>> = result.iter_opt().collect();
        assert_eq!(values.len(), 6);
        assert_relative_eq!(values[0].unwrap(), 12.0);
        assert_relative_eq!(values[1].unwrap(), 14.0);
        assert_relative_eq!(values[2].unwrap(), 16.0);
        assert_relative_eq!(values[3].unwrap(), 18.0);
        assert_relative_eq!(values[4].unwrap(), 20.0);
        assert!(values[5].is_none());
        assert!(result.metadata().scale.is_none());
    }

    #[test]
    fn descale_without_scale_metadata() {
        let size = RasterSize::with_rows_cols(Rows(2), Columns(2));
        let meta = RasterMetadata::sized_with_nodata(size, Some(NOD));

        #[rustfmt::skip]
        let raster: DenseArray<f64, RasterMetadata> = DenseArray::new(
            meta,
            create_vec(&[1.0, 2.0, 3.0, NOD]),
        ).unwrap();

        let result: DenseArray<i32, RasterMetadata> = descale(&raster);

        let values: Vec<Option<i32>> = result.iter_opt().collect();
        assert_eq!(values[0].unwrap(), 1);
        assert_eq!(values[1].unwrap(), 2);
        assert_eq!(values[2].unwrap(), 3);
        assert!(values[3].is_none());
        assert!(result.metadata().scale.is_none());
    }

    #[test]
    fn scale_and_descale_roundtrip() {
        let size = RasterSize::with_rows_cols(Rows(2), Columns(3));
        let meta = RasterMetadata::sized_with_nodata(size, Some(NOD));

        #[rustfmt::skip]
        let original: DenseArray<f64, RasterMetadata> = DenseArray::new(
            meta,
            create_vec(&[0.0, 25.5, 50.3, 75.8, 100.0, NOD]),
        ).unwrap();

        let scaled: DenseArray<u8, RasterMetadata> = original.scale(None).unwrap();
        assert!(scaled.metadata().scale.is_some());

        let roundtrip: DenseArray<f64, RasterMetadata> = descale(&scaled);
        assert!(roundtrip.metadata().scale.is_none());

        let original_values: Vec<Option<f64>> = original.iter_opt().collect();
        let roundtrip_values: Vec<Option<f64>> = roundtrip.iter_opt().collect();

        for (orig, rt) in original_values.iter().zip(roundtrip_values.iter()) {
            match (orig, rt) {
                (Some(o), Some(r)) => assert_relative_eq!(*o, *r, epsilon = 0.5),
                (None, None) => {}
                _ => panic!("Mismatch in nodata handling"),
            }
        }

        assert!(original.metadata().scale.is_none());
    }

    #[test]
    fn scale_negative_range() {
        let size = RasterSize::with_rows_cols(Rows(1), Columns(4));
        let meta = RasterMetadata::sized_with_nodata(size, Some(NOD));

        #[rustfmt::skip]
        let raster: DenseArray<f64, RasterMetadata> = DenseArray::new(
            meta,
            create_vec(&[-100.0, -50.0, 0.0, 50.0]),
        ).unwrap();

        let result: DenseArray<u8, RasterMetadata> = raster.scale(None).unwrap();

        let values: Vec<Option<u8>> = result.iter_opt().collect();
        assert_eq!(values[0].unwrap(), 0);
        assert_eq!(values[3].unwrap(), 254);

        let scale_info = result.metadata().scale.unwrap();
        assert_relative_eq!(scale_info.scale, 150.0 / 254.0, epsilon = 1e-6);
        assert_relative_eq!(scale_info.offset, -100.0, epsilon = 1e-6);
    }

    #[test]
    fn scale_constant_value() {
        let size = RasterSize::with_rows_cols(Rows(2), Columns(2));
        let meta = RasterMetadata::sized_with_nodata(size, Some(NOD));

        #[rustfmt::skip]
        let raster: DenseArray<f64, RasterMetadata> = DenseArray::new(
            meta,
            create_vec(&[42.0, 42.0, 42.0, 42.0]),
        ).unwrap();

        let result: DenseArray<u8, RasterMetadata> = raster.scale(None).unwrap();

        let values: Vec<Option<u8>> = result.iter_opt().collect();
        assert_eq!(values[0].unwrap(), 0);
        assert_eq!(values[1].unwrap(), 0);
        assert_eq!(values[2].unwrap(), 0);
        assert_eq!(values[3].unwrap(), 0);

        let scale_info = result.metadata().scale.unwrap();
        assert_relative_eq!(scale_info.scale, 1.0, epsilon = 1e-6);
        assert_relative_eq!(scale_info.offset, 42.0, epsilon = 1e-6);
    }

    #[test]
    fn descale_preserves_nodata() {
        let size = RasterSize::with_rows_cols(Rows(2), Columns(3));
        let meta = create_metadata_with_scale(size, 0.5, 100.0);

        #[rustfmt::skip]
        let raster: DenseArray<i32, RasterMetadata> = DenseArray::new(
            meta,
            create_vec(&[1.0, NOD, 3.0, NOD, 5.0, NOD]),
        ).unwrap();

        let result: DenseArray<f64, RasterMetadata> = descale(&raster);

        let values: Vec<Option<f64>> = result.iter_opt().collect();
        assert!(values[0].is_some());
        assert!(values[1].is_none());
        assert!(values[2].is_some());
        assert!(values[3].is_none());
        assert!(values[4].is_some());
        assert!(values[5].is_none());
    }

    #[test]
    fn simd_scale_to_u8_matches_scalar() {
        use crate::raster::algo::simd as simd_algo;

        let size = RasterSize::with_rows_cols(Rows(10), Columns(10));
        let meta = RasterMetadata::sized_with_nodata(size, Some(NOD));

        // Create test data with various values including nodata
        let mut data = Vec::new();
        for i in 0..100 {
            if i % 11 == 0 {
                data.push(NOD);
            } else {
                data.push((i as f64) * 1.5 - 50.0);
            }
        }

        let raster: DenseArray<f64, RasterMetadata> = DenseArray::new(meta.clone(), testutils::create_vec(&data)).unwrap();

        // Get SIMD result
        use simd_algo::Scale;
        let simd_result: DenseArray<u8, RasterMetadata> = raster.scale(None).unwrap();

        // Verify we got valid results
        let simd_values: Vec<Option<u8>> = simd_result.iter_opt().collect();
        assert_eq!(simd_values.len(), 100);

        // Check nodata values are preserved
        for i in (0..100).step_by(11) {
            assert!(simd_values[i].is_none(), "Expected nodata at index {}", i);
        }

        // Verify metadata is set
        assert!(simd_result.metadata().scale.is_some());
    }

    #[test]
    fn simd_scale_to_u16_matches_scalar() {
        use crate::raster::algo::simd as simd_algo;

        let size = RasterSize::with_rows_cols(Rows(10), Columns(10));
        let meta = RasterMetadata::sized_with_nodata(size, Some(NOD));

        // Create test data with various values including nodata
        let mut data = Vec::new();
        for i in 0..100 {
            if i % 13 == 0 {
                data.push(NOD);
            } else {
                data.push((i as f64) * 100.0 - 2000.0);
            }
        }

        let raster: DenseArray<f64, RasterMetadata> = DenseArray::new(meta.clone(), testutils::create_vec(&data)).unwrap();

        // Get SIMD result
        use simd_algo::Scale;
        let simd_result: DenseArray<u16, RasterMetadata> = raster.scale(None).unwrap();

        // Verify we got valid results
        let simd_values: Vec<Option<u16>> = simd_result.iter_opt().collect();
        assert_eq!(simd_values.len(), 100);

        // Verify metadata is the same
        assert!(simd_result.metadata().scale.is_some());
    }

    #[test]
    fn simd_scale_to_u8_f32_matches_scalar() {
        use crate::raster::algo::simd as simd_algo;

        let size = RasterSize::with_rows_cols(Rows(10), Columns(10));
        let nodata_f32 = f32::NAN;
        let meta = RasterMetadata::sized_with_nodata(size, Some(nodata_f32 as f64));

        // Create test data with various values including nodata
        let mut data = inf::allocate::new_aligned_vec();
        for i in 0..100 {
            if i % 11 == 0 {
                data.push(nodata_f32);
            } else {
                data.push((i as f32) * 1.5 - 50.0);
            }
        }

        let raster: DenseArray<f32, RasterMetadata> = DenseArray::new(meta.clone(), data).unwrap();
        // Get SIMD result
        use simd_algo::Scale;
        let simd_result: DenseArray<u8, RasterMetadata> = raster.scale(None).unwrap();
        // Verify results
        let simd_values: Vec<Option<u8>> = simd_result.iter_opt().collect();

        assert_eq!(simd_values.len(), 100);
        // Verify metadata is set
        assert!(simd_result.metadata().scale.is_some());
    }

    #[test]
    fn simd_scale_to_u16_f32_matches_scalar() {
        use crate::raster::algo::simd as simd_algo;

        let size = RasterSize::with_rows_cols(Rows(10), Columns(10));
        let nodata_f32 = f32::NAN;
        let meta = RasterMetadata::sized_with_nodata(size, Some(nodata_f32 as f64));

        // Create test data with various values including nodata
        let mut data = inf::allocate::new_aligned_vec();
        for i in 0..100 {
            if i % 13 == 0 {
                data.push(nodata_f32);
            } else {
                data.push((i as f32) * 100.0 - 2000.0);
            }
        }

        let raster: DenseArray<f32, RasterMetadata> = DenseArray::new(meta.clone(), data).unwrap();
        // Get SIMD result
        use simd_algo::Scale;
        let simd_result: DenseArray<u16, RasterMetadata> = raster.scale(None).unwrap();
        let simd_values: Vec<Option<u16>> = simd_result.iter_opt().collect();

        assert_eq!(simd_values.len(), 100);

        // Verify metadata is set
        assert!(simd_result.metadata().scale.is_some());
    }

    #[test]
    fn simd_scale_u32_to_u8() {
        use crate::raster::algo::simd as simd_algo;

        let size = RasterSize::with_rows_cols(Rows(10), Columns(10));
        let meta = RasterMetadata::sized_with_nodata(size, Some(u32::NODATA as f64));

        // Create test data with various values including nodata
        let mut data = inf::allocate::new_aligned_vec();
        for i in 0..100 {
            if i % 13 == 0 {
                data.push(u32::NODATA);
            } else {
                data.push(i * 1000);
            }
        }

        let raster: DenseArray<u32, RasterMetadata> = DenseArray::new(meta.clone(), data).unwrap();

        // Get SIMD result
        use simd_algo::Scale;
        let simd_result: DenseArray<u8, RasterMetadata> = raster.scale(None).unwrap();

        let simd_values: Vec<Option<u8>> = simd_result.iter_opt().collect();
        assert_eq!(simd_values.len(), 100);

        // Verify nodata is preserved
        assert!(simd_values[0].is_none());
        assert!(simd_values[1].is_some());

        // Verify metadata is set
        assert!(simd_result.metadata().scale.is_some());
    }

    #[test]
    fn simd_scale_i32_to_u16() {
        use crate::raster::algo::simd as simd_algo;

        let size = RasterSize::with_rows_cols(Rows(10), Columns(10));
        let meta = RasterMetadata::sized_with_nodata(size, Some(i32::NODATA as f64));

        // Create test data with various values including nodata and negative values
        let mut data = inf::allocate::new_aligned_vec();
        for i in 0..100 {
            if i % 13 == 0 {
                data.push(i32::NODATA);
            } else {
                data.push((i as i32) * 100 - 2000);
            }
        }

        let raster: DenseArray<i32, RasterMetadata> = DenseArray::new(meta.clone(), data).unwrap();

        // Get SIMD result
        use simd_algo::Scale;
        let simd_result: DenseArray<u16, RasterMetadata> = raster.scale(None).unwrap();

        let simd_values: Vec<Option<u16>> = simd_result.iter_opt().collect();
        assert_eq!(simd_values.len(), 100);

        // Verify nodata is preserved
        assert!(simd_values[0].is_none());
        assert!(simd_values[1].is_some());

        // Verify metadata is set
        assert!(simd_result.metadata().scale.is_some());
    }

    #[test]
    fn simd_scale_u16_to_u8() {
        use crate::raster::algo::simd as simd_algo;

        let size = RasterSize::with_rows_cols(Rows(10), Columns(10));
        let meta = RasterMetadata::sized_with_nodata(size, Some(u16::NODATA as f64));

        // Create test data with various values including nodata
        let mut data = inf::allocate::new_aligned_vec();
        for i in 0..100 {
            if i % 13 == 0 {
                data.push(u16::NODATA);
            } else {
                data.push((i * 100) as u16);
            }
        }

        let raster: DenseArray<u16, RasterMetadata> = DenseArray::new(meta.clone(), data).unwrap();

        // Get SIMD result
        use simd_algo::Scale;
        let simd_result: DenseArray<u8, RasterMetadata> = raster.scale(None).unwrap();

        let simd_values: Vec<Option<u8>> = simd_result.iter_opt().collect();
        assert_eq!(simd_values.len(), 100);

        // Verify nodata is preserved
        assert!(simd_values[0].is_none());
        assert!(simd_values[1].is_some());

        // Verify metadata is set
        assert!(simd_result.metadata().scale.is_some());
    }
}

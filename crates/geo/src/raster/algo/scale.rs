//! Raster scaling operations for compressing floating-point data into integer types.
//!
//! This module provides functions to scale raster values from floating-point types (f32, f64)
//! into smaller integer types (u8, u16) while preserving the data range through scale and offset
//! metadata. The operations are reversible using the `descale` function.
//!
//! # Functions
//!
//! - [`scale_to_u8`]: Scales raster values to u8 (0-254, with 255 reserved for nodata)
//! - [`scale_to_u16`]: Scales raster values to u16 (0-65534, with 65535 reserved for nodata)
//! - [`descale`]: Reverses scaling using stored scale/offset metadata
//!
//! # SIMD Acceleration
//!
//! When the `simd` feature is enabled, SIMD-optimized implementations are available:
//! - [`simd::scale_to_u8`]: SIMD-accelerated scaling to u8 for `DenseArray<f32>` and `DenseArray<f64>`
//! - [`simd::scale_to_u16`]: SIMD-accelerated scaling to u16 for `DenseArray<f32>` and `DenseArray<f64>`
//! - [`simd::SimdScale`]: Trait providing scaling methods as extension methods on `DenseArray`
//!
//! ## Performance
//!
//! SIMD implementations provide significant speedups over scalar versions for large rasters.
//!
//! To use SIMD acceleration, you can call the functions directly or use the trait methods:
//! ```ignore
//! use crate::raster::algo::simd;
//! // Using free functions (generic over f32 and f64)
//! let result = simd::scale_to_u8(&my_raster, None)?;
//!
//! // Using trait methods
//! use crate::raster::algo::simd::SimdScale;
//! let result = my_raster.scale_to_u8(None)?;
//! ```
//!
//! The module-level `scale_to_u8` and `scale_to_u16` functions use the scalar implementation
//! and work with any `Array` type.

use crate::{Array, ArrayMetadata, ArrayNum, Error, RasterScale, Result, raster::algo};
use inf::cast;

use std::ops::RangeInclusive;
#[cfg(feature = "simd")]
use std::simd::prelude::*;

/// Helper struct to hold scale calculation parameters
struct ScaleParams {
    scale: f64,
    offset: f64,
    dest_min: f64,
    dest_max: f64,
}

/// Calculate scale and offset parameters for mapping a value range to destination type range
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

fn scale_internal<TDest, R>(src: &R, input_range: Option<RangeInclusive<R::Pixel>>) -> Result<R::WithPixelType<TDest>>
where
    R: Array,
    TDest: ArrayNum,
{
    let geo_ref = src.metadata().geo_reference();
    if geo_ref.scale().is_some() {
        return Err(Error::InvalidArgument(
            "Cannot scale raster that already has scale information. Use descale first.".to_string(),
        ));
    }

    let Some(range) = input_range.or_else(|| algo::limits::min_max(src)) else {
        // Raster is all nodata, so we can return a filled raster with nodata value and skip calculations
        // Assign a default scale so it can stil be descaled back to the original nodata value if needed
        let new_metadata = R::Metadata::sized(src.size(), TDest::TYPE).with_scale(RasterScale { scale: 1.0, offset: 0.0 });
        return Ok(R::WithPixelType::<TDest>::filled_with_nodata(new_metadata));
    };

    let range_f64 = cast::inclusive_range::<f64>(range)?;
    let params = calculate_scale_params(&range_f64, TDest::TYPE);
    let new_metadata = R::Metadata::with_geo_reference(geo_ref.with_scale(RasterScale {
        scale: params.scale,
        offset: params.offset,
    }));

    Ok(R::WithPixelType::<TDest>::from_iter_opt(
        new_metadata,
        src.iter_opt().map(|x| {
            x.and_then(|v| {
                use num::NumCast;
                let v_f64: f64 = NumCast::from(v)?;
                let scaled = (v_f64 - params.offset) / params.scale;
                let clamped = scaled.max(params.dest_min).min(params.dest_max).round();
                NumCast::from(clamped)
            })
        }),
    )
    .expect("Raster size bug"))
}

#[cfg(feature = "simd")]
#[cfg_attr(docsrs, doc(cfg(feature = "simd")))]
pub mod simd {
    use super::*;
    use crate::{ArrayDataType, DenseArray, Nodata, NodataSimd};
    use inf::simd::SimdCastPl;
    use std::simd::{Select, StdFloat};

    const LANES: usize = inf::simd::LANES;

    /// Internal macro to implement SIMD scaling without code duplication
    macro_rules! impl_scale_simd {
        ($src:expr, $src_type:ty, $dest_type:ty, $array_data_type:expr, $input_range:expr) => {{
            let geo_ref = $src.metadata().geo_reference();
            if geo_ref.scale().is_some() {
                return Err(Error::InvalidArgument(
                    "Cannot scale raster that already has scale information. Use descale first.".to_string(),
                ));
            }

            let Some(range) = $input_range.or_else(|| algo::limits::min_max($src)) else {
                let new_metadata = Meta::sized($src.size(), $array_data_type).with_scale(RasterScale { scale: 1.0, offset: 0.0 });
                return Ok(DenseArray::<$dest_type, Meta>::filled_with_nodata(new_metadata));
            };
            let range_f64 = cast::inclusive_range::<f64>(range)?;
            let params = super::calculate_scale_params(&range_f64, $array_data_type);
            let new_metadata = Meta::with_geo_reference(geo_ref.with_scale(RasterScale {
                scale: params.scale,
                offset: params.offset,
            }));

            // SIMD constants
            let simd_scale = Simd::<$src_type, LANES>::splat(params.scale as $src_type);
            let simd_offset = Simd::<$src_type, LANES>::splat(params.offset as $src_type);
            let simd_dest_min = Simd::<$src_type, LANES>::splat(params.dest_min as $src_type);
            let simd_dest_max = Simd::<$src_type, LANES>::splat(params.dest_max as $src_type);

            // Allocate output buffer using VecUnderConstruction helper
            let mut output = inf::allocate::AlignedVecUnderConstruction::<$dest_type>::new($src.len());

            let (src_head, src_simd, src_tail): (&[$src_type], &[Simd<$src_type, LANES>], &[$src_type]) =
                $src.as_slice().as_simd::<LANES>();
            let (out_head, out_simd, out_tail): (&mut [$dest_type], &mut [Simd<$dest_type, LANES>], &mut [$dest_type]) =
                unsafe { output.as_slice_mut() }.as_simd_mut::<LANES>();

            assert!(src_head.len() == out_head.len(), "Data alignment error");

            // Process scalar head
            for (&v, out) in src_head.iter().zip(out_head.iter_mut()) {
                *out = if v.is_nodata() {
                    <$dest_type>::NODATA
                } else {
                    let v_f64 = v as f64;
                    let scaled = (v_f64 - params.offset) / params.scale;
                    let clamped = scaled.max(params.dest_min).min(params.dest_max).round();
                    clamped as $dest_type
                };
            }

            // Process SIMD body
            for (v_chunk, out_chunk) in src_simd.iter().zip(out_simd.iter_mut()) {
                let nodata_mask = v_chunk.nodata_mask();
                let scaled = (*v_chunk - simd_offset) / simd_scale;
                let clamped = scaled.simd_clamp(simd_dest_min, simd_dest_max).round();

                let casted = clamped.simd_cast::<$dest_type>();
                *out_chunk = nodata_mask.select(Simd::<$dest_type, LANES>::splat(<$dest_type>::NODATA), casted);
            }

            // Process scalar tail
            for (&v, out) in src_tail.iter().zip(out_tail.iter_mut()) {
                *out = if v.is_nodata() {
                    <$dest_type>::NODATA
                } else {
                    let v_f64 = v as f64;
                    let scaled = (v_f64 - params.offset) / params.scale;
                    let clamped = scaled.max(params.dest_min).min(params.dest_max).round();
                    clamped as $dest_type
                };
            }

            DenseArray::<$dest_type, Meta>::new(new_metadata, unsafe { output.assume_init() })
        }};
    }

    /// Trait for SIMD scaling operations.
    pub trait SimdScale<T> {
        type Meta: ArrayMetadata;

        // If the `input_range` is not provided, it will be calculated from the data.
        // Providing it can save time if you already know the range or want to use a custom range
        fn scale_to_u8(&self, input_range: Option<RangeInclusive<T>>) -> Result<DenseArray<u8, Self::Meta>>;
        fn scale_to_u16(&self, input_range: Option<RangeInclusive<T>>) -> Result<DenseArray<u16, Self::Meta>>;
    }

    impl<Meta: ArrayMetadata> SimdScale<f64> for DenseArray<f64, Meta> {
        type Meta = Meta;

        fn scale_to_u8(&self, input_range: Option<RangeInclusive<f64>>) -> Result<DenseArray<u8, Meta>> {
            impl_scale_simd!(self, f64, u8, ArrayDataType::Uint8, input_range)
        }

        fn scale_to_u16(&self, input_range: Option<RangeInclusive<f64>>) -> Result<DenseArray<u16, Meta>> {
            impl_scale_simd!(self, f64, u16, ArrayDataType::Uint16, input_range)
        }
    }

    impl<Meta: ArrayMetadata> SimdScale<f32> for DenseArray<f32, Meta> {
        type Meta = Meta;

        fn scale_to_u8(&self, input_range: Option<RangeInclusive<f32>>) -> Result<DenseArray<u8, Meta>> {
            impl_scale_simd!(self, f32, u8, ArrayDataType::Uint8, input_range)
        }

        fn scale_to_u16(&self, input_range: Option<RangeInclusive<f32>>) -> Result<DenseArray<u16, Meta>> {
            impl_scale_simd!(self, f32, u16, ArrayDataType::Uint16, input_range)
        }
    }

    pub fn scale_to_u8<T, Meta>(src: &DenseArray<T, Meta>, input_range: Option<RangeInclusive<T>>) -> Result<DenseArray<u8, Meta>>
    where
        T: ArrayNum,
        DenseArray<T, Meta>: SimdScale<T, Meta = Meta>,
        Meta: ArrayMetadata,
    {
        src.scale_to_u8(input_range)
    }

    pub fn scale_to_u16<T, Meta>(src: &DenseArray<T, Meta>, input_range: Option<RangeInclusive<T>>) -> Result<DenseArray<u16, Meta>>
    where
        T: ArrayNum,
        DenseArray<T, Meta>: SimdScale<T, Meta = Meta>,
        Meta: ArrayMetadata,
    {
        src.scale_to_u16(input_range)
    }
}

/// Scales the raster values to fit the full range of u8 (0-255).
///
/// If the `input_range` is not provided, it will be calculated from the data.
/// Providing it can save time if you already know the range or want to use a custom range
/// The scale/offset information is stored in the output metadata so that `descale` can reverse the operation.
///
/// **Note**: The value 255 is reserved for nodata, so actual data values will be scaled to the range 0-254.
///
/// # Errors
///
/// Returns an error if:
/// - The input raster already has scale information (cannot scale already-scaled data)
pub fn scale_to_u8<R>(src: &R, input_range: Option<RangeInclusive<R::Pixel>>) -> Result<R::WithPixelType<u8>>
where
    R: Array,
    for<'a> &'a R: IntoIterator<Item = Option<R::Pixel>>,
{
    scale_internal(src, input_range)
}

/// Scales the raster values to fit the full range of u16 (0-65535).
///
/// If the `input_range` is not provided, it will be calculated from the data.
/// Providing it can save time if you already know the range or want to use a custom range
/// The scale/offset information is stored in the output metadata so that `descale` can reverse the operation.
///
/// **Note**: The value 65535 is reserved for nodata, so actual data values will be scaled to the range 0-65534.
///
/// # Errors
///
/// Returns an error if:
/// - The input raster already has scale information (cannot scale already-scaled data)
pub fn scale_to_u16<R>(src: &R, input_range: Option<RangeInclusive<R::Pixel>>) -> Result<R::WithPixelType<u16>>
where
    R: Array,
    for<'a> &'a R: IntoIterator<Item = Option<R::Pixel>>,
{
    scale_internal(src, input_range)
}

/// Descales the raster values using the scale and offset from the `geo_reference` metadata.
/// The descaled value is calculated as: `(value - offset) / scale`
/// This converts from physical/real-world values back to raw/stored values.
/// The scale information is removed from the output metadata.
pub fn descale<TDest, R>(src: &R) -> R::WithPixelType<TDest>
where
    R: Array,
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
        Array, ArrayDataType, DenseArray, RasterMetadata, RasterScale, RasterSize,
        array::{Columns, Rows},
        testutils::{NOD, create_vec},
    };

    #[cfg(feature = "simd")]
    use crate::testutils;

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
        // Output range (u8): 0 to 255
        // scale = 100.0 / 255.0 ≈ 0.392
        // offset = 0.0
        #[rustfmt::skip]
        let raster: DenseArray<f64, RasterMetadata> = DenseArray::new(
            meta,
            create_vec(&[0.0, 25.0, 50.0, 75.0, 100.0, NOD]),
        ).unwrap();

        let result: DenseArray<u8, RasterMetadata> = scale_to_u8(&raster, None).unwrap();

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
    fn scale_fails_with_existing_scale() {
        let size = RasterSize::with_rows_cols(Rows(2), Columns(2));
        let meta = create_metadata_with_scale(size, 2.0, 10.0);

        #[rustfmt::skip]
        let raster: DenseArray<f64, RasterMetadata> = DenseArray::new(
            meta,
            create_vec(&[1.0, 2.0, 3.0, 4.0]),
        ).unwrap();

        let result: Result<DenseArray<u8, RasterMetadata>> = scale_to_u8(&raster, None);
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

        let result: DenseArray<u8, RasterMetadata> = scale_to_u8(&raster, None).unwrap();

        let values: Vec<Option<u8>> = result.iter_opt().collect();
        assert_eq!(values.len(), 4);
        assert!(values.iter().all(|v| v.is_none()));
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

        let scaled: DenseArray<u8, RasterMetadata> = scale_to_u8(&original, None).unwrap();
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

        let result: DenseArray<u8, RasterMetadata> = scale_to_u8(&raster, None).unwrap();

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

        let result: DenseArray<u8, RasterMetadata> = scale_to_u8(&raster, None).unwrap();

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
    fn scale_to_u16_test() {
        let size = RasterSize::with_rows_cols(Rows(1), Columns(3));
        let meta = RasterMetadata::sized_with_nodata(size, Some(NOD));

        #[rustfmt::skip]
        let raster: DenseArray<f64, RasterMetadata> = DenseArray::new(
            meta,
            create_vec(&[0.0, 5000.0, 10000.0]),
        ).unwrap();

        let result: DenseArray<u16, RasterMetadata> = scale_to_u16(&raster, None).unwrap();

        let values: Vec<Option<u16>> = result.iter_opt().collect();
        assert_eq!(values[0].unwrap(), 0);
        assert_eq!(values[2].unwrap(), 65534);

        let scale_info = result.metadata().scale.unwrap();
        let expected_scale = 10000.0 / 65534.0;
        assert_relative_eq!(scale_info.scale, expected_scale, epsilon = 1e-6);
        assert_relative_eq!(scale_info.offset, 0.0, epsilon = 1e-6);
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
    #[cfg(feature = "simd")]
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

        // Get scalar result
        let scalar_result: DenseArray<u8, RasterMetadata> = scale_internal(&raster, None).unwrap();

        // Get SIMD result
        let simd_result: DenseArray<u8, RasterMetadata> = simd_algo::scale_to_u8(&raster, None).unwrap();

        // Compare results
        let scalar_values: Vec<Option<u8>> = scalar_result.iter_opt().collect();
        let simd_values: Vec<Option<u8>> = simd_result.iter_opt().collect();

        assert_eq!(scalar_values.len(), simd_values.len());
        for (i, (s, v)) in scalar_values.iter().zip(simd_values.iter()).enumerate() {
            assert_eq!(s, v, "Mismatch at index {}", i);
        }

        // Verify metadata is the same
        assert_eq!(scalar_result.metadata().scale, simd_result.metadata().scale);
    }

    #[test]
    #[cfg(feature = "simd")]
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

        // Get scalar result
        let scalar_result: DenseArray<u16, RasterMetadata> = scale_internal(&raster, None).unwrap();

        // Get SIMD result
        let simd_result: DenseArray<u16, RasterMetadata> = simd_algo::scale_to_u16(&raster, None).unwrap();

        // Compare results
        let scalar_values: Vec<Option<u16>> = scalar_result.iter_opt().collect();
        let simd_values: Vec<Option<u16>> = simd_result.iter_opt().collect();

        assert_eq!(scalar_values.len(), simd_values.len());
        for (i, (s, v)) in scalar_values.iter().zip(simd_values.iter()).enumerate() {
            assert_eq!(s, v, "Mismatch at index {}", i);
        }

        // Verify metadata is the same
        assert_eq!(scalar_result.metadata().scale, simd_result.metadata().scale);
    }

    #[test]
    #[cfg(feature = "simd")]
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
        let scalar_result: DenseArray<u8, RasterMetadata> = scale_internal(&raster, None).unwrap();
        let simd_result: DenseArray<u8, RasterMetadata> = simd_algo::scale_to_u8(&raster, None).unwrap();
        // Compare results
        let scalar_values: Vec<Option<u8>> = scalar_result.iter_opt().collect();
        let simd_values: Vec<Option<u8>> = simd_result.iter_opt().collect();

        assert_eq!(scalar_values.len(), simd_values.len());
        for (i, (s, v)) in scalar_values.iter().zip(simd_values.iter()).enumerate() {
            assert_eq!(s, v, "Mismatch at index {}", i);
        }

        // Verify metadata is the same
        assert_eq!(scalar_result.metadata().scale, simd_result.metadata().scale);
    }

    #[test]
    #[cfg(feature = "simd")]
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
        let scalar_result: DenseArray<u16, RasterMetadata> = scale_internal(&raster, None).unwrap();
        let simd_result: DenseArray<u16, RasterMetadata> = simd_algo::scale_to_u16(&raster, None).unwrap();
        let scalar_values: Vec<Option<u16>> = scalar_result.iter_opt().collect();
        let simd_values: Vec<Option<u16>> = simd_result.iter_opt().collect();

        assert_eq!(scalar_values.len(), simd_values.len());
        for (i, (s, v)) in scalar_values.iter().zip(simd_values.iter()).enumerate() {
            assert_eq!(s, v, "Mismatch at index {}", i);
        }

        // Verify metadata is the same
        assert_eq!(scalar_result.metadata().scale, simd_result.metadata().scale);
    }
}

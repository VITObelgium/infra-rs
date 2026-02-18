use crate::{Array, ArrayMetadata, ArrayNum, Error, Result, raster::algo};
use inf::cast;
use num::NumCast;

fn scale<TDest, R>(src: &R) -> Result<R::WithPixelType<TDest>>
where
    R: Array,
    TDest: ArrayNum,
    for<'a> &'a R: IntoIterator<Item = Option<R::Pixel>>,
{
    let geo_ref = src.metadata().geo_reference();
    if geo_ref.scale().is_some() {
        return Err(Error::InvalidArgument(
            "Cannot scale raster that already has scale information. Use descale first.".to_string(),
        ));
    }

    let Some(range) = crate::raster::algo::limits::min_max(src) else {
        let new_metadata = R::Metadata::sized(src.size(), TDest::TYPE);
        return Ok(R::WithPixelType::<TDest>::filled_with_nodata(new_metadata));
    };

    let range_f64 = cast::inclusive_range::<f64>(range)?;
    let min_val = *range_f64.start();
    let max_val = *range_f64.end();

    let dest_range_f64 = cast::inclusive_range::<f64>(TDest::min_value()..=TDest::max_value())?;
    let dest_min_raw = *dest_range_f64.start();
    let dest_max_raw = *dest_range_f64.end();
    let dest_nodata = TDest::TYPE.default_nodata_value();

    let (dest_min, dest_max) = if dest_nodata == dest_max_raw {
        (dest_min_raw, dest_max_raw - 1.0)
    } else if dest_nodata == dest_min_raw {
        (dest_min_raw + 1.0, dest_max_raw)
    } else {
        (dest_min_raw, dest_max_raw)
    };

    let input_range = max_val - min_val;
    let output_range = dest_max - dest_min;

    let scale = if input_range > 0.0 { input_range / output_range } else { 1.0 };

    let offset = min_val - (dest_min * scale);

    let mut new_georef = geo_ref;
    new_georef.set_scale(Some(crate::RasterScale { scale, offset }));
    let new_metadata = R::Metadata::with_geo_reference(new_georef);

    Ok(R::WithPixelType::<TDest>::from_iter_opt(
        new_metadata,
        src.into_iter().map(|x| {
            x.and_then(|v| {
                let v_f64: f64 = NumCast::from(v)?;
                let scaled = (v_f64 - offset) / scale;
                let clamped = scaled.max(dest_min).min(dest_max).round();
                NumCast::from(clamped)
            })
        }),
    )
    .expect("Raster size bug"))
}

/// Scales the raster values to fit the full range of u8 (0-255).
///
/// This function automatically determines the optimal scale and offset based on the input data range.
/// The scale/offset information is stored in the output metadata so that `descale` can reverse the operation.
///
/// **Note**: The value 255 is reserved for nodata, so actual data values will be scaled to the range 0-254.
///
/// # Errors
///
/// Returns an error if:
/// - The input raster already has scale information (cannot scale already-scaled data)
pub fn scale_to_u8<R>(src: &R) -> Result<R::WithPixelType<u8>>
where
    R: Array,
    for<'a> &'a R: IntoIterator<Item = Option<R::Pixel>>,
{
    scale(src)
}

/// Scales the raster values to fit the full range of u16 (0-65535).
///
/// This function automatically determines the optimal scale and offset based on the input data range.
/// The scale/offset information is stored in the output metadata so that `descale` can reverse the operation.
///
/// **Note**: The value 65535 is reserved for nodata, so actual data values will be scaled to the range 0-65534.
///
/// # Errors
///
/// Returns an error if:
/// - The input raster already has scale information (cannot scale already-scaled data)
pub fn scale_to_u16<R>(src: &R) -> Result<R::WithPixelType<u16>>
where
    R: Array,
    for<'a> &'a R: IntoIterator<Item = Option<R::Pixel>>,
{
    scale(src)
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

        let result: DenseArray<u8, RasterMetadata> = scale_to_u8(&raster).unwrap();

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

        let result: Result<DenseArray<u8, RasterMetadata>> = scale_to_u8(&raster);
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

        let result: DenseArray<u8, RasterMetadata> = scale_to_u8(&raster).unwrap();

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

        let scaled: DenseArray<u8, RasterMetadata> = scale_to_u8(&original).unwrap();
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

        let result: DenseArray<u8, RasterMetadata> = scale_to_u8(&raster).unwrap();

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

        let result: DenseArray<u8, RasterMetadata> = scale_to_u8(&raster).unwrap();

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
    fn scale_to_i16() {
        let size = RasterSize::with_rows_cols(Rows(1), Columns(3));
        let meta = RasterMetadata::sized_with_nodata(size, Some(NOD));

        #[rustfmt::skip]
        let raster: DenseArray<f64, RasterMetadata> = DenseArray::new(
            meta,
            create_vec(&[0.0, 5000.0, 10000.0]),
        ).unwrap();

        let result: DenseArray<i16, RasterMetadata> = scale(&raster).unwrap();

        let values: Vec<Option<i16>> = result.iter_opt().collect();
        assert_eq!(values[0].unwrap(), -32767);
        assert_eq!(values[2].unwrap(), 32767);

        let scale_info = result.metadata().scale.unwrap();
        let expected_scale = 10000.0 / (32767.0 - (-32767.0));
        assert_relative_eq!(scale_info.scale, expected_scale, epsilon = 1e-6);
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

        let result: DenseArray<u16, RasterMetadata> = scale_to_u16(&raster).unwrap();

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
}

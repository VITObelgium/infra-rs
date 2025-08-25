use std::ops::RangeInclusive;

use crate::{Array, ArrayNum};
use itertools::Itertools;
use itertools::MinMaxResult::{MinMax, NoElements, OneElement};

pub fn min_max<R, T, Meta>(ras: &R) -> Option<RangeInclusive<T>>
where
    R: Array<Pixel = T, Metadata = Meta>,
    T: ArrayNum,
{
    match ras.iter_values().minmax() {
        NoElements => None,
        OneElement(x) => Some(x..=x),
        MinMax(x, y) => Some(x..=y),
    }
}

#[cfg(feature = "simd")]
#[cfg_attr(docsrs, doc(cfg(feature = "simd")))]
pub mod simd {
    use simd_macro::simd_bounds;

    use crate::NodataSimd;

    use super::*;
    use crate::densearrayutil;
    use std::simd::prelude::*;

    const LANES: usize = inf::simd::LANES;

    #[simd_bounds]
    pub fn min<R, T, Meta>(ras: &R) -> Option<T>
    where
        T: ArrayNum,
        R: Array<Pixel = T, Metadata = Meta>,
    {
        if ras.is_empty() {
            return None;
        }

        let mut min = T::max_value();
        let mut simd_min = Simd::splat(min);
        let mut has_data = false;
        let mut has_simd_data = Mask::splat(false);

        densearrayutil::simd::unary_simd(
            ras.as_slice(),
            |&v| {
                has_data |= !v.is_nodata();
                min = min.nodata_min(v);
            },
            |v| {
                let nodata_mask = v.nodata_mask();
                has_simd_data |= !nodata_mask;
                //simd_min = v.nodata_min(simd_min);
                simd_min = nodata_mask.select(simd_min, v.nodata_min(simd_min));
            },
        );

        match (has_data, has_simd_data.any()) {
            (false, false) => return None,
            (true, false) => {}
            (false, true) => {
                let valid_data = has_simd_data.select(simd_min, Simd::splat(min));
                min = valid_data.reduce_min_unchecked();
            }
            (true, true) => {
                let valid_data = has_simd_data.select(simd_min, Simd::splat(T::max_value()));
                min = min.nodata_min(valid_data.reduce_min_unchecked());
            }
        }

        Some(min)
    }

    #[simd_bounds]
    pub fn max<R, T, Meta>(ras: &R) -> Option<T>
    where
        T: ArrayNum,
        R: Array<Pixel = T, Metadata = Meta>,
    {
        if ras.is_empty() {
            return None;
        }

        let mut max = T::min_value();
        let mut simd_max = Simd::splat(max);
        let mut has_data = false;
        let mut has_simd_data = Mask::splat(false);

        densearrayutil::simd::unary_simd(
            ras.as_slice(),
            |&v| {
                has_data |= !v.is_nodata();
                max = max.nodata_max(v);
            },
            |v| {
                let nodata_mask = v.nodata_mask();
                has_simd_data |= !nodata_mask;
                simd_max = nodata_mask.select(simd_max, v.nodata_max(simd_max));
            },
        );

        match (has_data, has_simd_data.any()) {
            (false, false) => return None,
            (true, false) => {}
            (false, true) => {
                let valid_data = has_simd_data.select(simd_max, Simd::splat(max));
                max = valid_data.reduce_max_unchecked();
            }
            (true, true) => {
                let valid_data = has_simd_data.select(simd_max, Simd::splat(max));
                max = max.nodata_max(valid_data.reduce_max_unchecked());
            }
        }

        Some(max)
    }

    #[simd_bounds]
    pub fn min_max<R, T, Meta>(ras: &R) -> Option<RangeInclusive<T>>
    where
        T: ArrayNum,
        R: Array<Pixel = T, Metadata = Meta>,
    {
        if ras.is_empty() {
            return None;
        }

        if T::has_nan() {
            // Specialized implementation for floating point types where NaN is the nodata value
            // This uses the fact that some simd operations can ignore NaN values when calculating min/max
            // This is faster that the fixed point implementation where masks need to be created
            let mut min = T::NODATA;
            let mut max = T::NODATA;

            let mut simd_min = Simd::<T, LANES>::splat(min);
            let mut simd_max = Simd::<T, LANES>::splat(max);

            densearrayutil::simd::unary_simd(
                ras.as_slice(),
                |&v| {
                    min = min.nodata_min(v);
                    max = max.nodata_max(v);
                },
                |v| {
                    simd_min = v.nodata_min(simd_min);
                    simd_max = v.nodata_max(simd_max);
                },
            );

            let min = min.nodata_min(simd_min.reduce_min().unwrap_or(min));
            let max = max.nodata_max(simd_max.reduce_max().unwrap_or(max));

            match (min.is_nan(), max.is_nan()) {
                (true, true) => None,
                (false, false) => Some(min..=max),
                _ => {
                    // If a min could not be calculated, it means all values were NaN
                    // so there should also be no max
                    panic!("Unexpected NaN values in min_max calculation");
                }
            }
        } else {
            let mut min = T::max_value();
            let mut max = T::min_value();

            let mut simd_min = Simd::<T, LANES>::splat(min);
            let mut simd_max = Simd::<T, LANES>::splat(max);

            let mut has_data = false;
            let mut simd_lane_has_data = Mask::splat(false);

            densearrayutil::simd::unary_simd(
                ras.as_slice(),
                |&v| {
                    has_data |= !v.is_nodata();
                    min = min.nodata_min(v);
                    max = max.nodata_max(v);
                },
                |v| {
                    let nodata_mask = v.nodata_mask();
                    //has_simd_data |= !nodata_mask;
                    simd_min = nodata_mask.select(simd_min, v.min_unchecked(simd_min));
                    simd_max = nodata_mask.select(simd_max, v.max_unchecked(simd_max));
                },
            );

            // A second pass to check for nodata is measured to be faster ???
            densearrayutil::simd::unary_simd(
                ras.as_slice(),
                |_| {},
                |v| {
                    simd_lane_has_data |= !v.nodata_mask();
                },
            );

            match (has_data, simd_lane_has_data.any()) {
                (false, false) => return None,
                (true, false) => {}
                (false, true) => {
                    let valid_data = simd_lane_has_data.select(simd_min, Simd::splat(min));
                    min = valid_data.reduce_min_unchecked();
                    let valid_data = simd_lane_has_data.select(simd_max, Simd::splat(max));
                    max = valid_data.reduce_max_unchecked();
                }
                (true, true) => {
                    let valid_data = simd_lane_has_data.select(simd_min, Simd::splat(T::max_value()));
                    min = min.nodata_min(valid_data.reduce_min_unchecked());
                    let valid_data = simd_lane_has_data.select(simd_max, Simd::splat(T::min_value()));
                    max = max.nodata_max(valid_data.reduce_max_unchecked());
                }
            }

            Some(min..=max)
        }
    }
}

#[cfg(test)]
#[generic_tests::define]
mod unspecialized_generictests {

    use inf::{allocate, cast};
    use simd_macro::simd_bounds;

    use crate::{
        ArrayInterop, CellSize, GeoReference, Point, RasterSize, Result,
        array::{Columns, Rows},
        raster::DenseRaster,
        testutils::{self, NOD},
    };

    #[cfg(feature = "simd")]
    const LANES: usize = inf::simd::LANES;

    use super::*;

    #[test]
    #[simd_bounds(R::Pixel)]
    fn test_min_max_empty<R>() -> Result<()>
    where
        R: Array<Metadata = GeoReference>,
        R::WithPixelType<f64>: Array<Pixel = f64, Metadata = GeoReference>,
    {
        let meta = GeoReference::with_bottom_left_origin(
            "",
            RasterSize::with_rows_cols(Rows(0), Columns(0)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let raster = R::WithPixelType::<f64>::new(
            meta.clone(),
            allocate::new_aligned_vec(),
        )?.cast_to::<R::Pixel>();

        let range = min_max(&raster);
        assert!(range.is_none());

        #[cfg(feature = "simd")]
        {
            let range_simd = simd::min_max(&raster);
            assert!(range_simd.is_none());
        }

        Ok(())
    }

    #[test]
    #[simd_bounds(R::Pixel)]
    fn test_min_max_only_nodata<R>() -> Result<()>
    where
        R: Array<Metadata = GeoReference>,
        R::WithPixelType<f64>: Array<Pixel = f64, Metadata = GeoReference>,
    {
        let meta = GeoReference::with_bottom_left_origin(
            "",
            RasterSize::with_rows_cols(Rows(3), Columns(3)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let raster = R::WithPixelType::<f64>::new(
            meta.clone(),
            testutils::create_vec(&[
                NOD, NOD, NOD,
                NOD, NOD, NOD,
                NOD, NOD, NOD,
            ]),
        )?.cast_to::<R::Pixel>();

        let range = min_max(&raster);
        assert!(range.is_none());

        #[cfg(feature = "simd")]
        {
            let range_simd = simd::min_max(&raster);
            assert_eq!(range_simd, None);
        }

        Ok(())
    }

    #[test]
    #[simd_bounds(R::Pixel)]
    fn test_min_max_single_element<R>() -> Result<()>
    where
        R: Array<Metadata = GeoReference>,
        R::WithPixelType<f64>: Array<Pixel = f64, Metadata = GeoReference>,
    {
        let meta = GeoReference::with_bottom_left_origin(
            "",
            RasterSize::with_rows_cols(Rows(1), Columns(1)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let raster = R::WithPixelType::<f64>::new(
            meta.clone(),
            testutils::create_vec(&[5.0]),
        )?.cast_to::<R::Pixel>();

        let range = min_max(&raster);
        assert_eq!(range, Some(cast::inclusive_range::<R::Pixel>(5.0..=5.0)?));

        #[cfg(feature = "simd")]
        {
            let range_simd = simd::min_max(&raster);
            assert_eq!(range_simd, range.clone());
            assert_eq!(simd::min(&raster), Some(*range.clone().unwrap().start()));
            assert_eq!(simd::max(&raster), Some(*range.unwrap().end()));
        }

        Ok(())
    }

    #[test]
    #[simd_bounds(R::Pixel)]
    fn test_min_max_multiple_elements<R>() -> Result<()>
    where
        R: Array<Metadata = GeoReference>,
        R::WithPixelType<f64>: Array<Pixel = f64, Metadata = GeoReference>,
    {
        let meta = GeoReference::with_bottom_left_origin(
            "",
            RasterSize::with_rows_cols(Rows(3), Columns(3)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let raster = R::WithPixelType::<f64>::new(
            meta.clone(),
            testutils::create_vec(&[
                0.0, 0.0, 0.0,
                0.0, 0.0, 0.0,
                1.0, 2.0, 0.0,
            ]),
        )?.cast_to::<R::Pixel>();

        let range = min_max(&raster);
        assert_eq!(range, Some(cast::inclusive_range(0.0..=2.0)?));

        #[cfg(feature = "simd")]
        {
            let range_simd = simd::min_max(&raster);
            assert_eq!(range_simd, range.clone());
            assert_eq!(simd::min(&raster), Some(*range.clone().unwrap().start()));
            assert_eq!(simd::max(&raster), Some(*range.unwrap().end()));
        }

        Ok(())
    }

    #[test]
    #[simd_bounds(R::Pixel)]
    fn test_min_max_multiple_elements_nodata<R>() -> Result<()>
    where
        R: Array<Metadata = GeoReference>,
        R::WithPixelType<f64>: Array<Pixel = f64, Metadata = GeoReference> + ArrayInterop<Pixel = f64, Metadata = GeoReference>,
    {
        let meta = GeoReference::with_bottom_left_origin(
            "",
            RasterSize::with_rows_cols(Rows(3), Columns(4)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let raster = R::WithPixelType::<f64>::new_init_nodata(
            meta,
            testutils::create_vec(&[
                NOD, 0.0, -10.0, NOD,
                0.0, NOD, 0.0, NOD,
                NOD, 21.0, NOD, NOD,
            ]),
        )?.cast_to::<R::Pixel>();

        let range = min_max(&raster);
        assert_eq!(range, Some(cast::inclusive_range(-10.0..=21.0)?));

        #[cfg(feature = "simd")]
        {
            let range_simd = simd::min_max(&raster);
            assert_eq!(range_simd, range.clone());
            assert_eq!(simd::min(&raster), Some(*range.clone().unwrap().start()));
            assert_eq!(simd::max(&raster), Some(*range.unwrap().end()));
        }

        Ok(())
    }

    #[test]
    #[simd_bounds(R::Pixel)]
    #[cfg(feature = "simd")]
    fn test_min_max_random_elements<R>() -> Result<()>
    where
        R: Array<Metadata = GeoReference> + ArrayInterop,
        R::Pixel: rand::distr::uniform::SampleUniform,
    {
        let meta = GeoReference::with_bottom_left_origin(
            "",
            RasterSize::with_rows_cols(Rows(130), Columns(333)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let size = meta.raster_size() ;
        let raster = R::new_init_nodata(meta, testutils::create_random_vec_with_nodata(size, -127.0..=127.0, 100))?.cast_to::<R::Pixel>();

        let range = min_max(&raster);

        let range_simd = simd::min_max(&raster);
        assert_eq!(range_simd, range);
        assert_eq!(Some(*range_simd.clone().expect("Range expected").start()), simd::min(&raster));
        assert_eq!(Some(*range_simd.expect("Range expected").clone().end()), simd::max(&raster));

        Ok(())
    }

    #[instantiate_tests(<DenseRaster<i8>>)]
    mod denserasteri8 {}

    #[instantiate_tests(<DenseRaster<f32>>)]
    mod denserasterf32 {}
}

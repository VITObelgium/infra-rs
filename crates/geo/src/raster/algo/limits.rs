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

pub mod simd {
    use fearless_simd::{Simd, SimdBase, SimdElement};

    use super::*;
    use crate::{densearrayutil, simd::dispatch_array_num_simd};

    pub fn min<R, T, Meta>(ras: &R) -> Option<T>
    where
        T: ArrayNum,
        R: Array<Pixel = T, Metadata = Meta>,
    {
        min_max(ras).map(|range| *range.start())
    }

    pub fn max<R, T, Meta>(ras: &R) -> Option<T>
    where
        T: ArrayNum,
        R: Array<Pixel = T, Metadata = Meta>,
    {
        min_max(ras).map(|range| *range.end())
    }

    pub fn min_max<R, T, Meta>(ras: &R) -> Option<RangeInclusive<T>>
    where
        T: ArrayNum,
        R: Array<Pixel = T, Metadata = Meta>,
    {
        fearless_simd::dispatch!(crate::simd::level(), simd => min_max_dispatched(simd, ras.as_slice()))
    }

    #[inline(always)]
    fn min_max_dispatched<S: Simd, T: ArrayNum>(simd: S, data: &[T]) -> Option<RangeInclusive<T>> {
        macro_rules! run {
            ($simd_type:ty, $scalar:ty, $vector:ty, $simd:expr, $data:expr) => {{
                let data: &[$scalar] = bytemuck::cast_slice($data);
                min_max_kernel::<$simd_type, $scalar, $vector>($simd, data).map(|range| {
                    let min = num::cast::<$scalar, T>(*range.start()).expect("ArrayNum type must match its ArrayDataType");
                    let max = num::cast::<$scalar, T>(*range.end()).expect("ArrayNum type must match its ArrayDataType");
                    min..=max
                })
            }};
        }

        dispatch_array_num_simd!(T::TYPE, S, run, simd, data)
    }

    #[inline(always)]
    fn min_max_kernel<S, T, V>(simd: S, data: &[T]) -> Option<RangeInclusive<T>>
    where
        S: Simd,
        T: ArrayNum + SimdElement,
        V: SimdBase<S, Element = T>,
    {
        let mut scalar_min: Option<T> = None;
        let mut scalar_max: Option<T> = None;
        let mut vector_min = V::splat(simd, T::NODATA);
        let mut vector_max = V::splat(simd, T::NODATA);

        densearrayutil::simd::unary_simd::<S, T, V>(
            simd,
            data,
            |&value| {
                if !value.is_nodata() {
                    scalar_min = Some(scalar_min.map_or(value, |min| min.nodata_min(value)));
                    scalar_max = Some(scalar_max.map_or(value, |max| max.nodata_max(value)));
                }
            },
            |values| {
                vector_min = crate::simd::nodata_min::<S, T, V>(vector_min, values);
                vector_max = crate::simd::nodata_max::<S, T, V>(vector_max, values);
            },
        );

        if let Some(value) = crate::simd::reduce_min::<S, T, V>(vector_min) {
            scalar_min = Some(scalar_min.map_or(value, |min| min.nodata_min(value)));
        }
        if let Some(value) = crate::simd::reduce_max::<S, T, V>(vector_max) {
            scalar_max = Some(scalar_max.map_or(value, |max| max.nodata_max(value)));
        }

        match (scalar_min, scalar_max) {
            (Some(min), Some(max)) => Some(min..=max),
            (None, None) => None,
            _ => unreachable!("minimum and maximum data presence must match"),
        }
    }
}

#[cfg(test)]
#[generic_tests::define]
mod unspecialized_generictests {

    use inf::{allocate, cast};

    use crate::{
        ArrayInterop, CellSize, GeoReference, Point, RasterSize, Result,
        array::{Columns, Rows},
        raster::DenseRaster,
        testutils::{self, NOD},
    };

    use super::*;

    #[test]
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

        {
            let range_simd = simd::min_max(&raster);
            assert!(range_simd.is_none());
        }

        Ok(())
    }

    #[test]
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

        {
            let range_simd = simd::min_max(&raster);
            assert_eq!(range_simd, None);
        }

        Ok(())
    }

    #[test]
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

        {
            let range_simd = simd::min_max(&raster);
            assert_eq!(range_simd, range.clone());
            assert_eq!(simd::min(&raster), Some(*range.clone().unwrap().start()));
            assert_eq!(simd::max(&raster), Some(*range.unwrap().end()));
        }

        Ok(())
    }

    #[test]
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

        {
            let range_simd = simd::min_max(&raster);
            assert_eq!(range_simd, range.clone());
            assert_eq!(simd::min(&raster), Some(*range.clone().unwrap().start()));
            assert_eq!(simd::max(&raster), Some(*range.unwrap().end()));
        }

        Ok(())
    }

    #[test]
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

        {
            let range_simd = simd::min_max(&raster);
            assert_eq!(range_simd, range.clone());
            assert_eq!(simd::min(&raster), Some(*range.clone().unwrap().start()));
            assert_eq!(simd::max(&raster), Some(*range.unwrap().end()));
        }

        Ok(())
    }

    #[test]
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

use std::ops::Range;

use crate::{Array, ArrayNum};
use itertools::Itertools;
use itertools::MinMaxResult::{MinMax, NoElements, OneElement};

pub fn min_max<R, T, Meta>(ras: &R) -> Range<T>
where
    R: Array<Pixel = T, Metadata = Meta>,
    T: ArrayNum,
{
    match ras.iter_values().minmax() {
        NoElements => T::zero()..T::zero(),
        OneElement(x) => x..x,
        MinMax(x, y) => x..y,
    }
}

#[cfg(feature = "simd")]
pub mod simd {
    use crate::NodataSimd;

    use super::*;
    use crate::{Nodata as _, densearrayutil};
    use std::simd::prelude::*;

    const LANES: usize = inf::simd::LANES;

    pub fn min<R, Meta>(ras: &R) -> f32
    where
        R: Array<Pixel = f32, Metadata = Meta>,
    {
        use num::Float;

        let mut min = f32::max_value();
        let mut simd_min = Simd::splat(min);

        densearrayutil::simd::unary_simd(
            ras.as_slice(),
            |&v| {
                min = min.nodata_min(v);
            },
            |v| {
                simd_min = v.is_nan().select(simd_min, v.simd_min(simd_min));
            },
        );

        min.min(simd_min.reduce_min())
    }

    #[simd_macro::simd_bounds]
    pub fn min_max<R, T, Meta>(ras: &R) -> Range<T>
    where
        T: ArrayNum,
        R: Array<Pixel = T, Metadata = Meta>,
    {
        let mut min = T::max_value();
        let mut max = T::min_value();

        let mut simd_min = Simd::splat(min);
        let mut simd_max = Simd::splat(max);

        densearrayutil::simd::unary_simd(
            ras.as_slice(),
            |&v| {
                min = min.nodata_min(v);
                max = max.nodata_max(v);
            },
            |v| {
                let nodata: <std::simd::Simd<T, LANES> as NodataSimd>::NodataMask = v.is_nodata();
                simd_min = nodata.select(simd_min, v.nodata_min(simd_min));
                simd_max = nodata.select(simd_max, v.nodata_max(simd_max));
            },
        );

        min = min.nodata_min(simd_min.reduce_min_without_nodata_check());
        max = max.nodata_max(simd_max.reduce_max_without_nodata_check());

        min..max
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
    #[simd_bounds]
    fn test_min_max_empty<R, T: ArrayNum>() -> Result<()>
    where
        R: Array<Pixel = T, Metadata = GeoReference>,
        R::WithPixelType<f64>: Array<Pixel = f64, Metadata = GeoReference>,
    {
        let meta = GeoReference::with_origin(
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
        assert_eq!(range, cast::range::<R::Pixel>(0.0..0.0)?);

        Ok(())
    }

    #[test]
    #[simd_macro::simd_bounds]
    fn test_min_max_single_element<R, T: ArrayNum>() -> Result<()>
    where
        R: Array<Pixel = T, Metadata = GeoReference>,
        R::WithPixelType<f64>: Array<Pixel = f64, Metadata = GeoReference>,
    {
        let meta = GeoReference::with_origin(
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
        assert_eq!(range, cast::range::<R::Pixel>(5.0..5.0)?);

        Ok(())
    }

    #[test]
    #[simd_macro::simd_bounds]
    fn test_min_max_multiple_elements<R, T: ArrayNum>() -> Result<()>
    where
        R: Array<Pixel = T, Metadata = GeoReference>,
        R::WithPixelType<f64>: Array<Pixel = f64, Metadata = GeoReference>,
    {
        let meta = GeoReference::with_origin(
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
        assert_eq!(range, cast::range(0.0..2.0)?);

        #[cfg(feature = "simd")]
        {
            use inf::cast;

            let range_simd = simd::min_max(&raster);
            assert_eq!(range_simd, cast::range(range)?);
        }

        Ok(())
    }

    #[test]
    #[simd_macro::simd_bounds]
    fn test_min_max_multiple_elements_nodata<R, T: ArrayNum>() -> Result<()>
    where
        R: Array<Pixel = T, Metadata = GeoReference>,
        R::WithPixelType<f64>: Array<Pixel = f64, Metadata = GeoReference> + ArrayInterop<Pixel = f64, Metadata = GeoReference>,
    {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(3), Columns(3)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let raster = R::WithPixelType::<f64>::new_init_nodata(
            meta,
            testutils::create_vec(&[
                NOD, 0.0, -10.0,
                0.0, NOD, 0.0,
                1.0, 21.0, NOD,
            ]),
        )?;

        let range = min_max(&raster);
        assert_eq!(range, cast::range(-10.0..21.0)?);

        Ok(())
    }

    #[instantiate_tests(<DenseRaster<u8>, u8>)]
    mod denserasteru8 {}

    #[instantiate_tests(<DenseRaster<f32>, f32>)]
    mod denserasterf32 {}
}

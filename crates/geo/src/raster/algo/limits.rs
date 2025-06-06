use std::ops::Range;

use crate::{Array, ArrayNum, Nodata as _, densearrayutil};
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
    use super::*;
    use std::simd::prelude::*;

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

    pub fn min_max<R, Meta>(ras: &R) -> Range<f32>
    where
        R: Array<Pixel = f32, Metadata = Meta>,
    {
        use num::Float;

        let mut min = f32::max_value();
        let mut max = f32::min_value();

        let mut simd_min = Simd::splat(min);
        let mut simd_max = Simd::splat(max);

        densearrayutil::simd::unary_simd(
            ras.as_slice(),
            |&v| {
                min = min.nodata_min(v);
                max = max.nodata_max(v);
            },
            |v| {
                let nodata = v.is_nan();
                simd_min = nodata.select(simd_min, v.simd_min(simd_min));
                simd_max = nodata.select(simd_max, v.simd_max(simd_max));
            },
        );

        min = min.min(simd_min.reduce_min());
        max = max.max(simd_max.reduce_max());

        min..max
    }
}

#[cfg(test)]
#[generic_tests::define]
mod unspecialized_generictests {

    use inf::allocate;

    use crate::{
        ArrayInterop, CellSize, GeoReference, Point, RasterSize, Result,
        array::{Columns, Rows},
        raster::DenseRaster,
        testutils::NOD,
    };

    use super::*;

    #[test]
    fn test_min_max_empty<R: Array<Pixel = u8, Metadata = GeoReference>>() -> Result<()> {
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
        )?;

        let range = min_max(&raster);
        assert_eq!(range, 0.0..0.0);

        Ok(())
    }

    #[test]
    fn test_min_max_single_element<R: Array<Pixel = u8, Metadata = GeoReference>>() -> Result<()> {
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
            allocate::aligned_vec_from_slice(&[
                5.0,
            ]),
        )?;

        let range = min_max(&raster);
        assert_eq!(range, 5.0..5.0);

        Ok(())
    }

    #[test]
    fn test_min_max_multiple_elements<R: Array<Pixel = u8, Metadata = GeoReference>>() -> Result<()> {
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
            allocate::aligned_vec_from_slice(&[
                0.0, 0.0, 0.0,
                0.0, 0.0, 0.0,
                1.0, 2.0, 0.0,
            ]),
        )?;

        let range = min_max(&raster);
        assert_eq!(range, 0.0..2.0);

        #[cfg(feature = "simd")]
        {
            use inf::cast;

            let range_simd = simd::min_max(&raster.cast_to::<f32>());
            assert_eq!(range_simd, cast::range::<f32>(range)?);
        }

        Ok(())
    }

    #[test]
    fn test_min_max_multiple_elements_nodata<R: Array<Pixel = u8, Metadata = GeoReference>>() -> Result<()>
    where
        R::WithPixelType<f64>: ArrayInterop<Pixel = f64, Metadata = GeoReference>,
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
            allocate::aligned_vec_from_slice(&[
                NOD, 0.0, -10.0,
                0.0, NOD, 0.0,
                1.0, 21.0, NOD,
            ]),
        )?;

        let range = min_max(&raster);
        assert_eq!(range, -10.0..21.0);

        Ok(())
    }

    #[instantiate_tests(<DenseRaster<u8>>)]
    mod denseraster {}
}

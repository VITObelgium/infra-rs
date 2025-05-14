use std::ops::Range;

use crate::{Array, ArrayNum};
use itertools::Itertools;
use itertools::MinMaxResult::{MinMax, NoElements, OneElement};

pub fn min_max<R, T>(ras: &R) -> Range<T>
where
    R: Array<Pixel = T>,
    T: ArrayNum,
{
    match ras.iter_values().minmax() {
        NoElements => T::zero()..T::zero(),
        OneElement(x) => x..x,
        MinMax(x, y) => x..y,
    }
}

#[cfg(test)]
#[generic_tests::define]
mod unspecialized_generictests {

    use crate::{
        CellSize, GeoReference, Point, RasterSize,
        array::{Columns, Rows},
        raster::DenseRaster,
        testutils::NOD,
    };

    use super::*;

    #[test]
    fn test_min_max_empty<R: Array<Pixel = u8, Metadata = GeoReference, WithPixelType<u8> = R>>() {
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
            vec![
            ],
        ).unwrap();

        let range = min_max(&raster);
        assert_eq!(range, 0.0..0.0);
    }

    #[test]
    fn test_min_max_single_element<R: Array<Pixel = u8, Metadata = GeoReference, WithPixelType<u8> = R>>() {
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
            vec![
                5.0,
            ],
        ).unwrap();

        let range = min_max(&raster);
        assert_eq!(range, 5.0..5.0);
    }

    #[test]
    fn test_min_max_multiple_elements<R: Array<Pixel = u8, Metadata = GeoReference, WithPixelType<u8> = R>>() {
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
            vec![
                0.0, 0.0, 0.0,
                0.0, 0.0, 0.0,
                1.0, 2.0, 0.0,
            ],
        ).unwrap();

        let range = min_max(&raster);
        assert_eq!(range, 0.0..2.0);
    }

    #[test]
    fn test_min_max_multiple_elements_nodata<R: Array<Pixel = u8, Metadata = GeoReference, WithPixelType<u8> = R>>() {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(3), Columns(3)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let raster = R::WithPixelType::<f64>::new_process_nodata(
            meta,
            vec![
                NOD, 0.0, -10.0,
                0.0, NOD, 0.0,
                1.0, 21.0, NOD,
            ],
        ).unwrap();

        let range = min_max(&raster);
        assert_eq!(range, -10.0..21.0);
    }

    #[instantiate_tests(<DenseRaster<u8>>)]
    mod denseraster {}
}

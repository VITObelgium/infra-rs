use crate::Array;
use crate::Nodata;

pub fn replace_nodata_in_place<RasterType>(ras: &mut RasterType, new_value: RasterType::Pixel)
where
    RasterType: Array,
{
    ras.iter_mut().for_each(|x| {
        if x.is_nodata() {
            *x = new_value;
        }
    });
}

pub fn replace_nodata<RasterType>(ras: &RasterType, new_value: RasterType::Pixel) -> RasterType
where
    RasterType: Array,
{
    let mut result = ras.clone();
    replace_nodata_in_place(&mut result, new_value);
    result
}

pub fn turn_value_into_nodata<RasterType>(ras: &mut RasterType, value: RasterType::Pixel)
where
    RasterType: Array,
{
    ras.iter_mut().for_each(|x| {
        if *x == value {
            *x = RasterType::Pixel::nodata_value();
        }
    });
}

pub fn is_nodata<RasterType: Array>(input: &RasterType) -> RasterType::WithPixelType<u8> {
    RasterType::WithPixelType::<u8>::from_iter(
        input.metadata().clone(),
        input.iter().map(|x| Some(if x.is_nodata() { 1 } else { 0 })),
    )
}

pub fn is_data<RasterType: Array>(input: &RasterType) -> RasterType::WithPixelType<u8> {
    RasterType::WithPixelType::<u8>::from_iter(
        input.metadata().clone(),
        input.iter().map(|x| Some(if x.is_nodata() { 0 } else { 1 })),
    )
}

#[cfg(test)]
#[generic_tests::define]
mod generictests {
    use num::NumCast;

    use crate::{
        array::{Columns, Rows},
        testutils::{create_vec, NOD},
        DenseArray, RasterSize,
    };

    use super::*;

    #[test]
    fn replace_nodata<R: Array<Metadata = RasterSize>>() {
        let size = RasterSize::with_rows_cols(Rows(5), Columns(4));
        #[rustfmt::skip]
        let raster = R::new(
            size,
            create_vec(&[
                NOD, NOD,  4.0, 4.0,
                4.0, 8.0,  4.0, 9.0,
                2.0, 4.0,  NOD, 7.0,
                4.0, 4.0,  5.0, 8.0,
                3.0, NOD,  4.0, NOD,
            ]),
        );

        #[rustfmt::skip]
        let expected = R::new(
            size,
            create_vec(&[
                44.0, 44.0,  4.0,  4.0,
                 4.0,  8.0,  4.0,  9.0,
                 2.0,  4.0, 44.0,  7.0,
                 4.0,  4.0,  5.0,  8.0,
                 3.0, 44.0,  4.0, 44.0,
            ]),
        );

        assert_eq!(expected, super::replace_nodata(&raster, NumCast::from(44.0).unwrap()));
    }

    #[test]
    fn turn_value_into_nodata<R: Array<Metadata = RasterSize>>() {
        let size = RasterSize::with_rows_cols(Rows(5), Columns(4));
        #[rustfmt::skip]
        let mut raster = R::new(
            size,
            create_vec(&[
                NOD, NOD,  4.0, 4.0,
                4.0, 8.0,  4.0, 9.0,
                2.0, 4.0,  NOD, 7.0,
                4.0, 4.0,  5.0, 8.0,
                3.0, NOD,  4.0, NOD,
            ]),
        );

        #[rustfmt::skip]
        let expected = R::new(
            size,
            create_vec(&[
                 NOD,  NOD,  NOD,  NOD,
                 NOD,  8.0,  NOD,  9.0,
                 2.0,  NOD,  NOD,  7.0,
                 NOD,  NOD,  5.0,  8.0,
                 3.0,  NOD,  NOD,  NOD,
            ]),
        );

        super::turn_value_into_nodata(&mut raster, NumCast::from(4.0).unwrap());
        assert_eq!(expected, raster);
    }

    #[test]
    fn is_nodata<R: Array<Metadata = RasterSize>>() {
        let size = RasterSize::with_rows_cols(Rows(5), Columns(4));
        #[rustfmt::skip]
        let raster = R::new(
            size,
            create_vec(&[
                NOD, NOD,  4.0, 4.0,
                4.0, 8.0,  4.0, 9.0,
                2.0, 4.0,  NOD, 7.0,
                4.0, 4.0,  5.0, 8.0,
                3.0, NOD,  4.0, NOD,
            ]),
        );

        #[rustfmt::skip]
        let expected = R::WithPixelType::<u8>::new(
            size,
            vec![
                 1,  1,  0,  0,
                 0,  0,  0,  0,
                 0,  0,  1,  0,
                 0,  0,  0,  0,
                 0,  1,  0,  1,
            ],
        );

        assert_eq!(expected, super::is_nodata(&raster));
    }

    #[test]
    fn is_data<R: Array<Metadata = RasterSize>>() {
        let size = RasterSize::with_rows_cols(Rows(5), Columns(4));
        #[rustfmt::skip]
        let raster = R::new(
            size,
            create_vec(&[
                NOD, NOD,  4.0, 4.0,
                4.0, 8.0,  4.0, 9.0,
                2.0, 4.0,  NOD, 7.0,
                4.0, 4.0,  5.0, 8.0,
                3.0, NOD,  4.0, NOD,
            ]),
        );

        #[rustfmt::skip]
        let expected = R::WithPixelType::<u8>::new(
            size,
            vec![
                 0,  0,  1,  1,
                 1,  1,  1,  1,
                 1,  1,  0,  1,
                 1,  1,  1,  1,
                 1,  0,  1,  0,
            ],
        );

        assert_eq!(expected, super::is_data(&raster));
    }

    #[instantiate_tests(<DenseArray<i8>>)]
    mod denserasteri8 {}

    #[instantiate_tests(<DenseArray<u8>>)]
    mod denserasteru8 {}

    #[instantiate_tests(<DenseArray<i32>>)]
    mod denserasteri32 {}

    #[instantiate_tests(<DenseArray<u32>>)]
    mod denserasteru32 {}

    #[instantiate_tests(<DenseArray<i64>>)]
    mod denserasteri64 {}

    #[instantiate_tests(<DenseArray<u64>>)]
    mod denserasteru64 {}

    #[instantiate_tests(<DenseArray<f32>>)]
    mod denserasterf32 {}

    #[instantiate_tests(<DenseArray<f64>>)]
    mod denserasterf64 {}
}

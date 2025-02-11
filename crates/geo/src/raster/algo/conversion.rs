use crate::Array;

pub fn replace_value<RasterType>(ras: &mut RasterType, search_value: RasterType::Pixel, new_value: RasterType::Pixel)
where
    RasterType: Array,
{
    ras.iter_mut().for_each(|x| {
        if *x == search_value {
            *x = new_value;
        }
    });
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
    fn replace_value<R: Array<Metadata = RasterSize>>() {
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
        ).unwrap();

        #[rustfmt::skip]
        let expected = R::new(
            size,
            create_vec(&[
                 NOD,  NOD,  9.0,  9.0,
                 9.0,  8.0,  9.0,  9.0,
                 2.0,  9.0,  NOD,  7.0,
                 9.0,  9.0,  5.0,  8.0,
                 3.0,  NOD,  9.0,  NOD,
            ]),
        ).unwrap();

        super::replace_value(&mut raster, NumCast::from(4.0).unwrap(), NumCast::from(9.0).unwrap());
        assert_eq!(expected, raster);
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

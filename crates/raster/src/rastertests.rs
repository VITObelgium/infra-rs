#[cfg(test)]
#[generic_tests::define]
mod tests {
    use crate::{
        raster::RasterCreation,
        testutils::{create_vec, NOD},
        DenseRaster, Raster, RasterNum, RasterSize,
    };

    const SIZE: RasterSize = RasterSize::with_rows_cols(3, 3);

    #[test]
    fn test_add_raster_with_nodata<T: RasterNum<T>, R: Raster<T> + RasterCreation<T>>()
    where
        for<'a> &'a R: std::ops::Add<&'a R, Output = R>,
    {
        let raster1 = R::new(SIZE, create_vec(&[NOD, 2.0, 2.0, 3.0, NOD, 3.0, 1.0, 1.0, 0.0]));
        let raster2 = R::new(SIZE, create_vec(&[1.0, 3.0, 4.0, 5.0, NOD, 3.0, 3.0, 3.0, NOD]));
        let expected = R::new(SIZE, create_vec(&[NOD, 5.0, 6.0, 8.0, NOD, 6.0, 4.0, 4.0, NOD]));

        {
            let result = &raster1 + &raster2;
            assert_eq!(result, expected);
        }

        {
            let mut raster1 = raster1.clone();
            raster1 += &raster2;
            assert_eq!(raster1, expected);
        }

        {
            let mut raster1 = raster1.clone();
            let raster2 = raster2.clone();
            raster1 += raster2;
            assert_eq!(raster1, expected);
        }

        {
            let result = raster1 + raster2;
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_add_raster_with_nodata_inclusive<T: RasterNum<T>, R: Raster<T> + RasterCreation<T>>()
    where
        for<'a> &'a R: crate::ops::AddInclusive<&'a R, Output = R>,
    {
        use crate::ops::AddInclusive;

        let raster1 = R::new(SIZE, create_vec(&[NOD, 2.0, 2.0, 3.0, NOD, 3.0, 1.0, 1.0, 2.0]));
        let raster2 = R::new(SIZE, create_vec(&[1.0, 3.0, 4.0, 5.0, NOD, 3.0, 3.0, 3.0, NOD]));
        let expected = R::new(SIZE, create_vec(&[1.0, 5.0, 6.0, 8.0, NOD, 6.0, 4.0, 4.0, 2.0]));

        {
            let result = (&raster1).add_inclusive(&raster2);
            assert_eq!(result, expected);
        }

        {
            let mut raster1 = raster1.clone();
            raster1.add_assign_inclusive(&raster2);
            assert_eq!(raster1, expected);
        }

        {
            let mut raster1 = raster1.clone();
            let raster2 = raster2.clone();
            raster1.add_assign_inclusive(raster2);
            assert_eq!(raster1, expected);
        }

        {
            let result = raster1.add_inclusive(raster2);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_add_scalar_with_nodata<T: RasterNum<T>, R: Raster<T> + RasterCreation<T>>() {
        let raster1 = R::new(SIZE, create_vec(&[NOD, 2.0, 2.0, 3.0, NOD, 3.0, 1.0, 1.0, 0.0]));
        let expected = R::new(SIZE, create_vec(&[NOD, 6.0, 6.0, 7.0, NOD, 7.0, 5.0, 5.0, 4.0]));

        let scalar: T = num::NumCast::from(4.0).unwrap();

        {
            let mut raster1 = raster1.clone();
            raster1 += scalar;
            assert_eq!(raster1, expected);
        }

        {
            let result = raster1 + scalar;
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_subtract_raster_with_nodata<T: RasterNum<T>, R: Raster<T> + RasterCreation<T>>()
    where
        for<'a> &'a R: std::ops::Sub<&'a R, Output = R>,
    {
        let raster1 = R::new(SIZE, create_vec(&[NOD, 5.0, 9.0, 3.0, NOD, 13.0, 3.0, 4.0, 0.0]));
        let raster2 = R::new(SIZE, create_vec(&[1.0, 3.0, 4.0, 3.0, NOD, 3.0, 1.0, 3.0, NOD]));
        let expected = R::new(SIZE, create_vec(&[NOD, 2.0, 5.0, 0.0, NOD, 10.0, 2.0, 1.0, NOD]));

        {
            let result = &raster1 - &raster2;
            assert_eq!(result, expected);
        }

        {
            let mut raster1 = raster1.clone();
            raster1 -= &raster2;
            assert_eq!(raster1, expected);
        }

        {
            let mut raster1 = raster1.clone();
            let raster2 = raster2.clone();
            raster1 -= raster2;
            assert_eq!(raster1, expected);
        }

        {
            let result = raster1 - raster2;
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_subtract_raster_with_nodata_inclusive<T: RasterNum<T>, R: Raster<T> + RasterCreation<T>>()
    where
        for<'a> &'a R: crate::ops::SubInclusive<&'a R, Output = R>,
    {
        use crate::ops::SubInclusive;

        let raster1 = R::new(SIZE, create_vec(&[NOD, 5.0, 9.0, 3.0, NOD, 13.0, 3.0, 4.0, 8.0]));
        let raster2 = R::new(SIZE, create_vec(&[NOD, 3.0, 4.0, 3.0, NOD, 3.0, 1.0, 3.0, NOD]));
        let expected = R::new(SIZE, create_vec(&[NOD, 2.0, 5.0, 0.0, NOD, 10.0, 2.0, 1.0, 8.0]));

        {
            let result = (&raster1).sub_inclusive(&raster2);
            assert_eq!(result, expected);
        }

        {
            let mut raster1 = raster1.clone();
            raster1.sub_assign_inclusive(&raster2);
            assert_eq!(raster1, expected);
        }

        {
            let mut raster1 = raster1.clone();
            let raster2 = raster2.clone();
            raster1.sub_assign_inclusive(raster2);
            assert_eq!(raster1, expected);
        }

        {
            let result = raster1.sub_inclusive(raster2);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_subtract_scalar_with_nodata<T: RasterNum<T>, R: Raster<T> + RasterCreation<T>>() {
        let raster1 = R::new(SIZE, create_vec(&[NOD, 2.0, 2.0, 3.0, NOD, 3.0, 4.0, 8.0, 10.0]));
        let expected = R::new(SIZE, create_vec(&[NOD, 0.0, 0.0, 1.0, NOD, 1.0, 2.0, 6.0, 8.0]));

        let scalar: T = num::NumCast::from(2.0).unwrap();

        {
            let mut raster1 = raster1.clone();
            raster1 -= scalar;
            assert_eq!(raster1, expected);
        }

        {
            let result = raster1 - scalar;
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_multiply_raster_with_nodata<T: RasterNum<T>, R: Raster<T> + RasterCreation<T>>()
    where
        for<'a> &'a R: std::ops::Mul<&'a R, Output = R>,
    {
        let raster1 = R::new(SIZE, create_vec(&[NOD, 2.0, 2.0, 3.0, NOD, 3.0, 1.0, 1.0, 0.0]));
        let raster2 = R::new(SIZE, create_vec(&[1.0, 3.0, 3.0, 3.0, NOD, 3.0, 3.0, 3.0, NOD]));
        let expected = R::new(SIZE, create_vec(&[NOD, 6.0, 6.0, 9.0, NOD, 9.0, 3.0, 3.0, NOD]));

        {
            let result = &raster1 * &raster2;
            assert_eq!(result, expected);
        }

        {
            let result = raster1 * raster2;
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_multiply_scalar_with_nodata<T: RasterNum<T>, R: Raster<T> + RasterCreation<T>>() {
        let raster1 = R::new(SIZE, create_vec(&[NOD, 2.0, 2.0, 3.0, NOD, 3.0, 1.0, 1.0, 0.0]));
        let expected = R::new(SIZE, create_vec(&[NOD, 8.0, 8.0, 12.0, NOD, 12.0, 4.0, 4.0, 0.0]));

        let scalar: T = num::NumCast::from(4.0).unwrap();

        {
            let mut raster1 = raster1.clone();
            raster1 *= scalar;
            assert_eq!(raster1, expected);
        }

        {
            let result = raster1 * scalar;
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_divide_raster_with_nodata<T: RasterNum<T>, R: Raster<T> + RasterCreation<T>>()
    where
        for<'a> &'a R: std::ops::Div<&'a R, Output = R>,
    {
        let raster1 = R::new(SIZE, create_vec(&[NOD, 9.0, 6.0, 3.0, NOD, 3.0, 1.0, 12.0, 0.0]));
        let raster2 = R::new(SIZE, create_vec(&[1.0, 3.0, 2.0, 0.0, NOD, 3.0, 1.0, 3.0, NOD]));
        let expected = R::new(SIZE, create_vec(&[NOD, 3.0, 3.0, NOD, NOD, 1.0, 1.0, 4.0, NOD]));

        {
            let result = &raster1 / &raster2;
            assert_eq!(result, expected);
        }

        {
            let result = raster1 / raster2;
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_divide_scalar_with_nodata<T: RasterNum<T>, R: Raster<T> + RasterCreation<T>>() {
        let raster1 = R::new(SIZE, create_vec(&[NOD, 6.0, 3.0, 0.0, NOD, 3.0, 30.0, 12.0, 0.0]));
        let expected = R::new(SIZE, create_vec(&[NOD, 2.0, 1.0, 0.0, NOD, 1.0, 10.0, 4.0, 0.0]));

        let scalar: T = num::NumCast::from(3.0).unwrap();

        {
            let mut raster1 = raster1.clone();
            raster1 /= scalar;
            assert_eq!(raster1, expected);
        }

        {
            let mut raster1 = raster1.clone();
            raster1 /= T::zero();
            assert_eq!(raster1.nodata_count(), raster1.len());
        }

        {
            let result = raster1 / scalar;
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_sum<T: RasterNum<T>, R: Raster<T> + RasterCreation<T>>() {
        let ras = R::new(RasterSize { rows: 2, cols: 2 }, create_vec(&[1.0, 2.0, NOD, 4.0]));
        assert_eq!(ras.sum(), 7.0);
    }

    #[instantiate_tests(<i8, DenseRaster<i8>>)]
    mod denserasteri8 {}

    #[instantiate_tests(<u8, DenseRaster<u8>>)]
    mod denserasteru8 {}

    #[instantiate_tests(<i32, DenseRaster<i32>>)]
    mod denserasteri32 {}

    #[instantiate_tests(<u32, DenseRaster<u32>>)]
    mod denserasteru32 {}

    #[instantiate_tests(<i64, DenseRaster<i64>>)]
    mod denserasteri64 {}

    #[instantiate_tests(<u64, DenseRaster<u64>>)]
    mod denserasteru64 {}

    #[instantiate_tests(<f32, DenseRaster<f32>>)]
    mod denserasterf32 {}

    #[instantiate_tests(<f64, DenseRaster<f64>>)]
    mod denseraster64 {}
}

#[cfg(test)]
#[generic_tests::define]
mod tests {
    use crate::{
        Array, ArrayNumScalar, DenseArray, RasterSize,
        array::{Columns, Rows},
        testutils::{NOD, create_vec},
    };

    use num::Zero;

    const SIZE: RasterSize = RasterSize::with_rows_cols(Rows(3), Columns(3));

    #[test]
    fn test_add_raster_with_nodata<R: Array<Metadata = RasterSize>>()
    where
        for<'a> &'a R: std::ops::Add<&'a R, Output = R>,
    {
        let raster1 = R::new(SIZE, create_vec(&[NOD, 2.0, 2.0, 3.0, NOD, 3.0, 1.0, 1.0, 0.0])).unwrap();
        let raster2 = R::new(SIZE, create_vec(&[1.0, 3.0, 4.0, 5.0, NOD, 3.0, 3.0, 3.0, NOD])).unwrap();
        let expected = R::new(SIZE, create_vec(&[NOD, 5.0, 6.0, 8.0, NOD, 6.0, 4.0, 4.0, NOD])).unwrap();

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
    fn test_add_raster_with_nodata_inclusive<R: Array<Metadata = RasterSize>>()
    where
        for<'a> &'a R: crate::arrayops::AddInclusive<&'a R, Output = R>,
    {
        use crate::arrayops::AddInclusive;

        let raster1 = R::new(SIZE, create_vec(&[NOD, 2.0, 2.0, 3.0, NOD, 3.0, 1.0, 1.0, 2.0])).unwrap();
        let raster2 = R::new(SIZE, create_vec(&[1.0, 3.0, 4.0, 5.0, NOD, 3.0, 3.0, 3.0, NOD])).unwrap();
        let expected = R::new(SIZE, create_vec(&[1.0, 5.0, 6.0, 8.0, NOD, 6.0, 4.0, 4.0, 2.0])).unwrap();

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
    fn test_add_scalar_with_nodata<R: Array<Metadata = RasterSize>>() {
        let raster1 = R::new(SIZE, create_vec(&[NOD, 2.0, 2.0, 3.0, NOD, 3.0, 1.0, 1.0, 0.0])).unwrap();
        let expected = R::new(SIZE, create_vec(&[NOD, 6.0, 6.0, 7.0, NOD, 7.0, 5.0, 5.0, 4.0])).unwrap();

        let scalar: R::Pixel = num::NumCast::from(4.0).unwrap();

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
    fn test_subtract_raster_with_nodata<R: Array<Metadata = RasterSize>>()
    where
        for<'a> &'a R: std::ops::Sub<&'a R, Output = R>,
    {
        let raster1 = R::new(SIZE, create_vec(&[NOD, 5.0, 9.0, 3.0, NOD, 13.0, 3.0, 4.0, 0.0])).unwrap();
        let raster2 = R::new(SIZE, create_vec(&[1.0, 3.0, 4.0, 3.0, NOD, 3.0, 1.0, 3.0, NOD])).unwrap();
        let expected = R::new(SIZE, create_vec(&[NOD, 2.0, 5.0, 0.0, NOD, 10.0, 2.0, 1.0, NOD])).unwrap();

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
    fn test_subtract_raster_with_nodata_inclusive<R: Array<Metadata = RasterSize>>()
    where
        for<'a> &'a R: crate::arrayops::SubInclusive<&'a R, Output = R>,
    {
        use crate::arrayops::SubInclusive;

        let raster1 = R::new(SIZE, create_vec(&[NOD, 5.0, 9.0, 3.0, NOD, 13.0, 3.0, 4.0, 8.0])).unwrap();
        let raster2 = R::new(SIZE, create_vec(&[1.0, 3.0, 4.0, 3.0, NOD, 3.0, 1.0, 3.0, NOD])).unwrap();
        let expected = if R::Pixel::IS_SIGNED {
            R::new(SIZE, create_vec(&[-1.0, 2.0, 5.0, 0.0, NOD, 10.0, 2.0, 1.0, 8.0]))
        } else {
            R::new(SIZE, create_vec(&[NOD, 2.0, 5.0, 0.0, NOD, 10.0, 2.0, 1.0, 8.0]))
        }
        .unwrap();

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
    fn test_subtract_scalar_with_nodata<R: Array<Metadata = RasterSize>>() {
        let raster1 = R::new(SIZE, create_vec(&[NOD, 2.0, 2.0, 3.0, NOD, 3.0, 4.0, 8.0, 10.0])).unwrap();
        let expected = R::new(SIZE, create_vec(&[NOD, 0.0, 0.0, 1.0, NOD, 1.0, 2.0, 6.0, 8.0])).unwrap();

        let scalar: R::Pixel = num::NumCast::from(2.0).unwrap();

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
    fn test_multiply_raster_with_nodata<R: Array<Metadata = RasterSize>>()
    where
        for<'a> &'a R: std::ops::Mul<&'a R, Output = R>,
    {
        let raster1 = R::new(SIZE, create_vec(&[NOD, 2.0, 2.0, 3.0, NOD, 3.0, 1.0, 1.0, 0.0])).unwrap();
        let raster2 = R::new(SIZE, create_vec(&[1.0, 3.0, 3.0, 3.0, NOD, 3.0, 3.0, 3.0, NOD])).unwrap();
        let expected = R::new(SIZE, create_vec(&[NOD, 6.0, 6.0, 9.0, NOD, 9.0, 3.0, 3.0, NOD])).unwrap();

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
    fn test_multiply_scalar_with_nodata<R: Array<Metadata = RasterSize>>() {
        let raster1 = R::new(SIZE, create_vec(&[NOD, 2.0, 2.0, 3.0, NOD, 3.0, 1.0, 1.0, 0.0])).unwrap();
        let expected = R::new(SIZE, create_vec(&[NOD, 8.0, 8.0, 12.0, NOD, 12.0, 4.0, 4.0, 0.0])).unwrap();

        let scalar: R::Pixel = num::NumCast::from(4.0).unwrap();

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
    fn test_divide_raster_with_nodata<R: Array<Metadata = RasterSize>>()
    where
        for<'a> &'a R: std::ops::Div<&'a R, Output = R>,
    {
        let raster1 = R::new(SIZE, create_vec(&[NOD, 9.0, 6.0, 3.0, NOD, 3.0, 1.0, 12.0, 0.0])).unwrap();
        let raster2 = R::new(SIZE, create_vec(&[1.0, 3.0, 2.0, 0.0, NOD, 3.0, 1.0, 3.0, NOD])).unwrap();
        let expected = R::new(SIZE, create_vec(&[NOD, 3.0, 3.0, NOD, NOD, 1.0, 1.0, 4.0, NOD])).unwrap();

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
    fn test_divide_scalar_with_nodata<R: Array<Metadata = RasterSize>>() {
        let raster1 = R::new(SIZE, create_vec(&[NOD, 6.0, 3.0, 0.0, NOD, 3.0, 30.0, 12.0, 0.0])).unwrap();
        let expected = R::new(SIZE, create_vec(&[NOD, 2.0, 1.0, 0.0, NOD, 1.0, 10.0, 4.0, 0.0])).unwrap();

        let scalar: R::Pixel = num::NumCast::from(3.0).unwrap();

        {
            let mut raster1 = raster1.clone();
            raster1 /= scalar;
            assert_eq!(raster1, expected);
        }

        {
            let mut raster1 = raster1.clone();
            raster1 /= R::Pixel::zero();
            assert_eq!(raster1.nodata_count(), raster1.len());
        }

        {
            let result = raster1 / scalar;
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_sum<R: Array<Metadata = RasterSize>>() {
        let ras = R::new(RasterSize::with_rows_cols(Rows(2), Columns(2)), create_vec(&[1.0, 2.0, NOD, 4.0])).unwrap();
        assert_eq!(ras.sum(), 7.0);
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
    mod denseraster64 {}
}

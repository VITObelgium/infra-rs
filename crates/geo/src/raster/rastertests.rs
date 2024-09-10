#[cfg(test)]
#[generic_tests::define]
mod tests {
    use num::NumCast;

    use crate::{
        raster::{
            testutils::{NOD, *},
            DenseRaster, Raster, RasterNum,
        },
        GeoReference, RasterSize,
    };

    const META: GeoReference = GeoReference::without_spatial_reference(RasterSize::with_rows_cols(3, 3), Some(NOD));

    #[cfg(feature = "arrow")]
    use crate::raster::ArrowRaster;

    #[test]
    fn test_add_nodata<T: RasterNum<T>, R>()
    where
        for<'a> &'a R: std::ops::Add<&'a R, Output = R>,
        R: Raster<T> + std::ops::Add<R, Output = R>,
    {
        let raster1 = R::new(META, create_vec(&[NOD, 2.0, 2.0, 3.0, NOD, 3.0, 1.0, 1.0, 0.0]));
        let raster2 = R::new(META, create_vec(&[1.0, 3.0, 3.0, 3.0, NOD, 3.0, 3.0, 3.0, NOD]));
        let expected = R::new(META, create_vec(&[NOD, 5.0, 5.0, 6.0, NOD, 6.0, 4.0, 4.0, NOD]));

        {
            let result = &raster1 + &raster2;
            assert_eq!(result, expected);
        }

        {
            let result = raster1 + raster2;
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_multiply_nodata<T: RasterNum<T>, R>()
    where
        for<'a> &'a R: std::ops::Mul<&'a R, Output = R>,
        R: Raster<T> + std::ops::Mul<R, Output = R>,
    {
        let raster1 = R::new(META, create_vec(&[NOD, 2.0, 2.0, 3.0, NOD, 3.0, 1.0, 1.0, 0.0]));
        let raster2 = R::new(META, create_vec(&[1.0, 3.0, 3.0, 3.0, NOD, 3.0, 3.0, 3.0, NOD]));
        let expected = R::new(META, create_vec(&[NOD, 6.0, 6.0, 9.0, NOD, 9.0, 3.0, 3.0, NOD]));

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
    fn test_multiply_scalar<T: RasterNum<T>, R>()
    where
        for<'a> &'a R: std::ops::Mul<T, Output = R>,
        R: Raster<T> + std::ops::Mul<T, Output = R>,
    {
        let metadata = GeoReference::new(
            "EPSG:4326".to_string(),
            RasterSize { rows: 2, cols: 2 },
            [0.0, 0.0, 1.0, 1.0, 0.0, 0.0],
            Some(NOD),
        );

        let raster = R::new(metadata.clone(), create_vec(&[1.0, 2.0, NOD, 4.0]));
        let scalar: T = NumCast::from(2).unwrap();

        {
            let result = &raster * scalar;
            assert_eq!(to_f64(result.masked_data()), &[Some(2.0), Some(4.0), None, Some(8.0)]);
        }

        {
            let result = raster * scalar;
            assert_eq!(to_f64(result.masked_data()), &[Some(2.0), Some(4.0), None, Some(8.0)]);
        }
    }

    #[test]
    fn test_sum<T: RasterNum<T>, R: Raster<T>>() {
        let metadata = GeoReference::new(
            "EPSG:4326".to_string(),
            RasterSize { rows: 2, cols: 2 },
            [0.0, 0.0, 1.0, 1.0, 0.0, 0.0],
            Some(NOD),
        );

        let ras = R::new(metadata.clone(), create_vec(&[1.0, 2.0, NOD, 4.0]));
        assert_eq!(ras.sum(), 7.0);
    }

    #[instantiate_tests(<u8, DenseRaster<u8>>)]
    mod denserasteru8 {}

    #[instantiate_tests(<i32, DenseRaster<i32>>)]
    mod denserasteri32 {}

    #[instantiate_tests(<u32, DenseRaster<u32>>)]
    mod denserasteru32 {}

    #[instantiate_tests(<f32, DenseRaster<f32>>)]
    mod denserasterf32 {}

    #[instantiate_tests(<f64, DenseRaster<f64>>)]
    mod denseraster64 {}

    #[cfg(feature = "arrow")]
    #[instantiate_tests(<u8, ArrowRaster<u8>>)]
    mod arrowrasteru8 {}

    #[cfg(feature = "arrow")]
    #[instantiate_tests(<i32, ArrowRaster<i32>>)]
    mod arrowrasteri32 {}

    #[cfg(feature = "arrow")]
    #[instantiate_tests(<u32, ArrowRaster<u32>>)]
    mod arrowrasteru32 {}

    #[cfg(feature = "arrow")]
    #[instantiate_tests(<f32, ArrowRaster<f32>>)]
    mod arrowrasterf32 {}

    #[cfg(feature = "arrow")]
    #[instantiate_tests(<f64, ArrowRaster<f64>>)]
    mod arrowraster64 {}
}

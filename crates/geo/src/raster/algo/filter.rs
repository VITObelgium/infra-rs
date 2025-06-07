use crate::{Array, ArrayNum};

pub fn filter_value<R, T>(ras: &mut R, value: T)
where
    R: Array<Pixel = T>,
    T: ArrayNum,
{
    for v in ras.iter_mut() {
        if *v != value {
            *v = T::NODATA;
        }
    }
}

pub fn filter<R, T>(ras: &mut R, values_to_include: &[T])
where
    R: Array<Pixel = T>,
    T: ArrayNum,
{
    if values_to_include.len() == 1 {
        return filter_value(ras, values_to_include[0]);
    }

    for v in ras.iter_mut() {
        if !values_to_include.contains(v) {
            *v = T::NODATA;
        }
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
    fn test_filter_empty<R>() -> Result<()>
    where
        R: Array<Metadata = GeoReference>,
    {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(0), Columns(0)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        let mut raster = R::WithPixelType::<f64>::new(meta.clone(), allocate::new_aligned_vec())?;
        filter(&mut raster, &[1.0, 2.0]);
        Ok(())
    }

    #[test]
    fn test_filter_single_element<R>() -> Result<()>
    where
        R: Array<Pixel = u8, Metadata = GeoReference>,
        R::WithPixelType<f64>: ArrayInterop,
    {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(1), Columns(1)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        let mut raster = R::WithPixelType::<f64>::new_init_nodata(meta.clone(), allocate::aligned_vec_filled_with(5.0, 1))?;

        filter(&mut raster, &[5.0]);
        assert_eq!(raster.value(0), Some(5.0));

        filter(&mut raster, &[1.0]);
        assert_eq!(raster.value(0), None);

        Ok(())
    }

    #[test]
    fn test_filter_multiple_elements<R>() -> Result<()>
    where
        R: Array<Metadata = GeoReference>,
        R::WithPixelType<f64>: ArrayInterop,
    {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(3), Columns(3)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let mut raster = R::WithPixelType::<f64>::new_init_nodata(
            meta.clone(),
            allocate::aligned_vec_from_slice(&[
                1.0, 2.0, 2.0,
                3.0, 4.0, 5.0,
                1.0, 2.0, 9.0,
            ]),
        )?;

        filter(&mut raster, &[5.0]);

        #[rustfmt::skip]
        let expected = R::WithPixelType::<f64>::new_init_nodata(
            meta.clone(),
            allocate::aligned_vec_from_slice(&[
                NOD, NOD, NOD,
                NOD, NOD, 5.0,
                NOD, NOD, NOD,
            ]),
        )?;

        assert_eq!(expected, raster);

        Ok(())
    }

    #[test]
    fn test_min_max_multiple_elements_nodata<R>() -> Result<()>
    where
        R: Array<Metadata = GeoReference>,
        R::WithPixelType<f64>: ArrayInterop,
    {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(3), Columns(3)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let mut raster = R::WithPixelType::<f64>::new_init_nodata(
            meta.clone(),
            allocate::aligned_vec_from_slice(&[
                NOD, 4.0, -10.0,
                3.0, NOD, 0.0,
                1.0, 21.0, NOD,
            ]),
        )?;

        #[rustfmt::skip]
        let expected = R::WithPixelType::<f64>::new_init_nodata(
            meta,
            allocate::aligned_vec_from_slice(&[
                NOD, NOD, -10.0,
                NOD, NOD,   NOD,
                NOD, 21.0,  NOD,
            ]),
        )?;

        filter(&mut raster, &[-10.0, 21.0, 2.0]);
        assert_eq!(raster, expected);

        Ok(())
    }

    #[instantiate_tests(<DenseRaster<u8>>)]
    mod denseraster {}
}

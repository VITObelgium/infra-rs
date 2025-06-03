#[cfg(test)]
#[generic_tests::define]
mod generictests {

    use core::fmt;

    use num::NumCast;
    use tempdir::TempDir;

    use crate::{
        Array, ArrayNum, Cell, GeoReference, Point, Result,
        array::{Columns, Rows},
        raster::{DenseRaster, RasterIO},
        testutils::{NOD, workspace_test_data_dir},
    };

    #[test]
    fn test_read_dense_raster<T: ArrayNum + fmt::Debug, R: Array<Pixel = T, Metadata = GeoReference> + RasterIO>() -> Result<()> {
        let path = workspace_test_data_dir().join("landusebyte.tif");

        let ras = R::read(path.as_path())?;
        let meta = ras.metadata();

        assert_eq!(ras.columns(), Columns(2370));
        assert_eq!(ras.rows(), Rows(920));
        assert_eq!(ras.as_slice().len(), 2370 * 920);
        assert_eq!(
            ras.metadata().nodata(),
            Some(NumCast::from(NOD).expect("Failed to convert NOD to T"))
        );
        assert_eq!(ras.sum(), 163654749.0);
        assert_eq!(ras.nodata_count(), 805630);

        assert_eq!(meta.cell_size_x(), 100.0);
        assert_eq!(meta.cell_size_y(), -100.0);
        assert_eq!(meta.bottom_left(), Point::new(22000.0, 153000.0));

        Ok(())
    }

    fn verify_raster_meta(meta: &GeoReference) {
        assert_eq!(meta.columns().count(), 5);
        assert_eq!(meta.rows().count(), 4);
        assert_eq!(meta.cell_size_x(), 2.0);
        assert_eq!(meta.projected_epsg(), None);
        assert_eq!(meta.nodata(), Some(99.0));
    }

    #[test]
    fn read_write_raster_nodata_handling<T: ArrayNum + fmt::Debug, R: Array<Pixel = T, Metadata = GeoReference> + RasterIO>() -> Result<()>
    {
        let tmp_dir = TempDir::new("asc_write")?;
        let raster_path = tmp_dir.path().join("test.asc");

        std::fs::write(
            &raster_path,
            "NCOLS 5\n\
             NROWS 4\n\
             XLLCORNER 0.000000\n\
             YLLCORNER -10.000000\n\
             CELLSIZE 2.000000\n\
             NODATA_VALUE 99\n\
             0  1  2  3  4\n\
             5  6  7  8  9\n\
             0  0  0  0  0\n\
             0  0 99 99  0\n",
        )?;

        let mut raster = R::read(&raster_path)?;
        verify_raster_meta(raster.metadata());
        assert_eq!(raster.cell_value(Cell::from_row_col(0, 0)), Some(NumCast::from(0.0).unwrap()));
        assert_eq!(raster.cell_value(Cell::from_row_col(1, 4)), Some(NumCast::from(9.0).unwrap()));
        assert_eq!(raster.cell_value(Cell::from_row_col(3, 2)), None);
        assert_eq!(raster.cell_value(Cell::from_row_col(3, 3)), None);

        raster.write(&raster_path)?;

        // Read the raster again after writing, to make sure the nodata value is preserved
        let raster = R::read(&raster_path)?;
        verify_raster_meta(raster.metadata());
        assert_eq!(raster.cell_value(Cell::from_row_col(0, 0)), Some(NumCast::from(0.0).unwrap()));
        assert_eq!(raster.cell_value(Cell::from_row_col(1, 4)), Some(NumCast::from(9.0).unwrap()));
        assert_eq!(raster.cell_value(Cell::from_row_col(3, 2)), None);
        assert_eq!(raster.cell_value(Cell::from_row_col(3, 3)), None);

        Ok(())
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
}

#[cfg(test)]
mod tests {
    use inf::allocate;
    use tempdir::TempDir;

    use crate::{
        Array, ArrayInterop as _, Cell, Columns, DenseArray, GeoReference, Nodata, RasterSize, Result, Rows,
        raster::{DenseRaster, RasterIO},
    };

    #[test]
    fn write_raster() {
        let mut raster = DenseArray::<f32>::zeros(RasterSize::with_rows_cols(Rows(5), Columns(10)));
        let tmp_dir = TempDir::new("ras_write").unwrap();

        let raster_path = tmp_dir.path().join("test.asc");

        // Write the raster
        assert!(!raster_path.exists());
        raster.write(&raster_path).unwrap();
        assert!(raster_path.exists());

        // Make sure the file can be removed and is no longer locked by the dataset
        // This happened due to a bug not closing the dataset after writing
        std::fs::remove_file(&raster_path).unwrap();
    }

    #[test]
    fn test_read_write_nodata_handling_nan() -> Result<()> {
        let tmp_dir = TempDir::new("asc_write")?;
        let raster_path = tmp_dir.path().join("test.asc");

        std::fs::write(
            &raster_path,
            "NCOLS 5\n\
             NROWS 4\n\
             XLLCORNER 0.000000\n\
             YLLCORNER -10.000000\n\
             CELLSIZE 2.000000\n\
             NODATA_VALUE NaN\n\
             0.0  1  2  3  4\n\
             5  6  7  8  9\n\
             0  0  0  0  0\n\
             0  0 NaN NaN  0\n",
        )?;

        #[rustfmt::skip]
        let expected = DenseRaster::<f32>::new_init_nodata(
            GeoReference::without_spatial_reference(RasterSize::with_rows_cols(Rows(4), Columns(5)), Some(f64::NAN)),
            allocate::aligned_vec_from_slice(&[
                0.0, 1.0, 2.0, 3.0, 4.0,
                5.0, 6.0, 7.0, 8.0, 9.0,
                0.0, 0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, f32::NAN, f32::NAN, 0.0,
            ]),
        )?;

        let mut raster = DenseRaster::<f32>::read(&raster_path)?;
        assert!(raster.metadata().nodata().unwrap().is_nan());
        assert_eq!(raster, expected);

        let tif_raster = raster_path.with_extension("tif");
        raster.write(&tif_raster)?;

        let raster_read_back = DenseRaster::<f32>::read(&raster_path)?;
        assert_eq!(raster, raster_read_back);

        Ok(())
    }

    #[test]
    fn test_read_write_nodata_handling() -> Result<()> {
        let tmp_dir = TempDir::new("asc_write")?;
        let raster_path = tmp_dir.path().join("test.asc");

        std::fs::write(
            &raster_path,
            "NCOLS 5\n\
             NROWS 4\n\
             XLLCORNER 0.000000\n\
             YLLCORNER -10.000000\n\
             CELLSIZE 2.000000\n\
             NODATA_VALUE 999\n\
             999.0  1  2  3  4\n\
             5  6  7  8  9\n\
             0  0  0  0  0\n\
             0  0 999 999  0\n",
        )?;

        #[rustfmt::skip]
        let expected = DenseRaster::<f32>::new_init_nodata(
            GeoReference::without_spatial_reference(RasterSize::with_rows_cols(Rows(4), Columns(5)), Some(f64::NAN)),
            allocate::aligned_vec_from_slice(&[
                f32::NAN, 1.0, 2.0, 3.0, 4.0,
                5.0, 6.0, 7.0, 8.0, 9.0,
                0.0, 0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, f32::NAN, f32::NAN, 0.0,
            ]),
        )?;

        let first_cell = Cell::from_row_col(0, 0);

        let mut raster = DenseRaster::<f32>::read(&raster_path)?;
        assert_eq!(raster.metadata().nodata(), Some(999.0));
        assert_eq!(raster, expected);
        assert!(raster[first_cell].is_nodata());
        assert!(raster[first_cell].is_nan());

        let tif_raster = raster_path.with_extension("tif");
        raster.write(&tif_raster)?;
        assert!(raster[first_cell].is_nodata());
        assert!(raster[first_cell].is_nan());
        assert_eq!(raster, expected);

        // Use the low level GDAL API to read the raster back to actually verify the written nodata value
        let ds = gdal::Dataset::open(&tif_raster)?;
        let (cols, rows) = ds.raster_size();
        let buffer = ds.rasterband(1)?.read_band_as::<f32>()?;
        assert_eq!(buffer.len(), rows * cols);
        assert_eq!(buffer.data()[0], 999.0);

        let raster_read_back = DenseRaster::<f32>::read(&tif_raster)?;
        assert_eq!(raster_read_back.metadata().nodata(), Some(999.0));
        assert_eq!(raster, raster_read_back);
        assert!(raster_read_back[first_cell].is_nodata());
        assert!(raster_read_back[first_cell].is_nan());

        Ok(())
    }
}

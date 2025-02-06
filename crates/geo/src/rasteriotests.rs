#[cfg(test)]
#[generic_tests::define]
mod generictests {

    use core::fmt;

    use num::NumCast;
    use path_macro::path;
    use tempdir::TempDir;

    use crate::{
        array::{Columns, Rows},
        gdalinterop,
        raster::{DenseRaster, RasterIO},
        testutils::{workspace_test_data_dir, NOD},
        Array, ArrayNum, Cell, GeoReference, Point,
    };

    #[ctor::ctor]
    fn init() {
        let data_dir = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "target" / "data");

        let gdal_config = gdalinterop::Config {
            debug_logging: false,
            proj_db_search_location: data_dir,
            config_options: Vec::default(),
        };

        gdal_config.apply().expect("Failed to configure GDAL");
    }

    #[test]
    fn test_read_dense_raster<T: ArrayNum<T> + fmt::Debug, R: Array<Pixel = T, Metadata = GeoReference> + RasterIO>() {
        let path = workspace_test_data_dir().join("landusebyte.tif");

        let ras = R::read(path.as_path()).unwrap();
        let meta = ras.metadata();

        assert_eq!(ras.columns(), Columns(2370));
        assert_eq!(ras.rows(), Rows(920));
        assert_eq!(ras.as_slice().len(), 2370 * 920);
        assert_eq!(ras.metadata().nodata(), Some(NumCast::from(NOD).unwrap()));
        assert_eq!(ras.sum(), 163654749.0);
        assert_eq!(ras.nodata_count(), 805630);

        assert_eq!(meta.cell_size_x(), 100.0);
        assert_eq!(meta.cell_size_y(), -100.0);
        assert_eq!(meta.bottom_left(), Point::new(22000.0, 153000.0));
    }

    fn verify_raster_meta(meta: &GeoReference) {
        assert_eq!(meta.columns().count(), 5);
        assert_eq!(meta.rows().count(), 4);
        assert_eq!(meta.cell_size_x(), 2.0);
        assert_eq!(meta.projected_epsg(), None);
        assert_eq!(meta.nodata(), Some(99.0));
    }

    #[test]
    fn read_write_raster_nodata_handling<T: ArrayNum<T> + fmt::Debug, R: Array<Pixel = T, Metadata = GeoReference> + RasterIO>() {
        let tmp_dir = TempDir::new("asc_write").unwrap();
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
        )
        .unwrap();

        let mut raster = R::read(&raster_path).unwrap();
        verify_raster_meta(raster.metadata());
        assert_eq!(raster.cell_value(Cell::from_row_col(0, 0)), Some(NumCast::from(0.0).unwrap()));
        assert_eq!(raster.cell_value(Cell::from_row_col(1, 4)), Some(NumCast::from(9.0).unwrap()));
        assert_eq!(raster.cell_value(Cell::from_row_col(3, 2)), None);
        assert_eq!(raster.cell_value(Cell::from_row_col(3, 3)), None);

        raster.write(&raster_path).unwrap();

        // Read the raster again after writing, to make sure the nodata value is preserved
        let raster = R::read(&raster_path).unwrap();
        verify_raster_meta(raster.metadata());
        assert_eq!(raster.cell_value(Cell::from_row_col(0, 0)), Some(NumCast::from(0.0).unwrap()));
        assert_eq!(raster.cell_value(Cell::from_row_col(1, 4)), Some(NumCast::from(9.0).unwrap()));
        assert_eq!(raster.cell_value(Cell::from_row_col(3, 2)), None);
        assert_eq!(raster.cell_value(Cell::from_row_col(3, 3)), None);
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
    use tempdir::TempDir;

    use crate::{raster::RasterIO, Array, Columns, DenseArray, RasterSize, Rows};

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
}

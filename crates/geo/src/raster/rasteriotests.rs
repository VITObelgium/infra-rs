#[cfg(test)]
#[generic_tests::define]
mod tests {

    use core::fmt;

    use num::NumCast;
    use path_macro::path;

    use crate::{
        gdalinterop,
        raster::{DenseRaster, RasterIO},
        testutils::NOD,
        Array, GeoReference, Point, ArrayNum,
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
        let path = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data" / "landusebyte.tif");

        let ras = R::read(path.as_path()).unwrap();
        let meta = ras.metadata();

        assert_eq!(ras.width(), 2370);
        assert_eq!(ras.height(), 920);
        assert_eq!(ras.as_slice().len(), 2370 * 920);
        assert_eq!(ras.metadata().nodata(), Some(NumCast::from(NOD).unwrap()));
        assert_eq!(ras.sum(), 163654749.0);
        assert_eq!(ras.nodata_count(), 805630);

        assert_eq!(meta.cell_size_x(), 100.0);
        assert_eq!(meta.cell_size_y(), -100.0);
        assert_eq!(meta.bottom_left(), Point::new(22000.0, 153000.0));
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

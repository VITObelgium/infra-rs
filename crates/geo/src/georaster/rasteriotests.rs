#[cfg(test)]
#[generic_tests::define]
mod tests {

    use core::fmt;

    use num::NumCast;

    use crate::{
        gdalinterop,
        georaster::{testutils::NOD, DenseGeoRaster, GeoRaster, RasterIO, RasterNum},
        Point,
    };

    #[cfg(feature = "arrow")]
    use crate::georaster::ArrowRaster;

    #[ctor::ctor]
    fn init() {
        let data_dir = [env!("CARGO_MANIFEST_DIR"), "..", "..", "target", "data"]
            .iter()
            .collect();

        let gdal_config = gdalinterop::Config {
            debug_logging: false,
            proj_db_search_location: data_dir,
            config_options: Vec::default(),
        };

        gdal_config.apply().expect("Failed to configure GDAL");
    }

    #[test]
    fn test_read_dense_raster<T: RasterNum<T> + fmt::Debug, R: GeoRaster<T> + RasterIO<T, R>>() {
        let path: std::path::PathBuf = [env!("CARGO_MANIFEST_DIR"), "test", "data", "landusebyte.tif"]
            .iter()
            .collect();

        let ras = R::read(path.as_path()).unwrap();
        let meta = ras.geo_reference();

        assert_eq!(ras.width(), 2370);
        assert_eq!(ras.height(), 920);
        assert_eq!(ras.as_slice().len(), 2370 * 920);
        assert_eq!(ras.nodata_value(), Some(NumCast::from(NOD).unwrap()));
        assert_eq!(ras.sum(), 163654749.0);
        assert_eq!(ras.nodata_count(), 805630);

        assert_eq!(meta.cell_size_x(), 100.0);
        assert_eq!(meta.cell_size_y(), -100.0);
        assert_eq!(meta.bottom_left(), Point::new(22000.0, 153000.0));
    }

    #[instantiate_tests(<u8, DenseGeoRaster<u8>>)]
    mod denserasteru8 {}

    #[instantiate_tests(<i32, DenseGeoRaster<i32>>)]
    mod denserasteri32 {}

    #[instantiate_tests(<u32, DenseGeoRaster<u32>>)]
    mod denserasteru32 {}

    #[instantiate_tests(<f32, DenseGeoRaster<f32>>)]
    mod denserasterf32 {}

    #[instantiate_tests(<f64, DenseGeoRaster<f64>>)]
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

#[cfg(feature = "derive")]
mod derive {

    use inf::{crs::Epsg, gdalinterop, spatialreference::SpatialReference, CellSize, GeoMetadata, RasterSize};
    use path_macro::path;
    use vector::{
        io::DataframeIterator,
        polygoncoverage::{BurnValue, CoverageConfiguration},
        DataRow,
    };

    #[derive(vector_derive::DataRow)]
    struct PollutantData {
        #[vector(column = "Pollutant")]
        pollutant: String,
        #[vector(column = "Sector")]
        sector: String,
        value: f64,
        #[vector(skip)]
        not_in_csv: String,
    }

    #[derive(vector_derive::DataRow)]
    struct PollutantOptionalData {
        #[vector(column = "Pollutant")]
        pollutant: String,
        #[vector(column = "Sector")]
        sector: String,
        value: Option<f64>,
    }

    #[ctor::ctor]
    fn init() {
        let data_dir = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "target" / "data");
        if !data_dir.exists() {
            panic!("Proj.db data directory not found");
        }

        let gdal_config = gdalinterop::Config {
            debug_logging: true,
            proj_db_search_location: data_dir,
        };

        gdal_config.apply().expect("Failed to configure GDAL");
    }

    #[test]
    fn test_row_data_derive() {
        let path = path!(env!("CARGO_MANIFEST_DIR") / "test" / "data" / "road.csv");
        let mut iter = DataframeIterator::<PollutantData>::new(&path, None).unwrap();

        {
            let row = iter.next().unwrap().unwrap();
            assert_eq!(row.pollutant, "NO2");
            assert_eq!(row.sector, "A_PublicTransport");
            assert_eq!(row.value, 10.0);
            assert_eq!(row.not_in_csv, String::default());
        }

        {
            let row = iter.next().unwrap().unwrap();
            assert_eq!(row.pollutant, "NO2");
            assert_eq!(row.sector, "B_RoadTransport");
            assert_eq!(row.value, 11.5);
            assert_eq!(row.not_in_csv, String::default());
        }

        {
            let row = iter.next().unwrap().unwrap();
            assert_eq!(row.pollutant, "PM10");
            assert_eq!(row.sector, "B_RoadTransport");
            assert_eq!(row.value, 13.0);
            assert_eq!(row.not_in_csv, String::default());
        }

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_row_data_derive_missing() {
        let path = path!(env!("CARGO_MANIFEST_DIR") / "test" / "data" / "road_missing_data.csv");
        let mut iter = DataframeIterator::<PollutantData>::new(&path, None).unwrap();
        assert!(iter.nth(1).unwrap().is_err()); // The second line is incomplete (missing value)
        assert!(iter.next().unwrap().is_ok());
        assert!(iter.next().unwrap().is_ok());
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_row_data_derive_missing_optionals() {
        let path = path!(env!("CARGO_MANIFEST_DIR") / "test" / "data" / "road_missing_data.csv");
        let mut iter = DataframeIterator::<PollutantOptionalData>::new(&path, None).unwrap();

        {
            let row = iter.next().unwrap().unwrap();
            assert_eq!(row.pollutant, "NO2");
            assert_eq!(row.sector, "A_PublicTransport");
            assert_eq!(row.value, Some(10.0));
        }

        {
            let row = iter.next().unwrap().unwrap();
            assert_eq!(row.pollutant, "PM10");
            assert_eq!(row.sector, "A_PublicTransport");
            assert_eq!(row.value, None);
        }
    }

    #[test]
    fn test_iterate_features() {
        assert_eq!(PollutantData::field_names(), vec!["Pollutant", "Sector", "value"]);
    }

    #[test_log::test]
    fn test_polygon_coverage() {
        let path = path!(env!("CARGO_MANIFEST_DIR") / "test" / "data" / "boundaries.gpkg");

        let config = CoverageConfiguration {
            name_field: Some("Code3".to_string()),
            burn_value: BurnValue::Value(4.0),
            ..Default::default()
        };

        assert!(gdal::DriverManager::get_driver_by_name("GPKG").is_ok());

        //{GridDefinition::Vlops5km, "Vlops 5km", GeoMetadata(120, 144, -219000, -100000, {5000.0, -5000.0}, nan, s_belgianLambert72)},
        //{GridDefinition::Vlops1km, "Vlops 1km", GeoMetadata(120, 260, 11000.0, 140000.0, {1000.0, -1000.0}, nan, s_belgianLambert72)},

        let ds = vector::io::open_read_only(&path).unwrap();
        let output_extent = GeoMetadata::with_origin(
            SpatialReference::from_epsg(Epsg::from(31370))
                .unwrap()
                .to_wkt()
                .unwrap(),
            RasterSize { rows: 120, cols: 260 },
            (11000.0, 140000.0).into(),
            CellSize::square(1000.0),
            None::<f64>,
        )
        .warped_to_epsg(Epsg::from(4326))
        .unwrap();
        log::debug!("Output extent: {:?}", output_extent.projection());
        let coverages = vector::polygoncoverage::create_polygon_coverages(&ds, &output_extent, config).unwrap();

        assert_eq!(coverages.len(), 3); // 3 polygons in the dataset
    }
}

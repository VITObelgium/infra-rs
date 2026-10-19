use geo::{
    ArrayNum, Columns, Error, GeoReference, RasterSize, Rows,
    raster::{
        WriteRasterOptions,
        io::{write_raster_band, write_raster_bands},
    },
};

fn georeference(rows: i32, cols: i32) -> GeoReference {
    GeoReference::new(
        "",
        RasterSize::with_rows_cols(Rows(rows), Columns(cols)),
        [0.0, 10.0, 0.0, 0.0, 0.0, -10.0].into(),
        None,
        None,
    )
}

fn assert_rejected<T: ArrayNum>(bands: &[(&GeoReference, &[T])], message: &str) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("invalid.tif");
    let error = write_raster_bands(&path, bands, WriteRasterOptions::Default).unwrap_err();
    assert!(matches!(&error, Error::InvalidArgument(_)), "{error}");
    assert!(error.to_string().contains(message), "{error}");
    assert!(!path.exists(), "Validation must finish before creating output");
}

#[test]
fn rejects_empty_bands() {
    assert_rejected::<f32>(&[], "At least one raster band");
}

#[test]
fn rejects_nonpositive_dimensions() {
    for (rows, cols) in [(0, 2), (2, 0), (-1, 2), (2, -1)] {
        assert_rejected::<f32>(&[(&georeference(rows, cols), &[])], "dimensions must be positive");
    }
}

#[test]
fn rejects_mismatched_grids() {
    let first = georeference(2, 2);
    let mut projection = first.clone();
    projection.set_projection("EPSG:3857".into());
    let transform = GeoReference::new("", first.raster_size(), [1.0, 10.0, 0.0, 0.0, 0.0, -10.0].into(), None, None);
    for second in [georeference(1, 4), projection, transform] {
        assert_rejected(
            &[(&first, &[0.0_f32; 4]), (&second, &[1.0; 4])],
            "Band 2: raster size, projection and geotransform",
        );
    }
}

#[test]
fn rejects_incorrect_buffer_lengths_in_any_band() {
    let meta = georeference(2, 2);
    for invalid in [&[1.0_f32; 3][..], &[1.0_f32; 5][..]] {
        assert_rejected(&[(&meta, invalid)], "Band 1: expected 4 samples");
        assert_rejected(&[(&meta, &[0.0; 4]), (&meta, invalid)], "Band 2: expected 4 samples");
    }
}

#[test]
fn rejects_negative_nodata_for_unsigned_bands() {
    fn check<T: ArrayNum>(data: &[T]) {
        let first = georeference(2, 2);
        let second = first.copy_with_nodata(Some(-1.0));
        assert_rejected(&[(&first, data), (&second, data)], "Band 2: cannot store negative nodata");
    }

    check(&[0_u8; 4]);
    check(&[0_u16; 4]);
    check(&[0_u32; 4]);
    check(&[0_u64; 4]);
}

#[test]
fn single_band_writer_rejects_negative_nodata_for_unsigned_types() {
    fn check<T: ArrayNum>(data: &[T]) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("invalid.tif");
        let meta = georeference(2, 2).copy_with_nodata(Some(-1.0));
        let error = write_raster_band(&path, &meta, data, WriteRasterOptions::Default).unwrap_err();
        assert!(matches!(&error, Error::InvalidArgument(_)), "{error}");
        assert!(error.to_string().contains("cannot store negative nodata"), "{error}");
        assert!(!path.exists(), "Validation must finish before creating output");
    }

    check(&[0_u8; 4]);
    check(&[0_u16; 4]);
    check(&[0_u32; 4]);
    check(&[0_u64; 4]);
}

#[cfg(not(feature = "gdal"))]
#[test]
fn multiband_writing_requires_gdal() {
    let meta = georeference(2, 2);
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unsupported.tif");
    let error = write_raster_bands(&path, &[(&meta, &[0.0_f32; 4]), (&meta, &[1.0; 4])], WriteRasterOptions::Default).unwrap_err();
    assert!(matches!(&error, Error::Runtime(_)), "{error}");
    assert!(error.to_string().contains("requires the gdal feature"), "{error}");
    assert!(!path.exists());
}

#[cfg(feature = "gdal")]
mod round_trip {
    use super::*;

    use geo::{
        RasterScale, Result,
        crs::epsg::WGS84_WEB_MERCATOR,
        raster::{GeoTiffWriteOptions, NetcdfWriteOptions, formats::FormatProvider, io::RasterIO},
    };

    fn check_round_trip<T: ArrayNum + From<f32>>() -> Result<()> {
        let directory = tempfile::tempdir()?;
        for nodata in [None, Some(-9999.0), Some(f64::NAN)] {
            let mut first = georeference(2, 2).copy_with_nodata(nodata);
            first.set_projection_from_epsg(WGS84_WEB_MERCATOR)?;
            first.set_scale(Some(RasterScale { scale: 0.5, offset: -7.0 }));
            let mut second = first.clone();
            second.set_scale(Some(RasterScale {
                scale: -0.25,
                offset: 100.0,
            }));
            let missing = nodata.unwrap_or(42.0) as f32;
            let first_data = [-20.0, missing, 4.0, 8.0].map(<T as From<f32>>::from);
            let second_data = [100.0, 108.0, missing, 124.0].map(<T as From<f32>>::from);
            let bands = [(&first, first_data.as_slice()), (&second, second_data.as_slice())];

            for band_count in [1, 2] {
                for options in [
                    WriteRasterOptions::Default,
                    WriteRasterOptions::GeoTiff(GeoTiffWriteOptions::default()),
                ] {
                    let path = directory.path().join("round-trip.tif");
                    write_raster_bands(&path, &bands[..band_count], options)?;
                    let mut reader = RasterIO::open_read_only_force_format(&path, FormatProvider::Gdal)?;
                    assert_eq!(reader.band_count()?, band_count);
                    for (index, &(expected_meta, expected_data)) in bands[..band_count].iter().enumerate() {
                        assert_eq!(reader.data_type(index + 1)?, T::TYPE);
                        let (meta, data) = reader.read_raster_band::<T>(index + 1)?;
                        assert_eq!(meta.raster_size(), expected_meta.raster_size());
                        assert_eq!(meta.geo_transform(), expected_meta.geo_transform());
                        assert_eq!(meta.projected_epsg(), Some(WGS84_WEB_MERCATOR));
                        assert_eq!(meta.scale(), expected_meta.scale());
                        match nodata {
                            Some(value) if value.is_nan() => assert!(meta.nodata().is_some_and(f64::is_nan)),
                            _ => assert_eq!(meta.nodata(), nodata),
                        }
                        for (&actual, &expected) in data.iter().zip(expected_data) {
                            assert!(
                                actual == expected || (actual.is_nan() && expected.is_nan()),
                                "{actual:?} != {expected:?}"
                            );
                        }
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn netcdf_preserves_raw_rows_and_per_band_metadata() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("round-trip.nc");
        let mut first = georeference(4, 2).copy_with_nodata(Some(-9999.0));
        first.set_projection_from_epsg(WGS84_WEB_MERCATOR)?;
        first.set_scale(Some(RasterScale { scale: 0.5, offset: -7.0 }));
        let mut second = first.copy_with_nodata(Some(f64::NAN));
        second.set_scale(Some(RasterScale {
            scale: -0.25,
            offset: 100.0,
        }));
        let first_data = [-20.0_f32, -9999.0, 4.0, 8.0, 12.0, 16.0, 20.0, 24.0];
        let second_data = [100.0_f32, 108.0, 116.0, 124.0, f32::NAN, 140.0, 148.0, 156.0];
        let bands = [(&first, first_data.as_slice()), (&second, second_data.as_slice())];

        for write_bottom_up in [false, true] {
            write_raster_bands(&path, &bands, WriteRasterOptions::NetCdf(NetcdfWriteOptions { write_bottom_up }))?;

            let dataset = gdal::Dataset::open_ex(
                &path,
                gdal::DatasetOptions {
                    open_flags: gdal::GdalOpenFlags::GDAL_OF_MULTIDIM_RASTER,
                    allowed_drivers: Some(&["netCDF"]),
                    ..Default::default()
                },
            )?;
            let root_group = dataset.root_group()?;

            for (index, &(expected_meta, expected_data)) in bands.iter().enumerate() {
                let variable = format!("NETCDF:\"{}\":Band{}", path.display(), index + 1);
                let mut reader = RasterIO::open_read_only(variable)?;
                assert_eq!(reader.band_count()?, 1);
                assert_eq!(reader.data_type(1)?, <f32 as ArrayNum>::TYPE);
                let meta = reader.georeference(1)?;
                assert_eq!(meta.raster_size(), expected_meta.raster_size());
                assert_eq!(meta.geo_transform(), expected_meta.geo_transform());
                assert_eq!(meta.projected_epsg(), Some(WGS84_WEB_MERCATOR));
                assert_eq!(meta.scale(), expected_meta.scale());
                if index == 0 {
                    assert_eq!(meta.nodata(), Some(-9999.0));
                } else {
                    assert!(meta.nodata().is_some_and(f64::is_nan));
                }
                // The classic reader's automatic row orientation varies across GDAL
                // versions and configuration. Inspect native storage to test WRITE_BOTTOMUP.
                let rows = expected_meta.rows().count() as usize;
                let cols = expected_meta.columns().count() as usize;
                let array = root_group.open_md_array(&format!("Band{}", index + 1), Default::default())?;
                let dimensions = array.dimensions()?;
                assert_eq!(dimensions.iter().map(|dim| dim.size()).collect::<Vec<_>>(), [rows, cols]);
                let data = array.read_as::<f32>(vec![0, 0], vec![rows, cols])?;
                assert_eq!(data.len(), expected_data.len());
                for (row, actual_row) in data.chunks_exact(cols).enumerate() {
                    let source_row = if write_bottom_up { rows - 1 - row } else { row };
                    let expected_row = &expected_data[source_row * cols..(source_row + 1) * cols];
                    for (&actual, &expected) in actual_row.iter().zip(expected_row) {
                        assert!(
                            actual == expected || (actual.is_nan() && expected.is_nan()),
                            "write_bottom_up={write_bottom_up}, band {}: {actual:?} != {expected:?}",
                            index + 1
                        );
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn float32_preserves_raw_samples_and_band_metadata() -> Result<()> {
        check_round_trip::<f32>()
    }

    #[test]
    fn float64_preserves_raw_samples_and_band_metadata() -> Result<()> {
        check_round_trip::<f64>()
    }
}

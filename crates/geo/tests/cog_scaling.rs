#![cfg(all(feature = "gdal", feature = "raster-io-geotiff"))]

use std::path::Path;

use approx::assert_relative_eq;
use geo::{
    Array, ArrayDataType, ArrayNum, Cell, CellSize, Columns, GeoReference, Point, RasterScale, RasterSize, Result, Rows, Tile,
    ZoomLevelStrategy,
    cog::{CogCreationOptions, create_cog_tiles, create_multiband_cog_tiles},
    geotiff::{FIRST_BAND, GeoTiffReader},
    raster::{DenseRaster, RasterReadWrite, algo::descale, formats},
};

const NODATA: f32 = -9999.0;

fn source_georeference() -> GeoReference {
    let pixel_size = Tile::pixel_size_at_zoom_level(8, 256);
    // Match the GoogleMapsCompatible grid to avoid resampling the source pixels.
    GeoReference::with_top_left_origin(
        "EPSG:3857",
        RasterSize::with_rows_cols(Rows(2), Columns(4)),
        Point::new(2.0 * pixel_size, -3.0 * pixel_size),
        CellSize::square(pixel_size),
        Some(f64::from(NODATA)),
    )
}

fn write_source(path: &Path, georef: &GeoReference, pixels: &[f32]) -> Result<()> {
    let dataset = formats::gdal::create_in_memory_dataset_with_data(georef, pixels)?;
    geo::raster::algo::gdal::translate(&dataset, path, &["-of".into(), "GTiff".into()])?;
    Ok(())
}

fn scaling_options(data_type: ArrayDataType) -> CogCreationOptions {
    CogCreationOptions {
        min_zoom: Some(8),
        zoom_level_strategy: ZoomLevelStrategy::Manual(8),
        tile_size: 256,
        output_data_type: Some(data_type),
        compression: None,
        predictor: None,
        scale: true,
        ..Default::default()
    }
}

fn output_cell(output: &GeoReference, source: &GeoReference, index: usize) -> Cell {
    output.point_to_cell(source.cell_center(source.cell_at_index(index)))
}

fn assert_round_trip<T: ArrayNum>(encoded: &DenseRaster<T>, source: &GeoReference, pixels: &[f32], tolerance: f64) {
    let decoded: DenseRaster<f64> = descale(encoded);
    assert!(decoded.metadata().scale().is_none());
    for (index, &expected) in pixels.iter().enumerate() {
        let cell = output_cell(decoded.metadata(), source, index);
        if expected.to_bits() == NODATA.to_bits() {
            assert_eq!(decoded.cell_value(cell), None, "Nodata at source pixel {index}");
        } else {
            let actual = decoded.cell_value(cell).expect("Valid source pixel must remain valid");
            assert_relative_eq!(actual, f64::from(expected), epsilon = tolerance);
        }
    }
}

#[test]
fn uint16_scaling_preserves_endpoints_nodata_and_descales() -> Result<()> {
    let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
    let input = tmp.path().join("input.tif");
    let output = tmp.path().join("scaled.tif");
    let georef = source_georeference();
    let pixels = [-20.0, 0.0, NODATA, 80.0, 30.0, 50.0, NODATA, 60.0];
    write_source(&input, &georef, &pixels)?;
    create_cog_tiles(&input, &output, scaling_options(ArrayDataType::Uint16))?;

    let dataset = gdal::Dataset::open(&output)?;
    let band = dataset.rasterband(1)?;
    assert_eq!(band.band_type(), gdal::raster::GdalDataType::UInt16);
    assert_eq!(band.no_data_value(), Some(f64::from(u16::MAX)));

    let encoded = DenseRaster::<u16>::read_band(&output, 1)?;
    assert_eq!(encoded.metadata().nodata(), Some(f64::from(u16::MAX)));
    let scale = encoded.metadata().scale().expect("Inverse scale/offset must be persisted");
    let step = 100.0 / 65534.0;
    assert_relative_eq!(scale.scale, step, epsilon = 1e-12);
    assert_relative_eq!(scale.offset, -20.0, epsilon = 1e-12);
    assert_eq!(encoded.cell_value(output_cell(encoded.metadata(), &georef, 0)), Some(0));
    assert_eq!(encoded.cell_value(output_cell(encoded.metadata(), &georef, 3)), Some(65534));
    for (index, &value) in pixels.iter().enumerate() {
        let cell = output_cell(encoded.metadata(), &georef, index);
        if value.to_bits() == NODATA.to_bits() {
            assert_eq!(encoded.cell_value(cell), None);
            assert_eq!(encoded[cell], u16::MAX);
        } else {
            assert!((0..=65534).contains(&encoded.cell_value(cell).expect("Valid encoded pixel")));
        }
    }
    assert_round_trip(&encoded, &georef, &pixels, step + 1e-9);
    Ok(())
}

#[test]
fn multiband_scaling_persists_independent_inverse_scales() -> Result<()> {
    let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
    let georef = source_georeference();
    let bands = [
        [-10.0, 0.0, NODATA, 10.0, -5.0, 5.0, NODATA, 2.0],
        [100.0, 200.0, NODATA, 500.0, 300.0, 400.0, NODATA, 250.0],
    ];
    for (index, pixels) in bands.iter().enumerate() {
        write_source(&tmp.path().join(format!("band_{index}.tif")), &georef, pixels)?;
    }
    let output = tmp.path().join("scaled.tif");
    let pattern = tmp.path().join("band_*.tif");
    create_multiband_cog_tiles(
        pattern.to_str().expect("Temporary path must be UTF-8"),
        &output,
        scaling_options(ArrayDataType::Uint8),
        None,
    )?;
    let dataset = gdal::Dataset::open(&output)?;
    assert_eq!(dataset.raster_count(), 2);

    for (index, (min, max)) in [(-10.0, 10.0), (100.0, 500.0)].into_iter().enumerate() {
        let band = dataset.rasterband(index + 1)?;
        assert_eq!(band.band_type(), gdal::raster::GdalDataType::UInt8);
        assert_eq!(band.no_data_value(), Some(f64::from(u8::MAX)));
        let step = (max - min) / 254.0;
        assert_relative_eq!(band.scale().expect("Per-band inverse scale"), step, epsilon = 1e-12);
        assert_relative_eq!(band.offset().expect("Per-band inverse offset"), min, epsilon = 1e-12);

        let encoded = DenseRaster::<u8>::read_band(&output, index + 1)?;
        let scale = encoded.metadata().scale().expect("DenseRaster must read per-band scale/offset");
        assert_relative_eq!(scale.scale, step, epsilon = 1e-12);
        assert_relative_eq!(scale.offset, min, epsilon = 1e-12);
        assert_eq!(encoded.cell_value(output_cell(encoded.metadata(), &georef, 0)), Some(0));
        assert_eq!(encoded.cell_value(output_cell(encoded.metadata(), &georef, 3)), Some(254));
        assert!(encoded.iter_opt().flatten().all(|value| (0..=254).contains(&value)));
        assert_round_trip(&encoded, &georef, &bands[index], step + 1e-9);
    }
    Ok(())
}

fn assert_constant_band<T: ArrayNum>(path: &Path, source: &GeoReference, pixels: &[f32], max_valid: f64) -> Result<()> {
    let encoded = DenseRaster::<T>::read_band(path, 1)?;
    let scale = encoded.metadata().scale().expect("Constant input needs inverse scale/offset");
    assert!(scale.scale.is_finite());
    assert!(scale.offset.is_finite());
    let decoded: DenseRaster<f64> = descale(&encoded);
    let mut constant_code = None;
    for (index, &value) in pixels.iter().enumerate() {
        let cell = output_cell(encoded.metadata(), source, index);
        if value.to_bits() == NODATA.to_bits() {
            assert_eq!(encoded.cell_value(cell), None);
            assert_eq!(decoded.cell_value(cell), None);
        } else {
            let code = encoded.cell_value(cell).expect("Constant pixel must remain valid");
            let code_f64 = num::NumCast::from(code).expect("Encoded pixel fits in f64");
            assert!((0.0..=max_valid).contains(&code_f64));
            assert_eq!(*constant_code.get_or_insert(code), code);
            assert_relative_eq!(
                decoded.cell_value(cell).expect("Decoded constant pixel"),
                f64::from(value),
                epsilon = 1e-9
            );
        }
    }
    Ok(())
}

#[test]
fn constant_input_round_trips_for_uint8_and_uint16() -> Result<()> {
    let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
    let input = tmp.path().join("input.tif");
    let georef = source_georeference();
    let pixels = [42.0, 42.0, NODATA, 42.0, 42.0, 42.0, NODATA, 42.0];
    write_source(&input, &georef, &pixels)?;

    let output_u8 = tmp.path().join("scaled_u8.tif");
    create_cog_tiles(&input, &output_u8, scaling_options(ArrayDataType::Uint8))?;
    assert_constant_band::<u8>(&output_u8, &georef, &pixels, 254.0)?;

    let output_u16 = tmp.path().join("scaled_u16.tif");
    create_cog_tiles(&input, &output_u16, scaling_options(ArrayDataType::Uint16))?;
    assert_constant_band::<u16>(&output_u16, &georef, &pixels, 65534.0)?;
    Ok(())
}

#[test]
fn scaling_preserves_cog_grid_and_overview_round_trips() -> Result<()> {
    let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
    let input = tmp.path().join("input.tif");
    let unscaled_path = tmp.path().join("unscaled.tif");
    let scaled_path = tmp.path().join("scaled.tif");
    let georef = source_georeference();
    let pixels = [10.0, 10.0, NODATA, NODATA, 30.0, 30.0, NODATA, NODATA];
    write_source(&input, &georef, &pixels)?;
    let options = CogCreationOptions {
        min_zoom: Some(7),
        ..scaling_options(ArrayDataType::Uint8)
    };
    create_cog_tiles(
        &input,
        &unscaled_path,
        CogCreationOptions {
            scale: false,
            output_data_type: Some(ArrayDataType::Float32),
            ..options
        },
    )?;
    create_cog_tiles(&input, &scaled_path, options)?;

    let mut unscaled = GeoTiffReader::from_file(&unscaled_path)?;
    let mut scaled = GeoTiffReader::from_file(&scaled_path)?;
    assert_relative_eq!(
        scaled.metadata().geo_reference.geo_transform(),
        unscaled.metadata().geo_reference.geo_transform(),
        epsilon = 1e-9
    );
    assert_eq!(
        scaled.metadata().geo_reference.raster_size(),
        unscaled.metadata().geo_reference.raster_size()
    );
    assert_relative_eq!(scaled.metadata().geo_reference.cell_size_x(), georef.cell_size_x(), epsilon = 1e-9);
    assert_relative_eq!(scaled.metadata().geo_reference.cell_size_y(), georef.cell_size_y(), epsilon = 1e-9);
    // Level 0 is full resolution; alignment to zoom 7 pads the tiny source to 512 pixels.
    assert_eq!(unscaled.metadata().overviews.len(), 2);
    assert_eq!(scaled.metadata().overviews.len(), unscaled.metadata().overviews.len());
    let stored_scale = scaled.metadata().geo_reference.scale().expect("Persisted inverse scale/offset");
    assert_relative_eq!(stored_scale.scale, 20.0 / 254.0, epsilon = 1e-12);
    assert_relative_eq!(stored_scale.offset, 10.0, epsilon = 1e-12);

    for (level, size) in [512, 256].into_iter().enumerate() {
        assert_eq!(unscaled.metadata().overviews[level].raster_size, RasterSize::square(size));
        assert_eq!(
            scaled.metadata().overviews[level].raster_size,
            unscaled.metadata().overviews[level].raster_size
        );
        let reference = unscaled.read_overview_band_as::<f32, GeoReference>(level, FIRST_BAND)?;
        let encoded = scaled.read_overview_band_as::<u8, GeoReference>(level, FIRST_BAND)?;
        assert_relative_eq!(
            encoded.metadata().geo_transform(),
            reference.metadata().geo_transform(),
            epsilon = 1e-9
        );
        assert_eq!(encoded.metadata().raster_size(), reference.metadata().raster_size());
        assert_eq!(encoded.metadata().nodata(), Some(f64::from(u8::MAX)));
        assert_eq!(encoded.metadata().scale(), Some(stored_scale));
        let decoded: DenseRaster<f64> = descale(&encoded);
        let mut valid_count = 0;
        let mut nodata_count = 0;
        for ((expected, actual), &code) in reference.iter_opt().zip(decoded.iter_opt()).zip(encoded.as_slice()) {
            if let Some(expected) = expected {
                valid_count += 1;
                assert!(
                    [10.0, 30.0].contains(&expected),
                    "Nearest-neighbor overview must retain source values"
                );
                assert_eq!(code, if expected == 10.0 { 0 } else { 254 });
                assert_relative_eq!(actual.expect("Valid overview pixel"), f64::from(expected), epsilon = 1e-9);
            } else {
                nodata_count += 1;
                assert_eq!(code, u8::MAX, "Nodata encoding at level {level}");
                assert_eq!(actual, None, "Decoded nodata at level {level}");
            }
        }
        assert!(valid_count > 0, "Level {level} must exercise valid pixels");
        assert!(nodata_count > 0, "Level {level} must exercise nodata pixels");
    }
    Ok(())
}

#[test]
fn scaling_composes_source_scale_and_offset() -> Result<()> {
    let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
    let input = tmp.path().join("input.tif");
    let output = tmp.path().join("scaled.tif");
    let mut georef = source_georeference();
    let source_scale = RasterScale { scale: 2.5, offset: 100.0 };
    georef.set_scale(Some(source_scale));
    let pixels = [-20.0, 0.0, NODATA, 80.0, 30.0, 50.0, NODATA, 60.0];
    write_source(&input, &georef, &pixels)?;
    assert_eq!(DenseRaster::<f32>::read(&input)?.metadata().scale(), Some(source_scale));
    create_cog_tiles(&input, &output, scaling_options(ArrayDataType::Uint8))?;

    let encoded = DenseRaster::<u8>::read(&output)?;
    let scale = encoded.metadata().scale().expect("Composed inverse scale/offset");
    let step = 100.0 / 254.0 * source_scale.scale;
    assert_relative_eq!(scale.scale, step, epsilon = 1e-12);
    assert_relative_eq!(scale.offset, -20.0 * source_scale.scale + source_scale.offset, epsilon = 1e-12);
    assert_eq!(encoded.cell_value(output_cell(encoded.metadata(), &georef, 0)), Some(0));
    assert_eq!(encoded.cell_value(output_cell(encoded.metadata(), &georef, 3)), Some(254));
    let physical_pixels: Vec<f32> = pixels
        .iter()
        .map(|&value| {
            if value.to_bits() == NODATA.to_bits() {
                NODATA
            } else {
                value * 2.5 + 100.0
            }
        })
        .collect();
    assert_round_trip(&encoded, &georef, &physical_pixels, step + 1e-9);
    Ok(())
}

use std::{
    path::Path,
    process::{Command, Output},
};

use geo::{
    Array, ArrayDataType, ArrayNum, Columns, GeoReference, RasterScale, RasterSize, Rows,
    crs::epsg::WGS84_WEB_MERCATOR,
    geotiff::{BandIndex, ChunkDataLayout, GeoTiffReader},
    raster::{
        NetcdfWriteOptions, WriteRasterOptions,
        io::{RasterIO, write_raster_bands},
    },
};
use inf::cast;

const SIZE: usize = 8;
const ZOOM: u8 = 8;
const TILE_SIZE: usize = 256;
const FINITE_NODATA: f64 = -9999.0;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, Debug)]
enum Samples {
    Varying,
    Constant,
    Empty,
    EmptyLastBand,
}

#[derive(Clone, Copy, Debug)]
struct Source {
    nodata: Option<f64>,
    bands: usize,
    samples: Samples,
}

impl Source {
    fn raw(self, band: usize, row: usize, column: usize) -> Option<f64> {
        // Uniform 2x2 blocks make the nearest-neighbor overview oracle independent
        // of which source pixel GDAL selects within each block.
        let block = (row / 2) * 4 + column / 2;
        if matches!(self.samples, Samples::Empty)
            || (matches!(self.samples, Samples::EmptyLastBand) && band == self.bands)
            || (self.nodata.is_some() && block == 4 + band)
        {
            return None;
        }
        if matches!(self.samples, Samples::Constant) {
            return Some(42.0 * band as f64);
        }
        Some(match band {
            1 => -20.0 + block as f64 * 4.0,
            _ => 100.0 + block as f64 * 8.0,
        })
    }

    fn packing(band: usize) -> (f64, f64) {
        match band {
            1 => (0.5, -7.0),
            // Exercise reversed physical extrema as well as independent band masks.
            _ => (-0.25, 100.0),
        }
    }

    fn physical(self, band: usize, row: usize, column: usize) -> Option<f64> {
        let (scale, offset) = Self::packing(band);
        self.raw(band, row, column).map(|raw| raw * scale + offset)
    }

    fn range(self, band: usize, unscale: bool) -> (f64, f64) {
        let values = (0..SIZE).flat_map(|row| {
            (0..SIZE).filter_map(move |column| {
                if unscale {
                    self.physical(band, row, column)
                } else {
                    self.raw(band, row, column)
                }
            })
        });
        values.fold((f64::INFINITY, f64::NEG_INFINITY), |(min, max), value| {
            (min.min(value), max.max(value))
        })
    }
}

fn resolution() -> f64 {
    2.0 * std::f64::consts::PI * 6378137.0 / (TILE_SIZE as f64 * 2.0_f64.powi(i32::from(ZOOM)))
}

fn write_source<T: ArrayNum>(path: &Path, source: Source) -> TestResult {
    let mut georeferences = Vec::with_capacity(source.bands);
    let mut buffers = Vec::with_capacity(source.bands);
    for index in 1..=source.bands {
        let (scale, offset) = Source::packing(index);
        let mut georef = GeoReference::new(
            "",
            RasterSize::with_rows_cols(Rows(SIZE as i32), Columns(SIZE as i32)),
            [0.0, resolution(), 0.0, 0.0, 0.0, -resolution()].into(),
            source.nodata,
            Some(RasterScale { scale, offset }),
        );
        georef.set_projection_from_epsg(WGS84_WEB_MERCATOR)?;
        georeferences.push(georef);
        let values = (0..SIZE)
            .flat_map(|row| (0..SIZE).map(move |column| source.raw(index, row, column).unwrap_or(source.nodata.unwrap_or(f64::NAN))))
            .collect::<Vec<_>>();
        buffers.push(cast::slice::<T>(&values)?);
    }
    let bands = georeferences.iter().zip(buffers.iter().map(Vec::as_slice)).collect::<Vec<_>>();
    let options = if path.extension().is_some_and(|extension| extension == "nc") {
        // NetCDF selection retains stored row order; keep it north-up so the
        // raster band-stacking pipeline can consume the resulting VRT.
        WriteRasterOptions::NetCdf(NetcdfWriteOptions { write_bottom_up: false })
    } else {
        WriteRasterOptions::Default
    };
    write_raster_bands(path, &bands, options)?;
    Ok(())
}

fn run_createcog(input: &Path, output: &Path, args: &[&str], overviews: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_createcog"));
    command
        .env("RUST_LOG", "warn")
        .arg("--input")
        .arg(input)
        .arg("--output")
        .arg(output)
        .args(["--noprogress", "--tile-size", "256", "--max-zoom", "8", "--min-zoom"])
        .arg(if overviews { "7" } else { "8" })
        .arg("--aligned-levels")
        .arg(if overviews { "2" } else { "1" })
        .args(args);
    command
        .output()
        .unwrap_or_else(|error| panic!("Cannot execute {command:?}: {error}"))
}

fn diagnostics(output: &Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn is_nodata(value: f64, nodata: f64) -> bool {
    value == nodata || (value.is_nan() && nodata.is_nan())
}

fn assert_close(actual: f64, expected: f64) {
    assert!(
        actual.is_finite() && (actual - expected).abs() < 1e-10,
        "actual={actual}, expected={expected}"
    );
}

fn assert_pixels(source: Source, georef: &GeoReference, pixels: &[f64], index: usize, max_valid: Option<f64>, tolerance: f64) {
    let nodata = georef.nodata().expect("Output nodata metadata");
    let RasterScale { scale, offset } = georef.scale().unwrap_or(RasterScale { scale: 1.0, offset: 0.0 });
    let transform: [f64; 6] = georef.geo_transform().into();
    let size = (georef.columns().count() as usize, georef.rows().count() as usize);
    assert_eq!(pixels.len(), size.0 * size.1);
    let mut valid_count = 0;
    let mut missing_source_count = 0;
    let mut padding_count = 0;
    for row in 0..size.1 {
        for column in 0..size.0 {
            let source_column = ((transform[0] + (column as f64 + 0.5) * transform[1]) / resolution()).floor();
            let source_row = ((transform[3] + (row as f64 + 0.5) * transform[5]) / -resolution()).floor();
            let inside = (0.0..SIZE as f64).contains(&source_column) && (0.0..SIZE as f64).contains(&source_row);
            let expected = if inside {
                source.physical(index, source_row as usize, source_column as usize)
            } else {
                None
            };
            let raw = pixels[row * size.0 + column];
            if let Some(expected) = expected {
                valid_count += 1;
                assert!(
                    !is_nodata(raw, nodata),
                    "band {index}, source ({source_row},{source_column}): valid pixel became nodata"
                );
                assert!(raw.is_finite(), "band {index}: non-finite valid pixel");
                if let Some(max_valid) = max_valid {
                    assert!((0.0..=max_valid).contains(&raw), "band {index}: invalid code {raw}");
                }
                let decoded = raw * scale + offset;
                assert!(
                    (decoded - expected).abs() <= tolerance,
                    "band {index}, source ({source_row},{source_column}): raw={raw}, decoded={decoded}, expected={expected}"
                );
            } else {
                assert!(
                    is_nodata(raw, nodata),
                    "band {index}, output ({row},{column}): expected nodata {nodata}, got {raw}"
                );
                if inside {
                    missing_source_count += 1;
                } else {
                    padding_count += 1;
                }
            }
        }
    }
    assert!(valid_count > 0, "band {index}: test must exercise valid pixels");
    if source.nodata.is_some() {
        assert!(missing_source_count > 0, "band {index}: test must exercise source nodata");
    }
    assert!(padding_count > 0, "band {index}: test must exercise nodata padding");
}

fn assert_scaled_band<T: ArrayNum>(reader: &mut GeoTiffReader, source: Source, index: usize, unscale: bool, nodata: f64) -> TestResult {
    let band_index = BandIndex::new(index).unwrap();
    let raster = reader.read_raster_band_as::<T, GeoReference>(band_index)?;
    let georef = raster.metadata();
    assert_eq!(georef.nodata(), Some(nodata));
    let (min, max) = source.range(index, unscale);
    let (packing_scale, packing_offset) = if unscale { (1.0, 0.0) } else { Source::packing(index) };
    let inverse_scale = if min == max { 1.0 } else { (max - min) / (nodata - 1.0) };
    let scaling = georef.scale().expect("Output inverse scale/offset");
    assert_close(scaling.scale, inverse_scale * packing_scale);
    assert_close(scaling.offset, min * packing_scale + packing_offset);
    // The native integer types reserve their maximum for nodata, so Array's
    // internal nodata representation retains the exact encoded code here.
    let pixels = cast::slice::<f64>(raster.as_slice())?;
    assert_pixels(source, georef, &pixels, index, Some(nodata - 1.0), scaling.scale.abs() + 1e-9);
    assert!(pixels.contains(&0.0), "band {index}: minimum must map to zero");
    if min != max {
        assert!(pixels.contains(&(nodata - 1.0)), "band {index}: maximum must map below nodata");
    } else {
        assert!(pixels.iter().all(|&value| value == 0.0 || value == nodata));
    }

    for level in 1..reader.metadata().overviews.len() {
        let overview = reader.read_overview_band_as::<T, GeoReference>(level, band_index)?;
        assert_eq!(overview.metadata().nodata(), Some(nodata));
        assert_eq!(overview.metadata().scale(), Some(scaling));
        assert_eq!(overview.columns().count(), raster.columns().count() / 2);
        assert_eq!(overview.rows().count(), raster.rows().count() / 2);
        let mut transform: [f64; 6] = georef.geo_transform().into();
        transform[1] *= f64::from(raster.columns().count()) / f64::from(overview.columns().count());
        transform[5] *= f64::from(raster.rows().count()) / f64::from(overview.rows().count());
        let overview_georef = GeoReference::new(
            georef.projection(),
            overview.metadata().raster_size(),
            transform.into(),
            Some(nodata),
            Some(scaling),
        );
        let pixels = cast::slice::<f64>(overview.as_slice())?;
        assert_pixels(
            source,
            &overview_georef,
            &pixels,
            index,
            Some(nodata - 1.0),
            scaling.scale.abs() + 1e-9,
        );
    }
    Ok(())
}

fn assert_scaled_output(path: &Path, source: Source, bits: u8, unscale: bool, overviews: bool) -> TestResult {
    let mut reader = GeoTiffReader::from_file(path)?;
    let metadata = reader.metadata();
    assert_eq!(metadata.band_count as usize, source.bands);
    assert_eq!(metadata.geo_reference.projected_epsg(), Some(WGS84_WEB_MERCATOR));
    assert_eq!(metadata.data_layout, ChunkDataLayout::Tiled(TILE_SIZE as u32));
    assert_close(metadata.geo_reference.cell_size_x(), resolution());
    assert_close(metadata.geo_reference.cell_size_y(), -resolution());
    // GeoTiffReader counts full resolution as level zero.
    assert_eq!(metadata.overviews.len(), 1 + usize::from(overviews));
    assert_eq!(
        metadata.data_type,
        if bits == 8 { ArrayDataType::Uint8 } else { ArrayDataType::Uint16 }
    );
    for index in 1..=source.bands {
        match bits {
            8 => assert_scaled_band::<u8>(&mut reader, source, index, unscale, 255.0)?,
            16 => assert_scaled_band::<u16>(&mut reader, source, index, unscale, 65535.0)?,
            _ => unreachable!(),
        }
    }
    Ok(())
}

fn read_float_band(reader: &mut RasterIO, index: usize) -> TestResult<(GeoReference, Vec<f64>)> {
    // Read the stored type, without Array's nodata normalization or a casting
    // reader's nodata conversion, so finite sentinels and NaNs are checked as stored.
    match reader.data_type(index)? {
        ArrayDataType::Float32 => {
            let (georef, pixels) = reader.read_raster_band::<f32>(index)?;
            Ok((georef, cast::slice(&pixels)?))
        }
        ArrayDataType::Float64 => {
            let (georef, pixels) = reader.read_raster_band::<f64>(index)?;
            Ok((georef, pixels.into_iter().collect()))
        }
        other => panic!("Expected floating-point output, got {other:?}"),
    }
}

fn check_scaled_matrix<T: ArrayNum>(nodata: Option<f64>, samples: Samples, overviews: bool) -> TestResult {
    let directory = tempfile::tempdir()?;
    for bands in [1, 2] {
        let source = Source { nodata, bands, samples };
        let input = directory.path().join(format!("input-{bands}.tif"));
        write_source::<T>(&input, source)?;
        for (bits, option) in [(8, "8"), (16, "16")] {
            for unscale in [false, true] {
                let output = directory.path().join(format!("scaled-{bands}-{bits}-{unscale}.tif"));
                let mut args = vec!["--scale", option];
                if unscale {
                    args.push("--unscale");
                }
                let result = run_createcog(&input, &output, &args, overviews);
                assert!(
                    result.status.success(),
                    "{source:?}, bits={bits}, unscale={unscale}: {}",
                    diagnostics(&result)
                );
                assert_scaled_output(&output, source, bits, unscale, overviews)?;
            }
        }
    }
    Ok(())
}

#[test]
fn float32_nan_nodata_survives_output_scaling() -> TestResult {
    check_scaled_matrix::<f32>(Some(f64::NAN), Samples::Varying, false)
}

#[test]
fn float64_nan_nodata_survives_output_scaling() -> TestResult {
    check_scaled_matrix::<f64>(Some(f64::NAN), Samples::Varying, false)
}

#[test]
fn float32_finite_nodata_survives_output_scaling() -> TestResult {
    check_scaled_matrix::<f32>(Some(FINITE_NODATA), Samples::Varying, false)
}

#[test]
fn float64_finite_nodata_survives_output_scaling() -> TestResult {
    check_scaled_matrix::<f64>(Some(FINITE_NODATA), Samples::Varying, false)
}

#[test]
fn constant_bands_preserve_nan_and_finite_nodata_when_scaled() -> TestResult {
    for nodata in [f64::NAN, FINITE_NODATA] {
        check_scaled_matrix::<f32>(Some(nodata), Samples::Constant, false)?;
    }
    Ok(())
}

#[test]
fn scaled_overviews_preserve_nan_and_finite_nodata() -> TestResult {
    for nodata in [f64::NAN, FINITE_NODATA] {
        check_scaled_matrix::<f32>(Some(nodata), Samples::Varying, true)?;
    }
    Ok(())
}

#[test]
fn scaling_without_declared_nodata_preserves_valid_pixels_and_marks_padding() -> TestResult {
    check_scaled_matrix::<f32>(None, Samples::Varying, false)
}

#[test]
fn unscale_preserves_nan_and_finite_nodata_in_float_output() -> TestResult {
    let directory = tempfile::tempdir()?;
    for nodata in [f64::NAN, FINITE_NODATA] {
        for bands in [1, 2] {
            let source = Source {
                nodata: Some(nodata),
                bands,
                samples: Samples::Varying,
            };
            let input = directory.path().join("input.tif");
            write_source::<f64>(&input, source)?;
            for data_type in ["Float32", "Float64"] {
                let output = directory.path().join("unscaled.tif");
                let result = run_createcog(&input, &output, &["--unscale", "--output-data-type", data_type], false);
                assert!(result.status.success(), "{source:?}, {data_type}: {}", diagnostics(&result));
                let mut reader = RasterIO::open_read_only(&output)?;
                assert_eq!(reader.band_count()?, bands);
                let expected_type = if data_type == "Float32" {
                    ArrayDataType::Float32
                } else {
                    ArrayDataType::Float64
                };
                for index in 1..=bands {
                    assert_eq!(reader.data_type(index)?, expected_type);
                    let (georef, pixels) = read_float_band(&mut reader, index)?;
                    assert!(is_nodata(georef.nodata().expect("Nodata metadata"), nodata));
                    let scaling = georef.scale().unwrap_or(RasterScale { scale: 1.0, offset: 0.0 });
                    assert_eq!(scaling.scale, 1.0);
                    assert_eq!(scaling.offset, 0.0);
                    assert_pixels(source, &georef, &pixels, index, None, 1e-9);
                }
            }
        }
    }
    Ok(())
}

#[test]
fn all_nodata_bands_cannot_supply_a_scaling_range() -> TestResult {
    let directory = tempfile::tempdir()?;
    for nodata in [f64::NAN, FINITE_NODATA] {
        for (bands, samples) in [(1, Samples::Empty), (2, Samples::EmptyLastBand)] {
            let source = Source {
                nodata: Some(nodata),
                bands,
                samples,
            };
            let input = directory.path().join("input.tif");
            write_source::<f32>(&input, source)?;
            for bits in ["8", "16"] {
                for unscale in [false, true] {
                    let output = directory.path().join(format!("empty-{nodata}-{bands}-{bits}-{unscale}.tif"));
                    let mut args = vec!["--scale", bits];
                    if unscale {
                        args.push("--unscale");
                    }
                    let result = run_createcog(&input, &output, &args, false);
                    assert!(!result.status.success(), "{source:?}: {}", diagnostics(&result));
                    assert!(
                        String::from_utf8_lossy(&result.stderr).contains("Cannot determine scaling range"),
                        "{source:?}: {}",
                        diagnostics(&result)
                    );
                }
            }
        }
    }
    Ok(())
}

#[test]
fn netcdf_nan_nodata_survives_selection_unscale_and_scaling() -> TestResult {
    let directory = tempfile::tempdir()?;
    let source = Source {
        nodata: Some(f64::NAN),
        bands: 2,
        samples: Samples::Varying,
    };
    let input = directory.path().join("input.nc");
    write_source::<f32>(&input, source)?;
    for (bits, option) in [(8, "8"), (16, "16")] {
        for unscale in [false, true] {
            let output = directory.path().join(format!("netcdf-{bits}-{unscale}.tif"));
            let mut args = vec!["--variable", "Band1,Band2", "--dimensions", "y,x", "--scale", option];
            if unscale {
                args.push("--unscale");
            }
            let result = run_createcog(&input, &output, &args, false);
            assert!(result.status.success(), "{}", diagnostics(&result));
            assert_scaled_output(&output, source, bits, unscale, false)?;
        }
    }
    Ok(())
}

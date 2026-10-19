use std::{
    path::{Path, PathBuf},
    process::{Command, Output},
};

use gdal::{
    Dataset, DatasetOptions, DriverManager, GdalOpenFlags,
    raster::{Buffer, GdalDataType, RasterCreationOptions},
    spatial_ref::SpatialRef,
};
use geo::{
    Columns, GeoReference, RasterSize, Rows,
    raster::{GeoTiffWriteOptions, WriteRasterOptions, io::write_raster_band},
};

const WIDTH: usize = 5;
const HEIGHT: usize = 4;
const TILE_SIZE: usize = 256;
const NODATA: f64 = -32768.0;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy)]
struct Grid {
    transform: [f64; 6],
    zoom: i32,
}

fn resolution(zoom: i32) -> f64 {
    2.0 * std::f64::consts::PI * 6378137.0 / (TILE_SIZE as f64 * 2.0_f64.powi(zoom))
}

fn tiff_grid() -> Grid {
    // The equator/prime-meridian intersection is a Google Maps tile corner.
    let step = resolution(8);
    Grid {
        transform: [0.0, step, 0.0, 0.0, 0.0, -step],
        zoom: 8,
    }
}

const NETCDF_GRID: Grid = Grid {
    transform: [5723000.0, 100.0, 0.0, 2985000.0, 0.0, -100.0],
    zoom: 18,
};

#[derive(Clone, Copy)]
struct PackedBand {
    base: i32,
    scale: Option<f64>,
    offset: Option<f64>,
}

const PACKED: PackedBand = PackedBand {
    base: -123,
    scale: Some(0.01),
    offset: Some(-5.0),
};
const OTHER_PACKED: PackedBand = PackedBand {
    base: 801,
    scale: Some(0.125),
    offset: Some(17.0),
};
const UNSCALED: PackedBand = PackedBand {
    base: 42,
    scale: None,
    offset: None,
};

impl PackedBand {
    fn raw(self, row: usize, column: usize) -> Option<f64> {
        ((row, column) != (1, 3)).then_some(f64::from(self.base + 100 * row as i32 + 7 * column as i32))
    }

    fn physical(self, row: usize, column: usize) -> Option<f64> {
        self.raw(row, column)
            .map(|raw| raw * self.scale.unwrap_or(1.0) + self.offset.unwrap_or(0.0))
    }
}

fn create_raster(path: &Path, driver: &str, bands: &[PackedBand]) -> TestResult {
    let options = if driver == "netCDF" {
        RasterCreationOptions::from_iter(["WRITE_BOTTOMUP=NO"])
    } else {
        RasterCreationOptions::default()
    };
    let single_band_tiff = driver == "GTiff" && bands.len() == 1;
    let dataset = if single_band_tiff {
        let georef = GeoReference::new(
            SpatialRef::from_epsg(3857)?.to_wkt()?,
            RasterSize::with_rows_cols(Rows(HEIGHT as i32), Columns(WIDTH as i32)),
            tiff_grid().transform.into(),
            Some(NODATA),
            None,
        );
        let samples: Vec<i32> = (0..HEIGHT)
            .flat_map(|row| (0..WIDTH).map(move |column| bands[0].raw(row, column).unwrap_or(NODATA) as i32))
            .collect();
        write_raster_band(path, &georef, &samples, WriteRasterOptions::GeoTiff(GeoTiffWriteOptions::default()))?;
        // Set scale and offset separately below to preserve absent versus explicit identity metadata.
        Dataset::open_ex(
            path,
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_UPDATE | GdalOpenFlags::GDAL_OF_RASTER,
                ..Default::default()
            },
        )?
    } else {
        // geo's writer is single-band and cannot pass NetCDF's WRITE_BOTTOMUP option.
        let mut dataset = DriverManager::get_driver_by_name(driver)?.create_with_band_type_with_options::<i32, _>(
            path,
            WIDTH,
            HEIGHT,
            bands.len(),
            &options,
        )?;
        dataset.set_geo_transform(&tiff_grid().transform)?;
        dataset.set_spatial_ref(&SpatialRef::from_epsg(3857)?)?;
        dataset
    };
    for (index, &packed) in bands.iter().enumerate() {
        let mut band = dataset.rasterband(index + 1)?;
        band.set_no_data_value(Some(NODATA))?;
        if let Some(scale) = packed.scale {
            band.set_scale(scale)?;
        }
        if let Some(offset) = packed.offset {
            band.set_offset(offset)?;
        }
        if !single_band_tiff {
            let samples = (0..HEIGHT)
                .flat_map(|row| (0..WIDTH).map(move |column| packed.raw(row, column).unwrap_or(NODATA) as i32))
                .collect();
            band.write((0, 0), (WIDTH, HEIGHT), &mut Buffer::new((WIDTH, HEIGHT), samples))?;
        }
    }
    dataset.close()?;
    Ok(())
}

fn run_createcog(input: &Path, output: &Path, grid: Grid, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_createcog"));
    command
        .env("RUST_LOG", "warn")
        .arg("--input")
        .arg(input)
        .arg("--output")
        .arg(output)
        .args(["--noprogress", "--tile-size", "256", "--aligned-levels", "1", "--max-zoom"])
        .arg(grid.zoom.to_string())
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

fn convert(input: &Path, output: &Path, grid: Grid, args: &[&str], bands: usize) -> TestResult<Dataset> {
    let result = run_createcog(input, output, grid, args);
    assert!(result.status.success(), "{}", diagnostics(&result));
    assert!(
        !String::from_utf8_lossy(&result.stderr).contains("-a_scale/-a_offset are not applied by -unscale"),
        "Redundant scale/offset assignment during unscaling: {}",
        diagnostics(&result)
    );
    let dataset = Dataset::open(output)?;
    assert_eq!(dataset.driver().short_name(), "GTiff");
    assert_eq!(dataset.raster_count(), bands);
    assert_eq!(dataset.spatial_ref()?.auth_code()?, 3857);
    let transform = dataset.geo_transform()?;
    assert!((transform[1] - resolution(grid.zoom)).abs() < 1e-8, "{transform:?}");
    assert!((transform[5] + resolution(grid.zoom)).abs() < 1e-8, "{transform:?}");
    assert_eq!((transform[2], transform[4]), (0.0, 0.0));
    let (width, height) = dataset.raster_size();
    assert_eq!((width % TILE_SIZE, height % TILE_SIZE), (0, 0));
    assert!(
        width <= 4 * TILE_SIZE && height <= 4 * TILE_SIZE,
        "Unexpected output size {width}x{height}"
    );
    Ok(dataset)
}

fn assert_samples(
    dataset: &Dataset,
    index: usize,
    grid: Grid,
    decode: bool,
    tolerance: f64,
    expected: impl Fn(usize, usize) -> Option<f64>,
) -> TestResult {
    let band = dataset.rasterband(index)?;
    assert_eq!(band.block_size(), (TILE_SIZE, TILE_SIZE));
    let nodata = band.no_data_value().expect("Output must retain nodata semantics");
    let scale = if decode { band.scale().expect("Encoded output scale") } else { 1.0 };
    let offset = if decode {
        band.offset().expect("Encoded output offset")
    } else {
        0.0
    };
    let (width, height) = dataset.raster_size();
    let transform = dataset.geo_transform()?;
    let samples = band.read_as::<f64>((0, 0), (width, height), (width, height), None)?;
    let mut covered = [[false; WIDTH]; HEIGHT];
    for row in 0..height {
        for column in 0..width {
            let x = transform[0] + (column as f64 + 0.5) * transform[1];
            let y = transform[3] + (row as f64 + 0.5) * transform[5];
            let source_column = ((x - grid.transform[0]) / grid.transform[1]).floor();
            let source_row = ((y - grid.transform[3]) / grid.transform[5]).floor();
            let value = if (0.0..WIDTH as f64).contains(&source_column) && (0.0..HEIGHT as f64).contains(&source_row) {
                let (r, c) = (source_row as usize, source_column as usize);
                covered[r][c] = true;
                expected(r, c)
            } else {
                None
            };
            let raw = samples.data()[row * width + column];
            if let Some(value) = value {
                assert_ne!(raw, nodata, "band {index}, source ({source_row},{source_column})");
                let actual = raw * scale + offset;
                assert!(
                    (actual - value).abs() <= tolerance,
                    "band {index}, output ({row},{column}), source ({source_row},{source_column}): \
                     raw={raw}, decoded={actual}, expected={value}, tolerance={tolerance}"
                );
            } else {
                assert_eq!(raw, nodata, "band {index}, nodata at output ({row},{column})");
            }
        }
    }
    assert_eq!(covered, [[true; WIDTH]; HEIGHT], "band {index}: incomplete source coverage");
    Ok(())
}

fn assert_physical_band(dataset: &Dataset, index: usize, data_type: GdalDataType) -> TestResult {
    let band = dataset.rasterband(index)?;
    assert_eq!(band.band_type(), data_type, "band {index}");
    assert_eq!(band.no_data_value(), Some(NODATA), "Nodata must not be multiplied by input scale");
    assert_eq!(band.scale().unwrap_or(1.0), 1.0, "band {index}: input scale must be cleared");
    assert_eq!(band.offset().unwrap_or(0.0), 0.0, "band {index}: input offset must be cleared");
    Ok(())
}

fn assert_generated_physical(dataset: &Dataset, bands: &[PackedBand], data_type: GdalDataType) -> TestResult {
    for (index, &packed) in bands.iter().enumerate() {
        assert_physical_band(dataset, index + 1, data_type)?;
        assert_samples(dataset, index + 1, tiff_grid(), false, 1e-5, |row, column| {
            packed.physical(row, column)
        })?;
    }
    Ok(())
}

fn assert_rejected(input: &Path, output: &Path, args: &[&str], words: &[&str]) {
    let result = run_createcog(input, output, tiff_grid(), args);
    let detail = diagnostics(&result);
    assert!(!result.status.success(), "{detail}");
    assert!(!output.exists(), "Failed validation left a final output: {detail}");
    let stderr = String::from_utf8_lossy(&result.stderr).to_ascii_lowercase();
    for word in words {
        assert!(stderr.contains(&word.to_ascii_lowercase()), "Missing {word:?}: {detail}");
    }
}

#[test]
fn single_band_tiff_defaults_preserve_raw_samples_and_nodata() -> TestResult {
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("packed.tif");
    create_raster(&input, "GTiff", &[PACKED])?;
    let dataset = convert(&input, &directory.path().join("output.tif"), tiff_grid(), &[], 1)?;
    let band = dataset.rasterband(1)?;
    assert_eq!(band.band_type(), GdalDataType::Int32);
    assert_eq!(band.no_data_value(), Some(NODATA));
    assert_samples(&dataset, 1, tiff_grid(), false, 0.0, |row, column| PACKED.raw(row, column))
}

#[test]
fn multiband_tiff_defaults_preserve_raw_samples_and_nodata() -> TestResult {
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("packed.tif");
    let bands = [PACKED, OTHER_PACKED];
    create_raster(&input, "GTiff", &bands)?;
    let dataset = convert(&input, &directory.path().join("output.tif"), tiff_grid(), &[], bands.len())?;
    for (index, &packed) in bands.iter().enumerate() {
        let band = dataset.rasterband(index + 1)?;
        assert_eq!(band.band_type(), GdalDataType::Int32);
        assert_eq!(band.no_data_value(), Some(NODATA));
        assert_samples(&dataset, index + 1, tiff_grid(), false, 0.0, |row, column| packed.raw(row, column))?;
    }
    Ok(())
}

#[test]
fn tiff_glob_defaults_detect_multiband_and_preserve_sorted_file_then_band_order() -> TestResult {
    let directory = tempfile::tempdir()?;
    let later = [OTHER_PACKED, PackedBand { base: 1901, ..PACKED }];
    let earlier = [PACKED, PackedBand { base: 501, ..OTHER_PACKED }];
    create_raster(&directory.path().join("b.tif"), "GTiff", &later)?;
    create_raster(&directory.path().join("a.tif"), "GTiff", &earlier)?;
    let dataset = convert(
        &directory.path().join("[ab].tif"),
        &directory.path().join("output.tif"),
        tiff_grid(),
        &[],
        4,
    )?;
    for (index, packed) in [earlier[0], earlier[1], later[0], later[1]].into_iter().enumerate() {
        let band = dataset.rasterband(index + 1)?;
        assert_eq!(band.band_type(), GdalDataType::Int32);
        assert_eq!(band.no_data_value(), Some(NODATA));
        assert_samples(&dataset, index + 1, tiff_grid(), false, 0.0, |row, column| packed.raw(row, column))?;
    }
    Ok(())
}

#[test]
fn tiff_glob_matching_one_single_band_file_preserves_raw_samples() -> TestResult {
    let directory = tempfile::tempdir()?;
    create_raster(&directory.path().join("input.tif"), "GTiff", &[PACKED])?;
    let dataset = convert(
        &directory.path().join("input*.tif"),
        &directory.path().join("output.tif"),
        tiff_grid(),
        &[],
        1,
    )?;
    assert_samples(&dataset, 1, tiff_grid(), false, 0.0, |row, column| PACKED.raw(row, column))
}

#[test]
fn tiff_conversion_ignores_invalid_netcdf_dimensions() -> TestResult {
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("packed.tif");
    create_raster(&input, "GTiff", &[PACKED])?;
    let dataset = convert(
        &input,
        &directory.path().join("output.tif"),
        tiff_grid(),
        &["--dimensions", "not_a_valid_dimension_list"],
        1,
    )?;
    assert_eq!(dataset.rasterband(1)?.no_data_value(), Some(NODATA));
    assert_samples(&dataset, 1, tiff_grid(), false, 0.0, |row, column| PACKED.raw(row, column))
}

#[test]
fn tiff_conversion_rejects_netcdf_variable_selection_without_output() -> TestResult {
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("packed.tif");
    create_raster(&input, "GTiff", &[PACKED])?;
    assert_rejected(
        &input,
        &directory.path().join("output.tif"),
        &["--variable", "Band1"],
        &["--variable", "only supported for NetCDF"],
    );
    Ok(())
}

#[test]
fn single_band_unscale_defaults_to_float64_and_preserves_nodata() -> TestResult {
    let directory = tempfile::tempdir()?;
    for (driver, extension, args) in [
        ("GTiff", "tif", &["--unscale"][..]),
        ("netCDF", "nc", &["--unscale", "--variable", "Band1", "--dimensions", "y,x"][..]),
    ] {
        let input = directory.path().join(format!("packed.{extension}"));
        create_raster(&input, driver, &[PACKED])?;
        let dataset = convert(
            &input,
            &directory.path().join(format!("physical-{extension}.tif")),
            tiff_grid(),
            args,
            1,
        )?;
        assert_generated_physical(&dataset, &[PACKED], GdalDataType::Float64)?;
    }
    Ok(())
}

#[test]
fn multiband_unscale_uses_each_bands_scale_and_offset() -> TestResult {
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("packed.tif");
    let bands = [PACKED, OTHER_PACKED];
    create_raster(&input, "GTiff", &bands)?;
    let dataset = convert(
        &input,
        &directory.path().join("physical.tif"),
        tiff_grid(),
        &["--unscale", "--multi-band", "--output-data-type", "fLoAt32"],
        bands.len(),
    )?;
    assert_generated_physical(&dataset, &bands, GdalDataType::Float32)
}

#[test]
fn unscale_accepts_nonidentity_offset_only_and_scale_only() -> TestResult {
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("partial-metadata.tif");
    let bands = [
        PackedBand { scale: None, ..PACKED },
        PackedBand {
            offset: None,
            ..OTHER_PACKED
        },
    ];
    create_raster(&input, "GTiff", &bands)?;
    let dataset = convert(
        &input,
        &directory.path().join("physical.tif"),
        tiff_grid(),
        &["--unscale"],
        bands.len(),
    )?;
    assert_generated_physical(&dataset, &bands, GdalDataType::Float64)
}

#[test]
fn tiff_glob_unscale_is_alphabetical_then_band_major() -> TestResult {
    let directory = tempfile::tempdir()?;
    let later = [OTHER_PACKED, PackedBand { base: 1901, ..PACKED }];
    let earlier = [PACKED, PackedBand { base: 501, ..OTHER_PACKED }];
    create_raster(&directory.path().join("b.tif"), "GTiff", &later)?;
    create_raster(&directory.path().join("a.tif"), "GTiff", &earlier)?;
    let dataset = convert(
        &directory.path().join("[ab].tif"),
        &directory.path().join("physical.tif"),
        tiff_grid(),
        &["--unscale", "--output-data-type", "FLOAT64"],
        4,
    )?;
    assert_generated_physical(&dataset, &[earlier[0], earlier[1], later[0], later[1]], GdalDataType::Float64)
}

#[test]
fn datatype_only_cast_preserves_raw_pixels_and_packing_metadata() -> TestResult {
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("packed.tif");
    for bands in [&[PACKED][..], &[PACKED, OTHER_PACKED][..]] {
        create_raster(&input, "GTiff", bands)?;
        for (name, data_type) in [("Float32", GdalDataType::Float32), ("fLoAt64", GdalDataType::Float64)] {
            let dataset = convert(
                &input,
                &directory.path().join(format!("{name}-{}.tif", bands.len())),
                tiff_grid(),
                &["--output-data-type", name],
                bands.len(),
            )?;
            for (index, &packed) in bands.iter().enumerate() {
                let band = dataset.rasterband(index + 1)?;
                assert_eq!(band.band_type(), data_type);
                assert_eq!(band.scale(), packed.scale);
                assert_eq!(band.offset(), packed.offset);
                assert_eq!(band.no_data_value(), Some(NODATA));
                assert_samples(&dataset, index + 1, tiff_grid(), false, 0.0, |row, column| packed.raw(row, column))?;
            }
        }
    }
    Ok(())
}

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/uc-small.nc")
}

fn assert_fixture_unscale(variable: &str, base: usize, missing: (usize, usize, usize)) -> TestResult {
    let directory = tempfile::tempdir()?;
    let dataset = convert(
        &fixture(),
        &directory.path().join("physical.tif"),
        NETCDF_GRID,
        &[
            "--variable",
            variable,
            "--dimensions",
            "time,y,x",
            "--unscale",
            "--output-data-type",
            "Float32",
        ],
        3,
    )?;
    for time in 0..3 {
        assert_physical_band(&dataset, time + 1, GdalDataType::Float32)?;
        // Stored-index oracle, independent of GDAL's NetCDF raster orientation.
        assert_samples(&dataset, time + 1, NETCDF_GRID, false, 2e-5, |row, column| {
            ((time, row, column) != missing).then_some((base + 1000 * (time + 1) + 100 * row + column) as f64 * 0.01)
        })?;
    }
    Ok(())
}

#[test]
fn netcdf_wbgt_unscale_float32_decodes_all_times_and_nodata() -> TestResult {
    assert_fixture_unscale("WBGT", 0, (1, 2, 3))
}

#[test]
fn netcdf_transposed_unscale_preserves_stored_time_and_spatial_order() -> TestResult {
    assert_fixture_unscale("WBGT_transposed", 10000, (2, 0, 4))
}

#[test]
fn netcdf_glob_unscale_preserves_file_then_selected_variable_order() -> TestResult {
    let directory = tempfile::tempdir()?;
    let later = [OTHER_PACKED, PackedBand { base: 1901, ..PACKED }];
    let earlier = [PACKED, PackedBand { base: 501, ..OTHER_PACKED }];
    create_raster(&directory.path().join("b.nc"), "netCDF", &later)?;
    create_raster(&directory.path().join("a.nc"), "netCDF", &earlier)?;
    let dataset = convert(
        &directory.path().join("*.nc"),
        &directory.path().join("physical.tif"),
        tiff_grid(),
        &["--variable", "Band2,Band1", "--dimensions", "y,x", "--unscale"],
        4,
    )?;
    assert_generated_physical(&dataset, &[earlier[1], earlier[0], later[1], later[0]], GdalDataType::Float64)
}

#[test]
fn unscale_rejects_missing_and_explicit_identity_metadata_without_output() -> TestResult {
    let directory = tempfile::tempdir()?;
    for (name, band) in [
        ("missing", UNSCALED),
        (
            "identity",
            PackedBand {
                scale: Some(1.0),
                offset: Some(0.0),
                ..UNSCALED
            },
        ),
    ] {
        let input = directory.path().join(format!("{name}.tif"));
        create_raster(&input, "GTiff", &[band])?;
        assert_rejected(
            &input,
            &directory.path().join(format!("{name}-output.tif")),
            &["--unscale"],
            &["unscale", "band"],
        );
    }
    Ok(())
}

#[test]
fn unscale_rejects_a_later_unscaled_band_without_output() -> TestResult {
    let directory = tempfile::tempdir()?;
    for (driver, extension, selection) in [
        ("GTiff", "tif", &[][..]),
        ("netCDF", "nc", &["--variable", "Band1,Band2", "--dimensions", "y,x"][..]),
    ] {
        let input = directory.path().join(format!("mixed.{extension}"));
        create_raster(&input, driver, &[PACKED, UNSCALED])?;
        let mut args = vec!["--unscale"];
        args.extend_from_slice(selection);
        assert_rejected(&input, &directory.path().join("physical.tif"), &args, &["unscale", "band", "2"]);
    }
    Ok(())
}

#[test]
fn unscale_rejects_an_unscaled_band_in_a_later_glob_file_without_output() -> TestResult {
    let directory = tempfile::tempdir()?;
    for (driver, extension, selection) in [
        ("GTiff", "tif", &[][..]),
        ("netCDF", "nc", &["--variable", "Band1", "--dimensions", "y,x"][..]),
    ] {
        create_raster(&directory.path().join(format!("a.{extension}")), driver, &[PACKED])?;
        create_raster(&directory.path().join(format!("b.{extension}")), driver, &[UNSCALED])?;
        let mut args = vec!["--unscale"];
        args.extend_from_slice(selection);
        assert_rejected(
            &directory.path().join(format!("[ab].{extension}")),
            &directory.path().join("physical.tif"),
            &args,
            &["unscale", "band"],
        );
    }
    Ok(())
}

#[test]
fn datatype_scale_mismatch_is_rejected_before_opening_input() -> TestResult {
    let directory = tempfile::tempdir()?;
    for (bits, name, expected) in [("8", "UInt16", "uint8"), ("16", "Float32", "uint16")] {
        for unscale in [false, true] {
            let mut args = vec!["--scale", bits, "--output-data-type", name];
            if unscale {
                args.push("--unscale");
            }
            assert_rejected(
                &directory.path().join("does-not-exist.tif"),
                &directory.path().join("invalid.tif"),
                &args,
                &["--output-data-type", "--scale", "expected", expected],
            );
        }
    }
    Ok(())
}

#[test]
fn scale8_and_scale16_roundtrip_physical_values_with_or_without_unscale() -> TestResult {
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("packed.tif");
    for bands in [&[PACKED][..], &[PACKED, OTHER_PACKED][..]] {
        create_raster(&input, "GTiff", bands)?;
        for (bits, name, data_type, maximum) in [
            ("8", "UInt8", GdalDataType::UInt8, 254.0),
            ("16", "UInt16", GdalDataType::UInt16, 65534.0),
        ] {
            for (unscale, explicit_type) in [(true, false), (true, true), (false, true)] {
                let mut args = vec!["--scale", bits];
                if unscale {
                    args.push("--unscale");
                }
                if explicit_type {
                    args.extend(["--output-data-type", name]);
                }

                let dataset = convert(
                    &input,
                    &directory
                        .path()
                        .join(format!("encoded-{bits}-{unscale}-{explicit_type}-{}.tif", bands.len())),
                    tiff_grid(),
                    &args,
                    bands.len(),
                )?;
                for (index, &packed) in bands.iter().enumerate() {
                    let band = dataset.rasterband(index + 1)?;
                    assert_eq!(band.band_type(), data_type);
                    assert_eq!(band.no_data_value(), Some(maximum + 1.0));
                    let min = packed.physical(0, 0).unwrap();
                    let max = packed.physical(HEIGHT - 1, WIDTH - 1).unwrap();
                    let step = (max - min) / maximum;
                    assert!((band.scale().expect("New encoding scale") - step).abs() < 1e-10);
                    assert!((band.offset().expect("New encoding offset") - min).abs() < 1e-10);
                    assert_ne!(band.scale(), packed.scale);
                    assert_ne!(band.offset(), packed.offset);
                    // GDAL's scaled integer conversion need not round to nearest. Match geo's
                    // scaling tests: allow one quantization step plus floating-point slack,
                    // without relaxing the independent scale/offset checks above.
                    assert_samples(&dataset, index + 1, tiff_grid(), true, step + 1e-9, |row, column| {
                        packed.physical(row, column)
                    })?;
                }
            }
        }
    }
    Ok(())
}

use std::{
    path::{Path, PathBuf},
    process::{Command, Output},
};

use gdal::{
    Dataset, DriverManager, Metadata,
    raster::{Buffer, GdalDataType, RasterCreationOptions},
    spatial_ref::SpatialRef,
};
use geo::geotiff::{ChunkDataLayout, GeoTiffReader};

const TILE_SIZE: usize = 256;
const MAX_ZOOM: i32 = 18;
const SOURCE_TRANSFORM: [f64; 6] = [5723000.0, 100.0, 0.0, 2985000.0, 0.0, -100.0];
const NODATA: f64 = -32768.0;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/uc-small.nc")
}

fn run_createcog(output: &Path, selection: &[&str]) -> Output {
    run_createcog_with_input(&fixture(), output, selection)
}

fn run_createcog_with_input(input: &Path, output: &Path, selection: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_createcog"));
    command
        .arg("--input")
        .arg(input)
        .arg("--output")
        .arg(output)
        .args(["--noprogress", "--tile-size"])
        .arg(TILE_SIZE.to_string())
        .arg("--max-zoom")
        .arg(MAX_ZOOM.to_string())
        .args(["--aligned-levels", "1"])
        .args(selection);
    command
        .output()
        .unwrap_or_else(|error| panic!("Cannot execute {command:?}: {error}"))
}

fn command_diagnostics(output: &Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn assert_cog_layout(dataset: &Dataset, path: &Path) -> TestResult {
    assert_eq!(dataset.driver().short_name(), "GTiff");
    if let Some(layout) = dataset.metadata_item("LAYOUT", "IMAGE_STRUCTURE") {
        assert_eq!(layout, "COG");
    } else {
        // Assembled multiband COGs can expose their structure only via ghost metadata.
        let reader = GeoTiffReader::from_file(path)?;
        let metadata = reader.metadata();
        assert_eq!(metadata.data_layout, ChunkDataLayout::Tiled(TILE_SIZE as u32));
        assert_eq!(metadata.band_count as usize, dataset.raster_count());
        assert!(
            metadata.gdal_ghost_data.as_ref().is_some_and(|ghost| ghost.is_cog()),
            "Missing COG structural metadata in {}",
            path.display()
        );
    }
    Ok(())
}

fn assert_georeferencing(dataset: &Dataset) -> TestResult<[f64; 6]> {
    assert_eq!(dataset.spatial_ref()?.auth_code()?, 3857);
    let transform = dataset.geo_transform()?;
    assert!(transform.iter().all(|value| value.is_finite()), "{transform:?}");
    assert_eq!(transform[2], 0.0);
    assert_eq!(transform[4], 0.0);
    let resolution = 2.0 * std::f64::consts::PI * 6378137.0 / (TILE_SIZE as f64 * 2.0_f64.powi(MAX_ZOOM));
    assert!((transform[1] - resolution).abs() < 1e-8, "{transform:?}");
    assert!((transform[5] + resolution).abs() < 1e-8, "{transform:?}");

    let (width, height) = dataset.raster_size();
    assert_eq!(width % TILE_SIZE, 0);
    assert_eq!(height % TILE_SIZE, 0);
    let east = transform[0] + width as f64 * transform[1];
    let south = transform[3] + height as f64 * transform[5];
    let source_east = SOURCE_TRANSFORM[0] + 5.0 * SOURCE_TRANSFORM[1];
    let source_south = SOURCE_TRANSFORM[3] + 4.0 * SOURCE_TRANSFORM[5];
    let tile_extent = TILE_SIZE as f64 * resolution;
    for padding in [
        SOURCE_TRANSFORM[0] - transform[0],
        transform[3] - SOURCE_TRANSFORM[3],
        east - source_east,
        source_south - south,
    ] {
        assert!(
            (-1e-7..=tile_extent + 1e-7).contains(&padding),
            "Unexpected bounds: {transform:?}, {width}x{height}"
        );
    }
    Ok(transform)
}

fn expected_raw(variable: &str, time: usize, row: usize, column: usize) -> f64 {
    // Stored-index oracle from data/README.md, independent of GDAL's NetCDF raster view.
    let (base, missing) = match variable {
        "WBGT" => (0, (1, 2, 3)),
        "WBGT_transposed" => (10000, (2, 0, 4)),
        _ => panic!("No fixture oracle for {variable}"),
    };
    if (time, row, column) == missing {
        NODATA
    } else {
        (base + 1000 * (time + 1) + 100 * row + column) as f64
    }
}

fn assert_band_samples(
    dataset: &Dataset,
    band_index: usize,
    transform: [f64; 6],
    case: &str,
    expected_raw: impl Fn(usize, usize) -> f64,
) -> TestResult {
    let (width, height) = dataset.raster_size();
    let band = dataset.rasterband(band_index)?;
    assert_eq!(band.band_type(), GdalDataType::Int32, "{case}, band {band_index}");
    assert_eq!(band.no_data_value(), Some(NODATA), "{case}, band {band_index}");
    assert_eq!(band.block_size(), (TILE_SIZE, TILE_SIZE));
    // Read at native output resolution: GDAL must not resample for the assertion.
    let samples = band.read_as::<f64>((0, 0), (width, height), (width, height), None)?;
    let mut covered = [[false; 5]; 4];
    for output_row in 0..height {
        for output_column in 0..width {
            let x = transform[0] + (output_column as f64 + 0.5) * transform[1];
            let y = transform[3] + (output_row as f64 + 0.5) * transform[5];
            // Nearest-neighbour sampling chooses the source cell containing this center.
            let column = ((x - SOURCE_TRANSFORM[0]) / SOURCE_TRANSFORM[1]).floor();
            let row = ((y - SOURCE_TRANSFORM[3]) / SOURCE_TRANSFORM[5]).floor();
            let expected = if (0.0..5.0).contains(&column) && (0.0..4.0).contains(&row) {
                let (row, column) = (row as usize, column as usize);
                covered[row][column] = true;
                expected_raw(row, column)
            } else {
                NODATA
            };
            assert_eq!(
                samples.data()[output_row * width + output_column],
                expected,
                "{case}: band {band_index}, output ({output_row},{output_column}), \
                 source ({row},{column}), map center ({x},{y})"
            );
        }
    }
    assert_eq!(
        covered, [[true; 5]; 4],
        "{case}, band {band_index}: incomplete source coverage (including nodata)"
    );
    Ok(())
}

fn assert_final_samples(dataset: &Dataset, variables: &[&str], transform: [f64; 6], case: &str) -> TestResult {
    for (variable_index, variable) in variables.iter().enumerate() {
        for time in 0..3 {
            let band_index = variable_index * 3 + time + 1;
            assert_band_samples(dataset, band_index, transform, case, |row, column| {
                expected_raw(variable, time, row, column)
            })?;
        }
    }
    Ok(())
}

fn generated_sample(base: i32, row: usize, column: usize) -> f64 {
    if (row, column) == (1, 3) {
        NODATA
    } else {
        f64::from(base + 100 * row as i32 + column as i32)
    }
}

fn write_sample_bands(dataset: &mut Dataset, bases: &[i32]) -> TestResult {
    dataset.set_geo_transform(&SOURCE_TRANSFORM)?;
    dataset.set_spatial_ref(&SpatialRef::from_epsg(3857)?)?;
    for (index, &base) in bases.iter().enumerate() {
        let mut band = dataset.rasterband(index + 1)?;
        band.set_no_data_value(Some(NODATA))?;
        band.set_scale(0.01)?;
        band.set_offset(0.0)?;
        let samples = (0..4)
            .flat_map(|row| (0..5).map(move |column| generated_sample(base, row, column) as i32))
            .collect();
        band.write((0, 0), (5, 4), &mut Buffer::new((5, 4), samples))?;
    }
    Ok(())
}

fn create_sample_raster(path: &Path, driver: &str, bases: &[i32]) -> TestResult {
    // Keep the multidimensional NetCDF view north-up, like the checked-in fixture.
    let options = if driver == "netCDF" {
        RasterCreationOptions::from_iter(["WRITE_BOTTOMUP=NO"])
    } else {
        RasterCreationOptions::default()
    };
    let driver = DriverManager::get_driver_by_name(driver)?;
    let mut dataset = driver.create_with_band_type_with_options::<i32, _>(path, 5, 4, bases.len(), &options)?;
    write_sample_bands(&mut dataset, bases)?;
    dataset.close()?;
    Ok(())
}

fn create_time_series_netcdf(path: &Path, bases: &[i32]) -> TestResult {
    create_time_series_netcdf_with_row_order(path, bases, false)
}

fn create_time_series_netcdf_with_row_order(path: &Path, bases: &[i32], bottom_up: bool) -> TestResult {
    create_time_series_netcdf_with_metadata(path, bases, bottom_up, &[])
}

fn create_time_series_netcdf_with_metadata(path: &Path, bases: &[i32], bottom_up: bool, metadata: &[(&str, &str)]) -> TestResult {
    let mut source = DriverManager::get_driver_by_name("MEM")?.create_with_band_type::<i32, _>("", 5, 4, bases.len())?;
    write_sample_bands(&mut source, bases)?;
    // GDAL's NetCDF CreateCopy maps these bands onto one 3D variable, Band1(time,y,x).
    source.set_metadata_item("NETCDF_DIM_EXTRA", "{time}", "")?;
    source.set_metadata_item("NETCDF_DIM_time_DEF", &format!("{{{},6}}", bases.len()), "")?;
    // Descending coordinates distinguish stored slice order from chronological order.
    let times = (0..bases.len()).rev().map(|index| index.to_string()).collect::<Vec<_>>().join(",");
    source.set_metadata_item("NETCDF_DIM_time_VALUES", &format!("{{{times}}}"), "")?;
    source.set_metadata_item("time#units", "hours since 2024-01-01 00:00:00", "")?;
    source.set_metadata_item("time#standard_name", "time", "")?;
    for &(key, value) in metadata {
        source.set_metadata_item(key, value, "")?;
    }
    let driver = DriverManager::get_driver_by_name("netCDF")?;
    let options = RasterCreationOptions::from_iter([if bottom_up { "WRITE_BOTTOMUP=YES" } else { "WRITE_BOTTOMUP=NO" }]);
    source.create_copy(&driver, path, &options)?.close()?;
    Ok(())
}

fn assert_generated_cog(path: &Path, bases: &[i32], case: &str) -> TestResult {
    assert!(path.is_file(), "{case}: command succeeded without producing {}", path.display());
    let dataset = Dataset::open(path)?;
    assert_eq!(dataset.raster_count(), bases.len(), "{case}");
    assert_cog_layout(&dataset, path)?;
    let transform = assert_georeferencing(&dataset)?;
    for (index, &base) in bases.iter().enumerate() {
        let band_index = index + 1;
        assert_band_samples(&dataset, band_index, transform, case, |row, column| {
            generated_sample(base, row, column)
        })?;
        let band = dataset.rasterband(band_index)?;
        assert_eq!(band.scale(), Some(0.01), "{case}, band {band_index}: packed scale");
        assert_eq!(band.offset().unwrap_or(0.0), 0.0, "{case}, band {band_index}: zero offset");
    }
    Ok(())
}

#[test]
fn selected_netcdf_variables_preserve_raw_samples_in_final_cogs() -> TestResult {
    let cases: [(&str, &[&str], &[&str]); 4] = [
        ("primary_automatic_multiband", &["--variable", "WBGT"], &["WBGT"]),
        (
            "transposed_explicit_multiband",
            &["--variable", "WBGT_transposed", "--dimensions", "time,y,x", "--multi-band"],
            &["WBGT_transposed"],
        ),
        (
            "repeated_reverse_selection",
            &["--variable", "WBGT_transposed", "--variable", "WBGT"],
            &["WBGT_transposed", "WBGT"],
        ),
        (
            "comma_selection",
            &["--variable", "WBGT,WBGT_transposed"],
            &["WBGT", "WBGT_transposed"],
        ),
    ];
    for (case, selection, variables) in cases {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join(format!("{case}.tif"));
        let output = run_createcog(&path, selection);
        assert!(output.status.success(), "{case}: {}", command_diagnostics(&output));
        assert!(path.is_file(), "{case}: command succeeded without producing {}", path.display());
        let dataset = Dataset::open(&path)?;
        assert_eq!(dataset.raster_count(), 3 * variables.len(), "{case}");
        assert_cog_layout(&dataset, &path)?;
        let transform = assert_georeferencing(&dataset)?;
        assert_final_samples(&dataset, variables, transform, case)?;
        for band_index in 1..=dataset.raster_count() {
            let band = dataset.rasterband(band_index)?;
            assert_eq!(band.scale(), Some(0.01), "{case}, band {band_index}: packed scale");
            assert_eq!(band.offset().unwrap_or(0.0), 0.0, "{case}, band {band_index}: implicit zero offset");
        }
    }
    Ok(())
}

#[test]
fn netcdf_glob_sorts_files_with_automatic_multiband_detection() -> TestResult {
    assert_netcdf_glob_file_order(&[])
}

#[test]
fn netcdf_glob_sorts_files_with_explicit_multiband() -> TestResult {
    assert_netcdf_glob_file_order(&["--multi-band"])
}

fn assert_netcdf_glob_file_order(extra_args: &[&str]) -> TestResult {
    let directory = tempfile::tempdir()?;
    // Creation order is deliberately the reverse of lexical order, with distinct file samples.
    create_sample_raster(&directory.path().join("b.nc"), "netCDF", &[2000])?;
    create_sample_raster(&directory.path().join("a.nc"), "netCDF", &[1000])?;
    let pattern = directory.path().join("*.nc");
    let path = directory.path().join("sorted.tif");
    let mut selection = vec!["--variable", "Band1", "--dimensions", "y,x"];
    selection.extend_from_slice(extra_args);
    let output = run_createcog_with_input(&pattern, &path, &selection);
    assert!(output.status.success(), "{}", command_diagnostics(&output));
    assert_generated_cog(&path, &[1000, 2000], "lexical file order")
}

#[test]
fn netcdf_glob_is_file_major_then_selected_variable_order() -> TestResult {
    let directory = tempfile::tempdir()?;
    create_sample_raster(&directory.path().join("b.nc"), "netCDF", &[21000, 22000])?;
    create_sample_raster(&directory.path().join("a.nc"), "netCDF", &[11000, 12000])?;
    let pattern = directory.path().join("*.nc");
    let path = directory.path().join("variables.tif");
    let output = run_createcog_with_input(
        &pattern,
        &path,
        &["--variable", "Band2", "--variable", "Band1", "--dimensions", "y,x"],
    );
    assert!(output.status.success(), "{}", command_diagnostics(&output));
    assert_generated_cog(&path, &[12000, 11000, 22000, 21000], "file-major, reverse variable selection")
}

#[test]
fn generated_netcdf_variables_preserve_raw_samples() -> TestResult {
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("variables.nc");
    create_sample_raster(&input, "netCDF", &[11000, 12000])?;
    let path = directory.path().join("variables.tif");
    let output = run_createcog_with_input(&input, &path, &["--variable", "Band2,Band1", "--dimensions", "y,x"]);
    assert!(output.status.success(), "{}", command_diagnostics(&output));
    assert_generated_cog(&path, &[12000, 11000], "generated variables")
}

#[test]
fn generated_netcdf_time_slices_preserve_raw_samples() -> TestResult {
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("series.nc");
    create_time_series_netcdf(&input, &[1000, 2000, 3000])?;
    let path = directory.path().join("series.tif");
    let output = run_createcog_with_input(&input, &path, &["--variable", "Band1", "--dimensions", "time,y,x"]);
    assert!(output.status.success(), "{}", command_diagnostics(&output));
    assert_generated_cog(&path, &[1000, 2000, 3000], "generated time slices")?;
    let dataset = Dataset::open(&path)?;
    for (index, hour) in [2, 1, 0].into_iter().enumerate() {
        assert_eq!(
            dataset.rasterband(index + 1)?.description()?,
            format!("Band1: 2024-01-01T{hour:02}:00:00Z")
        );
    }
    Ok(())
}

#[test]
fn netcdf_band_descriptions_decode_time_units_and_preserve_fallback_labels() -> TestResult {
    let cases: [(&str, &[(&str, &str)], &[&str]); 7] = [
        (
            "fractional_negative_days",
            &[("time#units", "days since 2024-01-01"), ("NETCDF_DIM_time_VALUES", "{-0.5,0.25}")],
            &["Band1: 2023-12-31T12:00:00Z", "Band1: 2024-01-01T06:00:00Z"],
        ),
        (
            "milliseconds",
            &[
                ("time#units", "milliseconds since 2024-01-01T00:00:00Z"),
                ("NETCDF_DIM_time_VALUES", "{-500,1250}"),
            ],
            &["Band1: 2023-12-31T23:59:59.500Z", "Band1: 2024-01-01T00:00:01.250Z"],
        ),
        (
            "timezone",
            &[("time#units", "hours since 2024-01-01 02:00:00 +02:00")],
            &["Band1: 2024-01-01T01:00:00Z", "Band1: 2024-01-01T00:00:00Z"],
        ),
        ("unsupported_calendar", &[("time#calendar", "360_day")], &["Band1[0]", "Band1[1]"]),
        (
            "unsupported_units",
            &[("time#units", "months since 2024-01-01")],
            &["Band1[0]", "Band1[1]"],
        ),
        ("non_time_dimension", &[("time#units", "metres")], &["Band1[0]", "Band1[1]"]),
        ("invalid_epoch", &[("time#units", "hours since invalid")], &["Band1[0]", "Band1[1]"]),
    ];
    for (case, metadata, descriptions) in cases {
        let directory = tempfile::tempdir()?;
        let input = directory.path().join("series.nc");
        create_time_series_netcdf_with_metadata(&input, &[1000, 2000], false, metadata)?;
        let path = directory.path().join("series.tif");
        let output = run_createcog_with_input(&input, &path, &["--variable", "Band1", "--multi-band"]);
        assert!(output.status.success(), "{case}: {}", command_diagnostics(&output));
        let dataset = Dataset::open(&path)?;
        assert_cog_layout(&dataset, &path)?;
        for (index, expected) in descriptions.iter().enumerate() {
            assert_eq!(dataset.rasterband(index + 1)?.description()?, *expected, "{case}, band {index}");
        }
    }
    Ok(())
}

#[test]
fn bottom_up_netcdf_time_slices_are_flipped_north_up_for_multiband_cogs() -> TestResult {
    let directory = tempfile::tempdir()?;
    let input = directory.path().join("series.nc");
    create_time_series_netcdf_with_row_order(&input, &[1000, 2000, 3000], true)?;
    let path = directory.path().join("series.tif");
    // gdalbuildvrt -separate rejects a positive NS resolution, so the rows must be reversed.
    let output = run_createcog_with_input(&input, &path, &["--variable", "Band1", "--dimensions", "time,y,x", "--multi-band"]);
    assert!(output.status.success(), "{}", command_diagnostics(&output));
    assert_generated_cog(&path, &[1000, 2000, 3000], "bottom-up time slices")
}

#[test]
fn netcdf_glob_appends_all_time_slices_before_the_next_file() -> TestResult {
    let directory = tempfile::tempdir()?;
    create_time_series_netcdf(&directory.path().join("b.nc"), &[21000, 22000, 23000])?;
    create_time_series_netcdf(&directory.path().join("a.nc"), &[11000, 12000, 13000])?;
    let pattern = directory.path().join("*.nc");
    let path = directory.path().join("times.tif");
    let output = run_createcog_with_input(&pattern, &path, &["--variable", "Band1", "--dimensions", "time,y,x"]);
    assert!(output.status.success(), "{}", command_diagnostics(&output));
    assert_generated_cog(&path, &[11000, 12000, 13000, 21000, 22000, 23000], "file-major time slices")
}

#[test]
fn unmatched_netcdf_glob_is_rejected_without_creating_output() -> TestResult {
    let directory = tempfile::tempdir()?;
    let pattern = directory.path().join("*.nc");
    let path = directory.path().join("unmatched.tif");
    let output = run_createcog_with_input(&pattern, &path, &["--variable", "Band1", "--dimensions", "y,x"]);
    assert!(!output.status.success(), "{}", command_diagnostics(&output));
    assert!(!path.exists(), "{}", command_diagnostics(&output));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("No files match the input pattern") && stderr.contains(pattern.to_str().unwrap()),
        "{}",
        command_diagnostics(&output)
    );
    Ok(())
}

#[test]
fn missing_requested_variable_in_later_glob_file_is_rejected_without_creating_output() -> TestResult {
    let directory = tempfile::tempdir()?;
    create_sample_raster(&directory.path().join("b.nc"), "netCDF", &[21000])?;
    create_sample_raster(&directory.path().join("a.nc"), "netCDF", &[11000, 12000])?;
    let pattern = directory.path().join("*.nc");
    let path = directory.path().join("missing-later.tif");
    let output = run_createcog_with_input(&pattern, &path, &["--variable", "Band1,Band2", "--dimensions", "y,x"]);
    assert!(!output.status.success(), "{}", command_diagnostics(&output));
    assert!(!path.exists(), "{}", command_diagnostics(&output));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Cannot open NetCDF variable Band2"),
        "{}",
        command_diagnostics(&output)
    );
    Ok(())
}

#[test]
fn tiff_glob_still_appends_bands_in_file_major_order() -> TestResult {
    let directory = tempfile::tempdir()?;
    create_sample_raster(&directory.path().join("b.tif"), "GTiff", &[21000, 22000])?;
    create_sample_raster(&directory.path().join("a.tif"), "GTiff", &[11000, 12000])?;
    // Keep the output outside the glob, so it cannot become an input.
    let pattern = directory.path().join("[ab].tif");
    let path = directory.path().join("sorted.tif");
    let output = run_createcog_with_input(&pattern, &path, &["--multi-band"]);
    assert!(output.status.success(), "{}", command_diagnostics(&output));
    assert_generated_cog(&path, &[11000, 12000, 21000, 22000], "TIFF glob file-major order")
}

#[test]
fn missing_netcdf_variable_is_rejected_without_creating_output() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("missing.tif");
    let output = run_createcog(&path, &["--variable", "not_a_fixture_variable"]);
    assert!(!output.status.success(), "{}", command_diagnostics(&output));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("Cannot open NetCDF variable not_a_fixture_variable"),
        "{}",
        command_diagnostics(&output)
    );
    assert!(!path.exists());
    Ok(())
}

#[test]
fn ambiguous_netcdf_variables_require_explicit_selection() -> TestResult {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("ambiguous.tif");
    let output = run_createcog(&path, &[]);
    assert!(!output.status.success(), "{}", command_diagnostics(&output));
    let stderr = String::from_utf8_lossy(&output.stderr);
    for expected in ["Expected one numeric NetCDF variable", "WBGT", "WBGT_transposed", "--variable"] {
        assert!(stderr.contains(expected), "Expected {expected:?}: {}", command_diagnostics(&output));
    }
    assert!(!path.exists());
    Ok(())
}

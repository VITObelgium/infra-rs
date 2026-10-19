use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use gdal::{
    Dataset, DatasetOptions, GdalOpenFlags,
    raster::{Dimension, Group, MDArray},
};

use crate::input::resolve_local_input_paths;

pub struct PreparedInput {
    pub input: String,
    _directory: Option<tempfile::TempDir>,
}

/// Expose named `NetCDF` dimensions as a lazy raster view, without copying the cube.
pub fn prepare_input(input: &str, variables: &[String], dimensions: &[String], source_srs: Option<&str>) -> Result<PreparedInput> {
    let (path, variables) = parse_input_path(input, variables)?;
    let Some(paths) = resolve_local_input_paths(input, &path)? else {
        // Leave GDAL connection strings to the existing raster pipeline.
        ensure!(
            variables.is_empty(),
            "NetCDF variable selection requires a local input file: {}",
            path.display()
        );
        return Ok(PreparedInput {
            input: input.to_owned(),
            _directory: None,
        });
    };

    if !is_netcdf(&paths[0])? {
        ensure!(variables.is_empty(), "--variable is only supported for NetCDF inputs");
        return Ok(PreparedInput {
            input: input.to_owned(),
            _directory: None,
        });
    }

    validate_dimensions(dimensions)?;
    let vrt = build_vrt(&paths, variables, dimensions, source_srs)?;
    vrt.write_input()
}

fn parse_input_path(input: &str, variables: &[String]) -> Result<(PathBuf, Vec<String>)> {
    let Some(subdataset) = input.strip_prefix("NETCDF:") else {
        return Ok((PathBuf::from(input), variables.to_vec()));
    };

    let (filename, variable) = subdataset.rsplit_once(':').context("Expected NETCDF:\"filename\":variable")?;
    ensure!(
        variables.is_empty(),
        "Select variables using either --variable or a NETCDF subdataset, not both"
    );

    Ok((PathBuf::from(filename.trim_matches('"')), vec![variable.to_owned()]))
}

fn is_netcdf(path: &Path) -> Result<bool> {
    let dataset = Dataset::open_ex(
        path,
        DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_RASTER,
            ..Default::default()
        },
    )?;
    Ok(dataset.driver().short_name() == "netCDF")
}

fn validate_dimensions(dimensions: &[String]) -> Result<()> {
    ensure!(
        matches!(dimensions.len(), 2 | 3),
        "--dimensions must contain y,x or band,y,x dimension names"
    );
    ensure!(dimensions.iter().all(|name| !name.is_empty()), "Dimension names must not be empty");
    ensure!(
        dimensions.iter().enumerate().all(|(i, name)| !dimensions[..i].contains(name)),
        "Dimension names must be unique"
    );

    Ok(())
}

fn build_vrt(paths: &[PathBuf], mut variables: Vec<String>, dimensions: &[String], source_srs: Option<&str>) -> Result<NetcdfVrt> {
    let mut vrt = NetcdfVrt::default();

    for path in paths {
        let dataset = Dataset::open_ex(
            path,
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_MULTIDIM_RASTER,
                allowed_drivers: Some(&["netCDF"]),
                ..Default::default()
            },
        )
        .with_context(|| format!("Cannot open NetCDF input {}", path.display()))?;

        let group = dataset.root_group()?;
        if variables.is_empty() {
            variables = select_variables(&group, dimensions)?;
        }

        let path = path.canonicalize()?;
        let filename = escape_xml(path.to_str().context("NetCDF input path is not valid UTF-8")?);
        for variable in &variables {
            let array = group
                .open_md_array(variable, Default::default())
                .with_context(|| format!("Cannot open NetCDF variable {variable} in {}", path.display()))?;
            ensure!(array.datatype().class().is_numeric(), "NetCDF variable {variable} must be numeric");
            let dims = array.dimensions()?;
            let order = resolve_dimension_indices(variable, &dims, dimensions)?;
            let grid = Grid::from_variable(&group, &array, variable, &dims, &order, source_srs)?;
            vrt.check_grid(grid, &path)?;

            let data_type = gdal::raster::GdalDataType::try_from(array.datatype().numeric_datatype())?.name();
            vrt.check_sample_encoding(&data_type, array.no_data_value_as_double(), &path)?;
            vrt.append_bands(&array, variable, &filename, &data_type, &dims, &order);
        }
    }
    Ok(vrt)
}

fn select_variables(group: &Group<'_>, dimensions: &[String]) -> Result<Vec<String>> {
    let candidates = group
        .array_names(Default::default())
        .into_iter()
        .filter(|name| {
            group.open_md_array(name, Default::default()).is_ok_and(|array| {
                array.datatype().class().is_numeric()
                    && array.dimensions().is_ok_and(|dims| {
                        dims.len() == dimensions.len() && dimensions.iter().all(|name| dims.iter().any(|dim| dim.name() == *name))
                    })
            })
        })
        .collect::<Vec<_>>();

    ensure!(
        candidates.len() == 1,
        "Expected one numeric NetCDF variable with dimensions {}; found [{}]. Select explicitly with --variable",
        dimensions.join(","),
        candidates.join(", ")
    );

    Ok(candidates)
}

fn resolve_dimension_indices(variable: &str, dims: &[Dimension<'_>], dimensions: &[String]) -> Result<Vec<usize>> {
    let names = dims.iter().map(|dim| dim.name()).collect::<Vec<_>>();
    ensure!(
        names.len() == dimensions.len(),
        "Variable {variable} has dimensions [{}], expected [{}]",
        names.join(","),
        dimensions.join(",")
    );
    let order = dimensions
        .iter()
        .map(|name| {
            names
                .iter()
                .position(|candidate| candidate == name)
                .with_context(|| format!("Variable {variable} has no dimension {name}; available: {}", names.join(",")))
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(dims.iter().all(|dim| dim.size() > 0), "Variable {variable} has an empty dimension");
    Ok(order)
}

#[derive(PartialEq)]
struct Grid {
    width: usize,
    height: usize,
    transform: [f64; 6],
    srs: String,
}

impl Grid {
    fn from_variable(
        group: &Group<'_>,
        array: &MDArray<'_>,
        variable: &str,
        dims: &[Dimension<'_>],
        order: &[usize],
        source_srs: Option<&str>,
    ) -> Result<Self> {
        let y = &dims[order[order.len() - 2]];
        let x = &dims[order[order.len() - 1]];
        let (x_origin, x_step) = coordinate_grid(group, &x.name(), x.size())?;
        let (y_origin, y_step) = coordinate_grid(group, &y.name(), y.size())?;
        let srs = variable_srs(array, variable, source_srs)?;
        Ok(Self {
            width: x.size(),
            height: y.size(),
            transform: [x_origin, x_step, 0.0, y_origin, 0.0, y_step],
            srs,
        })
    }
}

fn variable_srs(array: &MDArray<'_>, variable: &str, source_srs: Option<&str>) -> Result<String> {
    if let Some(srs) = source_srs {
        return Ok(gdal::spatial_ref::SpatialRef::from_definition(srs)?.to_wkt()?);
    }
    Ok(array
        .spatial_reference()
        .with_context(|| format!("Variable {variable} has no CRS; supply --source-srs"))?
        .to_wkt()?)
}

#[derive(Default)]
struct NetcdfVrt {
    band_xml: String,
    band_index: usize,
    grid: Option<Grid>,
    sample_encoding: Option<(String, Option<String>)>,
}

impl NetcdfVrt {
    fn check_grid(&mut self, grid: Grid, path: &Path) -> Result<()> {
        if let Some(reference) = &self.grid {
            ensure!(
                reference == &grid,
                "Selected NetCDF variables across all input files must have the same grid and CRS ({})",
                path.display()
            );
        } else {
            self.grid = Some(grid);
        }
        Ok(())
    }

    fn check_sample_encoding(&mut self, data_type: &str, nodata: Option<f64>, path: &Path) -> Result<()> {
        let encoding = (data_type.to_owned(), nodata.map(|value| value.to_string()));
        if let Some(reference) = &self.sample_encoding {
            ensure!(
                reference == &encoding,
                "Selected NetCDF variables across all input files must have the same sample type and nodata value ({})",
                path.display()
            );
        } else {
            self.sample_encoding = Some(encoding);
        }
        Ok(())
    }

    fn append_bands(
        &mut self,
        array: &MDArray<'_>,
        variable: &str,
        filename: &str,
        data_type: &str,
        dims: &[Dimension<'_>],
        order: &[usize],
    ) {
        let transpose = order.iter().map(usize::to_string).collect::<Vec<_>>().join(",");
        let count = if order.len() == 3 { dims[order[0]].size() } else { 1 };
        let metadata = band_metadata(array);
        let source_array = escape_xml(&format!("/{}", variable.trim_start_matches('/')));
        for index in 0..count {
            self.band_index += 1;
            let band_index = self.band_index;
            let view = slice_view(order.len(), index);
            // The multidimensional API retains stored row order, unlike the classic
            // NetCDF driver. Signed coordinate spacing supplies the matching grid.
            self.band_xml.push_str(&format!(
                "<VRTRasterBand dataType=\"{data_type}\" band=\"{band_index}\">\
                 <Description>{}[{index}]</Description>{metadata}\
                 <ArraySource><DerivedArray><SingleSourceArray>\
                 <SourceFilename relativeToVRT=\"0\">{filename}</SourceFilename><SourceArray>{source_array}</SourceArray>\
                 </SingleSourceArray><Step><Transpose newOrder=\"{transpose}\"/></Step>{view}\
                 </DerivedArray></ArraySource></VRTRasterBand>",
                escape_xml(variable)
            ));
        }
    }

    fn write_input(self) -> Result<PreparedInput> {
        let Grid {
            width,
            height,
            transform,
            srs,
        } = self.grid.context("No NetCDF variables selected")?;
        let transform = transform.iter().map(f64::to_string).collect::<Vec<_>>().join(",");
        let band_xml = self.band_xml;
        let xml = format!(
            "<VRTDataset rasterXSize=\"{width}\" rasterYSize=\"{height}\"><SRS>{}</SRS><GeoTransform>{transform}</GeoTransform>{band_xml}</VRTDataset>",
            escape_xml(&srs)
        );
        let directory = tempfile::tempdir()?;
        let vrt = directory.path().join("netcdf.vrt");
        std::fs::write(&vrt, xml)?;
        Ok(PreparedInput {
            input: vrt.to_string_lossy().into_owned(),
            _directory: Some(directory),
        })
    }
}

fn band_metadata(array: &MDArray<'_>) -> String {
    let nodata = array
        .no_data_value_as_double()
        .map(|value| format!("<NoDataValue>{value}</NoDataValue>"))
        .unwrap_or_default();
    let scale = array
        .attribute("scale_factor")
        .map(|value| format!("<Scale>{}</Scale>", value.read_as_f64()))
        .unwrap_or_default();
    let offset = array
        .attribute("add_offset")
        .map(|value| format!("<Offset>{}</Offset>", value.read_as_f64()))
        .unwrap_or_default();
    let unit = escape_xml(&array.unit());
    format!("{nodata}{scale}{offset}<UnitType>{unit}</UnitType>")
}

fn slice_view(dimension_count: usize, index: usize) -> String {
    if dimension_count == 3 {
        return format!("<Step><View expr=\"[{index},:,:]\"/></Step>");
    }
    String::new()
}

fn coordinate_grid(group: &Group<'_>, name: &str, size: usize) -> Result<(f64, f64)> {
    let coordinate = group
        .open_md_array(name, Default::default())
        .with_context(|| format!("Missing coordinate variable {name}"))?;
    ensure!(
        coordinate.num_dimensions() == 1 && coordinate.num_elements() == size as u64 && size >= 2,
        "Coordinate {name} must be one-dimensional with at least two cell centers"
    );
    let mut values = coordinate.read_as::<f64>(vec![0], vec![size])?;
    if let Some(nodata) = coordinate.no_data_value_as_double() {
        ensure!(!values.contains(&nodata), "Coordinate {name} contains nodata");
    }
    let scale = coordinate.attribute("scale_factor").map_or(1.0, |value| value.read_as_f64());
    let offset = coordinate.attribute("add_offset").map_or(0.0, |value| value.read_as_f64());
    // Raster samples remain packed, but grid coordinates must be in CRS units.
    for value in &mut values {
        *value = *value * scale + offset;
    }
    let precision =
        if gdal::raster::GdalDataType::try_from(coordinate.datatype().numeric_datatype())? == gdal::raster::GdalDataType::Float32 {
            f64::from(f32::EPSILON)
        } else {
            f64::EPSILON
        };
    regular_grid(name, &values, precision)
}

fn regular_grid(name: &str, values: &[f64], precision: f64) -> Result<(f64, f64)> {
    ensure!(values.len() >= 2, "Coordinate {name} needs at least two cell centers");
    let step = (values[values.len() - 1] - values[0]) / (values.len() - 1) as f64;
    let magnitude = values.iter().copied().map(f64::abs).fold(0.0, f64::max);
    let tolerance = step.abs() * 1e-5 + magnitude * precision;
    ensure!(
        step.is_finite()
            && step != 0.0
            && values.windows(2).all(|pair| (pair[1] - pair[0]) * step > 0.0)
            && values
                .iter()
                .enumerate()
                .all(|(index, value)| value.is_finite() && (value - (values[0] + index as f64 * step)).abs() <= tolerance),
        "Coordinate {name} must be finite, monotonic and regularly spaced"
    );
    Ok((values[0] - step / 2.0, step))
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn fixture() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/uc-small.nc")
    }

    fn dimensions() -> Vec<String> {
        ["time", "y", "x"].map(str::to_owned).to_vec()
    }

    #[test]
    fn malformed_subdataset_selection_is_rejected() {
        let error = parse_input_path("NETCDF:filename", &[]).unwrap_err();
        assert_eq!(error.to_string(), "Expected NETCDF:\"filename\":variable");
    }

    #[test]
    fn subdataset_and_explicit_variable_selection_cannot_be_combined() {
        let error = parse_input_path("NETCDF:\"filename.nc\":WBGT", &["WBGT".into()]).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Select variables using either --variable or a NETCDF subdataset, not both"
        );
    }

    #[test]
    fn connection_strings_bypass_local_globbing_and_dimension_validation() -> Result<()> {
        let input = "https://example.com/weather.nc?variable=WBGT";
        let prepared = prepare_input(input, &[], &[], None)?;
        assert_eq!(prepared.input, input);
        assert!(prepared._directory.is_none());
        Ok(())
    }

    #[test]
    fn connection_strings_reject_variable_selection() {
        let input = "https://example.com/weather.nc?variable=WBGT";
        let error = prepare_input(input, &["WBGT".into()], &dimensions(), None).err().unwrap();
        assert!(error.to_string().contains("NetCDF variable selection requires a local input file"));
    }

    #[test]
    fn invalid_dimension_names_and_counts_are_rejected() {
        for names in [vec![], vec!["x"], vec!["time", "level", "y", "x"], vec!["", "x"], vec!["y", "y"]] {
            let dimensions = names.into_iter().map(str::to_owned).collect::<Vec<_>>();
            assert!(validate_dimensions(&dimensions).is_err(), "{dimensions:?}");
        }
    }

    #[test]
    fn selected_variables_are_transposed_to_time_y_x_without_flipping_rows() -> Result<()> {
        for (variable, base) in [("WBGT", 0), ("WBGT_transposed", 10000)] {
            let prepared = prepare_input(fixture().to_str().unwrap(), &[variable.into()], &dimensions(), None)?;
            let dataset = Dataset::open(&prepared.input)?;
            assert_eq!(dataset.raster_count(), 3);
            assert_eq!(dataset.raster_size(), (5, 4));
            assert_eq!(dataset.geo_transform()?, [5723000.0, 100.0, 0.0, 2985000.0, 0.0, -100.0]);
            assert_eq!(dataset.spatial_ref()?.auth_code()?, 3857);
            for t in 0..3 {
                let band = dataset.rasterband(t + 1)?;
                assert_eq!(band.no_data_value(), Some(-32768.0));
                assert_eq!(band.scale(), Some(0.01));
                let values = band.read_as::<i32>((0, 0), (5, 4), (5, 4), None)?;
                let expected = (0..4)
                    .flat_map(|r| {
                        (0..5).map(move |c| {
                            if (base == 0 && (t, r, c) == (1, 2, 3)) || (base != 0 && (t, r, c) == (2, 0, 4)) {
                                -32768
                            } else {
                                base + 1000 * (t as i32 + 1) + 100 * r + c
                            }
                        })
                    })
                    .collect::<Vec<_>>();
                assert_eq!(values.data(), &expected, "{variable}, time index {t}");
            }
        }
        Ok(())
    }

    #[test]
    fn multiple_variables_are_stacked_in_selection_order() -> Result<()> {
        let prepared = prepare_input(
            fixture().to_str().unwrap(),
            &["WBGT_transposed".into(), "WBGT".into()],
            &dimensions(),
            None,
        )?;
        let dataset = Dataset::open(&prepared.input)?;
        assert_eq!(dataset.raster_count(), 6);
        for (index, expected) in [11000, 12000, 13000, 1000, 2000, 3000].into_iter().enumerate() {
            assert_eq!(
                dataset.rasterband(index + 1)?.read_as::<i32>((0, 0), (1, 1), (1, 1), None)?.data(),
                &[expected]
            );
        }
        Ok(())
    }

    #[test]
    fn a_single_two_dimensional_variable_is_selected_automatically() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("surface.nc");
        let georef = geo::GeoReference::new(
            gdal::spatial_ref::SpatialRef::from_epsg(3857)?.to_wkt()?,
            geo::RasterSize::with_rows_cols(geo::Rows(4), geo::Columns(5)),
            [5723000.0, 100.0, 0.0, 2985000.0, 0.0, -100.0].into(),
            None,
            None,
        );
        let samples: Vec<i32> = (0..20).collect();
        geo::raster::io::write_raster_band(&path, &georef, &samples, geo::raster::WriteRasterOptions::Default)?;
        let prepared = prepare_input(path.to_str().unwrap(), &[], &["y".into(), "x".into()], None)?;
        let dataset = Dataset::open(&prepared.input)?;
        assert_eq!(dataset.raster_count(), 1);
        assert_eq!(dataset.raster_size(), (5, 4));
        assert_eq!(dataset.geo_transform()?, [5723000.0, 100.0, 0.0, 2984600.0, 0.0, 100.0]);
        assert_eq!(dataset.spatial_ref()?.auth_code()?, 3857);
        // GDAL's NetCDF writer stores rows bottom-up, so the signed coordinate
        // spacing and the sample order must agree rather than assuming north-up.
        let values = dataset.rasterband(1)?.read_as::<i32>((0, 0), (5, 4), (5, 4), None)?;
        let expected = (0..4)
            .rev()
            .flat_map(|row| (0..5).map(move |column| row * 5 + column))
            .collect::<Vec<_>>();
        assert_eq!(values.data(), &expected);
        Ok(())
    }

    #[test]
    fn subdataset_selection_handles_xml_characters_and_cleans_up_the_vrt() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("weather & data.nc");
        std::fs::copy(fixture(), &path)?;
        let prepared = prepare_input(
            &format!("NETCDF:\"{}\":WBGT", path.display()),
            &[],
            &dimensions(),
            Some("EPSG:3857"),
        )?;
        let vrt_path = PathBuf::from(&prepared.input);
        let dataset = Dataset::open(&vrt_path)?;
        assert_eq!(dataset.raster_count(), 3);
        assert_eq!(dataset.rasterband(1)?.read_as::<i32>((0, 0), (1, 1), (1, 1), None)?.data(), &[1000]);
        drop(dataset);
        drop(prepared);
        assert!(!vrt_path.exists());
        Ok(())
    }

    #[test]
    fn ambiguous_variables_require_explicit_selection() {
        let error = prepare_input(fixture().to_str().unwrap(), &[], &dimensions(), None)
            .err()
            .unwrap()
            .to_string();
        assert!(
            error.contains("--variable") && error.contains("WBGT") && error.contains("WBGT_transposed"),
            "{error}"
        );
    }

    #[test]
    fn invalid_variable_and_dimension_selections_are_rejected() {
        for (variables, dims) in [
            (vec!["missing".into()], dimensions()),
            (vec!["WBGT".into()], vec!["time".into(), "y".into(), "missing".into()]),
            (vec!["WBGT".into()], vec!["time".into(), "y".into(), "y".into()]),
            (vec!["WBGT".into()], vec!["y".into(), "x".into()]),
        ] {
            assert!(prepare_input(fixture().to_str().unwrap(), &variables, &dims, None).is_err());
        }
    }

    #[test]
    fn float32_coordinate_rounding_is_not_mistaken_for_an_irregular_grid() -> Result<()> {
        let values = [100.0_f32, 100.1, 100.2, 100.3].map(f64::from);
        let (origin, step) = regular_grid("x", &values, f64::from(f32::EPSILON))?;
        assert!((step - 0.1).abs() < 1e-5);
        assert!((origin - 99.95).abs() < 1e-5);
        Ok(())
    }

    #[test]
    fn irregular_coordinates_are_rejected() {
        for values in [&[0.0, 1.0, 3.0][..], &[0.0, 0.0], &[0.0, f64::NAN], &[0.0]] {
            assert!(regular_grid("x", values, f64::EPSILON).is_err());
        }
    }
}

use std::{
    io::{self, IsTerminal, Write},
    path::PathBuf,
    sync::Arc,
};

use clap::Parser;
use createtiles::TileCreationOptions;
use env_logger::{Env, Target, TimestampPrecision, WriteStyle};
use kdam::{BarExt, Column, RichProgress, tqdm};
use parking_lot::Mutex;

use crate::createtiles::{ZoomLevelSelection, create_cog_tiles, print_gdal_translate_command};

pub type Result<T> = anyhow::Result<T>;

mod createtiles;
mod input;
mod netcdf;
mod unscale;

struct ProgressWriter {
    progress: Arc<Mutex<RichProgress>>,
}

impl Write for ProgressWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let message = String::from_utf8_lossy(buffer);
        let message = message.trim_end_matches(['\r', '\n']);

        if !message.is_empty() {
            self.progress.lock().write(message).map_err(io::Error::other)?;
        }

        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Parser, Debug)]
#[clap(name = "createcog", about = "Create Cloud Optimized GeoTIFF")]
#[command(version)]
pub struct Opt {
    #[arg(long = "input", short = 'i')]
    pub input: String,

    #[arg(long = "output", short = 'o')]
    pub output: PathBuf,

    #[arg(long = "min-zoom")]
    pub min_zoom: Option<i32>,

    #[arg(long = "max-zoom")]
    pub max_zoom: Option<i32>,

    #[arg(long = "zoom-level-selection", short = 'z', value_name = "closest|prefer-higher|prefer-lower")]
    pub zoom_level_selection: Option<ZoomLevelSelection>,

    #[arg(long = "tile-size", default_value = "512")]
    pub tile_size: u32,

    #[arg(long = "multi-band")]
    pub multi_band: bool,

    /// Select `NetCDF` variables in output order; repeat the flag or separate names with commas.
    #[arg(long = "variable", value_delimiter = ',', num_args = 1)]
    pub variables: Vec<String>,

    /// `NetCDF` dimension names in output axis-role order: [band/time,]y,x, not source storage order.
    ///
    /// Supply 2 or 3 actual dimension names. The last name selects horizontal columns,
    /// the second-to-last selects vertical rows, and the optional first selects band slices.
    /// Source arrays are automatically transposed into this order. For a source stored
    /// as [time,x,y], use time,y,x when x is horizontal and y is vertical; time,x,y
    /// instead assigns x to rows and y to columns. Roles are positional, not inferred
    /// from the names.
    #[arg(long = "dimensions", value_delimiter = ',', default_value = "time,y,x", num_args = 1)]
    pub dimensions: Vec<String>,

    /// Scale to an unsigned 8-bit or 16-bit raster.
    #[arg(long = "scale", value_name = "8|16", value_parser = parse_scale_bits)]
    pub scale: Option<u8>,

    /// Select the output sample type (e.g. Float32/f32 or UInt16/u16); with --scale, it must match the encoded unsigned type.
    #[arg(long = "output-data-type", value_name = "TYPE", value_parser = parse_data_type)]
    pub output_data_type: Option<geo::ArrayDataType>,

    /// Decode physical = raw * scale + offset before output scaling; defaults to Float64 without --scale or --output-data-type.
    #[arg(long = "unscale")]
    pub unscale: bool,

    /// Assign a source CRS when the input has no projection metadata (for example, EPSG:4326 `NetCDF` data).
    #[arg(long = "source-srs")]
    pub source_srs: Option<String>,

    #[arg(long = "aligned-levels")]
    pub aligned_levels: Option<i32>,

    #[arg(long = "noprogress")]
    pub no_progress: bool,

    #[arg(long = "gdal-cmd")]
    pub print_command: bool,
}

fn parse_scale_bits(value: &str) -> std::result::Result<u8, String> {
    match value {
        "8" => Ok(8),
        "16" => Ok(16),
        _ => Err("Scaling bit depth must be 8 or 16".to_string()),
    }
}

fn parse_data_type(value: &str) -> std::result::Result<geo::ArrayDataType, String> {
    use geo::ArrayDataType;

    match value.to_ascii_lowercase().as_str() {
        "int8" | "i8" => Ok(ArrayDataType::Int8),
        "uint8" | "byte" | "u8" => Ok(ArrayDataType::Uint8),
        "int16" | "i16" => Ok(ArrayDataType::Int16),
        "uint16" | "u16" => Ok(ArrayDataType::Uint16),
        "int32" | "i32" => Ok(ArrayDataType::Int32),
        "uint32" | "u32" => Ok(ArrayDataType::Uint32),
        "int64" | "i64" => Ok(ArrayDataType::Int64),
        "uint64" | "u64" => Ok(ArrayDataType::Uint64),
        "float32" | "f32" => Ok(ArrayDataType::Float32),
        "float64" | "f64" => Ok(ArrayDataType::Float64),
        _ => Err(format!(
            "Invalid data type '{value}'; expected Int8, UInt8 (Byte), Int16, UInt16, Int32, UInt32, Int64, UInt64, Float32, or Float64; Rust aliases i8/u8, i16/u16, i32/u32, i64/u64, f32/f64 are also accepted (case-insensitive)"
        )),
    }
}

fn main() -> Result<()> {
    let opt = Opt::parse();

    let available_cores = std::thread::available_parallelism()?.get();
    let rayon_threads = (available_cores * 3 / 4).max(1);
    rayon::ThreadPoolBuilder::new().num_threads(rayon_threads).build_global()?;

    let stderr_is_terminal = std::io::stderr().is_terminal();
    kdam::term::init(stderr_is_terminal);

    let show_progress = !opt.no_progress && !opt.print_command;
    let progress = Arc::new(Mutex::new(RichProgress::new(
        tqdm!(total = 100, disable = !show_progress),
        vec![
            Column::Text("[bold blue]Creating COG".to_owned()),
            Column::Animation,
            Column::Percentage(1),
            Column::Text("•".to_owned()),
            Column::ElapsedTime,
            Column::Text("•".to_owned()),
            Column::RemainingTime,
        ],
    )));

    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or("warn"));
    logger.format_timestamp(Some(TimestampPrecision::Millis));
    if show_progress {
        logger.target(Target::Pipe(Box::new(ProgressWriter {
            progress: Arc::clone(&progress),
        })));

        let use_terminal_colors = std::env::var("RUST_LOG_STYLE").map_or(true, |style| style.eq_ignore_ascii_case("auto"));
        if stderr_is_terminal && use_terminal_colors {
            logger.write_style(WriteStyle::Always);
        }
    }
    logger.init();
    log::debug!("Using {rayon_threads} Rayon threads ({available_cores} cores available)");

    let gdal_config = geo::RuntimeConfiguration::builder()
        .config_options(vec![
            ("GDAL_DISABLE_READDIR_ON_OPEN".into(), "YES".into()),
            ("GDAL_PAM_ENABLED".into(), "NO".into()),
        ])
        .build();
    gdal_config.apply().expect("Failed to configure GDAL");

    let tile_opts = TileCreationOptions {
        min_zoom: opt.min_zoom,
        max_zoom: opt.max_zoom,
        tile_size: opt.tile_size,
        zoom_level_selection: opt.zoom_level_selection,
        multi_band: opt.multi_band,
        variables: opt.variables,
        dimensions: opt.dimensions,
        scale: opt.scale,
        output_data_type: opt.output_data_type,
        unscale: opt.unscale,
        source_srs: opt.source_srs,
        aligned_levels: opt.aligned_levels,
    };

    if opt.print_command {
        print_gdal_translate_command(&opt.input, tile_opts)?;
    } else {
        let progress_callback: &mut dyn FnMut(f64) = &mut |fraction: f64| {
            let _ = progress.lock().update_to((fraction * 100.0) as usize);
        };

        create_cog_tiles(&opt.input, opt.output, tile_opts, progress_callback)?;
        progress.lock().update_to(100)?;
        if show_progress {
            eprintln!();
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_options(extra: &[&str]) -> std::result::Result<Opt, clap::Error> {
        Opt::try_parse_from(
            ["createcog", "--input", "input.tif", "--output", "output.tif"]
                .into_iter()
                .chain(extra.iter().copied()),
        )
    }

    #[test]
    fn omitted_variable_leaves_selection_automatic() {
        assert!(parse_options(&[]).unwrap().variables.is_empty());
    }

    #[test]
    fn variable_accepts_a_single_name() {
        assert_eq!(parse_options(&["--variable", "WBGT"]).unwrap().variables, ["WBGT"]);
    }

    #[test]
    fn variable_accepts_repeated_and_comma_delimited_names_in_order() {
        let options = parse_options(&["--variable", "WBGT,TEMP", "--variable", "HUMIDITY"]).unwrap();
        assert_eq!(options.variables, ["WBGT", "TEMP", "HUMIDITY"]);
    }

    #[test]
    fn variable_requires_a_value() {
        assert!(parse_options(&["--variable"]).is_err());
    }

    #[test]
    fn dimensions_default_to_time_y_x() {
        assert_eq!(parse_options(&[]).unwrap().dimensions, ["time", "y", "x"]);
    }

    #[test]
    fn dimensions_accept_custom_names_in_order() {
        let options = parse_options(&["--dimensions", "forecast,latitude,longitude"]).unwrap();
        assert_eq!(options.dimensions, ["forecast", "latitude", "longitude"]);
    }

    #[test]
    fn dimensions_accept_two_spatial_names() {
        assert_eq!(
            parse_options(&["--dimensions", "latitude,longitude"]).unwrap().dimensions,
            ["latitude", "longitude"]
        );
    }

    #[test]
    fn dimensions_require_one_comma_delimited_argument() {
        assert!(parse_options(&["--dimensions"]).is_err());
        assert!(parse_options(&["--dimensions", "time", "y", "x"]).is_err());
    }

    #[test]
    fn netcdf_selection_combines_with_source_srs() {
        let options = parse_options(&["--variable", "WBGT", "--dimensions", "time,y,x", "--source-srs", "EPSG:4326"]).unwrap();
        assert_eq!(options.source_srs.as_deref(), Some("EPSG:4326"));
        assert_eq!(options.variables, ["WBGT"]);
        assert_eq!(options.dimensions, ["time", "y", "x"]);
    }

    #[test]
    fn omitted_scale_disables_scaling() {
        assert_eq!(parse_options(&[]).unwrap().scale, None);
    }

    #[test]
    fn scale_accepts_eight_or_sixteen_bits() {
        for (value, bits) in [("8", 8), ("16", 16)] {
            assert_eq!(parse_options(&["--scale", value]).unwrap().scale, Some(bits));
        }
    }

    #[test]
    fn scale_rejects_other_values() {
        for value in ["0", "7", "9", "15", "17", "32", "256", "-8", "true", "8.0"] {
            assert!(parse_options(&["--scale", value]).is_err(), "Unexpectedly accepted --scale {value}");
        }
    }

    #[test]
    fn scale_requires_a_value() {
        assert!(parse_options(&["--scale"]).is_err());
    }

    #[test]
    fn omitted_data_type_preserves_automatic_selection() {
        assert_eq!(parse_options(&[]).unwrap().output_data_type, None);
    }

    #[test]
    fn output_data_type_accepts_canonical_and_rust_names_case_insensitively() {
        use geo::ArrayDataType;

        for (name, expected) in [
            ("Int8", ArrayDataType::Int8),
            ("UInt8", ArrayDataType::Uint8),
            ("Byte", ArrayDataType::Uint8),
            ("Int16", ArrayDataType::Int16),
            ("UInt16", ArrayDataType::Uint16),
            ("Int32", ArrayDataType::Int32),
            ("UInt32", ArrayDataType::Uint32),
            ("Int64", ArrayDataType::Int64),
            ("UInt64", ArrayDataType::Uint64),
            ("Float32", ArrayDataType::Float32),
            ("Float64", ArrayDataType::Float64),
            ("i8", ArrayDataType::Int8),
            ("u8", ArrayDataType::Uint8),
            ("i16", ArrayDataType::Int16),
            ("u16", ArrayDataType::Uint16),
            ("i32", ArrayDataType::Int32),
            ("u32", ArrayDataType::Uint32),
            ("i64", ArrayDataType::Int64),
            ("u64", ArrayDataType::Uint64),
            ("f32", ArrayDataType::Float32),
            ("f64", ArrayDataType::Float64),
        ] {
            for value in [name.to_owned(), name.to_ascii_lowercase(), name.to_ascii_uppercase()] {
                assert_eq!(
                    parse_options(&["--output-data-type", &value]).unwrap().output_data_type,
                    Some(expected),
                    "Unexpected type for --output-data-type {value}"
                );
            }
        }
        assert_eq!(
            parse_options(&["--output-data-type", "fLoAt64"]).unwrap().output_data_type,
            Some(ArrayDataType::Float64)
        );
    }

    #[test]
    fn data_type_rejects_unknown_and_complex_types_with_guidance() {
        for value in [
            "", "Unknown", "CInt16", "CFloat32", "Float16", "Int128", "Float", " UInt8", "u128", "i128", "f16", "usize", "isize",
        ] {
            let error = parse_options(&["--output-data-type", value]).unwrap_err();
            assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
            let message = error.to_string();
            assert!(message.contains("Invalid data type"), "{message}");
            assert!(message.contains("UInt8 (Byte)"), "{message}");
            assert!(message.contains("Float64"), "{message}");
        }
    }

    #[test]
    fn output_data_type_requires_a_value() {
        assert!(parse_options(&["--output-data-type"]).is_err());
    }

    #[test]
    fn removed_data_type_option_is_rejected() {
        let error = parse_options(&["--data-type", "f32"]).unwrap_err();
        assert_eq!(error.kind(), clap::error::ErrorKind::UnknownArgument);
    }

    #[test]
    fn unscale_defaults_to_disabled() {
        assert!(!parse_options(&[]).unwrap().unscale);
    }

    #[test]
    fn unscale_is_a_boolean_flag_with_automatic_type_selection() {
        let options = parse_options(&["--unscale"]).unwrap();
        assert!(options.unscale);
        assert_eq!(options.output_data_type, None);
        assert!(parse_options(&["--unscale=true"]).is_err());
    }

    #[test]
    fn unscale_accepts_an_explicit_output_type() {
        let options = parse_options(&["--unscale", "--output-data-type", "Int16"]).unwrap();
        assert!(options.unscale);
        assert_eq!(options.output_data_type, Some(geo::ArrayDataType::Int16));
    }

    #[test]
    fn scale_and_unscale_parse_together_in_either_order() {
        for (value, bits) in [("8", 8), ("16", 16)] {
            for extra in [["--scale", value, "--unscale"], ["--unscale", "--scale", value]] {
                let options = parse_options(&extra).unwrap();
                assert_eq!(options.scale, Some(bits));
                assert!(options.unscale);
                assert_eq!(options.output_data_type, None);
            }
        }
    }

    #[test]
    fn scale_accepts_matching_data_type_names_with_or_without_unscale() {
        for (value, bits, name, expected) in [
            ("8", 8, "UInt8", geo::ArrayDataType::Uint8),
            ("8", 8, "Byte", geo::ArrayDataType::Uint8),
            ("16", 16, "UInt16", geo::ArrayDataType::Uint16),
            ("8", 8, "u8", geo::ArrayDataType::Uint8),
            ("16", 16, "u16", geo::ArrayDataType::Uint16),
        ] {
            for unscale in [false, true] {
                let mut extra = vec!["--scale", value, "--output-data-type", name];
                if unscale {
                    extra.push("--unscale");
                }
                let options = parse_options(&extra).unwrap();
                assert_eq!(options.scale, Some(bits));
                assert_eq!(options.output_data_type, Some(expected));
                assert_eq!(options.unscale, unscale);
            }
        }
    }

    #[test]
    fn scale_type_mismatches_are_left_to_create_opts_validation_not_clap() {
        for (bits, name) in [("8", "UInt16"), ("16", "Byte"), ("8", "Float64"), ("16", "Int16")] {
            assert!(parse_options(&["--unscale", "--scale", bits, "--output-data-type", name]).is_ok());
        }
    }
}

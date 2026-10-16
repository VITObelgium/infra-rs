use std::io::IsTerminal;
use std::path::PathBuf;

use clap::Parser;
use createtiles::TileCreationOptions;
use env_logger::{Env, TimestampPrecision};
use kdam::{BarExt, Column, RichProgress, tqdm};

use crate::createtiles::{ZoomLevelSelection, create_cog_tiles, print_gdal_translate_command};

pub type Result<T> = anyhow::Result<T>;

mod createtiles;

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

    /// Scale to an unsigned 8-bit or 16-bit raster.
    #[arg(long = "scale", value_name = "8|16", value_parser = parse_scale_bits)]
    pub scale: Option<u8>,

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

fn main() -> Result<()> {
    let opt = Opt::parse();

    kdam::term::init(std::io::stderr().is_terminal());

    env_logger::Builder::from_env(Env::default().default_filter_or("warn"))
        .format_timestamp(Some(TimestampPrecision::Millis))
        .init();

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
        scale: opt.scale,
        aligned_levels: opt.aligned_levels,
    };

    if opt.print_command {
        print_gdal_translate_command(&PathBuf::from(opt.input), tile_opts)?;
    } else {
        let mut pb = RichProgress::new(
            tqdm!(total = 100, disable = opt.no_progress),
            vec![
                Column::Text("[bold blue]Creating COG".to_owned()),
                Column::Animation,
                Column::Percentage(1),
                Column::Text("•".to_owned()),
                Column::ElapsedTime,
                Column::Text("•".to_owned()),
                Column::RemainingTime,
            ],
        );
        let progress: Option<&mut dyn FnMut(f64)> = if opt.no_progress {
            None
        } else {
            Some(&mut |fraction: f64| {
                let _ = pb.update_to((fraction * 100.0) as usize);
            })
        };
        create_cog_tiles(&opt.input, opt.output, tile_opts, progress)?;
        pb.update_to(100)?;
        eprintln!();
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
}

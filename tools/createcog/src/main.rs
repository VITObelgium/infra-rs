use std::path::PathBuf;

use clap::Parser;
use createtiles::TileCreationOptions;
use env_logger::{Env, TimestampPrecision};
use indicatif::{MultiProgress, ProgressBar};
use indicatif_log_bridge::LogWrapper;

use crate::createtiles::{ZoomLevelSelection, create_cog_tiles};

pub type Result<T> = anyhow::Result<T>;

mod createtiles;

#[derive(Parser, Debug)]
#[clap(name = "createcog", about = "Create Cloud Optimized GeoTIFF")]
pub struct Opt {
    #[arg(long = "input", short = 'i')]
    pub input: PathBuf,

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

    #[arg(long = "noprogress")]
    pub no_progress: bool,
}

fn main() -> Result<()> {
    let opt = Opt::parse();

    let logger = env_logger::Builder::from_env(Env::default().default_filter_or("warn"))
        .format_timestamp(Some(TimestampPrecision::Millis))
        .build();

    let multi = MultiProgress::new();
    let level = logger.filter();
    LogWrapper::new(multi.clone(), logger).try_init().unwrap();
    log::set_max_level(level);

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
    };

    let progress = multi.add(ProgressBar::new(100));
    let p = progress.clone();

    create_cog_tiles(&opt.input, opt.output, tile_opts)?;
    p.finish_with_message("COG creation done");

    Ok(())
}

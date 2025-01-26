use std::path::PathBuf;

use clap::Parser;
use createtiles::{create_mbtiles, TileCreationOptions};
use env_logger::{Env, TimestampPrecision};
use geo::ZoomLevelStrategy;
use indicatif::{MultiProgress, ProgressBar};
use indicatif_log_bridge::LogWrapper;
use inf::progressinfo::{CallbackProgress, ComputationStatus};

pub type Error = tiler::Error;
pub type Result<T> = tiler::Result<T>;

mod createtiles;
mod mbtilesdb;

#[derive(Parser, Debug)]
#[clap(name = "createtiles", about = "Preprocess raster tiles")]
pub struct Opt {
    #[clap(long = "input", short = 'i')]
    pub input: PathBuf,

    #[clap(long = "output", short = 'o')]
    pub output: PathBuf,

    #[clap(long = "min-zoom")]
    pub min_zoom: Option<i32>,

    #[clap(long = "max-zoom")]
    pub max_zoom: Option<i32>,

    #[clap(long = "noprogress")]
    pub no_progress: bool,
}

fn main() -> Result<()> {
    let opt = Opt::parse();

    let exe_dir = PathBuf::from(
        std::env::current_exe()
            .expect("Unable to get current executable path")
            .parent()
            .expect("Unable to get parent directory of executable"),
    );

    let logger = env_logger::Builder::from_env(Env::default().default_filter_or("warn"))
        .format_timestamp(Some(TimestampPrecision::Millis))
        .build();

    let multi = MultiProgress::new();
    let level = logger.filter();
    LogWrapper::new(multi.clone(), logger).try_init().unwrap();
    log::set_max_level(level);

    let gdal_config = geo::RuntimeConfiguration::builder()
        .proj_db(&exe_dir)
        .config_options(vec![
            ("GDAL_DISABLE_READDIR_ON_OPEN".into(), "YES".into()),
            ("GDAL_PAM_ENABLED".into(), "NO".into()),
        ])
        .build();
    gdal_config.apply().expect("Failed to configure GDAL");

    let tile_opts = TileCreationOptions {
        min_zoom: opt.min_zoom,
        max_zoom: opt.max_zoom,
        zoom_level_strategy: ZoomLevelStrategy::PreferHigher,
    };

    let progress = multi.add(ProgressBar::new(100));
    let p = progress.clone();
    create_mbtiles(
        &opt.input,
        opt.output,
        tile_opts,
        CallbackProgress::<(), _>::with_cb(move |pos, _| {
            progress.set_position((pos * 100.0) as u64);
            ComputationStatus::Continue
        }),
    )?;

    p.finish_with_message("Tile creation done");

    Ok(())
}

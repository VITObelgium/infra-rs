use std::{
    io::{self, IsTerminal, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use clap::Parser;
use createtiles::TileCreationOptions;
use env_logger::{Env, Target, TimestampPrecision, WriteStyle};
use kdam::{BarExt, Column, RichProgress, tqdm};

use crate::createtiles::{ZoomLevelSelection, create_cog_tiles, print_gdal_translate_command};

pub type Result<T> = anyhow::Result<T>;

mod createtiles;

struct ProgressWriter {
    progress: Arc<Mutex<RichProgress>>,
}

impl Write for ProgressWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let message = String::from_utf8_lossy(buffer);
        let message = message.trim_end_matches(['\r', '\n']);

        if !message.is_empty() {
            self.progress
                .lock()
                .map_err(|_| io::Error::other("progress bar lock poisoned"))?
                .write(message)
                .map_err(io::Error::other)?;
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

    #[arg(long = "scale")]
    pub scale: bool,

    /// Assign a source CRS when the input has no projection metadata (for example, EPSG:4326 NetCDF data).
    #[arg(long = "source-srs")]
    pub source_srs: Option<String>,

    #[arg(long = "aligned-levels")]
    pub aligned_levels: Option<i32>,

    #[arg(long = "noprogress")]
    pub no_progress: bool,

    #[arg(long = "gdal-cmd")]
    pub print_command: bool,
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
        scale: opt.scale,
        source_srs: opt.source_srs,
        aligned_levels: opt.aligned_levels,
    };

    if opt.print_command {
        print_gdal_translate_command(&PathBuf::from(opt.input), tile_opts)?;
    } else {
        let mut update_progress = |fraction: f64| {
            if let Ok(mut progress) = progress.lock() {
                let _ = progress.update_to((fraction * 100.0) as usize);
            }
        };
        let progress_callback: Option<&mut dyn FnMut(f64)> = show_progress.then_some(&mut update_progress);

        create_cog_tiles(&opt.input, opt.output, tile_opts, progress_callback)?;
        progress
            .lock()
            .map_err(|_| anyhow::anyhow!("progress bar lock poisoned"))?
            .update_to(100)?;
        if show_progress {
            eprintln!();
        }
    }

    Ok(())
}

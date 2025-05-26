use std::path::PathBuf;

use clap::Parser;
use comfy_table::Table;
use env_logger::{Env, TimestampPrecision};
use geo::{
    Array as _, ArrayNum, Columns, Coordinate, DenseArray, RasterSize, Rows, Tile,
    raster::{
        RasterIO,
        algo::{self, RasterStats},
    },
};
use indicatif::{MultiProgress, ProgressBar};
use indicatif_log_bridge::LogWrapper;
use inf::progressinfo::{CallbackProgress, ComputationStatus};
use raster_tile::RasterTileCastIO;
use reqwest::Client;

pub type Error = raster_tile::Error;
pub type Result<T> = raster_tile::Result<T>;

#[derive(Parser, Debug)]
#[clap(name = "tiles2raster", about = "Reassemble a raster from tiles")]
pub struct Opt {
    #[clap(long = "url", short = 'u')]
    pub url: String,

    #[clap(long = "output", short = 'o')]
    pub output: PathBuf,

    #[clap(long = "zoom")]
    pub zoom: i32,

    #[clap(long = "coord1")]
    pub coord1: String,

    #[clap(long = "coord2")]
    pub coord2: String,

    #[clap(long = "tile-size", default_value = "256")]
    pub tile_size: u16,

    #[clap(long = "stats")]
    pub calc_stats: bool,

    #[clap(long = "noprogress")]
    pub no_progress: bool,
}

fn bounds_from_coords(coord1: &str, coord2: &str) -> Result<geo::LatLonBounds> {
    let coords = coord1.split(',').collect::<Vec<_>>();
    if coords.len() != 2 {
        return Err(Error::InvalidArgument("Invalid coordinate format".to_string()));
    }
    let lat1 = coords[0].parse::<f64>().expect("Invalid latitude");
    let lon1 = coords[1].parse::<f64>().expect("Invalid longitude");

    let coords = coord2.split(',').collect::<Vec<_>>();
    if coords.len() != 2 {
        return Err(Error::InvalidArgument("Invalid coordinate format".to_string()));
    }
    let lat2 = coords[0].parse::<f64>().expect("Invalid latitude");
    let lon2 = coords[1].parse::<f64>().expect("Invalid longitude");

    Ok(geo::LatLonBounds::hull(
        Coordinate::latlon(lat1, lon1),
        Coordinate::latlon(lat2, lon2),
    ))
}

fn print_raster_stats<T: ArrayNum>(stats: &Option<RasterStats<T>>) {
    if let Some(stats) = stats {
        let mut table = Table::new();
        table
            .set_header(vec!["Statistics", "Value"])
            .add_row(vec!["Minimum", &stats.min.to_string()])
            .add_row(vec!["Maximum", &stats.max.to_string()])
            .add_row(vec!["Mean", &stats.mean.to_string()])
            .add_row(vec!["Standard Deviation", &stats.stddev.to_string()])
            .add_row(vec!["Median", &stats.median.to_string()])
            .add_row(vec!["Value Count", &stats.value_count.to_string()])
            .add_row(vec!["Sum", &stats.sum.to_string()])
            .add_row(vec!["Quantiles", &format!("{:?}", stats.quantiles)]);

        println!("{table}");
    } else {
        println!("No statistics available for the raster.");
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let opt = Opt::parse();

    let logger = env_logger::Builder::from_env(Env::default().default_filter_or("warn"))
        .format_timestamp(Some(TimestampPrecision::Millis))
        .build();

    let multi = MultiProgress::new();
    let level = logger.filter();
    LogWrapper::new(multi.clone(), logger).try_init().unwrap();
    log::set_max_level(level);

    let progress = multi.add(ProgressBar::new(100));
    let p = progress.clone();

    let bounds = bounds_from_coords(&opt.coord1, &opt.coord2)?;
    let raster_size = RasterSize::with_rows_cols(Rows(opt.tile_size as i32), Columns(opt.tile_size as i32));

    let mut raster = raster_tile::utils::reassemble_raster_from_tiles::<f32, _>(
        bounds,
        opt.zoom,
        opt.tile_size,
        CallbackProgress::<(), _>::with_cb(move |pos, _| {
            progress.set_position((pos * 100.0) as u64);
            ComputationStatus::Continue
        }),
        async |tile: Tile| {
            let url = opt
                .url
                .replace("{x}", &tile.x().to_string())
                .replace("{y}", &tile.y().to_string())
                .replace("{z}", &tile.z().to_string());

            let request = Client::new().get(url);
            let result = request.send().await;

            match result {
                Ok(response) => {
                    let bytes = response
                        .bytes()
                        .await
                        .map_err(|err| Error::Runtime(format!("Failed to read tile data: {}", err)))?;

                    if bytes.is_empty() {
                        Ok(DenseArray::<f32>::filled_with_nodata(raster_size))
                    } else {
                        Ok(DenseArray::<f32>::from_tile_bytes_autodetect_format_with_cast(bytes.as_ref())?)
                    }
                }
                Err(err) => Err(Error::Runtime(format!("Failed to fetch tile: {}", err))),
            }
        },
    )
    .await?;

    raster.write(&opt.output)?;

    p.finish_with_message("Raster creation done");

    if opt.calc_stats {
        print_raster_stats(&algo::statistics(&raster, &[0.0, 0.25, 0.5, 0.75, 1.0]).expect("Failed to calculate statistics"));
    }

    Ok(())
}

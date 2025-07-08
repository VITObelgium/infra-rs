use rayon::prelude::*;
use std::path::{Path, PathBuf};

use geo::{GeoReference, Tile, ZoomLevelStrategy, crs};
use inf::progressinfo::AsyncProgressNotification;
use tiler::{TileData, TileFormat, TileProvider, WarpingTileProvider, tileproviderfactory};

pub type Result<T> = tiler::Result<T>;

pub struct TileCreationOptions {
    pub min_zoom: Option<i32>,
    pub max_zoom: Option<i32>,
    pub zoom_level_strategy: ZoomLevelStrategy,
    pub tile_size: u16,
    pub tile_format: TileFormat,
}

use std::sync::mpsc;
use std::sync::mpsc::Receiver;

use crate::mbtilesdb;
use crate::mbtilesmetadata::Metadata;

pub fn write_tiles_to_mbtiles(
    mbtiles_meta: Metadata,
    db_path: PathBuf,
    rx: Receiver<(Tile, TileData)>,
    progress: impl AsyncProgressNotification,
) -> Result<()> {
    let mut mbtiles = mbtilesdb::MbtilesDb::new(&db_path)?;
    mbtiles.start_transaction()?;

    let mbtiles_meta: Vec<(String, String)> = mbtiles_meta.into();
    mbtiles.insert_metadata(&mbtiles_meta)?;

    for (tile, tile_data) in rx {
        mbtiles.insert_tile_data(&tile, tile_data.data)?;
        match progress.tick() {
            Ok(_) => {}
            Err(inf::Error::Cancelled) => {
                break;
            }
            Err(e) => {
                log::error!("Error updating progress: {e:?}");
                break;
            }
        }
    }

    log::info!("Commiting transaction");
    mbtiles.commit_transaction()?;
    log::info!("Commiting transaction done");
    Ok(())
}

pub fn create_cog_tiles(input: &Path, output: PathBuf, opts: TileCreationOptions) -> Result<()> {
    let src_ds = geo::raster::io::dataset::open_read_only(input)?;
    let mut options = vec![
        "-f".to_string(),
        "COG".to_string(),
        "-co".to_string(),
        format!("BLOCKSIZE={}", opts.tile_size),
        "-co".to_string(),
        "TILING_SCHEME=GoogleMapsCompatible".to_string(),
        "-co".to_string(),
        "ADD_ALPHA=NO".to_string(),
        "-co".to_string(),
        "STATISTICS=YES".to_string(),
        "-co".to_string(),
        "OVERVIEWS=IGNORE_EXISTING".to_string(),
        "-co".to_string(),
        "RESAMPLING=NEAREST".to_string(),
        "-co".to_string(),
        "OVERVIEW_RESAMPLING=NEAREST".to_string(),
        "-co".to_string(),
        "NUM_THREADS=ALL_CPUS".to_string(),
        "-co".to_string(),
        "COMPRESS=LZW".to_string(),
        "-co".to_string(),
        "ALIGNED_LEVELS=6".to_string(),
        "-co".to_string(),
        "PREDICTOR=YES".to_string(),
    ];

    match opts.zoom_level_strategy {
        ZoomLevelStrategy::Manual(zoom) => {
            options.push("-co".to_string());
            options.push(format!("ZOOM_LEVEL={}", zoom));
        }
        ZoomLevelStrategy::Closest => {
            options.push("-co".to_string());
            options.push("ZOOM_LEVEL_STRATEGY=AUTO".to_string());
        }
        ZoomLevelStrategy::PreferHigher => {
            options.push("-co".to_string());
            options.push("ZOOM_LEVEL_STRATEGY=UPPER".to_string());
        }
        ZoomLevelStrategy::PreferLower => {
            options.push("-co".to_string());
            options.push("ZOOM_LEVEL_STRATEGY=LOWER".to_string());
        }
    }

    geo::raster::algo::warp_to_disk_cli(&src_ds, &output, &options, &vec![])?;

    Ok(())
}

pub fn create_mbtiles(
    input: &Path,
    output: PathBuf,
    mut opts: TileCreationOptions,
    progress: impl AsyncProgressNotification,
) -> Result<()> {
    let georef = GeoReference::from_file(input)?;

    if output.exists() {
        std::fs::remove_file(&output)?;
    }

    let min_zoom = opts.min_zoom.unwrap_or(0);
    if let Some(zoom) = opts.max_zoom {
        if zoom < 0 {
            return Err(tiler::Error::Runtime("Max zoom level must be greater than 0".to_string()));
        }

        opts.zoom_level_strategy = ZoomLevelStrategy::Manual(zoom);
    }
    let max_zoom = opts
        .max_zoom
        .unwrap_or_else(|| Tile::zoom_level_for_pixel_size(georef.cell_size_x(), opts.zoom_level_strategy));
    progress.reset(2u64.pow(max_zoom as u32));

    let tiler_options = tileproviderfactory::TileProviderOptions {
        calculate_stats: true,
        zoom_level_strategy: opts.zoom_level_strategy,
    };

    let tiler = WarpingTileProvider::new(input, &tiler_options)?;
    let layer = tiler.layers().into_iter().next().unwrap();

    let mbtiles_meta = Metadata::new(&layer, min_zoom, max_zoom, Vec::default());

    let (tx, rx) = mpsc::channel();

    let storage_thread = std::thread::spawn(|| match write_tiles_to_mbtiles(mbtiles_meta, output, rx, progress) {
        Ok(_) => {}
        Err(e) => {
            log::error!("Error writing tiles to mbtiles: {e:?}");
        }
    });

    let mut tiles = if min_zoom == 0 {
        vec![Tile { x: 0, y: 0, z: 0 }]
    } else {
        let meta = georef.warped_to_epsg(crs::epsg::WGS84)?;
        let top_left = Tile::for_coordinate(meta.top_left().into(), min_zoom);
        let bottom_right = Tile::for_coordinate(meta.bottom_right().into(), min_zoom);

        let mut tiles = Vec::new();
        for x in top_left.x..=bottom_right.x {
            for y in top_left.y..=bottom_right.y {
                tiles.push(Tile { x, y, z: min_zoom });
            }
        }

        tiles
    };

    let mut current_zoom = 0;
    while current_zoom <= max_zoom {
        let mut child_tiles = Vec::new();
        child_tiles.par_extend(tiles.into_par_iter().flat_map(|tile| {
            let tile_request = tiler::TileRequest {
                tile,
                dpi_ratio: 1,
                tile_size: opts.tile_size,
                tile_format: opts.tile_format,
            };

            match tiler.get_tile(layer.id, &tile_request) {
                Ok(tile_data) => {
                    if !tile_data.data.is_empty() {
                        match tx.send((tile, tile_data)) {
                            Ok(_) => {}
                            Err(e) => {
                                log::error!("Error sending tile data: {e:?}");
                            }
                        }

                        return tile.direct_children().to_vec();
                    }
                }
                Err(e) => log::error!("Error getting tile data for {e:?}"),
            }

            Vec::default()
        }));

        tiles = child_tiles;
        current_zoom += 1;
    }

    drop(tx);
    storage_thread.join().unwrap();

    Ok(())
}

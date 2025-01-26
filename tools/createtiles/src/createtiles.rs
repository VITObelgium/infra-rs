use rayon::prelude::*;
use std::path::{Path, PathBuf};

use geo::{GeoReference, Tile, ZoomLevelStrategy};
use inf::progressinfo::AsyncProgressNotification;
use tiler::{tileproviderfactory, TileData, TileProvider, WarpingTileProvider};

pub type Result<T> = tiler::Result<T>;

pub struct TileCreationOptions {
    pub min_zoom: Option<i32>,
    pub max_zoom: Option<i32>,
    pub zoom_level_strategy: ZoomLevelStrategy,
}

use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};

use crate::mbtilesdb;

pub fn write_tiles_to_mbtiles(
    db_path: PathBuf,
    rx: Receiver<(Tile, TileData)>,
    progress: impl AsyncProgressNotification,
) -> Result<()> {
    let mut mbtiles = mbtilesdb::MbtilesDb::new(&db_path)?;
    mbtiles.start_transaction()?;

    for (tile, tile_data) in rx {
        mbtiles.insert_tile_data(&tile, tile_data.data)?;
        log::debug!("Stored tile: {:?}", tile);
        match progress.tick() {
            Ok(_) => {}
            Err(inf::Error::Cancelled) => {
                break;
            }
            Err(e) => {
                log::error!("Error updating progress: {:?}", e);
                break;
            }
        }
    }

    log::info!("Commiting transaction");
    mbtiles.commit_transaction()?;
    log::info!("Commiting transaction done");
    Ok(())
}

pub fn create_mbtiles(
    input: &Path,
    output: PathBuf,
    mut opts: TileCreationOptions,
    progress: impl AsyncProgressNotification,
) -> Result<()> {
    let meta = GeoReference::from_file(input)?;

    if output.exists() {
        std::fs::remove_file(&output)?;
    }

    //let min_zoom = opts.min_zoom.unwrap_or(0);
    if let Some(zoom) = opts.max_zoom {
        if zoom < 0 {
            return Err(tiler::Error::Runtime(
                "Max zoom level must be greater than 0".to_string(),
            ));
        }

        opts.zoom_level_strategy = ZoomLevelStrategy::Manual(zoom);
    }
    let max_zoom = opts
        .max_zoom
        .unwrap_or_else(|| Tile::zoom_level_for_pixel_size(meta.cell_size_x(), opts.zoom_level_strategy));
    progress.reset(2u64.pow(max_zoom as u32));
    //let tile_extent = meta.aligned_to_xyz_tiles_for_zoom_level(opts.min_zoom);

    let tiler_options = tileproviderfactory::TileProviderOptions {
        calculate_stats: true,
        zoom_level_strategy: opts.zoom_level_strategy,
    };

    let tiler = WarpingTileProvider::new(input, &tiler_options)?;
    let layer = tiler.layers().into_iter().next().unwrap();

    let (tx, rx): (Sender<(Tile, TileData)>, Receiver<(Tile, TileData)>) = mpsc::channel();

    let storage_thread = std::thread::spawn(|| match write_tiles_to_mbtiles(output, rx, progress) {
        Ok(_) => {}
        Err(e) => {
            log::error!("Error writing tiles to mbtiles: {:?}", e);
        }
    });

    let mut tiles = vec![Tile { x: 0, y: 0, z: 0 }];
    let mut current_zoom = 0;
    while current_zoom <= max_zoom {
        let mut child_tiles = Vec::new();
        child_tiles.par_extend(tiles.into_par_iter().flat_map(|tile| {
            let tile_request = tiler::TileRequest {
                tile,
                dpi_ratio: 1,
                tile_format: tiler::TileFormat::RasterTile,
            };

            match tiler.get_tile(layer.id, &tile_request) {
                Ok(tile_data) => {
                    if !tile_data.data.is_empty() {
                        match tx.send((tile, tile_data)) {
                            Ok(_) => {}
                            Err(e) => {
                                log::error!("Error sending tile data: {:?}", e);
                            }
                        }

                        return tile.direct_children().to_vec();
                    }
                }
                Err(e) => log::error!("Error getting tile data for {:?}", e),
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

use geo::cog::PredictorSelection;
use geo::geotiff::Compression;
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
    pub tile_size: u32,
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
    let mut zoom_level_strategy = opts.zoom_level_strategy;
    if let Some(max_zoom) = opts.max_zoom {
        zoom_level_strategy = ZoomLevelStrategy::Manual(max_zoom);
    }

    let cog_opts = geo::cog::CogCreationOptions {
        min_zoom: opts.min_zoom,
        zoom_level_strategy,
        tile_size: opts.tile_size,
        compression: Some(Compression::Lzw),
        predictor: Some(PredictorSelection::Automatic),
        allow_sparse: true,
        output_data_type: None,
        aligned_levels: Some(2),
    };

    geo::cog::create_cog_tiles(input, &output, cog_opts)?;

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
        .unwrap_or_else(|| Tile::zoom_level_for_pixel_size(georef.cell_size_x(), opts.zoom_level_strategy, opts.tile_size));
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

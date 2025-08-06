use anyhow::bail;
use geo::cog::PredictorSelection;
use geo::raster::Compression;
use std::path::{Path, PathBuf};
use strum::EnumString;

use crate::Result;
use geo::ZoomLevelStrategy;

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumString)]
#[strum(serialize_all = "kebab_case")]
pub enum ZoomLevelSelection {
    Closest,
    PreferHigher,
    PreferLower,
}

pub struct TileCreationOptions {
    pub min_zoom: Option<i32>,
    pub max_zoom: Option<i32>,
    pub zoom_level_selection: Option<ZoomLevelSelection>,
    pub tile_size: u32,
}

pub fn create_cog_tiles(input: &Path, output: PathBuf, opts: TileCreationOptions) -> Result<()> {
    let zoom_level_strategy = match (opts.zoom_level_selection, opts.max_zoom) {
        (Some(_), Some(_)) => bail!("Cannot specify both zoom level selection and max zoom"),
        (None, Some(max_zoom)) => ZoomLevelStrategy::Manual(max_zoom),
        (None | Some(ZoomLevelSelection::Closest), None) => ZoomLevelStrategy::Closest,
        (Some(ZoomLevelSelection::PreferHigher), None) => ZoomLevelStrategy::PreferHigher,
        (Some(ZoomLevelSelection::PreferLower), None) => ZoomLevelStrategy::PreferLower,
    };

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

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
    pub multi_band: bool,
}

fn create_opts(opts: TileCreationOptions) -> Result<geo::cog::CogCreationOptions> {
    let zoom_level_strategy = match (opts.zoom_level_selection, opts.max_zoom) {
        (Some(_), Some(_)) => bail!("Cannot specify both zoom level selection and max zoom"),
        (None, Some(max_zoom)) => ZoomLevelStrategy::Manual(max_zoom),
        (None | Some(ZoomLevelSelection::Closest), None) => ZoomLevelStrategy::Closest,
        (Some(ZoomLevelSelection::PreferHigher), None) => ZoomLevelStrategy::PreferHigher,
        (Some(ZoomLevelSelection::PreferLower), None) => ZoomLevelStrategy::PreferLower,
    };

    Ok(geo::cog::CogCreationOptions {
        min_zoom: opts.min_zoom,
        zoom_level_strategy,
        tile_size: opts.tile_size,
        compression: Some(Compression::Lzw),
        predictor: Some(PredictorSelection::Automatic),
        allow_sparse: true,
        output_data_type: None,
        aligned_levels: Some(2),
    })
}

pub fn print_gdal_translate_command(input: &Path, opts: TileCreationOptions) -> Result<()> {
    let args = geo::cog::create_gdal_args(input, create_opts(opts)?)?;
    println!("Gdal cmd:\n {}", args.join(" "));
    Ok(())
}

pub fn create_cog_tiles(input: &str, output: PathBuf, opts: TileCreationOptions) -> Result<()> {
    let multi_band = opts.multi_band;
    let cog_create_opts = create_opts(opts)?;

    if multi_band {
        Ok(geo::cog::create_multiband_cog_tiles(input, &output, cog_create_opts)?)
    } else {
        Ok(geo::cog::create_cog_tiles(&PathBuf::from(input), &output, cog_create_opts)?)
    }
}

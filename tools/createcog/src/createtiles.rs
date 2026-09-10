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
    pub scale: bool,
    pub aligned_levels: Option<i32>,
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
        compression: Some(Compression::Zstd),
        predictor: Some(PredictorSelection::Automatic),
        allow_sparse: true,
        output_data_type: None,
        aligned_levels: opts.aligned_levels.or(Some(2)),
        scale: opts.scale,
    })
}

pub fn print_gdal_translate_command(input: &Path, opts: TileCreationOptions) -> Result<()> {
    let uses_multiband_pipeline = opts.multi_band || geo::raster::formats::gdal::open_dataset_read_only(input)?.raster_count() > 1;
    if uses_multiband_pipeline {
        println!("Pipeline: create one single-band COG per input band, then assemble the compressed tiles into a multiband COG");
        return Ok(());
    }

    let args = geo::cog::create_gdal_warp_args(input, create_opts(opts)?)?;
    println!("Gdal cmd:\n {}", args.join(" "));
    Ok(())
}

pub fn create_cog_tiles(input: &str, output: PathBuf, opts: TileCreationOptions, mut progress: Option<&mut dyn FnMut(f64)>) -> Result<()> {
    let multi_band = opts.multi_band || geo::raster::formats::gdal::open_dataset_read_only(Path::new(input))?.raster_count() > 1;
    let cog_create_opts = create_opts(opts)?;

    if multi_band {
        let (_temporary_directory, band_cogs) = geo::cog::create_temporary_band_cogs(input, cog_create_opts)?;
        if let Some(progress) = progress.as_mut() {
            progress(0.8);
        }

        geo::geotiff::assemble_band_cogs(&band_cogs, &output)?;
        if let Some(progress) = progress.as_mut() {
            progress(1.0);
        }
        Ok(())
    } else {
        Ok(geo::cog::create_cog_tiles(&PathBuf::from(input), &output, cog_create_opts)?)
    }
}

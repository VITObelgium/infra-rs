use anyhow::bail;
use geo::cog::PredictorSelection;
use geo::raster::Compression;
use std::path::{Path, PathBuf};
use strum::EnumString;

use crate::Result;
use geo::{ArrayDataType, ZoomLevelStrategy};

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
    pub scale: Option<u8>,
    pub source_srs: Option<String>,
    pub aligned_levels: Option<i32>,
}

fn create_opts(opts: &TileCreationOptions) -> Result<geo::cog::CogCreationOptions> {
    let zoom_level_strategy = match (opts.zoom_level_selection, opts.max_zoom) {
        (Some(_), Some(_)) => bail!("Cannot specify both zoom level selection and max zoom"),
        (None, Some(max_zoom)) => ZoomLevelStrategy::Manual(max_zoom),
        (None | Some(ZoomLevelSelection::Closest), None) => ZoomLevelStrategy::Closest,
        (Some(ZoomLevelSelection::PreferHigher), None) => ZoomLevelStrategy::PreferHigher,
        (Some(ZoomLevelSelection::PreferLower), None) => ZoomLevelStrategy::PreferLower,
    };

    let output_data_type = match opts.scale {
        None => None,
        Some(8) => Some(ArrayDataType::Uint8),
        Some(16) => Some(ArrayDataType::Uint16),
        Some(_) => bail!("Scaling bit depth must be 8 or 16"),
    };

    Ok(geo::cog::CogCreationOptions {
        min_zoom: opts.min_zoom,
        zoom_level_strategy,
        tile_size: opts.tile_size,
        compression: Some(Compression::Zstd),
        predictor: Some(PredictorSelection::Automatic),
        allow_sparse: true,
        output_data_type,
        aligned_levels: opts.aligned_levels.or(Some(2)),
        scale: opts.scale.is_some(),
    })
}

pub fn print_gdal_translate_command(input: &Path, opts: TileCreationOptions) -> Result<()> {
    let uses_multiband_pipeline = opts.multi_band || geo::raster::formats::gdal::open_dataset_read_only(input)?.raster_count() > 1;
    if uses_multiband_pipeline {
        println!("Pipeline: create one single-band COG per input band, then assemble the compressed tiles into a multiband COG");
        return Ok(());
    }

    let args = geo::cog::create_gdal_warp_args(input, create_opts(&opts)?)?;
    println!("Gdal cmd:\n {}", args.join(" "));
    Ok(())
}

pub fn create_cog_tiles(input: &str, output: PathBuf, opts: TileCreationOptions, mut progress: Option<&mut dyn FnMut(f64)>) -> Result<()> {
    let multi_band = opts.multi_band || geo::raster::formats::gdal::open_dataset_read_only(Path::new(input))?.raster_count() > 1;
    let cog_create_opts = create_opts(&opts)?;

    if multi_band {
        let mut warp_progress = |fraction: f64| {
            if let Some(progress) = progress.as_mut() {
                progress(fraction * 0.8);
            }
        };
        let (_temporary_directory, band_cogs) =
            geo::cog::create_temporary_band_cogs(input, cog_create_opts, opts.source_srs.as_deref(), Some(&mut warp_progress))?;
        geo::geotiff::assemble_band_cogs(&band_cogs, &output)?;
        if let Some(progress) = progress.as_mut() {
            progress(1.0);
        }
        Ok(())
    } else {
        Ok(geo::cog::create_cog_tiles(&PathBuf::from(input), &output, cog_create_opts)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(scale: Option<u8>) -> TileCreationOptions {
        TileCreationOptions {
            min_zoom: None,
            max_zoom: None,
            zoom_level_selection: None,
            tile_size: 512,
            multi_band: false,
            scale,
            aligned_levels: None,
        }
    }

    #[test]
    fn scale_selects_the_output_data_type() -> Result<()> {
        for (bits, expected) in [
            (None, None),
            (Some(8), Some(ArrayDataType::Uint8)),
            (Some(16), Some(ArrayDataType::Uint16)),
        ] {
            let result = create_opts(options(bits))?;
            assert_eq!(result.scale, bits.is_some());
            assert_eq!(result.output_data_type, expected);
        }
        Ok(())
    }

    #[test]
    fn invalid_scale_is_rejected() {
        for bits in [0, 9, 32, 255] {
            assert!(create_opts(options(Some(bits))).is_err());
        }
    }
}

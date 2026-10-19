use anyhow::bail;
use geo::cog::{self, PredictorSelection};
use geo::raster::Compression;
use std::path::{Path, PathBuf};
use strum::EnumString;

use crate::{Result, input, unscale};
use geo::{ArrayDataType, ZoomLevelStrategy, geotiff};

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
    pub variables: Vec<String>,
    pub dimensions: Vec<String>,
    pub scale: Option<u8>,
    pub output_data_type: Option<ArrayDataType>,
    pub unscale: bool,
    pub source_srs: Option<String>,
    pub aligned_levels: Option<i32>,
}

fn determine_output_data_type(opts: &TileCreationOptions) -> Result<Option<ArrayDataType>> {
    match opts.scale {
        None => Ok(opts.output_data_type.or(opts.unscale.then_some(ArrayDataType::Float64))),
        Some(bits @ (8 | 16)) => {
            let encoded_type = if bits == 8 { ArrayDataType::Uint8 } else { ArrayDataType::Uint16 };
            if let Some(requested) = opts.output_data_type
                && requested != encoded_type
            {
                bail!("--output-data-type {requested} does not match --scale {bits}; expected {encoded_type}");
            }
            Ok(Some(encoded_type))
        }
        Some(_) => bail!("Scaling bit depth must be 8 or 16"),
    }
}

fn create_opts(opts: &TileCreationOptions) -> Result<geo::cog::CogCreationOptions> {
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
        output_data_type: determine_output_data_type(opts)?,
        aligned_levels: opts.aligned_levels.or(Some(2)),
        scale: opts.scale.is_some(),
    })
}

struct PreparedRasterInput {
    source: input::PreparedInput,
    unscaled: Option<unscale::PreparedInput>,
}

impl PreparedRasterInput {
    fn new(input: &str, opts: &TileCreationOptions) -> Result<Self> {
        let prepared_input = input::prepare_input(input, &opts.variables, &opts.dimensions, opts.source_srs.as_deref())?;
        let unscaled = if opts.unscale {
            Some(unscale::prepare_input(&prepared_input.input)?)
        } else {
            None
        };
        Ok(Self {
            source: prepared_input,
            unscaled,
        })
    }

    fn as_str(&self) -> &str {
        match &self.unscaled {
            Some(unscaled) => &unscaled.input,
            None => &self.source.input,
        }
    }

    fn as_path(&self) -> &Path {
        Path::new(self.as_str())
    }

    fn uses_multiband_pipeline(&self, opts: &TileCreationOptions) -> Result<bool> {
        if opts.multi_band {
            return Ok(true);
        }
        if let Some(unscaled) = &self.unscaled
            && unscaled.multi_band
        {
            return Ok(true);
        }
        if let Some(paths) = input::resolve_local_input_paths(self.as_str(), self.as_path())?
            && (paths.len() > 1 || !self.as_path().is_file())
        {
            // The per-band pipeline expands globs, including those matching only one file.
            return Ok(true);
        }
        Ok(geo::raster::formats::gdal::open_dataset_read_only(self.as_path())?.raster_count() > 1)
    }
}

pub fn print_gdal_translate_command(input: &str, opts: TileCreationOptions) -> Result<()> {
    let cog_create_opts = create_opts(&opts)?;
    let prepared = PreparedRasterInput::new(input, &opts)?;
    if prepared.uses_multiband_pipeline(&opts)? {
        println!("Pipeline: create one single-band COG per input band, then assemble the compressed tiles into a multiband COG");
        return Ok(());
    }

    let args = geo::cog::create_gdal_warp_args(prepared.as_path(), cog_create_opts)?;
    println!("Gdal cmd:\n {}", args.join(" "));
    Ok(())
}

pub fn create_cog_tiles(input: &str, output: PathBuf, opts: TileCreationOptions, mut progress: Option<&mut dyn FnMut(f64)>) -> Result<()> {
    let cog_create_opts = create_opts(&opts)?;
    let prepared = PreparedRasterInput::new(input, &opts)?;

    if prepared.uses_multiband_pipeline(&opts)? {
        if let Some(progress) = progress.as_mut() {
            progress(0.0);
        }
        let mut warp_progress = |fraction: f64| {
            if let Some(progress) = progress.as_mut() {
                progress(fraction * 0.9);
            }
        };
        let (_temporary_directory, band_cogs) = cog::create_temporary_band_cogs(
            prepared.as_str(),
            cog_create_opts,
            opts.source_srs.as_deref(),
            Some(&mut warp_progress),
        )?;
        geotiff::assemble_band_cogs(&band_cogs, &output)?;
        if let Some(progress) = progress.as_mut() {
            progress(1.0);
        }
        Ok(())
    } else {
        Ok(cog::create_cog_tiles(prepared.as_path(), &output, cog_create_opts)?)
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
            variables: Vec::new(),
            dimensions: vec!["time".into(), "y".into(), "x".into()],
            scale,
            output_data_type: None,
            unscale: false,
            source_srs: None,
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
            let result = create_opts(&options(bits))?;
            assert_eq!(result.scale, bits.is_some());
            assert_eq!(result.output_data_type, expected);
        }
        Ok(())
    }

    #[test]
    fn invalid_scale_is_rejected() {
        for bits in [0, 9, 32, 255] {
            assert_eq!(
                create_opts(&options(Some(bits))).unwrap_err().to_string(),
                "Scaling bit depth must be 8 or 16"
            );
        }
    }

    #[test]
    fn explicit_output_type_casts_without_enabling_scaling() -> Result<()> {
        for data_type in [
            ArrayDataType::Int8,
            ArrayDataType::Uint8,
            ArrayDataType::Int16,
            ArrayDataType::Uint16,
            ArrayDataType::Int32,
            ArrayDataType::Uint32,
            ArrayDataType::Int64,
            ArrayDataType::Uint64,
            ArrayDataType::Float32,
            ArrayDataType::Float64,
        ] {
            let opts = TileCreationOptions {
                output_data_type: Some(data_type),
                ..options(None)
            };
            let result = create_opts(&opts)?;
            assert_eq!(result.output_data_type, Some(data_type));
            assert!(!result.scale);
        }
        Ok(())
    }

    #[test]
    fn unscale_defaults_to_float64_without_enabling_output_rescaling() -> Result<()> {
        let opts = TileCreationOptions {
            unscale: true,
            ..options(None)
        };
        let result = create_opts(&opts)?;
        assert_eq!(result.output_data_type, Some(ArrayDataType::Float64));
        assert!(!result.scale);
        Ok(())
    }

    #[test]
    fn explicit_type_overrides_unscale_float64_default() -> Result<()> {
        for data_type in [ArrayDataType::Int16, ArrayDataType::Float32, ArrayDataType::Float64] {
            let opts = TileCreationOptions {
                output_data_type: Some(data_type),
                unscale: true,
                ..options(None)
            };
            let result = create_opts(&opts)?;
            assert_eq!(result.output_data_type, Some(data_type));
            assert!(!result.scale);
        }
        Ok(())
    }

    #[test]
    fn scale_selects_encoded_type_with_or_without_unscale_and_matching_explicit_type() -> Result<()> {
        for (bits, encoded_type) in [(8, ArrayDataType::Uint8), (16, ArrayDataType::Uint16)] {
            for unscale in [false, true] {
                for output_data_type in [None, Some(encoded_type)] {
                    let opts = TileCreationOptions {
                        output_data_type,
                        unscale,
                        ..options(Some(bits))
                    };
                    let result = create_opts(&opts)?;
                    assert!(result.scale);
                    assert_eq!(result.output_data_type, Some(encoded_type));
                }
            }
        }
        Ok(())
    }

    #[test]
    fn scale_rejects_mismatched_explicit_types_with_or_without_unscale() {
        for (bits, encoded_type) in [(8, ArrayDataType::Uint8), (16, ArrayDataType::Uint16)] {
            for requested in [
                ArrayDataType::Int8,
                ArrayDataType::Uint8,
                ArrayDataType::Int16,
                ArrayDataType::Uint16,
                ArrayDataType::Int32,
                ArrayDataType::Uint32,
                ArrayDataType::Int64,
                ArrayDataType::Uint64,
                ArrayDataType::Float32,
                ArrayDataType::Float64,
            ] {
                if requested == encoded_type {
                    continue;
                }
                for unscale in [false, true] {
                    let opts = TileCreationOptions {
                        output_data_type: Some(requested),
                        unscale,
                        ..options(Some(bits))
                    };
                    assert_eq!(
                        create_opts(&opts).unwrap_err().to_string(),
                        format!("--output-data-type {requested} does not match --scale {bits}; expected {encoded_type}")
                    );
                }
            }
        }
    }

    #[test]
    fn both_entrypoints_validate_type_mismatches_before_preparing_or_opening_input() {
        for unscale in [false, true] {
            let expected = "--output-data-type float32 does not match --scale 8; expected uint8";
            let invalid_options = || TileCreationOptions {
                output_data_type: Some(ArrayDataType::Float32),
                unscale,
                ..options(Some(8))
            };
            let input = "missing-input-for-option-validation.tif";
            assert_eq!(
                print_gdal_translate_command(input, invalid_options()).unwrap_err().to_string(),
                expected
            );
            assert_eq!(
                create_cog_tiles(input, PathBuf::from("unused-output.tif"), invalid_options(), None)
                    .unwrap_err()
                    .to_string(),
                expected
            );
        }
    }
}

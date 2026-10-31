use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};

use crate::input::resolve_local_input_paths;

pub struct PreparedInput {
    pub input: String,
    pub multi_band: bool,
    _directory: tempfile::TempDir,
}

/// Decode packed input samples lazily, independently of output encoding.
/// Done by creating a temporary VRT file for each input dataset, with -unscale applied to all bands.
pub fn prepare_input(input: &str) -> Result<PreparedInput> {
    let paths = resolve_local_input_paths(input, Path::new(input))?.unwrap_or_else(|| vec![PathBuf::from(input)]);
    let directory = tempfile::tempdir()?;
    let mut total_bands = 0;
    let mut vrt_paths = Vec::with_capacity(paths.len());
    for (index, path) in paths.iter().enumerate() {
        let dataset = geo::raster::formats::gdal::open_dataset_read_only(path)?;
        let band_count = dataset.raster_count();
        ensure!(band_count > 0, "Input {} has no raster bands to unscale", path.display());
        for band_index in 1..=band_count {
            let band = dataset.rasterband(band_index)?;
            validate_scaling(band.scale(), band.offset()).with_context(|| {
                format!(
                    "Cannot --unscale input {} band {band_index} (output band {})",
                    path.display(),
                    total_bands + band_index
                )
            })?;
        }
        total_bands += band_count;

        let vrt = directory.path().join(format!("input-{index:08}.vrt"));
        // Float64 avoids truncating fractional decoded values to the packed input
        // type. The warp/output encoder applies the requested final datatype later.
        // -unscale already clears band scale/offset metadata after decoding.
        let options = ["-of", "VRT", "-unscale", "-ot", "Float64"].map(str::to_owned);
        geo::raster::algo::gdal::translate(&dataset, &vrt, &options)?.close()?;
        vrt_paths.push(vrt);
    }

    let input = if vrt_paths.len() == 1 {
        vrt_paths.remove(0)
    } else {
        directory.path().join("input-*.vrt")
    };

    Ok(PreparedInput {
        input: input.to_string_lossy().into_owned(),
        multi_band: total_bands > 1,
        _directory: directory,
    })
}

fn validate_scaling(scale: Option<f64>, offset: Option<f64>) -> Result<()> {
    let scale = scale.unwrap_or(1.0);
    let offset = offset.unwrap_or(0.0);
    ensure!(scale.is_finite() && offset.is_finite(), "Input scale and offset must be finite");
    ensure!(scale != 1.0 || offset != 0.0, "Input band is not scaled (scale=1, offset=0)");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unmatched_input_pattern_is_rejected() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let pattern = directory.path().join("nonexistent-*.tif");
        let error = prepare_input(pattern.to_str().unwrap()).err().unwrap();
        assert!(error.to_string().contains("No files match the input pattern"));
        Ok(())
    }

    #[test]
    fn unscaled_or_nonfinite_coefficients_are_rejected() {
        for (scale, offset) in [
            (None, None),
            (Some(1.0), Some(0.0)),
            (Some(f64::NAN), Some(0.0)),
            (Some(0.01), Some(f64::INFINITY)),
        ] {
            assert!(validate_scaling(scale, offset).is_err());
        }
    }

    #[test]
    fn scale_only_offset_only_and_constant_scaling_are_supported() -> Result<()> {
        for (scale, offset) in [(Some(0.01), None), (None, Some(-5.0)), (Some(0.0), Some(7.0)), (Some(-1.0), None)] {
            validate_scaling(scale, offset)?;
        }
        Ok(())
    }
}

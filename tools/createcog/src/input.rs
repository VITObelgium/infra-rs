use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};

use crate::netcdf;

/// A GDAL-readable raster input, optionally backed by a temporary `NetCDF` view.
pub struct PreparedInput {
    pub input: String,
    pub(super) band_input: Option<String>,
    pub(super) _directory: Option<tempfile::TempDir>,
}

impl PreparedInput {
    /// Conversions use small per-band views; the combined view is retained for inspection.
    pub fn conversion_input(&self) -> &str {
        self.band_input.as_deref().unwrap_or(&self.input)
    }
}

/// Prepare local `NetCDF` selections; leave other raster formats and GDAL connection strings unchanged.
pub fn prepare_input(input: &str, variables: &[String], dimensions: &[String], source_srs: Option<&str>) -> Result<PreparedInput> {
    let (path, variables) = netcdf::parse_input_path(input, variables)?;
    let Some(paths) = resolve_local_input_paths(input, &path)? else {
        ensure!(
            variables.is_empty(),
            "NetCDF variable selection requires a local input file: {}",
            path.display()
        );
        return Ok(PreparedInput {
            input: input.to_owned(),
            band_input: None,
            _directory: None,
        });
    };

    if netcdf::is_netcdf(&paths[0])? {
        return netcdf::prepare_input(&paths, variables, dimensions, source_srs);
    }

    ensure!(variables.is_empty(), "--variable is only supported for NetCDF inputs");
    Ok(PreparedInput {
        input: input.to_owned(),
        band_input: None,
        _directory: None,
    })
}

/// Resolve a local file or sorted glob matches; return `None` for inputs left to GDAL.
/// `path` may be extracted from a subdataset, while `input` is the original connection string.
pub fn resolve_local_input_paths(input: &str, path: &Path) -> Result<Option<Vec<PathBuf>>> {
    if path.is_file() {
        return Ok(Some(vec![path.canonicalize()?]));
    }
    if input.contains("://") || !path.to_string_lossy().contains(['*', '?', '[']) {
        return Ok(None);
    }

    let pattern = path.to_str().context("Input pattern is not valid UTF-8")?;
    let mut paths = glob::glob(pattern)?.collect::<std::result::Result<Vec<_>, _>>()?;
    paths.sort();
    ensure!(!paths.is_empty(), "No files match the input pattern: {pattern}");
    Ok(Some(paths))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_files_are_canonicalized() -> Result<()> {
        let directory = tempfile::tempdir_in(".")?;
        let path = directory.path().join("input.nc");
        std::fs::write(&path, [])?;
        assert_eq!(
            resolve_local_input_paths(path.to_str().unwrap(), &path)?,
            Some(vec![path.canonicalize()?])
        );
        Ok(())
    }

    #[test]
    fn literal_files_take_precedence_over_glob_characters() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("input[1].nc");
        std::fs::write(&path, [])?;
        std::fs::write(directory.path().join("input1.nc"), [])?;
        assert_eq!(
            resolve_local_input_paths(path.to_str().unwrap(), &path)?,
            Some(vec![path.canonicalize()?])
        );
        Ok(())
    }

    #[test]
    fn glob_matches_are_sorted() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let first = directory.path().join("a.nc");
        let second = directory.path().join("b.nc");
        std::fs::write(&second, [])?;
        std::fs::write(&first, [])?;
        let pattern = directory.path().join("*.nc");
        assert_eq!(
            resolve_local_input_paths(pattern.to_str().unwrap(), &pattern)?,
            Some(vec![first, second])
        );
        Ok(())
    }

    #[test]
    fn unmatched_patterns_are_rejected() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let pattern = directory.path().join("*.nc");
        let error = resolve_local_input_paths(pattern.to_str().unwrap(), &pattern).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("No files match the input pattern: {}", pattern.display())
        );
        Ok(())
    }

    #[test]
    fn connection_strings_are_not_expanded_as_globs() -> Result<()> {
        let input = "https://example.com/weather.nc?variable=WBGT";
        assert_eq!(resolve_local_input_paths(input, Path::new(input))?, None);
        Ok(())
    }

    #[test]
    fn missing_files_without_wildcards_are_left_to_the_caller() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("missing.nc");
        assert_eq!(resolve_local_input_paths(path.to_str().unwrap(), &path)?, None);
        Ok(())
    }
}

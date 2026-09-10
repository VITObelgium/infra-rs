use std::path::PathBuf;

use crate::{Error, Result, raster};

use super::{CogCreationOptions, create_cog_tiles};

/// Creates one temporary COG for every band in every file matching `input`.
///
/// A literal path can be passed for a single multiband input. Glob matches are
/// sorted, and bands are processed in their one-based GDAL band order.
pub fn create_temporary_band_cogs(input: &str, options: CogCreationOptions) -> Result<(tempfile::TempDir, Vec<PathBuf>)> {
    let mut input_paths = glob::glob(input)?.collect::<std::result::Result<Vec<_>, _>>()?;
    input_paths.sort();

    if input_paths.is_empty() {
        return Err(Error::InvalidArgument(format!("No files match the input pattern: {input}")));
    }

    let temporary_directory = tempfile::tempdir()?;
    let mut cog_paths = Vec::new();

    for input_path in input_paths {
        let band_count = raster::formats::gdal::open_dataset_read_only(&input_path)?.raster_count();

        if band_count == 1 {
            let cog_path = temporary_directory.path().join(format!("band-{:04}.cog.tif", cog_paths.len()));
            create_cog_tiles(&input_path, &cog_path, options)?;
            cog_paths.push(cog_path);
            continue;
        }

        for band_index in 1..=band_count {
            let index = cog_paths.len();
            let band_path = temporary_directory.path().join(format!("band-{index:04}.tif"));
            let cog_path = temporary_directory.path().join(format!("band-{index:04}.cog.tif"));
            let translate_options = ["-of".to_string(), "GTiff".to_string(), "-b".to_string(), band_index.to_string()];

            raster::algo::gdal::translate_file(&input_path, &band_path, &translate_options)?;
            create_cog_tiles(&band_path, &cog_path, options)?;
            cog_paths.push(cog_path);
        }
    }

    Ok((temporary_directory, cog_paths))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cog::CogCreationOptions, testutils};

    #[test_log::test]
    fn creates_one_cog_per_band_of_a_multiband_file() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("multiband_cog.tif");
        let band_count = raster::formats::gdal::open_dataset_read_only(&input)?.raster_count();

        let (temporary_directory, cogs) = create_temporary_band_cogs(&input.to_string_lossy(), CogCreationOptions::default())?;

        assert_eq!(cogs.len(), band_count);
        for path in &cogs {
            assert_eq!(raster::formats::gdal::open_dataset_read_only(path)?.raster_count(), 1);
        }

        let temporary_path = temporary_directory.path().to_path_buf();
        drop(temporary_directory);
        assert!(!temporary_path.exists());

        Ok(())
    }

    #[test_log::test]
    fn creates_cogs_for_every_file_in_a_glob() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let source = testutils::workspace_test_data_dir().join("landusebyte.tif");
        std::fs::copy(&source, directory.path().join("a.tif"))?;
        std::fs::copy(&source, directory.path().join("b.tif"))?;

        let pattern = directory.path().join("*.tif").to_string_lossy().into_owned();
        let (_temporary_directory, cogs) = create_temporary_band_cogs(&pattern, CogCreationOptions::default())?;

        assert_eq!(cogs.len(), 2);
        assert!(cogs.iter().all(|path| path.exists()));

        Ok(())
    }

    #[test]
    fn rejects_an_empty_glob() {
        let directory = tempfile::tempdir().expect("failed to create temporary directory");
        let pattern = directory.path().join("*.tif").to_string_lossy().into_owned();
        let result = create_temporary_band_cogs(&pattern, CogCreationOptions::default());

        assert!(matches!(result, Err(Error::InvalidArgument(message)) if message.contains("No files match")));
    }
}

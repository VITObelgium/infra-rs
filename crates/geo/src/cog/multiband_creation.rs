use std::path::PathBuf;

use crate::{Error, Result, raster};

use super::{CogCreationOptions, creation::create_cog_tiles_for_band};

/// Creates one temporary COG for every band in every file matching `input`.
///
/// A literal path can be passed for a single multiband input. Glob matches are
/// sorted, and bands are processed in their one-based GDAL band order.
pub fn create_temporary_band_cogs(
    input: &str,
    options: CogCreationOptions,
    source_srs: Option<&str>,
    mut progress: Option<&mut dyn FnMut(f64)>,
) -> Result<(tempfile::TempDir, Vec<PathBuf>)> {
    let mut input_paths = glob::glob(input)?.collect::<std::result::Result<Vec<_>, _>>()?;
    input_paths.sort();

    if input_paths.is_empty() {
        return Err(Error::InvalidArgument(format!("No files match the input pattern: {input}")));
    }

    let temporary_directory = tempfile::tempdir()?;
    let mut band_jobs = Vec::new();

    for input_path in input_paths {
        let band_count = raster::formats::gdal::open_dataset_read_only(&input_path)?.raster_count();

        for band_index in 1..=band_count {
            let index = band_jobs.len();
            let cog_path = temporary_directory.path().join(format!("band-{index:04}.cog.tif"));
            band_jobs.push((input_path.clone(), cog_path, band_index));
        }
    }

    let band_count = band_jobs.len();
    let source_srs = source_srs.map(str::to_owned);

    #[cfg(feature = "rayon")]
    let warp_results = {
        use rayon::prelude::*;
        use std::sync::mpsc;

        let (progress_sender, progress_receiver) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            band_jobs
                .par_iter()
                .map(|(input_path, cog_path, band_index)| {
                    create_cog_tiles_for_band(input_path, cog_path, options, *band_index, source_srs.as_deref()).map(|()| {
                        progress_sender.send(()).ok();
                        cog_path.clone()
                    })
                })
                .collect::<Result<Vec<_>>>()
        });

        if let Some(progress) = progress.as_mut() {
            for completed in 1..=band_count {
                if progress_receiver.recv().is_err() {
                    break;
                }
                progress(completed as f64 / band_count as f64);
            }
        }

        worker
            .join()
            .map_err(|_| Error::Runtime("Temporary band warp thread panicked".to_string()))?
    };

    #[cfg(not(feature = "rayon"))]
    let warp_results = band_jobs
        .iter()
        .enumerate()
        .map(|(completed, (input_path, cog_path, band_index))| {
            create_cog_tiles_for_band(input_path, cog_path, options, *band_index, source_srs.as_deref()).map(|()| {
                if let Some(progress) = progress.as_mut() {
                    progress((completed + 1) as f64 / band_count as f64);
                }
                cog_path.clone()
            })
        })
        .collect::<Result<Vec<_>>>();

    Ok((temporary_directory, warp_results?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cog::CogCreationOptions, testutils};

    #[test_log::test]
    fn creates_one_cog_per_band_of_a_multiband_file() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("multiband_cog.tif");
        let band_count = raster::formats::gdal::open_dataset_read_only(&input)?.raster_count();

        let mut progress_updates = Vec::new();
        let (temporary_directory, cogs) = create_temporary_band_cogs(
            &input.to_string_lossy(),
            CogCreationOptions::default(),
            None,
            Some(&mut |progress| progress_updates.push(progress)),
        )?;

        assert_eq!(cogs.len(), band_count);
        assert_eq!(progress_updates.len(), band_count);
        assert_eq!(progress_updates.last(), Some(&1.0));
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
        let (_temporary_directory, cogs) = create_temporary_band_cogs(&pattern, CogCreationOptions::default(), None, None)?;

        assert_eq!(cogs.len(), 2);
        assert!(cogs.iter().all(|path| path.exists()));

        Ok(())
    }

    #[test]
    fn rejects_an_empty_glob() {
        let directory = tempfile::tempdir().expect("failed to create temporary directory");
        let pattern = directory.path().join("*.tif").to_string_lossy().into_owned();
        let result = create_temporary_band_cogs(&pattern, CogCreationOptions::default(), None, None);

        assert!(matches!(result, Err(Error::InvalidArgument(message)) if message.contains("No files match")));
    }
}

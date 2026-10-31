use std::path::{Path, PathBuf};

use crate::{Error, Result, raster};

use super::{CogCreationOptions, creation::create_cog_tiles_for_band};

/// Creates one temporary COG for every band in every file matching `input`.
///
/// A literal path can be passed for a single multiband input. Glob matches are
/// sorted, and bands are processed in their one-based GDAL band order.
/// Progress reports completed bands divided by the total number of bands, once
/// per successfully processed band. Callbacks run on the calling thread.
pub fn create_temporary_band_cogs(
    input: &str,
    options: CogCreationOptions,
    source_srs: Option<&str>,
    mut progress: Option<&mut dyn FnMut(f64)>,
) -> Result<(tempfile::TempDir, Vec<PathBuf>)> {
    let temporary_directory = tempfile::tempdir()?;
    let band_jobs = plan_band_jobs(input, temporary_directory.path())?;
    let total_bands = band_jobs.len();
    let mut completed_bands = 0;

    #[cfg(feature = "rayon")]
    let warp_results = std::thread::scope(|scope| {
        use rayon::prelude::*;
        use std::sync::mpsc;

        let (sender, receiver) = mpsc::channel();
        let worker = scope.spawn(move || {
            band_jobs
                .par_iter()
                .map(|(input_path, cog_path, band_index)| {
                    create_cog_tiles_for_band(input_path, cog_path, options, *band_index, source_srs)?;
                    sender.send(()).ok();
                    Ok(cog_path.clone())
                })
                .collect::<Result<Vec<_>>>()
        });

        for () in receiver {
            completed_bands += 1;
            if let Some(progress) = progress.as_mut() {
                progress(completed_bands as f64 / total_bands as f64);
            }
        }

        worker
            .join()
            .map_err(|_| Error::Runtime("Temporary band warp thread panicked".to_string()))?
    });

    #[cfg(not(feature = "rayon"))]
    let warp_results = band_jobs
        .iter()
        .map(|(input_path, cog_path, band_index)| {
            create_cog_tiles_for_band(input_path, cog_path, options, *band_index, source_srs)?;
            completed_bands += 1;
            if let Some(progress) = progress.as_mut() {
                progress(completed_bands as f64 / total_bands as f64);
            }
            Ok(cog_path.clone())
        })
        .collect::<Result<Vec<_>>>();

    Ok((temporary_directory, warp_results?))
}

fn plan_band_jobs(input: &str, directory: &Path) -> Result<Vec<(PathBuf, PathBuf, usize)>> {
    let mut input_paths = glob::glob(input)?.collect::<std::result::Result<Vec<_>, _>>()?;
    input_paths.sort();

    if input_paths.is_empty() {
        return Err(Error::InvalidArgument(format!("No files match the input pattern: {input}")));
    }

    let mut band_jobs = Vec::new();
    for input_path in input_paths {
        let band_count = raster::formats::gdal::open_dataset_read_only(&input_path)?.raster_count();
        for band_index in 1..=band_count {
            let index = band_jobs.len();
            let cog_path = directory.join(format!("band-{index:04}.cog.tif"));
            band_jobs.push((input_path.clone(), cog_path, band_index));
        }
    }
    Ok(band_jobs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cog::CogCreationOptions, testutils};

    #[test_log::test]
    fn creates_one_cog_per_band_of_a_multiband_file() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("multiband_cog.tif");
        let band_count = raster::formats::gdal::open_dataset_read_only(&input)?.raster_count();

        let calling_thread = std::thread::current().id();
        let mut progress_updates = Vec::new();
        let (temporary_directory, cogs) = create_temporary_band_cogs(
            &input.to_string_lossy(),
            CogCreationOptions {
                scale: true,
                ..Default::default()
            },
            None,
            Some(&mut |progress| {
                assert_eq!(std::thread::current().id(), calling_thread);
                progress_updates.push(progress);
            }),
        )?;

        assert_eq!(cogs.len(), band_count);
        assert_eq!(
            progress_updates,
            (1..=band_count)
                .map(|completed| completed as f64 / band_count as f64)
                .collect::<Vec<_>>()
        );
        for path in &cogs {
            let dataset = raster::formats::gdal::open_dataset_read_only(path)?;
            assert_eq!(dataset.raster_count(), 1);
            let band = dataset.rasterband(1)?;
            assert_eq!(band.band_type(), gdal::raster::GdalDataType::UInt8);
            assert!(band.scale().is_some());
            assert!(band.offset().is_some());
        }

        let temporary_path = temporary_directory.path().to_path_buf();
        drop(temporary_directory);
        assert!(!temporary_path.exists());

        Ok(())
    }

    #[test_log::test]
    fn failed_scaling_does_not_report_a_completed_band() {
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let mut progress_updates = Vec::new();
        let result = create_temporary_band_cogs(
            &input.to_string_lossy(),
            CogCreationOptions {
                scale: true,
                output_data_type: Some(crate::ArrayDataType::Float32),
                ..Default::default()
            },
            None,
            Some(&mut |progress| progress_updates.push(progress)),
        );

        assert!(matches!(result, Err(Error::InvalidArgument(message)) if message.contains("Scaling only supports")));
        assert!(progress_updates.is_empty());
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

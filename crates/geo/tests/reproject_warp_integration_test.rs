#![cfg_attr(feature = "simd", feature(portable_simd, allocator_api))]

#[cfg(all(test, any(feature = "proj", feature = "proj4rs")))]
mod tests {
    use approx::assert_relative_eq;
    use geo::{
        Array, ArrayNum, CellSize, Columns, Error, RasterSize, Result, Rows,
        crs::{self},
        raster::{
            self, DenseRaster, RasterReadWrite,
            algo::{self, NumThreads, TargetPixelAlignment, TargetSrs, WarpOptions, WarpTargetSize, warp},
        },
    };
    use path_macro::path;
    use std::collections::HashMap;
    use std::time::Duration;
    use std::{path::Path, process::Command};
    use tempfile::TempDir;

    #[cfg(feature = "simd")]
    const LANES: usize = inf::simd::LANES;

    /// Dumps comparison timings to stdout
    fn dump_comparison_timings(timings: HashMap<String, (Duration, Duration)>) {
        if timings.is_empty() {
            return;
        }

        println!("\n=== Geo vs GDAL Performance Comparison ===");

        // Sort by geo duration (longest first)
        let mut sorted_timings: Vec<_> = timings.into_iter().collect();
        sorted_timings.sort_by(|a, b| b.1.0.cmp(&a.1.0));

        let mut total_geo_duration = Duration::new(0, 0);
        let mut total_gdal_duration = Duration::new(0, 0);

        println!("{:<50} {:>10} {:>10} {:>14}", "Test Name", "Geo (s)", "GDAL (s)", "Speedup");
        println!("{}", "=".repeat(87));

        for (test_name, (geo_duration, gdal_duration)) in &sorted_timings {
            total_geo_duration += *geo_duration;
            total_gdal_duration += *gdal_duration;

            let speedup = if gdal_duration.as_secs_f64() > 0.0 {
                geo_duration.as_secs_f64() / gdal_duration.as_secs_f64()
            } else {
                f64::INFINITY
            };

            let speedup_str = if speedup < 1.0 {
                format!("\x1b[32m{:>6.2}x faster\x1b[0m", 1.0 / speedup)
            } else if speedup > 1.0 {
                format!("\x1b[31m{:>6.2}x slower\x1b[0m", speedup)
            } else {
                "same".to_string()
            };

            println!(
                "{:<50} {:>10.3} {:>10.3} {:>14}",
                test_name,
                geo_duration.as_secs_f64(),
                gdal_duration.as_secs_f64(),
                speedup_str
            );

            // use granite_rs::*;
            // println!(
            //     "{}",
            //     bars(
            //         "Warp comparison",
            //         &[
            //             ("geo".to_string(), geo_duration.as_secs_f64()),
            //             ("gdal".to_string(), gdal_duration.as_secs_f64()),
            //         ],
            //         &def_plot()
            //     )
            // );
        }

        println!("{}", "=".repeat(87));
        println!(
            "{:<50} {:>10.3} {:>10.3}",
            "TOTAL",
            total_geo_duration.as_secs_f64(),
            total_gdal_duration.as_secs_f64()
        );

        println!("{}", "=".repeat(87));
    }

    fn workspace_test_data_dir() -> std::path::PathBuf {
        path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data")
    }

    fn test_results_output_dir() -> std::path::PathBuf {
        path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "reproject_debug")
    }

    fn create_gdalwarp_args(opts: &WarpOptions, src_path: &Path, dst_path: &Path) -> Vec<String> {
        let mut args = vec![src_path.to_string_lossy().to_string(), dst_path.to_string_lossy().to_string()];
        args.extend(raster::algo::gdal::warp_options_to_gdalwarp_cli_args(opts));
        args
    }

    /// Execute gdalwarp as external process
    fn run_gdalwarp(args: &[String]) -> Result<()> {
        log::info!("Gdal warp cmd: {}", args.join(" "));
        let output = Command::new("gdalwarp")
            .args(args)
            .output()
            .map_err(|e| Error::Runtime(format!("Failed to execute gdalwarp: {}. Make sure gdalwarp is in PATH.", e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(Error::Runtime(format!(
                "gdalwarp failed with exit code: {}\nstdout: {}\nstderr: {}",
                output.status.code().unwrap_or(-1),
                stdout,
                stderr
            )));
        }

        Ok(())
    }

    fn compare_raster_metadata<T: geo::ArrayNum>(raster1: &DenseRaster<T>, raster2: &DenseRaster<T>, bbox_tolerance: f64) {
        assert_eq!(
            raster1.metadata().raster_size(),
            raster2.metadata().raster_size(),
            "Raster sizes differ: {:?} vs {:?}",
            raster1.metadata().raster_size(),
            raster2.metadata().raster_size()
        );

        assert_relative_eq!(raster1.metadata().cell_size(), raster2.metadata().cell_size(), epsilon = 1e-6,);

        assert_eq!(
            raster1.metadata().projected_epsg(),
            raster2.metadata().projected_epsg(),
            "EPSG codes differ: {:?} vs {:?}",
            raster1.metadata().projected_epsg(),
            raster2.metadata().projected_epsg()
        );

        assert_relative_eq!(
            raster1.metadata().bounding_box(),
            raster2.metadata().bounding_box(),
            epsilon = bbox_tolerance
        );
    }

    fn compare_raster_contents<T>(raster1: &DenseRaster<T>, raster2: &DenseRaster<T>, cell_diff_percentage_tolerance: f64) -> Result<()>
    where
        T: geo::ArrayNum,
    {
        let diff = algo::array_diff(raster1, raster2)?;
        assert!(
            diff.mismatches.len() as f64 / raster1.len() as f64 * 100.0 < cell_diff_percentage_tolerance,
            "Raster contents differ too much: {} mismatches out of {} cells ({:.2}%)",
            diff.mismatches.len(),
            raster1.len(),
            diff.mismatches.len() as f64 / raster1.len() as f64 * 100.0
        );

        Ok(())
    }

    #[geo::simd_bounds]
    #[cfg(feature = "gdal")]
    fn warp_using_linked_gdal<T: ArrayNum>(input: &Path, tmp_dir: &TempDir, opts: &WarpOptions) -> Result<DenseRaster<T>> {
        let output_path = tmp_dir.path().join("gdal_warped.tif");

        let gdalwarp_args = raster::algo::gdal::warp_options_to_gdalwarp_cli_args(opts);
        let src_ds = gdal::Dataset::open(input)?;
        raster::algo::gdal::warp_to_disk_cli(&src_ds, &output_path, &gdalwarp_args, &Vec::default())?;
        DenseRaster::<T>::read(&output_path)
    }

    #[geo::simd_bounds]
    #[allow(dead_code)]
    /// This assumes the gdalwarp binary is available in the PATH.
    fn warp_using_gdal_binary<T: ArrayNum>(input: &Path, tmp_dir: &TempDir, opts: &WarpOptions) -> Result<DenseRaster<T>> {
        let output_path = tmp_dir.path().join("gdal_warped.tif");

        // Run GDAL warp via external process with equivalent settings
        let gdalwarp_args = create_gdalwarp_args(opts, input, &output_path);
        run_gdalwarp(&gdalwarp_args)?;

        DenseRaster::<T>::read(&output_path)
    }

    #[geo::simd_bounds]
    fn warp_using_gdal<T: ArrayNum>(input: &Path, opts: &WarpOptions) -> Result<DenseRaster<T>> {
        let tmp_dir = TempDir::new()?;

        #[cfg(feature = "gdal")]
        return warp_using_linked_gdal(input, &tmp_dir, opts);

        #[cfg(not(feature = "gdal"))]
        return warp_using_gdal_binary(input, &tmp_dir, opts);
    }

    #[geo::simd_bounds]
    fn store_test_output<T: ArrayNum>(geo: DenseRaster<T>, gdal: DenseRaster<T>, name: &str) -> Result<()> {
        let output_dir = test_results_output_dir();
        geo.into_write(output_dir.join(format!("{}_geo.tif", name)))?;
        gdal.into_write(output_dir.join(format!("{}_gdal.tif", name)))?;
        Ok(())
    }

    #[geo::simd_bounds]
    fn run_comparison<T: ArrayNum>(
        input: &Path,
        opts: &WarpOptions,
        name: &str,
        bbox_tolerance: f64,
        raster_diff_tolerance: f64,
        timings: &mut HashMap<String, (Duration, Duration)>,
    ) -> Result<()> {
        log::debug!("[{name}] Running warp comparison");

        let start = std::time::Instant::now();
        let geo_raster = warp(&DenseRaster::<T>::read(input)?, opts)?;
        let geo_duration = start.elapsed();
        log::debug!("[{name}] Geo warp duration: {geo_duration:?}");

        let start = std::time::Instant::now();
        let gdal_raster = warp_using_gdal(input, opts)?;
        let gdal_duration = start.elapsed();
        log::debug!("[{name}] Gdal warp duration {gdal_duration:?}");

        // Store the timing comparison
        timings.insert(name.to_string(), (geo_duration, gdal_duration));

        #[cfg(not(debug_assertions))]
        if gdal_duration < geo_duration {
            log::warn!(
                "[{name}] !!! GDAL warp was faster than Geo warp: {:.2}% faster !!!",
                (1.0 - geo_duration.as_secs_f64() / gdal_duration.as_secs_f64()).abs() * 100.0
            );
        } else {
            log::debug!(
                "[{name}] Geo warp was faster than GDAL warp: {:.2}% faster",
                (1.0 - gdal_duration.as_secs_f64() / geo_duration.as_secs_f64()).abs() * 100.0
            );
        }

        compare_raster_metadata(&geo_raster, &gdal_raster, bbox_tolerance);
        compare_raster_contents(&geo_raster, &gdal_raster, raster_diff_tolerance)?;
        store_test_output(geo_raster, gdal_raster, name)
    }

    fn integration_warp_vs_gdalwarp_source_size(timings: &mut HashMap<String, (Duration, Duration)>) -> Result<()> {
        let input_path = workspace_test_data_dir().join("landusebyte.tif");
        run_comparison::<u8>(
            &input_path,
            &WarpOptions {
                error_threshold: 0.0,
                target_size: WarpTargetSize::Source,
                target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
                ..Default::default()
            },
            "source_size_et_0",
            1.0,
            0.5,
            timings,
        )
    }

    fn integration_warp_vs_gdalwarp_source_size_mt(timings: &mut HashMap<String, (Duration, Duration)>) -> Result<()> {
        let input_path = workspace_test_data_dir().join("landusebyte.tif");
        run_comparison::<u8>(
            &input_path,
            &WarpOptions {
                error_threshold: 0.0,
                target_size: WarpTargetSize::Source,
                target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
                num_threads: NumThreads::AllCpus,
            },
            "source_size_et_0_mt",
            1.0,
            0.5,
            timings,
        )
    }

    fn integration_warp_vs_gdalwarp_source_size_error_threshold(timings: &mut HashMap<String, (Duration, Duration)>) -> Result<()> {
        let input_path = workspace_test_data_dir().join("landusebyte.tif");
        run_comparison::<u8>(
            &input_path,
            &WarpOptions {
                error_threshold: 0.125,
                target_size: WarpTargetSize::Source,
                target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
                ..Default::default()
            },
            "source_size_et_0.125",
            1.0,
            5.0,
            timings,
        )
    }

    fn integration_warp_vs_gdalwarp_source_size_error_threshold_mt(timings: &mut HashMap<String, (Duration, Duration)>) -> Result<()> {
        let input_path = workspace_test_data_dir().join("landusebyte.tif");
        run_comparison::<u8>(
            &input_path,
            &WarpOptions {
                error_threshold: 0.125,
                target_size: WarpTargetSize::Source,
                target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
                num_threads: NumThreads::AllCpus,
            },
            "source_size_et_0.125_mt",
            1.0,
            5.0,
            timings,
        )
    }

    fn integration_warp_vs_gdalwarp_fixed_size(timings: &mut HashMap<String, (Duration, Duration)>) -> Result<()> {
        let input_path = workspace_test_data_dir().join("landusebyte.tif");
        run_comparison::<u8>(
            &input_path,
            &WarpOptions {
                error_threshold: 0.0,
                target_size: WarpTargetSize::Sized(RasterSize::with_rows_cols(Rows(500), Columns(800))),
                target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
                ..Default::default()
            },
            "fixed_size_et_0",
            1.0,
            0.5,
            timings,
        )
    }

    fn integration_warp_vs_gdalwarp_fixed_size_error_threshold(timings: &mut HashMap<String, (Duration, Duration)>) -> Result<()> {
        let input_path = workspace_test_data_dir().join("landusebyte.tif");
        run_comparison::<u8>(
            &input_path,
            &WarpOptions {
                error_threshold: 0.125,
                target_size: WarpTargetSize::Sized(RasterSize::with_rows_cols(Rows(500), Columns(800))),
                target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
                ..Default::default()
            },
            "fixed_size_et_0.125",
            1.0,
            5.0,
            timings,
        )
    }

    fn integration_warp_vs_gdalwarp_cell_size(timings: &mut HashMap<String, (Duration, Duration)>) -> Result<()> {
        let input_path = workspace_test_data_dir().join("landusebyte.tif");
        run_comparison::<u8>(
            &input_path,
            &WarpOptions {
                error_threshold: 0.0,
                target_size: WarpTargetSize::CellSize(CellSize::square(75.0), TargetPixelAlignment::No),
                target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
                ..Default::default()
            },
            "cell_size_et_0",
            5.0,
            0.5,
            timings,
        )
    }

    fn integration_warp_vs_gdalwarp_cell_size_error_threshold(timings: &mut HashMap<String, (Duration, Duration)>) -> Result<()> {
        let input_path = workspace_test_data_dir().join("landusebyte.tif");
        run_comparison::<u8>(
            &input_path,
            &WarpOptions {
                error_threshold: 0.125,
                target_size: WarpTargetSize::CellSize(CellSize::square(75.0), TargetPixelAlignment::No),
                target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
                ..Default::default()
            },
            "cell_size_et_0.125",
            5.0,
            5.0,
            timings,
        )
    }

    fn integration_warp_vs_gdalwarp_cell_size_target_aligned_pixels(timings: &mut HashMap<String, (Duration, Duration)>) -> Result<()> {
        let input_path = workspace_test_data_dir().join("landusebyte.tif");
        run_comparison::<u8>(
            &input_path,
            &WarpOptions {
                error_threshold: 0.125,
                target_size: WarpTargetSize::CellSize(CellSize::square(100.0), TargetPixelAlignment::Yes),
                target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
                ..Default::default()
            },
            "cell_size_tap_et_0.125",
            1e-6,
            0.5,
            timings,
        )
    }

    fn slow_test_integration_warp_vs_gdalwarp_cell_size_target_aligned_pixels_10m_mt(
        timings: &mut HashMap<String, (Duration, Duration)>,
    ) -> Result<()> {
        let input_path = workspace_test_data_dir().join("landusebyte.tif");
        run_comparison::<u8>(
            &input_path,
            &WarpOptions {
                error_threshold: 0.125,
                target_size: WarpTargetSize::CellSize(CellSize::square(10.0), TargetPixelAlignment::Yes),
                target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
                num_threads: NumThreads::AllCpus,
            },
            "cell_size_tap_10m_mt_et_0.125",
            1e-6,
            1.0,
            timings,
        )
    }

    #[test_log::test]
    fn run_all_warp_integration_tests() -> Result<()> {
        println!("Running all warp integration tests sequentially...\n");

        let mut timings = HashMap::new();

        integration_warp_vs_gdalwarp_source_size(&mut timings)?;
        integration_warp_vs_gdalwarp_source_size_mt(&mut timings)?;
        integration_warp_vs_gdalwarp_source_size_error_threshold(&mut timings)?;
        integration_warp_vs_gdalwarp_source_size_error_threshold_mt(&mut timings)?;
        integration_warp_vs_gdalwarp_fixed_size(&mut timings)?;
        integration_warp_vs_gdalwarp_fixed_size_error_threshold(&mut timings)?;
        integration_warp_vs_gdalwarp_cell_size(&mut timings)?;
        integration_warp_vs_gdalwarp_cell_size_error_threshold(&mut timings)?;
        integration_warp_vs_gdalwarp_cell_size_target_aligned_pixels(&mut timings)?;
        slow_test_integration_warp_vs_gdalwarp_cell_size_target_aligned_pixels_10m_mt(&mut timings)?;

        dump_comparison_timings(timings);

        Ok(())
    }
}

#[cfg(all(test, any(feature = "proj", feature = "proj4rs")))]
mod tests {
    use approx::assert_relative_eq;
    use geo::{
        Array, ArrayNum, CellSize, Columns, Error, RasterSize, Result, Rows,
        crs::{self},
        raster::{
            self, DenseRaster, RasterIO,
            algo::{self, TargetPixelAlignment, TargetSrs, WarpOptions, WarpTargetSize, reproject},
        },
    };
    use path_macro::path;
    use std::{path::Path, process::Command};
    use tempfile::TempDir;

    fn workspace_test_data_dir() -> std::path::PathBuf {
        path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data")
    }

    fn warp_options_to_gdalwarp_args(opts: &WarpOptions) -> Vec<String> {
        let mut args = Vec::default();

        // Handle target size based on WarpTargetSize
        match &opts.target_size {
            WarpTargetSize::Source => {
                // Default behavior - GDAL will try to preserve resolution
            }
            WarpTargetSize::Sized(raster_size) => {
                args.extend([
                    "-ts".to_string(),
                    raster_size.cols.count().to_string(),
                    raster_size.rows.count().to_string(),
                ]);
            }
            WarpTargetSize::CellSize(cell_size, alignment) => {
                args.extend(["-tr".to_string(), cell_size.x().to_string(), cell_size.y().abs().to_string()]);
                if let TargetPixelAlignment::Yes = alignment {
                    args.push("-tap".to_string()); // Target aligned pixels
                }
            }
        }

        match &opts.target_srs {
            TargetSrs::Epsg(epsg) => {
                args.extend(["-t_srs".to_string(), format!("{}", epsg)]);
            }
            TargetSrs::Proj4(proj4) => {
                args.extend(["-t_srs".to_string(), proj4.clone()]);
            }
        }

        // Error threshold (corresponds to -et option in gdalwarp)
        args.extend(["-et".to_string(), opts.error_threshold.to_string()]);

        // Multi-threading
        if opts.all_cpus {
            args.extend(["-multi".to_string(), "-wo".to_string(), "NUM_THREADS=ALL_CPUS".to_string()]);
        }

        // Output format
        args.extend(["-of".to_string(), "GTiff".to_string()]);

        args
    }

    fn create_gdalwarp_args(opts: &WarpOptions, src_path: &Path, dst_path: &Path) -> Vec<String> {
        let mut args = vec![src_path.to_string_lossy().to_string(), dst_path.to_string_lossy().to_string()];
        args.extend(warp_options_to_gdalwarp_args(opts));
        args
    }

    /// Execute gdalwarp as external process
    fn run_gdalwarp(args: &[String]) -> Result<()> {
        println!("Gdal warp cmd: {}", args.join(" "));
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

    #[cfg(feature = "gdal")]
    fn warp_using_linked_gdal<T: ArrayNum>(input: &Path, tmp_dir: &TempDir, opts: &WarpOptions) -> Result<DenseRaster<T>> {
        let output_path = tmp_dir.path().join("gdal_warped.tif");

        let gdalwarp_args = warp_options_to_gdalwarp_args(opts);
        let src_ds = gdal::Dataset::open(input)?;
        raster::algo::warp_to_disk_cli(&src_ds, &output_path, &gdalwarp_args, &Vec::default())?;
        DenseRaster::<T>::read(&output_path)
    }

    #[allow(dead_code)]
    /// This assumes the gdalwarp binary is available in the PATH.
    fn warp_using_gdal_binary<T: ArrayNum>(input: &Path, tmp_dir: &TempDir, opts: &WarpOptions) -> Result<DenseRaster<T>> {
        let output_path = tmp_dir.path().join("gdal_warped.tif");

        // Run GDAL warp via external process with equivalent settings
        let gdalwarp_args = create_gdalwarp_args(opts, input, &output_path);
        run_gdalwarp(&gdalwarp_args)?;

        DenseRaster::<T>::read(&output_path)
    }

    fn warp_using_gdal<T: ArrayNum>(input: &Path, opts: &WarpOptions) -> Result<DenseRaster<T>> {
        let tmp_dir = TempDir::new()?;

        #[cfg(feature = "gdal")]
        return warp_using_linked_gdal(input, &tmp_dir, opts);

        #[cfg(not(feature = "gdal"))]
        return warp_using_gdal_binary(input, &tmp_dir, opts);
    }

    #[test_log::test]
    fn test_reproject_vs_gdalwarp_source_size() -> Result<()> {
        let input_path = workspace_test_data_dir().join("landusebyte.tif");

        let warp_opts = WarpOptions {
            error_threshold: 0.0,
            target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
            ..Default::default()
        };
        let gdal_raster = warp_using_gdal(&input_path, &warp_opts)?;
        let our_raster = reproject(&DenseRaster::<u8>::read(&input_path)?, &warp_opts)?;

        compare_raster_metadata(&our_raster, &gdal_raster, 20.0);
        compare_raster_contents(&our_raster, &gdal_raster, 7.5)?;

        Ok(())
    }

    #[test_log::test]
    fn test_reproject_vs_gdalwarp_fixed_size() -> Result<()> {
        let input_path = workspace_test_data_dir().join("landusebyte.tif");

        let target_size = RasterSize::with_rows_cols(Rows(500), Columns(800));
        let warp_opts = WarpOptions {
            error_threshold: 0.0,
            target_size: WarpTargetSize::Sized(target_size),
            target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
            ..Default::default()
        };
        let gdal_raster = warp_using_gdal(&input_path, &warp_opts)?;
        let our_raster = reproject(&DenseRaster::<u8>::read(&input_path)?, &warp_opts)?;

        compare_raster_metadata(&our_raster, &gdal_raster, 20.0);
        compare_raster_contents(&our_raster, &gdal_raster, 7.5)?;

        Ok(())
    }

    #[test_log::test]
    fn test_reproject_vs_gdalwarp_cell_size() -> Result<()> {
        let input_path = workspace_test_data_dir().join("landusebyte.tif");

        let target_cell_size = CellSize::square(75.0);
        let warp_opts = WarpOptions {
            error_threshold: 0.0,
            target_size: WarpTargetSize::CellSize(target_cell_size, TargetPixelAlignment::No),
            target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
            ..Default::default()
        };
        let gdal_raster = warp_using_gdal(&input_path, &warp_opts)?;
        let our_raster = reproject(&DenseRaster::<u8>::read(&input_path)?, &warp_opts)?;

        compare_raster_metadata(&our_raster, &gdal_raster, 20.0);
        compare_raster_contents(&our_raster, &gdal_raster, 7.5)?;

        Ok(())
    }

    // #[test_log::test]
    // fn test_reproject_vs_gdalwarp_cell_size_target_aligned_pixels() -> Result<()> {
    //     let input_path = workspace_test_data_dir().join("landusebyte.tif");

    //     let target_cell_size = CellSize::square(75.0);
    //     let warp_opts = WarpOptions {
    //         error_threshold: 0.0,
    //         target_size: WarpTargetSize::CellSize(target_cell_size, TargetPixelAlignment::Yes), // Use source size to match GDAL's default behavior
    //         target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
    //         ..Default::default()
    //     };
    //     let gdal_raster = warp_using_gdal(&input_path, &warp_opts)?;
    //     let our_raster = reproject(&DenseRaster::<u8>::read(&input_path)?, &warp_opts)?;

    //     compare_raster_metadata(&our_raster, &gdal_raster, 20.0);
    //     compare_raster_contents(&our_raster, &gdal_raster, 7.5)?;

    //     Ok(())
    // }
}

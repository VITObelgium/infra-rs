use std::path::{Path, PathBuf};

use crate::{
    ArrayDataType, Error, GeoReference, Result, Tile, ZoomLevelStrategy, crs,
    raster::{self, Compression},
};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum PredictorSelection {
    Horizontal,
    FloatingPoint,
    Automatic,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct CogCreationOptions {
    pub min_zoom: Option<i32>,
    pub zoom_level_strategy: ZoomLevelStrategy,
    pub tile_size: u32,
    pub compression: Option<Compression>,
    pub predictor: Option<PredictorSelection>,
    pub allow_sparse: bool,
    pub output_data_type: Option<ArrayDataType>,
    pub aligned_levels: Option<i32>,
    pub scale: bool,
}

impl Default for CogCreationOptions {
    fn default() -> Self {
        Self {
            min_zoom: None,
            zoom_level_strategy: ZoomLevelStrategy::Closest,
            tile_size: 512,
            compression: Some(Compression::Lzw),
            predictor: Some(PredictorSelection::Automatic),
            allow_sparse: true,
            output_data_type: None,
            aligned_levels: None,
            scale: false,
        }
    }
}

fn gdal_bool_name(value: bool) -> &'static str {
    match value {
        true => "TRUE",
        false => "FALSE",
    }
}

fn gdal_compression_name(compression: Option<Compression>) -> &'static str {
    match compression {
        Some(Compression::Lzw) => "LZW",
        Some(Compression::Zstd) => "ZSTD",
        Some(Compression::Deflate) => "DEFLATE",
        Some(Compression::Lerc) => "LERC",
        Some(Compression::LercDeflate) => "LERC_DEFLATE",
        Some(Compression::LercZstd) => "LERC_ZSTD",
        None => "NONE",
    }
}

fn gdal_data_type_name(data_type: ArrayDataType) -> &'static str {
    match data_type {
        ArrayDataType::Uint8 => "Byte",
        ArrayDataType::Uint16 => "UInt16",
        ArrayDataType::Uint32 => "UInt32",
        ArrayDataType::Uint64 => "UInt64",
        ArrayDataType::Int8 => "Int8",
        ArrayDataType::Int16 => "Int16",
        ArrayDataType::Int32 => "Int32",
        ArrayDataType::Int64 => "Int64",
        ArrayDataType::Float32 => "Float32",
        ArrayDataType::Float64 => "Float64",
    }
}

fn gdal_predictor_name(predictor: Option<PredictorSelection>) -> &'static str {
    match predictor {
        Some(PredictorSelection::Horizontal) => "STANDARD",
        Some(PredictorSelection::FloatingPoint) => "FLOATING_POINT",
        Some(PredictorSelection::Automatic) => "YES",
        None => "NO",
    }
}

fn cog_creation_options(opts: &CogCreationOptions) -> Vec<String> {
    vec![
        "-f".to_string(),
        "COG".to_string(),
        "-co".into(),
        "INTERLEAVE=TILE".into(),
        "-co".to_string(),
        format!("BLOCKSIZE={}", opts.tile_size),
        "-co".to_string(),
        "ADD_ALPHA=NO".to_string(),
        "-co".to_string(),
        "STATISTICS=YES".to_string(),
        "-co".to_string(),
        "RESAMPLING=NEAREST".to_string(),
        "-co".to_string(),
        "OVERVIEW_RESAMPLING=NEAREST".to_string(),
        "-co".to_string(),
        "NUM_THREADS=ALL_CPUS".to_string(),
        "-co".to_string(),
        format!("SPARSE_OK={}", gdal_bool_name(opts.allow_sparse)),
        "-co".to_string(),
        format!("COMPRESS={}", gdal_compression_name(opts.compression)),
        "-co".to_string(),
        format!("PREDICTOR={}", gdal_predictor_name(opts.predictor)),
    ]
}

pub fn create_gdal_warp_args(input: &Path, opts: CogCreationOptions) -> Result<Vec<String>> {
    let mut overview_option = "IGNORE_EXISTING";

    let mut options = cog_creation_options(&opts);
    options.extend(["-co".to_string(), "TILING_SCHEME=GoogleMapsCompatible".to_string()]);

    match opts.zoom_level_strategy {
        ZoomLevelStrategy::Manual(zoom) => {
            options.push("-co".to_string());
            options.push(format!("ZOOM_LEVEL={zoom}"));
        }
        ZoomLevelStrategy::Closest => {
            options.push("-co".to_string());
            options.push("ZOOM_LEVEL_STRATEGY=AUTO".to_string());
        }
        ZoomLevelStrategy::PreferHigher => {
            options.push("-co".to_string());
            options.push("ZOOM_LEVEL_STRATEGY=UPPER".to_string());
        }
        ZoomLevelStrategy::PreferLower => {
            options.push("-co".to_string());
            options.push("ZOOM_LEVEL_STRATEGY=LOWER".to_string());
        }
    }

    let georef = GeoReference::from_file(input)?.warped_to_epsg(crs::epsg::WGS84_WEB_MERCATOR)?;
    if georef.nodata().is_none() {
        let data_type = match opts.output_data_type {
            Some(dt) => dt,
            None => raster::io::detect_data_type(input, 1)?,
        };
        options.push("-dstnodata".to_string());
        options.push(format!("{}", data_type.default_nodata_value()));
    }

    if let Some(min_zoom) = opts.min_zoom {
        let georef = GeoReference::from_file(input)?.warped_to_epsg(crs::epsg::WGS84_WEB_MERCATOR)?;
        let tile_size_offset = (opts.tile_size / 256 - 1) as i32;
        let max_zoom = Tile::zoom_level_for_pixel_size(georef.cell_size_x(), opts.zoom_level_strategy, opts.tile_size);

        let mut overview_count = (max_zoom - min_zoom) as usize;
        if overview_count == 0 && tile_size_offset > 0 {
            // Zoom levels for larger tiles sizes are offset by the factor of tile_size above 256
            // Reoffset if the overview count is zero otherwise the min zoom level gets ignored
            overview_count += tile_size_offset as usize;
        }

        if overview_count > 0 {
            options.extend([
                "-co".to_string(),
                format!("OVERVIEW_COUNT={overview_count}"),
                "-co".to_string(),
                format!("ALIGNED_LEVELS={}", opts.aligned_levels.unwrap_or(overview_count as i32 + 1)),
            ]);
        } else {
            overview_option = "NONE";
        }
    } else if let Some(aligned_levels) = opts.aligned_levels {
        options.extend(["-co".to_string(), format!("ALIGNED_LEVELS={aligned_levels}")]);
    }

    options.extend(["-overwrite".to_string(), "-co".to_string(), format!("OVERVIEWS={overview_option}")]);

    // Don't set the output data type if scaling is requested, the raster will be scaled in a postprocessing step
    if !opts.scale
        && let Some(output_type) = opts.output_data_type
    {
        options.push("-ot".to_string());
        options.push(gdal_data_type_name(output_type).to_string());
    }

    Ok(options)
}

fn create_scaled_cog_output_args(opts: &CogCreationOptions) -> Result<Vec<String>> {
    assert!(opts.scale);

    let (scaled_data_type, nodata) = match opts.output_data_type {
        Some(ArrayDataType::Uint8) | None => (ArrayDataType::Uint8, u8::MAX.to_string()),
        Some(ArrayDataType::Uint16) => (ArrayDataType::Uint16, u16::MAX.to_string()),
        Some(_) => {
            return Err(Error::InvalidArgument(
                "Scaling only supports output data type: u8 or u16".to_string(),
            ));
        }
    };

    // Preserve the grid established by the warp instead of applying a tiling scheme again.
    let mut options = cog_creation_options(opts);
    options.extend([
        "-ot".to_string(),
        gdal_data_type_name(scaled_data_type).to_string(),
        "-co".to_string(),
        "OVERVIEWS=FORCE_USE_EXISTING".to_string(),
        "-a_nodata".to_string(),
        nodata,
    ]);

    Ok(options)
}

struct BandScaling {
    src_min: f64,
    src_max: f64,
    dst_max: f64,
    scale: f64,
    offset: f64,
}

fn calculate_band_scaling(source_band: &gdal::raster::RasterBand<'_>, index: usize, output_max: f64) -> Result<BandScaling> {
    // Approximate extrema can push valid outliers into the reserved nodata value.
    let stats = source_band
        .get_statistics(true, false)
        .map_err(|err| Error::Runtime(format!("Cannot determine scaling range for band {index}: {err}")))?
        .ok_or_else(|| Error::Runtime(format!("No statistics available for scaling band {index}")))?;
    if !stats.min.is_finite() || !stats.max.is_finite() || stats.min > stats.max {
        return Err(Error::Runtime(format!("Invalid scaling range for band {index}")));
    }

    let (src_min, src_max, dst_max, inverse_scale) = if stats.min == stats.max {
        // Encode constants as zero; their inverse offset recovers the original value.
        (0.0, 1.0, 0.0, 1.0)
    } else {
        (stats.min, stats.max, output_max, (stats.max - stats.min) / output_max)
    };
    let source_scale = source_band.scale().unwrap_or(1.0);
    let source_offset = source_band.offset().unwrap_or(0.0);
    let scale = inverse_scale * source_scale;
    let offset = stats.min * source_scale + source_offset;
    if !inverse_scale.is_finite()
        || inverse_scale == 0.0
        || !(dst_max / (src_max - src_min)).is_finite()
        || !scale.is_finite()
        || !offset.is_finite()
    {
        return Err(Error::Runtime(format!("Scaling coefficients are out of range for band {index}")));
    }

    Ok(BandScaling {
        src_min,
        src_max,
        dst_max,
        scale,
        offset,
    })
}

fn scale_cog(cog_path: &Path, opts: CogCreationOptions) -> Result<()> {
    // Default scaling from 0 to max value is not sufficient for preserving the nodata values
    // So we explicitely need to set the scaling range for each band

    let mut translate_options = create_scaled_cog_output_args(&opts)?;
    let output_max = opts.output_data_type.unwrap_or(ArrayDataType::Uint8).default_nodata_value() - 1.0;
    let scaled_tmp_path = cog_path.with_file_name(format!(".temp.{}", cog_path.file_name().unwrap().to_string_lossy()));
    {
        let source = raster::formats::gdal::open_dataset_read_only(cog_path)?;
        // Set inverse scaling metadata on a VRT, not on a completed COG whose layout must remain intact.
        let scaled_source = raster::algo::gdal::translate(&source, Path::new(""), &["-of".into(), "VRT".into()])?;

        for index in 1..=source.raster_count() {
            let source_band = source.rasterband(index)?;
            let scaling = calculate_band_scaling(&source_band, index, output_max)?;
            translate_options.extend([
                if source.raster_count() == 1 {
                    "-scale".to_string()
                } else {
                    format!("-scale_{index}")
                },
                scaling.src_min.to_string(),
                scaling.src_max.to_string(),
                "0".to_string(),
                scaling.dst_max.to_string(),
            ]);

            // The scale and metedata have to be explicitely set, otherwise they are not included in scaled cog metadata
            let mut band = scaled_source.rasterband(index)?;
            band.set_scale(scaling.scale)?;
            band.set_offset(scaling.offset)?;
        }

        raster::algo::gdal::translate(&scaled_source, &scaled_tmp_path, &translate_options)?;
    }

    std::fs::rename(&scaled_tmp_path, cog_path)?;
    Ok(())
}

pub fn create_multiband_cog_tiles(
    input: &str,
    output: &Path,
    opts: CogCreationOptions,
    progress: Option<&mut dyn FnMut(f64)>,
) -> Result<()> {
    let file_paths: Vec<PathBuf> = glob::glob(input)?.filter_map(|entry| entry.ok()).collect();
    for path in file_paths.iter() {
        log::info!("Input file: {:?}", path);
    }

    if file_paths.is_empty() {
        return Err(Error::InvalidArgument(format!("No files match the input pattern: {}", input)));
    }

    let datasets = file_paths
        .iter()
        .map(raster::formats::gdal::open_dataset_read_only)
        .map(|res| res.and_then(create_vrt_with_nodata))
        .collect::<Result<Vec<_>>>()?;

    let vrt_options = gdal::programs::raster::BuildVRTOptions::new(["-separate", "-strict"])?;
    let src_ds = gdal::programs::raster::build_vrt(None, &datasets, Some(vrt_options))?;

    let options = create_gdal_warp_args(&file_paths[0], opts)?;
    raster::algo::gdal::warp_to_disk_cli(&src_ds, output, &options, &vec![("INIT_DEST".into(), "NO_DATA".into())], progress)?;

    if opts.scale {
        scale_cog(output, opts)?;
    }

    Ok(())
}

/// Creates a VRT wrapper around a source dataset that adds a nodata value if it doesn't have one already.
/// This allows setting nodata metadata without modifying the original read-only file.
fn create_vrt_with_nodata(src_ds: gdal::Dataset) -> Result<gdal::Dataset> {
    let band_nr = 1;
    let band = src_ds.rasterband(band_nr)?;

    if band.no_data_value().is_some() {
        return Ok(src_ds);
    }

    let data_type = ArrayDataType::try_from(band.band_type())?;
    let nodata_value = data_type.default_nodata_value();

    // Build a single-source in-memory VRT dataset with a band-level nodata override.
    let vrt_opts = gdal::programs::raster::BuildVRTOptions::new(vec!["-vrtnodata".to_string(), nodata_value.to_string()])?;

    let datasets = vec![src_ds];
    Ok(gdal::programs::raster::build_vrt(None, &datasets, Some(vrt_opts))?)
}

pub fn create_cog_tiles(input: &Path, output: &Path, opts: CogCreationOptions) -> Result<()> {
    let options = create_gdal_warp_args(input, opts)?;
    let src_ds = raster::formats::gdal::open_dataset_read_only(input)?;
    // If the source doesn't have a nodata value, create a VRT wrapper that adds it
    // This way we don't modify the read-only source dataset.
    let src_ds = create_vrt_with_nodata(src_ds)?;

    raster::algo::gdal::warp_to_disk_cli(&src_ds, output, &options, &vec![("INIT_DEST".into(), "NO_DATA".into())], None)?;
    if opts.scale {
        scale_cog(output, opts)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        Array, Cell,
        geotiff::GeoTiffReader,
        raster::{DenseRaster, Predictor, RasterReadWrite},
        testutils,
    };
    use approx::assert_relative_eq;

    use super::*;

    #[test]
    fn cog_creation_256px() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        {
            // Manually specified zoom level
            let opts = CogCreationOptions {
                min_zoom: Some(6),
                zoom_level_strategy: ZoomLevelStrategy::Manual(7),
                tile_size: 256,
                compression: Some(Compression::Lzw),
                predictor: Some(PredictorSelection::Horizontal),
                allow_sparse: true,
                output_data_type: Some(ArrayDataType::Uint8),
                aligned_levels: None,
                scale: false,
            };

            create_cog_tiles(&input, &output, opts)?;
            let cog = GeoTiffReader::from_file(&output)?;
            let meta = cog.metadata();

            assert_relative_eq!(
                meta.geo_reference.cell_size_x(),
                Tile::pixel_size_at_zoom_level(7, meta.chunk_row_length()),
                epsilon = 1e-6
            );
            assert_eq!(meta.overviews.len(), 2); // 6 to 7
            assert_eq!(meta.chunk_row_length(), 256);
            assert_eq!(meta.compression, Some(Compression::Lzw));
            assert_eq!(meta.predictor, Some(Predictor::Horizontal));
            assert_eq!(meta.data_type, ArrayDataType::Uint8);
        }

        {
            // Closest zoom level
            let opts = CogCreationOptions {
                min_zoom: Some(6),
                zoom_level_strategy: ZoomLevelStrategy::Closest,
                tile_size: 256,
                compression: Some(Compression::Lzw),
                predictor: Some(PredictorSelection::FloatingPoint),
                allow_sparse: true,
                output_data_type: Some(ArrayDataType::Float32),
                aligned_levels: None,
                scale: false,
            };

            create_cog_tiles(&input, &output, opts)?;
            let cog = GeoTiffReader::from_file(&output)?;
            let meta = cog.metadata();

            assert_relative_eq!(
                meta.geo_reference.cell_size_x(),
                Tile::pixel_size_at_zoom_level(10, meta.chunk_row_length()),
                epsilon = 1e-6
            );
            assert_eq!(meta.overviews.len(), 5); // 5 levels from 6 to 10
            assert_eq!(meta.chunk_row_length(), 256);
            assert_eq!(meta.compression, Some(Compression::Lzw));
            assert_eq!(meta.predictor, Some(Predictor::FloatingPoint));
            assert_eq!(meta.data_type, ArrayDataType::Float32);
        }

        {
            // Upper zoom level
            let opts = CogCreationOptions {
                min_zoom: Some(6),
                zoom_level_strategy: ZoomLevelStrategy::PreferHigher,
                tile_size: 256,
                compression: Some(Compression::Lzw),
                predictor: Some(PredictorSelection::Horizontal),
                allow_sparse: true,
                output_data_type: None,
                aligned_levels: None,
                scale: false,
            };

            create_cog_tiles(&input, &output, opts)?;
            let cog = GeoTiffReader::from_file(&output)?;
            let meta = cog.metadata();

            assert_eq!(
                meta.geo_reference.cell_size_x(),
                Tile::pixel_size_at_zoom_level(10, meta.chunk_row_length())
            );
            assert_eq!(meta.overviews.len(), 5); // 5 levels from 6 to 10
            assert_eq!(meta.chunk_row_length(), 256);
        }

        {
            // Lower zoom level
            let opts = CogCreationOptions {
                min_zoom: Some(7),
                zoom_level_strategy: ZoomLevelStrategy::PreferLower,
                tile_size: 256,
                compression: Some(Compression::Lzw),
                predictor: Some(PredictorSelection::Horizontal),
                allow_sparse: true,
                output_data_type: None,
                aligned_levels: None,
                scale: false,
            };

            create_cog_tiles(&input, &output, opts)?;
            let cog = GeoTiffReader::from_file(&output)?;
            let meta = cog.metadata();

            assert_eq!(
                meta.geo_reference.cell_size_x(),
                Tile::pixel_size_at_zoom_level(9, meta.chunk_row_length())
            );
            assert_eq!(meta.overviews.len(), 3); // 5 levels from 7 to 9
            assert_eq!(meta.chunk_row_length(), 256);
        }

        Ok(())
    }

    #[test]
    fn cog_creation_512px() -> Result<()> {
        const TILE_SIZE: u32 = 512;

        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        {
            // Manually specified zoom level
            let opts = CogCreationOptions {
                min_zoom: Some(6),
                zoom_level_strategy: ZoomLevelStrategy::Manual(7),
                tile_size: TILE_SIZE,
                compression: Some(Compression::Lzw),
                predictor: Some(PredictorSelection::Horizontal),
                allow_sparse: true,
                output_data_type: Some(ArrayDataType::Uint8),
                aligned_levels: None,
                scale: false,
            };

            create_cog_tiles(&input, &output, opts)?;
            let cog = GeoTiffReader::from_file(&output)?;
            let meta = cog.metadata();

            assert_relative_eq!(
                meta.geo_reference.cell_size_x(),
                Tile::pixel_size_at_zoom_level(7, meta.chunk_row_length()),
                epsilon = 1e-6
            );
            assert_eq!(meta.overviews.len(), 2); // from 6 to 7
            assert_eq!(meta.chunk_row_length(), TILE_SIZE);
            assert_eq!(meta.compression, Some(Compression::Lzw));
            assert_eq!(meta.predictor, Some(Predictor::Horizontal));
            assert_eq!(meta.data_type, ArrayDataType::Uint8);
        }

        {
            // Closest zoom level
            let opts = CogCreationOptions {
                min_zoom: Some(6),
                zoom_level_strategy: ZoomLevelStrategy::Closest,
                tile_size: TILE_SIZE,
                compression: Some(Compression::Lzw),
                predictor: Some(PredictorSelection::Horizontal),
                allow_sparse: true,
                output_data_type: Some(ArrayDataType::Float32),
                aligned_levels: None,
                scale: false,
            };

            create_cog_tiles(&input, &output, opts)?;
            let cog = GeoTiffReader::from_file(&output)?;
            let meta = cog.metadata();

            assert_relative_eq!(
                meta.geo_reference.cell_size_x(),
                Tile::pixel_size_at_zoom_level(9, meta.chunk_row_length()),
                epsilon = 1e-6
            );
            assert_eq!(meta.overviews.len(), 4); // from 6 to 9
            assert_eq!(meta.chunk_row_length(), TILE_SIZE);
            assert_eq!(meta.compression, Some(Compression::Lzw));
            assert_eq!(meta.predictor, Some(Predictor::Horizontal));
            assert_eq!(meta.data_type, ArrayDataType::Float32);
        }

        {
            // Upper zoom level
            let opts = CogCreationOptions {
                min_zoom: Some(6),
                zoom_level_strategy: ZoomLevelStrategy::PreferHigher,
                tile_size: TILE_SIZE,
                compression: Some(Compression::Lzw),
                predictor: Some(PredictorSelection::Horizontal),
                allow_sparse: true,
                output_data_type: None,
                aligned_levels: None,
                scale: false,
            };

            create_cog_tiles(&input, &output, opts)?;
            let cog = GeoTiffReader::from_file(&output)?;
            let meta = cog.metadata();

            assert_relative_eq!(
                meta.geo_reference.cell_size_x(),
                Tile::pixel_size_at_zoom_level(9, meta.chunk_row_length()),
                epsilon = 1e-6
            );
            assert_eq!(meta.overviews.len(), 4); // from 6 to 9
            assert_eq!(meta.chunk_row_length(), TILE_SIZE);
        }

        {
            // Lower zoom level
            let opts = CogCreationOptions {
                min_zoom: Some(7),
                zoom_level_strategy: ZoomLevelStrategy::PreferLower,
                tile_size: TILE_SIZE,
                compression: Some(Compression::Lzw),
                predictor: Some(PredictorSelection::Horizontal),
                allow_sparse: true,
                output_data_type: None,
                aligned_levels: None,
                scale: false,
            };

            create_cog_tiles(&input, &output, opts)?;
            let cog = GeoTiffReader::from_file(&output)?;
            let meta = cog.metadata();

            assert_relative_eq!(
                meta.geo_reference.cell_size_x(),
                Tile::pixel_size_at_zoom_level(8, meta.chunk_row_length()),
                epsilon = 1e-6
            );
            assert_eq!(meta.overviews.len(), 2); // from 7 to 8
            assert_eq!(meta.chunk_row_length(), TILE_SIZE);
        }

        Ok(())
    }

    #[test]
    fn scaled_single_band_cog_should_preserve_nodata_and_range() -> Result<()> {
        let raster = create_tiny_scaled_cog()?;
        let pixels = raster.as_slice();

        assert_eq!(pixels[2], 255, "Declared -9999 nodata must become 255");
        assert_eq!(pixels[4], 255, "Declared -9999 nodata must become 255");
        assert_eq!(pixels, &[0, 42, 255, 85, 255, 127, 169, 212, 254]);
        Ok(())
    }

    #[test]
    fn scaled_single_band_cog_should_preserve_min_max_when_descaled() -> Result<()> {
        let raster = create_tiny_scaled_cog()?;
        let scale = raster.metadata().scale().expect("Scaled output must include inverse scale/offset");
        let quantization_step = (16.0 - 10.0) / 254.0;
        assert_relative_eq!(scale.scale, quantization_step, epsilon = 1e-12);
        assert_relative_eq!(scale.offset, 10.0, epsilon = 1e-12);
        let descaled: DenseRaster<f64> = raster::algo::descale(&raster);
        let (min, max) = descaled
            .iter_opt()
            .flatten()
            .fold((f64::INFINITY, f64::NEG_INFINITY), |(min, max), value| {
                (min.min(value), max.max(value))
            });

        assert_relative_eq!(min, 10.0, epsilon = quantization_step);
        assert_relative_eq!(max, 16.0, epsilon = quantization_step);
        Ok(())
    }

    fn create_tiny_scaled_cog() -> Result<DenseRaster<u8>> {
        use crate::{CellSize, Point, RasterSize};

        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let input = tmp.path().join("input.tif");
        let output = tmp.path().join("scaled.tif");
        let pixels = [10.0f32, 11.0, -9999.0, 12.0, -9999.0, 13.0, 14.0, 15.0, 16.0];
        let pixel_size = Tile::pixel_size_at_zoom_level(8, 256);
        let georef = GeoReference::with_top_left_origin(
            "EPSG:3857",
            RasterSize::square(3),
            Point::new(2.0 * pixel_size, -3.0 * pixel_size),
            CellSize::square(pixel_size),
            Some(-9999.0),
        );
        {
            let dataset = raster::formats::gdal::create_in_memory_dataset_with_data(&georef, &pixels)?;
            raster::algo::gdal::translate(&dataset, &input, &["-of".into(), "COG".into()])?;
        }
        create_cog_tiles(
            &input,
            &output,
            CogCreationOptions {
                tile_size: 256,
                scale: true,
                ..Default::default()
            },
        )?;

        let mut bounds = georef;
        bounds.set_scale(GeoReference::from_file(&output)?.scale());
        let result = DenseRaster::<u8>::read_bounds(&output, &bounds, 1)?;
        assert_eq!(result.metadata().nodata(), Some(255.0));

        Ok(result)
    }

    #[test]
    fn cog_creation_source_has_no_nodata() -> Result<()> {
        // test case for source datasets that have no nodata value defined
        // This used to create invalid output COGs where the nodata at the edges due to the rotation of the warp
        // contained 0 values that were not marked as nodata in the output COG, since it has no nodata value

        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let mut meta = GeoReference::from_file(&testutils::workspace_test_data_dir().join("landusebyte.tif"))?;
        meta.set_nodata(None);
        let input = tmp.path().join("input.tif");
        let output = tmp.path().join("cog.tif");

        {
            let mut raster = DenseRaster::<f32>::filled_with(Some(1.0), meta);
            raster.write(&input)?;
        }

        create_cog_tiles(&input, &output, CogCreationOptions::default())?;

        let cog_raster = DenseRaster::<f32>::read(&output)?;
        assert_eq!(None, cog_raster.cell_value(Cell::from_row_col(0, 0)));

        // Verify the we didn't modify the input dataset
        assert_eq!(None, DenseRaster::<f32>::read(&input)?.metadata().nodata());

        Ok(())
    }
}

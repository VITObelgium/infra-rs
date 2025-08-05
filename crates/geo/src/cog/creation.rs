use std::path::Path;

use crate::{ArrayDataType, GeoReference, Result, Tile, ZoomLevelStrategy, crs, geotiff::Compression, raster};

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

pub fn create_cog_tiles(input: &Path, output: &Path, opts: CogCreationOptions) -> Result<()> {
    let src_ds = raster::io::dataset::open_read_only(input)?;
    let mut overview_option = "IGNORE_EXISTING";

    let mut options = vec![
        "-overwrite".to_string(),
        "-f".to_string(),
        "COG".to_string(),
        "-co".to_string(),
        format!("BLOCKSIZE={}", opts.tile_size),
        "-co".to_string(),
        "TILING_SCHEME=GoogleMapsCompatible".to_string(),
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
    ];

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

    if let Some(min_zoom) = opts.min_zoom {
        let georef = GeoReference::from_file(input)?.warped_to_epsg(crs::epsg::WGS84_WEB_MERCATOR)?;
        let tile_size_offset = (opts.tile_size / 256 - 1) as i32;
        let max_zoom = Tile::zoom_level_for_pixel_size(georef.cell_size_x(), opts.zoom_level_strategy, opts.tile_size);

        let mut overview_count = (max_zoom - min_zoom) as usize;
        if overview_count == 0 && tile_size_offset > 0 {
            // Zoome levels for larger tiles sizes are offset by the factor of tile_size above 256
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

    options.extend(["-co".to_string(), format!("OVERVIEWS={overview_option}")]);

    if let Some(output_type) = opts.output_data_type {
        options.push("-ot".to_string());
        options.push(gdal_data_type_name(output_type).to_string());
    }

    raster::algo::warp_to_disk_cli(&src_ds, output, &options, &vec![("INIT_DEST".into(), "NO_DATA".into())])?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;

    use crate::{
        geotiff::{GeoTiffReader, Predictor},
        testutils,
    };

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
}

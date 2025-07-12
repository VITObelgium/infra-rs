use std::path::Path;

use crate::{ArrayDataType, GeoReference, Result, Tile, ZoomLevelStrategy, cog::Compression, raster};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum PredictorSelection {
    Horizontal,
    FloatingPoint,
    Automatic,
}

pub struct CogCreationOptions {
    pub min_zoom: Option<i32>,
    pub zoom_level_strategy: ZoomLevelStrategy,
    pub tile_size: u16,
    pub compression: Option<Compression>,
    pub predictor: Option<PredictorSelection>,
    pub allow_sparse: bool,
    pub output_data_type: Option<ArrayDataType>,
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
        "OVERVIEWS=IGNORE_EXISTING".to_string(),
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
        let georef = GeoReference::from_file(input)?;
        let max_zoom = Tile::zoom_level_for_pixel_size(georef.cell_size_x(), opts.zoom_level_strategy) - (opts.tile_size / 256 - 1) as i32;

        let overview_count = (max_zoom - min_zoom) as usize;

        options.extend([
            "-co".to_string(),
            format!("OVERVIEW_COUNT={overview_count}"),
            "-co".to_string(),
            format!("ALIGNED_LEVELS={}", overview_count + 1),
        ]);
    }

    if let Some(output_type) = opts.output_data_type {
        options.push("-ot".to_string());
        options.push(gdal_data_type_name(output_type).to_string());
    }

    raster::algo::warp_to_disk_cli(&src_ds, output, &options, &vec![])?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        cog::{CogAccessor, Predictor},
        testutils,
    };

    use super::*;

    #[test]
    fn cog_creation() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        let opts = CogCreationOptions {
            min_zoom: Some(5),
            zoom_level_strategy: ZoomLevelStrategy::Manual(6),
            tile_size: 256,
            compression: Some(Compression::Lzw),
            predictor: Some(PredictorSelection::Horizontal),
            allow_sparse: true,
            output_data_type: Some(ArrayDataType::Uint8),
        };

        create_cog_tiles(&input, &output, opts)?;
        let cog = CogAccessor::from_file(&output)?;
        let meta = cog.meta_data();

        assert_eq!(meta.min_zoom, 5);
        assert_eq!(meta.max_zoom, 6);
        assert_eq!(meta.tile_size, 256);
        assert_eq!(meta.compression, Some(Compression::Lzw));
        assert_eq!(meta.predictor, Some(Predictor::Horizontal));
        assert_eq!(meta.data_type, ArrayDataType::Uint8);

        Ok(())
    }
}

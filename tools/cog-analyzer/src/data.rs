//! Data loading module for reading chunk and tile data from COG files.
//!
//! This module provides functions to load and normalize pixel data from
//! COG chunks and web tiles for visualization in the TUI.

use std::fs::File;

use geo::cog::WebTilesReader;
use geo::geotiff::tileio::read_tile_data;
use geo::geotiff::{BandIndex, GeoTiffMetadata};
use geo::{AnyDenseArray, Array, ArrayDataType, DenseArray, Tile};
use image::{DynamicImage, RgbImage};
use inf::colormap::{ColorMapDirection, ColorMapPreset, ProcessedColorMap};

use crate::Result;
use crate::tabs::chunks::ChunkData;
use crate::tabs::webtiles::TileData;

/// Get the Turbo colormap for visualization.
fn turbo_colormap() -> ProcessedColorMap {
    ProcessedColorMap::create_for_preset(ColorMapPreset::Turbo, ColorMapDirection::Regular)
}

/// Convert normalized pixel data to an RGB image using the Turbo colormap.
fn pixels_to_image(pixels: &[u8], width: u32, height: u32) -> DynamicImage {
    let colormap = turbo_colormap();
    let mut img = RgbImage::new(width, height);

    for (i, &value) in pixels.iter().enumerate() {
        let x = (i as u32) % width;
        let y = (i as u32) / width;
        if x < width && y < height {
            let color = colormap.get_color_by_value(value);
            img.put_pixel(x, y, image::Rgb([color.r, color.g, color.b]));
        }
    }

    DynamicImage::ImageRgb8(img)
}

/// Load chunk data from a COG file and normalize it to grayscale (0-255).
///
/// # Arguments
/// * `file_path` - Path to the COG file
/// * `overview_index` - Index of the overview to read from
/// * `chunk_index` - Index of the chunk within the overview
/// * `_band_index` - Band to read (1-based) - currently unused, chunks are read directly
/// * `metadata` - COG metadata for normalization
///
/// # Returns
/// A `ChunkData` struct with normalized pixel values ready for visualization.
pub fn load_chunk_data(
    file_path: &std::path::Path,
    overview_index: usize,
    chunk_index: usize,
    _band_index: BandIndex,
    metadata: &GeoTiffMetadata,
) -> Result<ChunkData> {
    let overview = metadata
        .overviews
        .get(overview_index)
        .ok_or_else(|| anyhow::anyhow!("Overview index {} out of range", overview_index))?;

    let chunk_location = overview
        .chunk_locations
        .get(chunk_index)
        .ok_or_else(|| anyhow::anyhow!("Chunk index {} out of range", chunk_index))?;

    // Get tile size from metadata
    let tile_size = match metadata.data_layout {
        geo::geotiff::ChunkDataLayout::Tiled(size) => size,
        geo::geotiff::ChunkDataLayout::Striped(_) => 256, // Default fallback
    };

    // Check if chunk is sparse
    if chunk_location.is_sparse() {
        return Ok(ChunkData {
            overview_index,
            chunk_index,
            pixels: Vec::new(),
            width: tile_size,
            height: tile_size,
            band_index: 1,
            is_sparse: true,
            image: None,
            image_state: None,
            show_hires: false,
        });
    }

    let mut file = File::open(file_path)?;

    // Read the chunk directly using the tileio function based on data type
    let nodata = metadata.geo_reference.nodata();
    let compression = metadata.compression;
    let predictor = metadata.predictor;

    let (chunk_pixels, width, height): (Vec<f64>, u32, u32) = match metadata.data_type {
        ArrayDataType::Uint8 => {
            let array: DenseArray<u8> = read_tile_data(chunk_location, tile_size, nodata, compression, predictor, &mut file)?;
            let w = array.columns().count() as u32;
            let h = array.rows().count() as u32;
            (array.as_ref().iter().map(|&v| v as f64).collect(), w, h)
        }
        ArrayDataType::Uint16 => {
            let array: DenseArray<u16> = read_tile_data(chunk_location, tile_size, nodata, compression, predictor, &mut file)?;
            let w = array.columns().count() as u32;
            let h = array.rows().count() as u32;
            (array.as_ref().iter().map(|&v| v as f64).collect(), w, h)
        }
        ArrayDataType::Uint32 => {
            let array: DenseArray<u32> = read_tile_data(chunk_location, tile_size, nodata, compression, predictor, &mut file)?;
            let w = array.columns().count() as u32;
            let h = array.rows().count() as u32;
            (array.as_ref().iter().map(|&v| v as f64).collect(), w, h)
        }
        ArrayDataType::Uint64 => {
            let array: DenseArray<u64> = read_tile_data(chunk_location, tile_size, nodata, compression, predictor, &mut file)?;
            let w = array.columns().count() as u32;
            let h = array.rows().count() as u32;
            (array.as_ref().iter().map(|&v| v as f64).collect(), w, h)
        }
        ArrayDataType::Int8 => {
            let array: DenseArray<i8> = read_tile_data(chunk_location, tile_size, nodata, compression, predictor, &mut file)?;
            let w = array.columns().count() as u32;
            let h = array.rows().count() as u32;
            (array.as_ref().iter().map(|&v| v as f64).collect(), w, h)
        }
        ArrayDataType::Int16 => {
            let array: DenseArray<i16> = read_tile_data(chunk_location, tile_size, nodata, compression, predictor, &mut file)?;
            let w = array.columns().count() as u32;
            let h = array.rows().count() as u32;
            (array.as_ref().iter().map(|&v| v as f64).collect(), w, h)
        }
        ArrayDataType::Int32 => {
            let array: DenseArray<i32> = read_tile_data(chunk_location, tile_size, nodata, compression, predictor, &mut file)?;
            let w = array.columns().count() as u32;
            let h = array.rows().count() as u32;
            (array.as_ref().iter().map(|&v| v as f64).collect(), w, h)
        }
        ArrayDataType::Int64 => {
            let array: DenseArray<i64> = read_tile_data(chunk_location, tile_size, nodata, compression, predictor, &mut file)?;
            let w = array.columns().count() as u32;
            let h = array.rows().count() as u32;
            (array.as_ref().iter().map(|&v| v as f64).collect(), w, h)
        }
        ArrayDataType::Float32 => {
            let array: DenseArray<f32> = read_tile_data(chunk_location, tile_size, nodata, compression, predictor, &mut file)?;
            let w = array.columns().count() as u32;
            let h = array.rows().count() as u32;
            (array.as_ref().iter().map(|&v| v as f64).collect(), w, h)
        }
        ArrayDataType::Float64 => {
            let array: DenseArray<f64> = read_tile_data(chunk_location, tile_size, nodata, compression, predictor, &mut file)?;
            let w = array.columns().count() as u32;
            let h = array.rows().count() as u32;
            (array.as_ref().to_vec(), w, h)
        }
    };

    // Normalize to grayscale
    let pixels = normalize_to_grayscale(&chunk_pixels, metadata);

    // Create the image for high-res rendering
    let image = pixels_to_image(&pixels, width, height);

    Ok(ChunkData {
        overview_index,
        chunk_index,
        pixels,
        width,
        height,
        band_index: 1,
        is_sparse: false,
        image: Some(image),
        image_state: None,
        show_hires: false,
    })
}

/// Load tile data from a COG file via `WebTilesReader`.
///
/// # Arguments
/// * `file_path` - Path to the COG file
/// * `tile` - The web tile to read
/// * `band_index` - Band to read (1-based)
/// * `webtiles_reader` - `WebTiles` reader for accessing the tile
///
/// # Returns
/// A `TileData` struct with normalized pixel values ready for visualization.
pub fn load_tile_data(
    file_path: &std::path::Path,
    tile: Tile,
    band_index: BandIndex,
    webtiles_reader: &WebTilesReader,
) -> Result<TileData> {
    let mut file = File::open(file_path)?;

    let array_opt = webtiles_reader.read_tile_data(&tile, band_index, &mut file)?;

    let Some(array) = array_opt else {
        // Tile doesn't exist
        return Ok(TileData {
            tile,
            pixels: Vec::new(),
            width: webtiles_reader.tile_info().tile_size,
            height: webtiles_reader.tile_info().tile_size,
            band_index: band_index.get(),
            image: None,
            image_state: None,
            show_hires: false,
        });
    };

    let (pixels, width, height) = normalize_array_to_grayscale(&array, webtiles_reader.cog_metadata());

    // Create the image for high-res rendering
    let image = pixels_to_image(&pixels, width, height);

    Ok(TileData {
        tile,
        pixels,
        width,
        height,
        band_index: band_index.get(),
        image: Some(image),
        image_state: None,
        show_hires: false,
    })
}

/// Normalize pixel values to grayscale (0-255).
fn normalize_to_grayscale(data: &[f64], metadata: &GeoTiffMetadata) -> Vec<u8> {
    if data.is_empty() {
        return Vec::new();
    }

    // Get min/max from statistics if available, otherwise compute from data
    let (min_val, max_val) = if let Some(ref stats) = metadata.statistics {
        (stats.minimum_value, stats.maximum_value)
    } else {
        let nodata = metadata.geo_reference.nodata();
        let valid_data: Vec<f64> = data
            .iter()
            .filter(|&&v| !v.is_nan() && nodata.is_none_or(|nd| (v - nd).abs() > f64::EPSILON))
            .copied()
            .collect();

        if valid_data.is_empty() {
            return vec![0; data.len()];
        }

        let min = valid_data.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        let max = valid_data.iter().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        (min, max)
    };

    let range = max_val - min_val;
    let nodata = metadata.geo_reference.nodata();

    data.iter()
        .map(|&v| {
            if v.is_nan() || nodata.is_some_and(|nd| (v - nd).abs() < f64::EPSILON) {
                0u8 // NoData shown as black
            } else if range.abs() < f64::EPSILON {
                128u8 // All same value -> mid gray
            } else {
                let normalized = (v - min_val) / range;
                (normalized.clamp(0.0, 1.0) * 255.0) as u8
            }
        })
        .collect()
}

/// Normalize an `AnyDenseArray` to grayscale (0-255).
fn normalize_array_to_grayscale(array: &AnyDenseArray, metadata: &GeoTiffMetadata) -> (Vec<u8>, u32, u32) {
    // Get dimensions using the Array trait methods via AnyDenseArray
    let width = array.columns().count() as u32;
    let height = array.rows().count() as u32;

    // Convert array to f64 values based on variant
    let data: Vec<f64> = match array {
        AnyDenseArray::U8(arr) => arr.as_ref().iter().map(|&v| v as f64).collect(),
        AnyDenseArray::U16(arr) => arr.as_ref().iter().map(|&v| v as f64).collect(),
        AnyDenseArray::U32(arr) => arr.as_ref().iter().map(|&v| v as f64).collect(),
        AnyDenseArray::U64(arr) => arr.as_ref().iter().map(|&v| v as f64).collect(),
        AnyDenseArray::I8(arr) => arr.as_ref().iter().map(|&v| v as f64).collect(),
        AnyDenseArray::I16(arr) => arr.as_ref().iter().map(|&v| v as f64).collect(),
        AnyDenseArray::I32(arr) => arr.as_ref().iter().map(|&v| v as f64).collect(),
        AnyDenseArray::I64(arr) => arr.as_ref().iter().map(|&v| v as f64).collect(),
        AnyDenseArray::F32(arr) => arr.as_ref().iter().map(|&v| v as f64).collect(),
        AnyDenseArray::F64(arr) => arr.as_ref().to_vec(),
    };

    let pixels = normalize_to_grayscale(&data, metadata);
    (pixels, width, height)
}

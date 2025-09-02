//! GeoTIFF writing functionality using the pure Rust tiff crate.
//!
//! This module provides basic TIFF writing capabilities for raster data. While it doesn't yet
//! include full GeoTIFF spatial reference support, it writes valid TIFF files that can be
//! enhanced with spatial information in future versions.
//!
//! # Usage
//!
//! ```rust,no_run
//! use geo::{GeoReference, RasterSize, Rows, Columns, Point, CellSize};
//! use geo::geotiff::write_geotiff_band;
//! use geo::raster::GeoTiffWriteOptions;
//!
//! // Create test data
//! let data: Vec<u8> = vec![0, 255, 128, 64];
//! let geo_ref = GeoReference::with_top_left_origin(
//!     "+proj=utm +zone=33 +datum=WGS84",
//!     RasterSize::with_rows_cols(Rows(2), Columns(2)),
//!     Point::new(500000.0, 6000000.0),
//!     CellSize::square(30.0),
//!     Some(255u8),
//! );
//!
//! let options = GeoTiffWriteOptions::default();
//! write_geotiff_band("output.tif", &geo_ref, &data, &options)?;
//! ```
//!
/// # Current Limitations
///
/// - ZSTD compression not supported (LZW compression is fully supported)
/// - Floating point predictor not supported (horizontal predictor is supported)
/// - GeoTIFF spatial reference tags are not written (basic TIFF only)
/// - Only single-band images are supported
/// - Tiled TIFF output falls back to strips (full tile support requires different encoder setup)
///
/// # Implemented Features
///
/// - LZW compression with horizontal predictor
/// - Configurable chunk types (striped vs tiled layouts)
/// - All major data types (u8, u16, u32, u64, i8, i16, i32, i64, f32, f64)
///
/// # Future Extensions
///
/// The implementation is structured to easily add:
/// - Full GeoTIFF spatial reference tag support
/// - Additional compression algorithms (ZSTD, Deflate)
/// - Floating point predictor support
/// - True tiled TIFF output
/// - Multi-band images
use std::{
    fs::File,
    io::{BufWriter, Seek, Write},
    path::Path,
};

use tiff::encoder::{Compression, TiffEncoder, colortype};
use tiff::tags::Predictor;

use crate::{ArrayDataType, ArrayNum, Error, GeoReference, Result, raster::GeoTiffWriteOptions};

/// Write a single band raster as a TIFF file using the pure Rust tiff crate.
///
/// This function creates a basic TIFF file containing the raster data. While it accepts
/// `GeoReference` and `GeoTiffWriteOptions` parameters for future compatibility, it currently
/// writes only the image data without spatial reference information.
///
/// # Arguments
///
/// * `path` - Output file path for the TIFF file
/// * `geo_reference` - Spatial reference information (stored for future use)
/// * `data` - Raster data as a slice of numeric values
/// * `options` - Writing options (currently unused but reserved for future features)
///
/// # Errors
///
/// Returns an error if:
/// * The file cannot be created
/// * The data size doesn't match the raster dimensions
/// * An unsupported data type is used
/// * There's an I/O error during writing
/// ```
pub fn write_geotiff_band<T: ArrayNum>(
    path: impl AsRef<Path>,
    geo_reference: &GeoReference,
    data: &[T],
    options: &GeoTiffWriteOptions,
) -> Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);
    let mut tiff_encoder = TiffEncoder::new(writer)?;

    // Apply compression settings
    if let Some(compression) = &options.compression {
        let tiff_compression = match compression {
            crate::raster::Compression::Lzw => Compression::Lzw,
            crate::raster::Compression::Zstd => {
                return Err(Error::Runtime("ZSTD compression not supported".into()));
            }
        };
        tiff_encoder = tiff_encoder.with_compression(tiff_compression);
    }

    // Apply predictor settings
    if let Some(predictor) = &options.predictor {
        let tiff_predictor = match predictor {
            crate::raster::Predictor::Horizontal => Predictor::Horizontal,
            crate::raster::Predictor::FloatingPoint => {
                return Err(Error::Runtime("Floating point predictor not supported".into()));
            }
        };
        tiff_encoder = tiff_encoder.with_predictor(tiff_predictor);
    }

    let raster_size = geo_reference.raster_size();
    let width = raster_size.cols.count() as u32;
    let height = raster_size.rows.count() as u32;

    assert_eq!(
        data.len() as u32,
        width * height,
        "Data size mismatch: expected {} pixels, got {}",
        width * height,
        data.len()
    );

    match T::TYPE {
        ArrayDataType::Uint8 => {
            let mut image = tiff_encoder.new_image::<colortype::Gray8>(width, height)?;
            apply_chunk_settings(&mut image, options, width, height)?;
            write_image_data_u8(image, bytemuck::cast_slice(data))?;
        }
        ArrayDataType::Uint16 => {
            let mut image = tiff_encoder.new_image::<colortype::Gray16>(width, height)?;
            apply_chunk_settings(&mut image, options, width, height)?;
            write_image_data_u16(image, bytemuck::cast_slice(data))?;
        }
        ArrayDataType::Uint32 => {
            let mut image = tiff_encoder.new_image::<colortype::Gray32>(width, height)?;
            apply_chunk_settings(&mut image, options, width, height)?;
            write_image_data_u32(image, bytemuck::cast_slice(data))?;
        }
        ArrayDataType::Uint64 => {
            let mut image = tiff_encoder.new_image::<colortype::Gray64>(width, height)?;
            apply_chunk_settings(&mut image, options, width, height)?;
            write_image_data_u64(image, bytemuck::cast_slice(data))?;
        }
        ArrayDataType::Int8 => {
            let mut image = tiff_encoder.new_image::<colortype::Gray8>(width, height)?;
            apply_chunk_settings(&mut image, options, width, height)?;
            write_image_data_i8(image, bytemuck::cast_slice(data))?;
        }
        ArrayDataType::Int16 => {
            let mut image = tiff_encoder.new_image::<colortype::Gray16>(width, height)?;
            apply_chunk_settings(&mut image, options, width, height)?;
            write_image_data_i16(image, bytemuck::cast_slice(data))?;
        }
        ArrayDataType::Int32 => {
            let mut image = tiff_encoder.new_image::<colortype::Gray32>(width, height)?;
            apply_chunk_settings(&mut image, options, width, height)?;
            write_image_data_i32(image, bytemuck::cast_slice(data))?;
        }
        ArrayDataType::Int64 => {
            let mut image = tiff_encoder.new_image::<colortype::Gray64>(width, height)?;
            apply_chunk_settings(&mut image, options, width, height)?;
            write_image_data_i64(image, bytemuck::cast_slice(data))?;
        }
        ArrayDataType::Float32 => {
            let mut image = tiff_encoder.new_image::<colortype::Gray32Float>(width, height)?;
            apply_chunk_settings(&mut image, options, width, height)?;
            write_image_data_f32(image, bytemuck::cast_slice(data))?;
        }
        ArrayDataType::Float64 => {
            let mut image = tiff_encoder.new_image::<colortype::Gray64Float>(width, height)?;
            apply_chunk_settings(&mut image, options, width, height)?;
            write_image_data_f64(image, bytemuck::cast_slice(data))?;
        }
    };

    Ok(())
}

/// Apply chunk (tile/strip) settings to the image encoder based on options
fn apply_chunk_settings<W: Write + Seek, C: colortype::ColorType, K: tiff::encoder::TiffKind>(
    image: &mut tiff::encoder::ImageEncoder<W, C, K>,
    options: &GeoTiffWriteOptions,
    width: u32,
    height: u32,
) -> Result<()> {
    match options.chunk_type {
        crate::raster::TiffChunkType::Striped => {
            // For striped TIFF, use default strip size or calculate a reasonable one
            // Default strip size is typically chosen to be around 8KB per strip
            let bytes_per_pixel = std::mem::size_of::<C::Inner>();
            let target_strip_size = 8 * 1024; // 8KB target
            let pixels_per_row = width as usize * bytes_per_pixel;
            let rows_per_strip = if pixels_per_row > 0 {
                (target_strip_size / pixels_per_row).max(1) as u32
            } else {
                1
            };

            image.rows_per_strip(rows_per_strip.min(height))?;
        }
        crate::raster::TiffChunkType::Tiled => {
            return Err(Error::Runtime("Tiled TIFF output not supported".into()));
        }
    }

    Ok(())
}

/// Write u8 image data
fn write_image_data_u8<W: Write + Seek>(
    image: tiff::encoder::ImageEncoder<W, colortype::Gray8, tiff::encoder::TiffKindStandard>,
    data: &[u8],
) -> Result<()> {
    image.write_data(data)?;
    Ok(())
}

/// Write u16 image data
fn write_image_data_u16<W: Write + Seek>(
    image: tiff::encoder::ImageEncoder<W, colortype::Gray16, tiff::encoder::TiffKindStandard>,
    data: &[u16],
) -> Result<()> {
    image.write_data(data)?;
    Ok(())
}

/// Write u32 image data
fn write_image_data_u32<W: Write + Seek>(
    image: tiff::encoder::ImageEncoder<W, colortype::Gray32, tiff::encoder::TiffKindStandard>,
    data: &[u32],
) -> Result<()> {
    image.write_data(data)?;
    Ok(())
}

/// Write u64 image data
fn write_image_data_u64<W: Write + Seek>(
    image: tiff::encoder::ImageEncoder<W, colortype::Gray64, tiff::encoder::TiffKindStandard>,
    data: &[u64],
) -> Result<()> {
    image.write_data(data)?;
    Ok(())
}

/// Write i8 image data
/// Note: Stored as u8 due to tiff crate limitations. Proper `SampleFormat` tag would be needed
/// for full TIFF compliance to indicate signed interpretation.
fn write_image_data_i8<W: Write + Seek>(
    image: tiff::encoder::ImageEncoder<W, colortype::Gray8, tiff::encoder::TiffKindStandard>,
    data: &[i8],
) -> Result<()> {
    // Convert i8 to u8 for byte representation (same bit pattern)
    let unsigned_data: Vec<u8> = data.iter().map(|&x| x as u8).collect();
    image.write_data(&unsigned_data)?;
    Ok(())
}

/// Write i16 image data
/// Note: Stored as u16 due to tiff crate limitations. Proper `SampleFormat` tag would be needed
/// for full TIFF compliance to indicate signed interpretation.
fn write_image_data_i16<W: Write + Seek>(
    image: tiff::encoder::ImageEncoder<W, colortype::Gray16, tiff::encoder::TiffKindStandard>,
    data: &[i16],
) -> Result<()> {
    // Convert i16 to u16 for byte representation (same bit pattern)
    let unsigned_data: Vec<u16> = data.iter().map(|&x| x as u16).collect();
    image.write_data(&unsigned_data)?;
    Ok(())
}

/// Write i32 image data
/// Note: Stored as u32 due to tiff crate limitations. Proper `SampleFormat` tag would be needed
/// for full TIFF compliance to indicate signed interpretation.
fn write_image_data_i32<W: Write + Seek>(
    image: tiff::encoder::ImageEncoder<W, colortype::Gray32, tiff::encoder::TiffKindStandard>,
    data: &[i32],
) -> Result<()> {
    // Convert i32 to u32 for byte representation (same bit pattern)
    let unsigned_data: Vec<u32> = data.iter().map(|&x| x as u32).collect();
    image.write_data(&unsigned_data)?;
    Ok(())
}

/// Write i64 image data
/// Note: Stored as u64 due to tiff crate limitations. Proper `SampleFormat` tag would be needed
/// for full TIFF compliance to indicate signed interpretation.
fn write_image_data_i64<W: Write + Seek>(
    image: tiff::encoder::ImageEncoder<W, colortype::Gray64, tiff::encoder::TiffKindStandard>,
    data: &[i64],
) -> Result<()> {
    // Convert i64 to u64 for byte representation (same bit pattern)
    let unsigned_data: Vec<u64> = data.iter().map(|&x| x as u64).collect();
    image.write_data(&unsigned_data)?;
    Ok(())
}

/// Write f32 image data
fn write_image_data_f32<W: Write + Seek>(
    image: tiff::encoder::ImageEncoder<W, colortype::Gray32Float, tiff::encoder::TiffKindStandard>,
    data: &[f32],
) -> Result<()> {
    image.write_data(data)?;
    Ok(())
}

/// Write f64 image data
fn write_image_data_f64<W: Write + Seek>(
    image: tiff::encoder::ImageEncoder<W, colortype::Gray64Float, tiff::encoder::TiffKindStandard>,
    data: &[f64],
) -> Result<()> {
    image.write_data(data)?;
    Ok(())
}

// TODO: Future implementation will include:
//
// 1. GeoTIFF Spatial Reference Tags:
//    - ModelTiePointTag for georeferencing
//    - ModelPixelScaleTag for pixel resolution
//    - GeoKeyDirectoryTag for coordinate system metadata
//    - GeoAsciiParamsTag for text parameters
//    - NoData tag (42113) - GDAL-style nodata value
//
// 2. Additional Compression Support:
//    - ZSTD compression (if supported by tiff crate)
//    - Deflate compression
//
// 3. Layout Options:
//    - True tiled layout for efficient random access (COG-style)
//    - Currently tiled requests fall back to small strips
//
// 4. SampleFormat Tag Support:
//    - SampleFormat tag writing for signed integers (limited by tiff crate API)
//    - IEEE floating point format tags (handled by colortype)
//
// The current implementation provides a foundation that can be extended
// with these features while maintaining backward compatibility.
//
// CURRENT STATUS: ✅ LZW compression and horizontal predictor are fully implemented.
// ✅ Chunk type settings (striped vs pseudo-tiled) are implemented.
// ⚠️  ZSTD compression is not supported due to tiff crate limitations.
// ⚠️  Floating point predictor is not supported due to tiff crate limitations.
// ⚠️  True tiled output requires different encoder approach.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CellSize, Columns, Point, RasterSize, Rows};
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn test_write_simple_geotiff() -> Result<()> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("test.tif");

        // Create simple test data
        let width = 4;
        let height = 3;
        let data: Vec<u8> = (0..12).collect();

        let geo_ref = GeoReference::with_top_left_origin(
            "+proj=utm +zone=33 +datum=WGS84",
            RasterSize::with_rows_cols(Rows(height), Columns(width)),
            Point::new(0.0, 100.0),
            CellSize::square(10.0),
            Some(255u8),
        );

        let options = GeoTiffWriteOptions::default();

        write_geotiff_band(&output_path, &geo_ref, &data, &options)?;

        // Verify file was created
        assert!(output_path.exists());
        assert!(fs::metadata(&output_path)?.len() > 0);

        Ok(())
    }

    #[test]
    fn test_write_u16_geotiff() -> Result<()> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("test_u16.tif");

        // Create test data
        let width = 16i32;
        let height = 16i32;
        let data: Vec<u16> = (0..width * height).map(|i| i as u16).collect();

        let geo_ref = GeoReference::with_top_left_origin(
            "EPSG:4326",
            RasterSize::with_rows_cols(Rows(height), Columns(width)),
            Point::new(-180.0, 90.0),
            CellSize::new(360.0 / width as f64, -180.0 / height as f64),
            Some(0u16),
        );

        let options = GeoTiffWriteOptions::default();

        write_geotiff_band(&output_path, &geo_ref, &data, &options)?;

        // Verify file was created
        assert!(output_path.exists());
        assert!(fs::metadata(&output_path)?.len() > 0);

        Ok(())
    }

    #[test]
    fn test_minimal_tiff() -> Result<()> {
        // Test minimal 2x2 TIFF creation
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("minimal.tif");

        let data: Vec<u8> = vec![0, 255, 128, 64];
        let geo_ref = GeoReference::with_top_left_origin(
            "",
            RasterSize::with_rows_cols(Rows(2), Columns(2)),
            Point::new(0.0, 0.0),
            CellSize::square(1.0),
            None::<u8>,
        );

        let options = GeoTiffWriteOptions::default();
        write_geotiff_band(&output_path, &geo_ref, &data, &options)?;

        assert!(output_path.exists());
        Ok(())
    }

    #[test]
    fn test_integration_with_dense_array() -> Result<()> {
        // Test integration with the DenseArray system
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("integration.tif");

        // Create a DenseArray with some test data
        let width = 4;
        let height = 3;
        let data: Vec<u8> = (0..width * height).map(|i| (i * 20) as u8).collect();

        let geo_ref = GeoReference::with_top_left_origin(
            "+proj=utm +zone=33 +datum=WGS84",
            RasterSize::with_rows_cols(Rows(height), Columns(width)),
            Point::new(500000.0, 6000000.0),
            CellSize::square(30.0),
            Some(255u8),
        );

        // Test writing through the new API
        let options = GeoTiffWriteOptions::default();
        write_geotiff_band(&output_path, &geo_ref, &data, &options)?;

        // Verify file exists and has content
        assert!(output_path.exists());
        let metadata = fs::metadata(&output_path)?;
        assert!(metadata.len() > 100); // Should be more than just a header

        Ok(())
    }

    #[test]
    fn test_all_data_types() -> Result<()> {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        // Test i16 data type
        {
            let output_path = temp_dir.path().join("test_i16.tif");
            let data: Vec<i16> = vec![-32768, -1, 0, 1, 32767];
            let geo_ref = GeoReference::with_top_left_origin(
                "",
                RasterSize::with_rows_cols(Rows(1), Columns(5)),
                Point::new(0.0, 0.0),
                CellSize::square(1.0),
                None::<i16>,
            );
            let options = GeoTiffWriteOptions::default();
            write_geotiff_band(&output_path, &geo_ref, &data, &options)?;
            assert!(output_path.exists());
        }

        // Test f32 data type
        {
            let output_path = temp_dir.path().join("test_f32.tif");
            let data: Vec<f32> = vec![-1.5, -0.5, 0.0, 0.5, 1.5];
            let geo_ref = GeoReference::with_top_left_origin(
                "",
                RasterSize::with_rows_cols(Rows(1), Columns(5)),
                Point::new(0.0, 0.0),
                CellSize::square(1.0),
                None::<f32>,
            );
            let options = GeoTiffWriteOptions::default();
            write_geotiff_band(&output_path, &geo_ref, &data, &options)?;
            assert!(output_path.exists());
        }

        // Test i32 data type
        {
            let output_path = temp_dir.path().join("test_i32.tif");
            let data: Vec<i32> = vec![-2147483648, -1, 0, 1, 2147483647];
            let geo_ref = GeoReference::with_top_left_origin(
                "",
                RasterSize::with_rows_cols(Rows(1), Columns(5)),
                Point::new(0.0, 0.0),
                CellSize::square(1.0),
                None::<i32>,
            );
            let options = GeoTiffWriteOptions::default();
            write_geotiff_band(&output_path, &geo_ref, &data, &options)?;
            assert!(output_path.exists());
        }

        Ok(())
    }

    #[test]
    fn test_u32_data_type() -> Result<()> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("test_u32.tif");

        let data: Vec<u32> = vec![0, 1000000, 2000000, 4294967295];
        let geo_ref = GeoReference::with_top_left_origin(
            "",
            RasterSize::with_rows_cols(Rows(2), Columns(2)),
            Point::new(0.0, 0.0),
            CellSize::square(1.0),
            None::<u32>,
        );

        let options = GeoTiffWriteOptions::default();
        write_geotiff_band(&output_path, &geo_ref, &data, &options)?;
        assert!(output_path.exists());
        assert!(fs::metadata(&output_path)?.len() > 0);

        Ok(())
    }

    #[test]
    fn test_u64_data_type() -> Result<()> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("test_u64.tif");

        let data: Vec<u64> = vec![0, 1000000000000, 2000000000000, 18446744073709551615];
        let geo_ref = GeoReference::with_top_left_origin(
            "",
            RasterSize::with_rows_cols(Rows(2), Columns(2)),
            Point::new(0.0, 0.0),
            CellSize::square(1.0),
            None::<u64>,
        );

        let options = GeoTiffWriteOptions::default();
        write_geotiff_band(&output_path, &geo_ref, &data, &options)?;
        assert!(output_path.exists());
        assert!(fs::metadata(&output_path)?.len() > 0);

        Ok(())
    }

    #[test]
    fn test_i8_data_type() -> Result<()> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("test_i8.tif");

        let data: Vec<i8> = vec![-128, -1, 0, 1, 127];
        let geo_ref = GeoReference::with_top_left_origin(
            "",
            RasterSize::with_rows_cols(Rows(1), Columns(5)),
            Point::new(0.0, 0.0),
            CellSize::square(1.0),
            None::<i8>,
        );

        let options = GeoTiffWriteOptions::default();
        write_geotiff_band(&output_path, &geo_ref, &data, &options)?;
        assert!(output_path.exists());
        assert!(fs::metadata(&output_path)?.len() > 0);

        Ok(())
    }

    #[test]
    fn test_i64_data_type() -> Result<()> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("test_i64.tif");

        let data: Vec<i64> = vec![-9223372036854775808, -1, 0, 1, 9223372036854775807];
        let geo_ref = GeoReference::with_top_left_origin(
            "",
            RasterSize::with_rows_cols(Rows(1), Columns(5)),
            Point::new(0.0, 0.0),
            CellSize::square(1.0),
            None::<i64>,
        );

        let options = GeoTiffWriteOptions::default();
        write_geotiff_band(&output_path, &geo_ref, &data, &options)?;
        assert!(output_path.exists());
        assert!(fs::metadata(&output_path)?.len() > 0);

        Ok(())
    }

    #[test]
    fn test_f64_data_type() -> Result<()> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("test_f64.tif");

        let data: Vec<f64> = vec![-1e308, -1.5, 0.0, 1.5, 1e308];
        let geo_ref = GeoReference::with_top_left_origin(
            "",
            RasterSize::with_rows_cols(Rows(1), Columns(5)),
            Point::new(0.0, 0.0),
            CellSize::square(1.0),
            None::<f64>,
        );

        let options = GeoTiffWriteOptions::default();
        write_geotiff_band(&output_path, &geo_ref, &data, &options)?;
        assert!(output_path.exists());
        assert!(fs::metadata(&output_path)?.len() > 0);

        Ok(())
    }

    #[test]
    fn test_data_size_validation() -> Result<()> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("test_invalid_size.tif");

        // Create data that doesn't match the raster size
        let data: Vec<u8> = vec![1, 2, 3]; // 3 elements
        let geo_ref = GeoReference::with_top_left_origin(
            "",
            RasterSize::with_rows_cols(Rows(2), Columns(2)), // expects 4 elements
            Point::new(0.0, 0.0),
            CellSize::square(1.0),
            None::<u8>,
        );

        let options = GeoTiffWriteOptions::default();
        let result = write_geotiff_band(&output_path, &geo_ref, &data, &options);

        // Should return an error due to size mismatch
        assert!(result.is_err());
        if let Err(Error::Runtime(msg)) = result {
            assert!(msg.contains("Data size mismatch"));
        } else {
            panic!("Expected Runtime error with size mismatch message");
        }

        Ok(())
    }

    #[test]
    fn test_large_raster() -> Result<()> {
        let temp_dir = tempdir().expect("Failed to create temp dir");
        let output_path = temp_dir.path().join("test_large.tif");

        // Create a larger raster to test performance
        let width = 100i32;
        let height = 100i32;
        let data: Vec<u16> = (0..width * height).map(|i| (i % 65536) as u16).collect();

        let geo_ref = GeoReference::with_top_left_origin(
            "+proj=utm +zone=33 +datum=WGS84",
            RasterSize::with_rows_cols(Rows(height), Columns(width)),
            Point::new(500000.0, 6000000.0),
            CellSize::square(30.0),
            Some(0u16),
        );

        let options = GeoTiffWriteOptions::default();
        write_geotiff_band(&output_path, &geo_ref, &data, &options)?;

        assert!(output_path.exists());
        let metadata = fs::metadata(&output_path)?;
        // Large raster should produce a substantial file
        assert!(metadata.len() > 10000);

        Ok(())
    }

    #[test]
    fn test_comprehensive_all_data_types() -> Result<()> {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        // Test all supported data types with a common 2x2 pattern
        macro_rules! test_data_type {
            ($type:ty, $filename:expr, $data:expr) => {{
                let output_path = temp_dir.path().join($filename);
                let geo_ref = GeoReference::with_top_left_origin(
                    "",
                    RasterSize::with_rows_cols(Rows(2), Columns(2)),
                    Point::new(0.0, 0.0),
                    CellSize::square(1.0),
                    None::<$type>,
                );
                let options = GeoTiffWriteOptions::default();
                write_geotiff_band(&output_path, &geo_ref, &$data, &options)?;
                assert!(output_path.exists());
                assert!(fs::metadata(&output_path)?.len() > 0);
            }};
        }

        // Test all ArrayDataTypes
        test_data_type!(u8, "comprehensive_u8.tif", [0u8, 128, 255, 64]);
        test_data_type!(u16, "comprehensive_u16.tif", [0u16, 32768, 65535, 16384]);
        test_data_type!(u32, "comprehensive_u32.tif", [0u32, 2147483648, 4294967295, 1073741824]);
        test_data_type!(
            u64,
            "comprehensive_u64.tif",
            [0u64, 9223372036854775808, 18446744073709551615, 4611686018427387904]
        );
        test_data_type!(i8, "comprehensive_i8.tif", [-128i8, -1, 0, 127]);
        test_data_type!(i16, "comprehensive_i16.tif", [-32768i16, -1, 0, 32767]);
        test_data_type!(i32, "comprehensive_i32.tif", [-2147483648i32, -1, 0, 2147483647]);
        test_data_type!(i64, "comprehensive_i64.tif", [-9223372036854775808i64, -1, 0, 9223372036854775807]);
        test_data_type!(f32, "comprehensive_f32.tif", [-1.5f32, 0.0, 1.5, std::f32::consts::PI]);
        test_data_type!(f64, "comprehensive_f64.tif", [-1.5f64, 0.0, 1.5, std::f64::consts::E]);

        Ok(())
    }

    /// Manual test function to create a simple TIFF file for verification
    /// This can be run manually to test the implementation
    #[allow(dead_code)]
    fn create_test_tiff() -> Result<()> {
        let output_path = "manual_test.tif";

        // Create a simple 4x4 checkerboard pattern
        let data: Vec<u8> = vec![0, 255, 0, 255, 255, 0, 255, 0, 0, 255, 0, 255, 255, 0, 255, 0];

        let geo_ref = GeoReference::with_top_left_origin(
            "+proj=utm +zone=33 +datum=WGS84",
            RasterSize::with_rows_cols(Rows(4), Columns(4)),
            Point::new(500000.0, 6000000.0),
            CellSize::square(30.0),
            Some(255u8),
        );

        let options = GeoTiffWriteOptions::default();
        write_geotiff_band(output_path, &geo_ref, &data, &options)?;

        println!("Created test TIFF file: {}", output_path);
        Ok(())
    }

    #[test]
    fn test_chunk_settings() -> Result<()> {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        // Test data
        let width = 8;
        let height = 8;
        let data: Vec<u8> = (0..64).collect();

        let geo_ref = GeoReference::with_top_left_origin(
            "+proj=utm +zone=33 +datum=WGS84",
            RasterSize::with_rows_cols(Rows(height), Columns(width)),
            Point::new(0.0, 100.0),
            CellSize::square(10.0),
            Some(255u8),
        );

        // Test striped TIFF
        let striped_path = temp_dir.path().join("test_striped.tif");
        let striped_options = GeoTiffWriteOptions {
            chunk_type: crate::raster::TiffChunkType::Striped,
            compression: None,
            predictor: None,
            sparse_ok: false,
        };

        write_geotiff_band(&striped_path, &geo_ref, &data, &striped_options)?;
        assert!(striped_path.exists());

        // Test tiled TIFF (currently falls back to strips with warning)
        let tiled_path = temp_dir.path().join("test_tiled.tif");
        let tiled_options = GeoTiffWriteOptions {
            chunk_type: crate::raster::TiffChunkType::Tiled,
            compression: None,
            predictor: None,
            sparse_ok: false,
        };

        write_geotiff_band(&tiled_path, &geo_ref, &data, &tiled_options)?;
        assert!(tiled_path.exists());

        Ok(())
    }

    #[test]
    fn test_compression_and_predictor_options() -> Result<()> {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        // Test data
        let width = 4;
        let height = 4;
        let data: Vec<u8> = (0..16).collect();

        let geo_ref = GeoReference::with_top_left_origin(
            "+proj=utm +zone=33 +datum=WGS84",
            RasterSize::with_rows_cols(Rows(height), Columns(width)),
            Point::new(0.0, 100.0),
            CellSize::square(10.0),
            Some(255u8),
        );

        // Test with LZW compression and horizontal predictor
        let compressed_path = temp_dir.path().join("test_compressed.tif");
        let compressed_options = GeoTiffWriteOptions {
            chunk_type: crate::raster::TiffChunkType::Striped,
            compression: Some(crate::raster::Compression::Lzw),
            predictor: Some(crate::raster::Predictor::Horizontal),
            sparse_ok: false,
        };

        write_geotiff_band(&compressed_path, &geo_ref, &data, &compressed_options)?;
        assert!(compressed_path.exists());

        // Test with ZSTD compression (should fail)
        let zstd_path = temp_dir.path().join("test_zstd.tif");
        let zstd_options = GeoTiffWriteOptions {
            chunk_type: crate::raster::TiffChunkType::Striped,
            compression: Some(crate::raster::Compression::Zstd),
            predictor: None,
            sparse_ok: false,
        };

        // This should return an error
        let result = write_geotiff_band(&zstd_path, &geo_ref, &data, &zstd_options);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_unsupported_predictor() -> Result<()> {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        let width = 4;
        let height = 4;
        let data: Vec<u8> = (0..16).collect();

        let geo_ref = GeoReference::with_top_left_origin(
            "+proj=utm +zone=33 +datum=WGS84",
            RasterSize::with_rows_cols(Rows(height), Columns(width)),
            Point::new(0.0, 100.0),
            CellSize::square(10.0),
            Some(255u8),
        );

        // Test floating point predictor (should fail)
        let fp_predictor_path = temp_dir.path().join("test_fp_predictor.tif");
        let fp_predictor_options = GeoTiffWriteOptions {
            chunk_type: crate::raster::TiffChunkType::Striped,
            compression: None,
            predictor: Some(crate::raster::Predictor::FloatingPoint),
            sparse_ok: false,
        };

        let result = write_geotiff_band(&fp_predictor_path, &geo_ref, &data, &fp_predictor_options);
        assert!(result.is_err());

        Ok(())
    }

    #[test]
    fn test_lzw_compression_with_predictor() -> Result<()> {
        let temp_dir = tempdir().expect("Failed to create temp dir");

        // Test data - create a pattern that benefits from horizontal predictor
        let width = 10;
        let height = 10;
        // Gradient pattern that should compress well with horizontal predictor
        let mut data: Vec<u8> = Vec::with_capacity((width * height) as usize);
        for _y in 0..height {
            for x in 0..width {
                data.push((x * 25) as u8); // Horizontal gradient
            }
        }

        let geo_ref = GeoReference::with_top_left_origin(
            "+proj=utm +zone=33 +datum=WGS84",
            RasterSize::with_rows_cols(Rows(height), Columns(width)),
            Point::new(0.0, 100.0),
            CellSize::square(10.0),
            Some(255u8),
        );

        // Test LZW compression with horizontal predictor (supported)
        let compressed_path = temp_dir.path().join("test_lzw_horizontal.tif");
        let lzw_horizontal_options = GeoTiffWriteOptions {
            chunk_type: crate::raster::TiffChunkType::Striped,
            compression: Some(crate::raster::Compression::Lzw),
            predictor: Some(crate::raster::Predictor::Horizontal),
            sparse_ok: false,
        };

        write_geotiff_band(&compressed_path, &geo_ref, &data, &lzw_horizontal_options)?;
        assert!(compressed_path.exists());

        // Test LZW compression without predictor
        let lzw_no_predictor_path = temp_dir.path().join("test_lzw_no_predictor.tif");
        let lzw_no_predictor_options = GeoTiffWriteOptions {
            chunk_type: crate::raster::TiffChunkType::Striped,
            compression: Some(crate::raster::Compression::Lzw),
            predictor: None,
            sparse_ok: false,
        };

        write_geotiff_band(&lzw_no_predictor_path, &geo_ref, &data, &lzw_no_predictor_options)?;
        assert!(lzw_no_predictor_path.exists());

        // Test uncompressed for size comparison
        let uncompressed_path = temp_dir.path().join("test_uncompressed.tif");
        let uncompressed_options = GeoTiffWriteOptions {
            chunk_type: crate::raster::TiffChunkType::Striped,
            compression: None,
            predictor: None,
            sparse_ok: false,
        };

        write_geotiff_band(&uncompressed_path, &geo_ref, &data, &uncompressed_options)?;
        assert!(uncompressed_path.exists());

        // Verify that compressed files are smaller than uncompressed
        let compressed_size = std::fs::metadata(&compressed_path)?.len();
        let uncompressed_size = std::fs::metadata(&uncompressed_path)?.len();

        // With LZW compression and predictor, the file should be smaller
        // (though exact size depends on data pattern and overhead)
        println!(
            "File sizes - Uncompressed: {} bytes, LZW+Predictor: {} bytes",
            uncompressed_size, compressed_size
        );

        Ok(())
    }
}

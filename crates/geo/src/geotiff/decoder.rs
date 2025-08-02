use std::io::{Read, Seek};

use tiff::{
    decoder::{Decoder, ifd::Value},
    tags::Tag,
};

use crate::{
    ArrayDataType, Columns, Error, GeoReference, RasterSize, Result, Rows, crs,
    geotiff::{
        ChunkDataLayout, Compression, GeoTiffMetadata, Predictor, TiffChunkLocation, TiffStats, projectioninfo::ModelType,
        reader::TiffOverview, stats,
    },
};

use super::ProjectionInfo;

pub fn parse_geotiff_metadata<R: Read + Seek>(stream: R) -> Result<GeoTiffMetadata> {
    let mut decoder = tiff::decoder::Decoder::new(stream)?.with_limits(tiff::decoder::Limits::unlimited());
    parse_cog_header(&mut decoder)
}

fn is_tiled<R: Read + Seek>(decoder: &mut Decoder<R>) -> bool {
    decoder.get_chunk_type() == tiff::decoder::ChunkType::Tile
}

#[allow(dead_code)]
fn band_count<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<usize> {
    let color_type = decoder.colortype()?;
    let num_bands: usize = match color_type {
        tiff::ColorType::Multiband { bit_depth: _, num_samples } => num_samples as usize,
        tiff::ColorType::Gray(_) => 1,
        tiff::ColorType::RGB(_) => 3,
        _ => {
            return Err(Error::Runtime("Unsupported tiff color type".into()));
        }
    };
    Ok(num_bands)
}

fn read_pixel_scale<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<(f64, f64)> {
    if let Ok(values) = decoder.get_tag_f64_vec(Tag::ModelPixelScaleTag) {
        if values.len() < 2 {
            return Err(Error::Runtime("ModelPixelScale must have at least 2 values".into()));
        }

        Ok((values[0], values[1]))
    } else {
        Err(Error::Runtime("ModelPixelScale tag not found".into()))
    }
}

fn read_tie_points<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<[f64; 6]> {
    if let Ok(values) = decoder.get_tag_f64_vec(Tag::ModelTiepointTag) {
        if values.len() != 6 {
            return Err(Error::Runtime("ModelPixelScale must have 6 values".into()));
        }

        let mut tie_points = [0.0; 6];
        tie_points.copy_from_slice(&values[0..6]);
        Ok(tie_points)
    } else {
        Err(Error::Runtime("ModelPixelScale tag not found".into()))
    }
}

fn read_model_transformation<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<[f64; 8]> {
    if let Ok(values) = decoder.get_tag_f64_vec(Tag::ModelTransformationTag) {
        if values.len() < 8 {
            return Err(Error::Runtime("ModelPixelScale must have 16 values".into()));
        }

        let mut transform = [0.0; 8];
        transform.copy_from_slice(&values[0..8]);
        Ok(transform)
    } else {
        Err(Error::Runtime("ModelTransformation tag not found".into()))
    }
}

fn read_gdal_metadata<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<Option<TiffStats>> {
    if let Ok(gdal_metadata) = decoder.get_tag_ascii_string(Tag::Unknown(42112)) {
        return Ok(Some(stats::parse_statistics(&gdal_metadata)?));
    }

    Ok(None)
}

fn read_raster_size<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<RasterSize> {
    Ok(RasterSize::with_rows_cols(
        Rows(decoder.get_tag_u32(Tag::ImageLength)? as i32),
        Columns(decoder.get_tag_u32(Tag::ImageWidth)? as i32),
    ))
}

fn read_geo_transform<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<[f64; 6]> {
    let mut valid_transform = false;
    let mut geo_transform = [0.0; 6];

    let (pixel_scale_x, pixel_scale_y) = read_pixel_scale(decoder)?;
    geo_transform[1] = pixel_scale_x;
    geo_transform[5] = -pixel_scale_y;

    if let Ok(transform) = read_model_transformation(decoder) {
        geo_transform[0] = transform[3];
        geo_transform[1] = transform[0];
        geo_transform[2] = transform[1];
        geo_transform[3] = transform[7];
        geo_transform[4] = transform[4];
        geo_transform[5] = transform[5];
        valid_transform = true;
    }

    if let Ok(tie_points) = read_tie_points(decoder) {
        if geo_transform[1] == 0.0 || geo_transform[5] == 0.0 {
            return Err(Error::Runtime("No cell sizes present in geotiff".into()));
        }

        geo_transform[0] = tie_points[3] - tie_points[0] * geo_transform[1];
        geo_transform[3] = tie_points[4] - tie_points[1] * geo_transform[5];
        valid_transform = true;
    } else {
        log::debug!("No tie points info");
    }

    if !valid_transform {
        return Err(Error::Runtime("Failed to obtain pixel transformation from tiff".into()));
    }

    Ok(geo_transform)
}

fn read_nodata_value<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<Option<f64>> {
    if let Ok(nodata_str) = decoder.get_tag_ascii_string(Tag::GdalNodata) {
        Ok(nodata_str.parse::<f64>().ok())
    } else {
        Ok(None)
    }
}

fn read_projection_info<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<Option<ProjectionInfo>> {
    let key_dir = decoder.get_tag_u16_vec(Tag::GeoKeyDirectoryTag)?;
    if key_dir.len() < 4 {
        return Ok(None);
    }

    if key_dir[0] != 1 {
        return Err(Error::Runtime(format!("Unexpected key directory version: {}", key_dir[0])));
    }

    let mut proj_info = ProjectionInfo::default();

    for key in key_dir[4..].as_chunks::<4>().0 {
        match key[0] {
            1024 => {
                // Geographic Type GeoKey
                if key[1] == 0 {
                    match key[2] {
                        1 => proj_info.model_type = ModelType::Projected,
                        2 => proj_info.model_type = ModelType::Geographic,
                        3 => proj_info.model_type = ModelType::Geocentric,
                        _ => {
                            return Err(Error::Runtime(format!("Unsupported model type: {}", key[2])));
                        }
                    }
                } else {
                    return Err(Error::Runtime("Only inline model keys are supported".into()));
                }
            }
            2048 => {
                // Geographic Coordinate Reference System GeoKey
                if key[1] == 0 && key[2] == 1 {
                    proj_info.geographic_epsg = Some(crs::Epsg::from(key[3] as u32));
                } else {
                    return Err(Error::Runtime("Only inline EPSG codes are supported".into()));
                }
            }
            3072 => {
                // Projected Coordinate Reference System GeoKey
                if key[1] == 0 && key[2] == 1 {
                    proj_info.projected_epsg = Some(crs::Epsg::from(key[3] as u32));
                } else {
                    return Err(Error::Runtime("Only inline EPSG codes are supported".into()));
                }
            }
            _ => {}
        }
    }

    Ok(Some(proj_info))
}

fn parse_cog_header<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<GeoTiffMetadata> {
    let bits_per_sample = match decoder.get_tag(Tag::BitsPerSample) {
        Ok(Value::Short(bits)) => bits,
        Ok(Value::List(_)) => {
            return Err(Error::InvalidArgument("Alpha channels are not supported".into()));
        }
        _ => {
            return Err(Error::InvalidArgument("Unexpected bit depth information".into()));
        }
    };

    let data_type = match (decoder.get_tag(Tag::SampleFormat)?, bits_per_sample) {
        (Value::Short(1), 8) => ArrayDataType::Uint8,
        (Value::Short(1), 16) => ArrayDataType::Uint16,
        (Value::Short(1), 32) => ArrayDataType::Uint32,
        (Value::Short(1), 64) => ArrayDataType::Uint64,
        (Value::Short(2), 8) => ArrayDataType::Int8,
        (Value::Short(2), 16) => ArrayDataType::Int16,
        (Value::Short(2), 32) => ArrayDataType::Int32,
        (Value::Short(2), 64) => ArrayDataType::Int64,
        (Value::Short(3), 32) => ArrayDataType::Float32,
        (Value::Short(3), 64) => ArrayDataType::Float64,
        (data_type, _) => {
            return Err(Error::InvalidArgument(format!(
                "Unsupported data type: {data_type:?} {bits_per_sample}"
            )));
        }
    };

    let samples_per_pixel = decoder.get_tag_u32(Tag::SamplesPerPixel)?;
    if samples_per_pixel != 1 {
        // When we will support multi-band COGs, the unpredict functions will need to be adjusted accordingly
        // or will will need to use a different approach to handle multi-band data (e.g vec of DenseArray)
        return Err(Error::InvalidArgument(format!(
            "Only single band COGs are supported ({samples_per_pixel} bands found)",
        )));
    }

    let compression = match decoder.get_tag_u32(Tag::Compression)? {
        1 => None,
        5 => Some(Compression::Lzw),
        _ => {
            return Err(Error::InvalidArgument(format!(
                "Only LZW compressed COGs are supported ({})",
                decoder.get_tag_u32(Tag::Compression)?
            )));
        }
    };

    let predictor = match decoder.get_tag_u32(Tag::Predictor) {
        Ok(2) => Some(Predictor::Horizontal),
        Ok(3) => Some(Predictor::FloatingPoint),
        _ => None,
    };

    let statistics = read_gdal_metadata(decoder)?;
    let geo_transform = read_geo_transform(decoder)?;
    let raster_size = read_raster_size(decoder)?;
    let nodata = read_nodata_value(decoder)?;
    let projection = read_projection_info(decoder)?;
    let is_tiled = is_tiled(decoder);

    let data_layout = if is_tiled {
        let tile_size = decoder.get_tag_u32(Tag::TileWidth)?;
        if tile_size != decoder.get_tag_u32(Tag::TileLength)? {
            return Err(Error::InvalidArgument("Only square tiles are supported".into()));
        }

        ChunkDataLayout::Tiled(tile_size)
    } else {
        let rows = decoder.get_tag_u32(Tag::RowsPerStrip)?;
        ChunkDataLayout::Striped(rows)
    };

    // Now loop over the image directories to collect the tile offsets and sizes for the main raster image and all overviews.
    let mut overviews = Vec::new();

    loop {
        let image_width = decoder.get_tag_u32(Tag::ImageWidth)?;
        let image_height = decoder.get_tag_u32(Tag::ImageLength)?;

        let (offsets, byte_counts) = if is_tiled {
            (
                decoder.get_tag_u64_vec(Tag::TileOffsets)?,
                decoder.get_tag_u64_vec(Tag::TileByteCounts)?,
            )
        } else {
            (
                decoder.get_tag_u64_vec(Tag::StripOffsets)?,
                decoder.get_tag_u64_vec(Tag::StripByteCounts)?,
            )
        };

        debug_assert_eq!(offsets.len(), byte_counts.len());

        let mut tile_locations = Vec::with_capacity(offsets.len());
        offsets.iter().zip(byte_counts.iter()).for_each(|(offset, byte_count)| {
            tile_locations.push(TiffChunkLocation {
                offset: *offset,
                size: *byte_count,
            });
        });

        overviews.push(TiffOverview {
            raster_size: RasterSize::with_rows_cols(Rows(image_height as i32), Columns(image_width as i32)),
            chunk_locations: tile_locations,
        });

        if !decoder.more_images() {
            break;
        }

        decoder.next_image()?;
    }

    let epsg = projection
        .and_then(|proj| proj.epsg().map(|epsg| epsg.to_string()))
        .unwrap_or_default();

    Ok(GeoTiffMetadata {
        data_layout,
        data_type,
        band_count: samples_per_pixel,
        compression,
        predictor,
        geo_reference: GeoReference::new(epsg, raster_size, geo_transform, nodata),
        statistics,
        overviews,
    })
}

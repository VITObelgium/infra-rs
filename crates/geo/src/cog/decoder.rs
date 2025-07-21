use std::io::{Read, Seek};

use tiff::{decoder::ifd::Value, tags::Tag};

use crate::{
    ArrayDataType, Columns, Error, GeoReference, RasterSize, Result, Rows, Tile, ZoomLevelStrategy,
    cog::{CogMetadata, CogStats, CogTileLocation, Compression, Predictor, projectioninfo::ModelType, reader::PyramidInfo, stats},
    crs,
};

use super::ProjectionInfo;

pub struct CogDecoder<R: Read + Seek> {
    /// TIFF decoder
    decoder: tiff::decoder::Decoder<R>,
}

impl<R: Read + Seek> CogDecoder<R> {
    pub fn new(stream: R) -> Result<Self> {
        Ok(Self {
            decoder: tiff::decoder::Decoder::new(stream)?.with_limits(tiff::decoder::Limits::unlimited()),
        })
    }

    fn is_tiled(&mut self) -> Result<bool> {
        Ok(self.decoder.tile_count()? > 0)
    }

    #[allow(dead_code)]
    fn band_count(&mut self) -> Result<usize> {
        let color_type = self.decoder.colortype()?;
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

    fn read_pixel_scale(&mut self) -> Result<(f64, f64)> {
        if let Ok(values) = self.decoder.get_tag_f64_vec(Tag::ModelPixelScaleTag) {
            if values.len() < 2 {
                return Err(Error::Runtime("ModelPixelScale must have at least 2 values".into()));
            }

            Ok((values[0], values[1]))
        } else {
            Err(Error::Runtime("ModelPixelScale tag not found".into()))
        }
    }

    fn read_tie_points(&mut self) -> Result<[f64; 6]> {
        if let Ok(values) = self.decoder.get_tag_f64_vec(Tag::ModelTiepointTag) {
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

    fn read_model_transformation(&mut self) -> Result<[f64; 8]> {
        if let Ok(values) = self.decoder.get_tag_f64_vec(Tag::ModelTransformationTag) {
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

    fn read_gdal_metadata(&mut self) -> Result<Option<CogStats>> {
        if let Ok(gdal_metadata) = self.decoder.get_tag_ascii_string(Tag::Unknown(42112)) {
            return Ok(Some(stats::parse_statistics(&gdal_metadata)?));
        }

        Ok(None)
    }

    fn read_raster_size(&mut self) -> Result<RasterSize> {
        Ok(RasterSize::with_rows_cols(
            Rows(self.decoder.get_tag_u32(Tag::ImageLength)? as i32),
            Columns(self.decoder.get_tag_u32(Tag::ImageWidth)? as i32),
        ))
    }

    fn read_geo_transform(&mut self) -> Result<[f64; 6]> {
        let mut valid_transform = false;
        let mut geo_transform = [0.0; 6];

        let (pixel_scale_x, pixel_scale_y) = self.read_pixel_scale()?;
        geo_transform[1] = pixel_scale_x;
        geo_transform[5] = -pixel_scale_y;

        if let Ok(transform) = self.read_model_transformation() {
            geo_transform[0] = transform[3];
            geo_transform[1] = transform[0];
            geo_transform[2] = transform[1];
            geo_transform[3] = transform[7];
            geo_transform[4] = transform[4];
            geo_transform[5] = transform[5];
            valid_transform = true;
        }

        if let Ok(tie_points) = self.read_tie_points() {
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

    fn read_nodata_value(&mut self) -> Result<Option<f64>> {
        if let Ok(nodata_str) = self.decoder.get_tag_ascii_string(Tag::GdalNodata) {
            Ok(nodata_str.parse::<f64>().ok())
        } else {
            Ok(None)
        }
    }

    fn read_projection_info(&mut self) -> Result<Option<ProjectionInfo>> {
        let key_dir = self.decoder.get_tag_u16_vec(Tag::GeoKeyDirectoryTag)?;
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

    pub fn parse_cog_header(&mut self) -> Result<CogMetadata> {
        if !self.is_tiled()? {
            return Err(Error::InvalidArgument("Only tiled TIFFs are supported".into()));
        }

        let tile_size = self.decoder.get_tag_u32(Tag::TileWidth)? as u16;
        if tile_size != self.decoder.get_tag_u32(Tag::TileLength)? as u16 {
            return Err(Error::InvalidArgument("Only square tiles are supported".into()));
        }

        let bits_per_sample = match self.decoder.get_tag(Tag::BitsPerSample) {
            Ok(Value::Short(bits)) => bits,
            Ok(Value::List(_)) => {
                return Err(Error::InvalidArgument("Alpha channels are not supported".into()));
            }
            _ => {
                return Err(Error::InvalidArgument("Unexpected bit depth information".into()));
            }
        };

        let data_type = match (self.decoder.get_tag(Tag::SampleFormat)?, bits_per_sample) {
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

        let samples_per_pixel = self.decoder.get_tag_u32(Tag::SamplesPerPixel)?;
        if samples_per_pixel != 1 {
            // When we will support multi-band COGs, the unpredict functions will need to be adjusted accordingly
            // or will will need to use a different approach to handle multi-band data (e.g vec of DenseArray)
            return Err(Error::InvalidArgument(format!(
                "Only single band COGs are supported ({samples_per_pixel} bands found)",
            )));
        }

        let compression = match self.decoder.get_tag_u32(Tag::Compression)? {
            1 => None,
            5 => Some(Compression::Lzw),
            _ => {
                return Err(Error::InvalidArgument(format!(
                    "Only LZW compressed COGs are supported ({})",
                    self.decoder.get_tag_u32(Tag::Compression)?
                )));
            }
        };

        let predictor = match self.decoder.get_tag_u32(Tag::Predictor) {
            Ok(2) => Some(Predictor::Horizontal),
            Ok(3) => Some(Predictor::FloatingPoint),
            _ => None,
        };

        let statistics = self.read_gdal_metadata()?;
        let geo_transform = self.read_geo_transform()?;
        let raster_size = self.read_raster_size()?;
        let nodata = self.read_nodata_value()?;
        let projection = self.read_projection_info()?;

        // Now loop over the image directories to collect the tile offsets and sizes for the main raster image and all overviews.
        let max_zoom = Tile::zoom_level_for_pixel_size(geo_transform[1], ZoomLevelStrategy::Closest) - ((tile_size / 256) - 1) as i32;
        let mut current_zoom = max_zoom;
        let mut pyramids = Vec::new();
        //let mut min_zoom = max_zoom;

        loop {
            let image_width = self.decoder.get_tag_u32(Tag::ImageWidth)?;
            let image_height = self.decoder.get_tag_u32(Tag::ImageLength)?;

            let tile_offsets = self.decoder.get_tag_u64_vec(Tag::TileOffsets)?;
            let tile_byte_counts = self.decoder.get_tag_u64_vec(Tag::TileByteCounts)?;
            debug_assert_eq!(tile_offsets.len(), tile_byte_counts.len());

            let mut tile_locations = Vec::with_capacity(tile_offsets.len());
            tile_offsets.iter().zip(tile_byte_counts.iter()).for_each(|(offset, byte_count)| {
                tile_locations.push(CogTileLocation {
                    offset: *offset,
                    size: *byte_count,
                });
            });

            pyramids.push(PyramidInfo {
                zoom_level: current_zoom,
                raster_size: RasterSize::with_rows_cols(Rows(image_height as i32), Columns(image_width as i32)),
                tile_locations,
            });

            if !self.decoder.more_images() {
                break;
            }

            current_zoom -= 1;
            self.decoder.next_image()?;
        }

        let epsg = projection
            .and_then(|proj| proj.epsg().map(|epsg| epsg.to_string()))
            .unwrap_or_default();

        Ok(CogMetadata {
            min_zoom: current_zoom,
            max_zoom,
            tile_size,
            data_type,
            band_count: samples_per_pixel,
            compression,
            predictor,
            geo_reference: GeoReference::new(epsg, raster_size, geo_transform, nodata),
            statistics,
            pyramids,
        })
    }
}

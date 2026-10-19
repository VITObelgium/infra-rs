use std::io::{Read, Seek};

use tiff::{
    decoder::{Decoder, ifd::Value},
    tags::{Tag, Type},
};

use crate::{
    ArrayDataType, Columns, Error, GeoReference, RasterScale, RasterSize, Result, Rows, crs,
    geotiff::{
        ChunkDataLayout, GeoTiffMetadata, TiffChunkLocation, gdalmetadata, metadata::Interleave, projectioninfo::ModelType,
        reader::TiffOverview,
    },
    raster::{Compression, Predictor},
};

use super::ProjectionInfo;

pub fn parse_geotiff_metadata<R: Read + Seek>(stream: R) -> Result<GeoTiffMetadata> {
    let mut decoder = Decoder::new(stream)?.with_limits(tiff::decoder::Limits::unlimited());
    parse_cog_header(&mut decoder)
}

fn optional_tag<R: Read + Seek, T>(
    decoder: &mut Decoder<R>,
    tag: Tag,
    field_type: Type,
    convert: impl FnOnce(Value) -> tiff::TiffResult<T>,
) -> Result<Option<T>> {
    let Some(entry) = decoder.image_ifd().find_entry(tag) else {
        return Ok(None);
    };
    if entry.field_type() != field_type {
        return Err(Error::InvalidArgument(format!(
            "TIFF tag {} must have type {field_type:?}, found {:?}",
            tag.to_u16(),
            entry.field_type()
        )));
    }

    // Present values outside the buffered COG header must request more data,
    // rather than being treated as absent optional metadata.
    Ok(Some(convert(decoder.get_tag(tag)?)?))
}

fn optional_ascii<R: Read + Seek>(decoder: &mut Decoder<R>, tag: Tag) -> Result<Option<String>> {
    let Some(entry) = decoder.image_ifd().find_entry(tag) else {
        return Ok(None);
    };
    if entry.field_type() != Type::ASCII {
        return Err(Error::InvalidArgument(format!(
            "TIFF tag {} must have type ASCII, found {:?}",
            tag.to_u16(),
            entry.field_type()
        )));
    }

    // The library's string getter truncates at internal NULs and rejects inline
    // UTF-8. Read the counted value and remove only its required final terminator.
    let mut value = tiff::tags::ValueBuffer::empty(Type::ASCII);
    decoder
        .image_ifd()
        .find_tag_buf(tag, &mut value)?
        .ok_or_else(|| Error::Runtime(format!("TIFF tag {} disappeared while reading its value", tag.to_u16())))?;
    let bytes = value
        .as_bytes()
        .strip_suffix(&[0])
        .ok_or_else(|| Error::InvalidArgument(format!("TIFF ASCII tag {} is not NUL-terminated", tag.to_u16())))?;
    let text = std::str::from_utf8(bytes)
        .map_err(|error| Error::InvalidArgument(format!("TIFF ASCII tag {} is not UTF-8: {error}", tag.to_u16())))?;
    Ok(Some(text.to_owned()))
}

fn optional_f64_array<R: Read + Seek, const N: usize>(decoder: &mut Decoder<R>, tag: Tag) -> Result<Option<[f64; N]>> {
    optional_tag(decoder, tag, Type::DOUBLE, Value::into_f64_vec)?
        .map(|values| {
            values.try_into().map_err(|values: Vec<f64>| {
                Error::InvalidArgument(format!("TIFF tag {} requires {N} values, found {}", tag.to_u16(), values.len()))
            })
        })
        .transpose()
}

fn read_raster_size<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<RasterSize> {
    let dimension = |value: u32| {
        i32::try_from(value)
            .ok()
            .filter(|&value| value > 0)
            .ok_or_else(|| Error::InvalidArgument(format!("TIFF image dimension {value} must be in 1..=i32::MAX")))
    };
    Ok(RasterSize::with_rows_cols(
        Rows(dimension(decoder.get_tag_u32(Tag::ImageLength)?)?),
        Columns(dimension(decoder.get_tag_u32(Tag::ImageWidth)?)?),
    ))
}

fn read_geo_transform<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<[f64; 6]> {
    let scale: Option<[f64; 3]> = optional_f64_array(decoder, Tag::ModelPixelScaleTag)?;
    let matrix: Option<[f64; 16]> = optional_f64_array(decoder, Tag::ModelTransformationTag)?;
    let tiepoints = optional_tag(decoder, Tag::ModelTiepointTag, Type::DOUBLE, Value::into_f64_vec)?;
    let mut transform = [0.0; 6];
    if let Some(scale) = scale {
        transform[1] = scale[0];
        transform[5] = -scale[1];
    }
    let mut valid_transform = false;
    if let Some(matrix) = matrix {
        transform = [matrix[3], matrix[0], matrix[1], matrix[7], matrix[4], matrix[5]];
        valid_transform = true;
    }
    if let Some(points) = tiepoints {
        let (points, remainder) = points.as_chunks::<6>();
        let Some(point) = points.first().filter(|_| remainder.is_empty()) else {
            return Err(Error::InvalidArgument(
                "ModelTiepoint must contain one or more groups of 6 values".into(),
            ));
        };
        if transform[1] == 0.0 || transform[5] == 0.0 {
            return Err(Error::Runtime("No cell sizes present in geotiff".into()));
        }
        transform[0] = point[3] - point[0] * transform[1];
        transform[3] = point[4] - point[1] * transform[5];
        valid_transform = true;
    }
    if !valid_transform {
        return Err(Error::Runtime("Failed to obtain pixel transformation from tiff".into()));
    }
    Ok(transform)
}

fn read_projection_info<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<Option<ProjectionInfo>> {
    let Some(key_dir) = optional_tag(decoder, Tag::GeoKeyDirectoryTag, Type::SHORT, Value::into_u16_vec)? else {
        return Ok(None);
    };
    if key_dir.len() < 4 || key_dir.len() < 4 + usize::from(key_dir[3]) * 4 {
        return Err(Error::InvalidArgument("GeoKeyDirectory has a truncated header or key list".into()));
    }
    if key_dir[0] != 1 {
        return Err(Error::Runtime(format!("Unexpected key directory version: {}", key_dir[0])));
    }
    let mut proj_info = ProjectionInfo::default();
    for key in key_dir[4..4 + usize::from(key_dir[3]) * 4].as_chunks::<4>().0 {
        match key[0] {
            1024 => {
                if key[1] == 0 && key[2] == 1 {
                    match key[3] {
                        1 => proj_info.model_type = ModelType::Projected,
                        2 => proj_info.model_type = ModelType::Geographic,
                        3 => proj_info.model_type = ModelType::Geocentric,
                        _ => return Err(Error::Runtime(format!("Unsupported model type: {}", key[3]))),
                    }
                } else {
                    return Err(Error::Runtime("Only inline model keys are supported".into()));
                }
            }
            2048 => {
                if key[1] == 0 && key[2] == 1 {
                    proj_info.geographic_epsg = Some(crs::Epsg::from(key[3]));
                } else {
                    return Err(Error::Runtime("Only inline EPSG codes are supported".into()));
                }
            }
            3072 => {
                if key[1] == 0 && key[2] == 1 {
                    proj_info.projected_epsg = Some(crs::Epsg::from(key[3]));
                } else {
                    return Err(Error::Runtime("Only inline EPSG codes are supported".into()));
                }
            }
            _ => {}
        }
    }
    Ok(Some(proj_info))
}

fn read_data_type<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<ArrayDataType> {
    let bits = decoder.find_tag_unsigned_vec::<u16>(Tag::BitsPerSample)?.unwrap_or_else(|| vec![1]);
    let formats = decoder.find_tag_unsigned_vec::<u16>(Tag::SampleFormat)?.unwrap_or_else(|| vec![1]);
    let (Some(&bits_per_sample), Some(&format)) = (bits.first(), formats.first()) else {
        return Err(Error::InvalidArgument("TIFF image has an empty sample list".into()));
    };
    if bits.iter().any(|&value| value != bits_per_sample) || formats.iter().any(|&value| value != format) {
        return Err(Error::InvalidArgument("Mixed sample types or bit depths are not supported".into()));
    }
    Ok(match (format, bits_per_sample) {
        (1, 8) => ArrayDataType::Uint8,
        (1, 16) => ArrayDataType::Uint16,
        (1, 32) => ArrayDataType::Uint32,
        (1, 64) => ArrayDataType::Uint64,
        (2, 8) => ArrayDataType::Int8,
        (2, 16) => ArrayDataType::Int16,
        (2, 32) => ArrayDataType::Int32,
        (2, 64) => ArrayDataType::Int64,
        (3, 32) => ArrayDataType::Float32,
        (3, 64) => ArrayDataType::Float64,
        _ => {
            return Err(Error::InvalidArgument(format!(
                "Unsupported sample format/bit depth: {format}/{bits_per_sample}"
            )));
        }
    })
}

fn read_compression<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<Option<Compression>> {
    Ok(match decoder.find_tag_unsigned::<u16>(Tag::Compression)?.unwrap_or(1) {
        1 => None,
        5 => Some(Compression::Lzw),
        8 => Some(Compression::Deflate),
        34887 => {
            let parameters = optional_tag(decoder, Tag::Unknown(50674), Type::LONG, Value::into_u32_vec)?
                .ok_or_else(|| Error::InvalidArgument("LERC compression requires LercParameters".into()))?;
            let &[_, additional_compression] = parameters.as_slice() else {
                return Err(Error::InvalidArgument("LercParameters must contain exactly 2 values".into()));
            };
            match additional_compression {
                0 => Some(Compression::Lerc),
                1 => Some(Compression::LercDeflate),
                2 => Some(Compression::LercZstd),
                code => {
                    return Err(Error::InvalidArgument(format!(
                        "Unsupported LERC additional compression type ({code})"
                    )));
                }
            }
        }
        50000 => Some(Compression::Zstd),
        code => return Err(Error::InvalidArgument(format!("Unsupported TIFF compression ({code})"))),
    })
}

fn read_data_layout<R: Read + Seek>(decoder: &mut Decoder<R>, raster_size: RasterSize) -> Result<ChunkDataLayout> {
    if decoder.get_chunk_type() == tiff::decoder::ChunkType::Tile {
        let tile_size = decoder.get_tag_u32(Tag::TileWidth)?;
        if tile_size == 0 || tile_size > i32::MAX as u32 {
            return Err(Error::InvalidArgument("TIFF tile dimension must be in 1..=i32::MAX".into()));
        }
        if tile_size != decoder.get_tag_u32(Tag::TileLength)? {
            return Err(Error::InvalidArgument("Only square tiles are supported".into()));
        }
        Ok(ChunkDataLayout::Tiled(tile_size))
    } else {
        let height = raster_size.rows.count() as u32;
        let rows = decoder.find_tag_unsigned::<u32>(Tag::RowsPerStrip)?.unwrap_or(height).min(height);
        if rows == 0 {
            return Err(Error::InvalidArgument("TIFF RowsPerStrip must be nonzero".into()));
        }
        Ok(ChunkDataLayout::Striped(rows))
    }
}

fn parse_cog_header<R: Read + Seek>(decoder: &mut Decoder<R>) -> Result<GeoTiffMetadata> {
    let raster_size = read_raster_size(decoder)?;
    let data_type = read_data_type(decoder)?;
    let band_count = u32::from(decoder.find_tag_unsigned::<u16>(Tag::SamplesPerPixel)?.unwrap_or(1));
    if band_count == 0 {
        return Err(Error::InvalidArgument("TIFF SamplesPerPixel must be nonzero".into()));
    }
    let compression = read_compression(decoder)?;
    let predictor = match decoder.find_tag_unsigned::<u16>(Tag::Predictor)?.unwrap_or(1) {
        1 => None,
        2 => Some(Predictor::Horizontal),
        3 => Some(Predictor::FloatingPoint),
        code => return Err(Error::InvalidArgument(format!("Unsupported TIFF predictor ({code})"))),
    };
    let data_layout = read_data_layout(decoder, raster_size)?;
    let transform = read_geo_transform(decoder)?;
    let nodata = optional_ascii(decoder, Tag::GdalNodata)?
        .as_deref()
        .map(str::parse::<f64>)
        .transpose()?;
    let epsg = read_projection_info(decoder)?
        .and_then(|projection| projection.epsg().map(|epsg| epsg.to_string()))
        .unwrap_or_default();
    let gdal_metadata = optional_ascii(decoder, Tag::Unknown(42112))?
        .map(|xml| gdalmetadata::parse_gdal_metadata(&xml))
        .transpose()?
        .unwrap_or_default();
    let planar_configuration = decoder.find_tag_unsigned::<u16>(Tag::PlanarConfiguration)?.unwrap_or(1);
    let interleave = gdal_metadata.interleave.unwrap_or(if planar_configuration == 1 && band_count > 1 {
        Interleave::Pixel
    } else {
        Interleave::Band
    });
    let raster_scale = gdal_metadata
        .band_metadata
        .first()
        .and_then(|band| match (band.scale, band.offset) {
            (Some(scale), Some(offset)) => Some(RasterScale { scale, offset }),
            (Some(scale), None) => Some(RasterScale { scale, offset: 0.0 }),
            (None, Some(offset)) => Some(RasterScale { scale: 1.0, offset }),
            (None, None) => None,
        });

    let mut overviews = Vec::new();
    loop {
        let raster_size = read_raster_size(decoder)?;
        let (offsets, byte_counts) = if decoder.get_chunk_type() == tiff::decoder::ChunkType::Tile {
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
        if offsets.len() != byte_counts.len() {
            return Err(Error::InvalidArgument(format!(
                "IFD {} has {} chunk offsets but {} byte counts",
                overviews.len(),
                offsets.len(),
                byte_counts.len()
            )));
        }
        let chunk_locations = offsets
            .into_iter()
            .zip(byte_counts)
            .map(|(offset, size)| {
                offset
                    .checked_add(size)
                    .ok_or_else(|| Error::InvalidArgument("TIFF chunk range overflows u64".into()))?;
                Ok(TiffChunkLocation { offset, size })
            })
            .collect::<Result<Vec<_>>>()?;
        overviews.push(TiffOverview {
            raster_size,
            chunk_locations,
        });
        if !decoder.more_images() {
            break;
        }
        decoder.next_image()?;
    }

    Ok(GeoTiffMetadata {
        data_layout,
        data_type,
        band_count,
        compression,
        predictor,
        geo_reference: GeoReference::new(epsg, raster_size, transform.into(), nodata, raster_scale),
        statistics: gdal_metadata.statistics,
        overviews,
        interleave,
        gdal_ghost_data: None,
        band_metadata: gdal_metadata.band_metadata,
        data_bounds: gdal_metadata.data_bounds,
    })
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use tiff::{
        Directory,
        decoder::ifd::Entry,
        encoder::{DirectoryEncoder, TiffEncoder, TiffKind, TiffKindBig, TiffKindStandard},
    };

    use crate::geotiff::{ParseFromBufferError, io::COG_HEADER_SIZE};

    use super::*;

    fn test_tiff(
        levels: usize,
        configure: impl FnMut(usize, &mut DirectoryEncoder<'_, &mut Cursor<Vec<u8>>, TiffKindStandard>) -> Result<()>,
    ) -> Result<Vec<u8>> {
        test_tiff_kind::<TiffKindStandard>(levels, configure)
    }

    fn test_tiff_kind<K: TiffKind>(
        levels: usize,
        mut configure: impl FnMut(usize, &mut DirectoryEncoder<'_, &mut Cursor<Vec<u8>>, K>) -> Result<()>,
    ) -> Result<Vec<u8>> {
        let mut stream = Cursor::new(Vec::new());
        {
            let mut encoder = TiffEncoder::<_, K>::new_generic(&mut stream)?;
            for level in 0..levels {
                let mut directory = encoder.image_directory()?;
                if level == 0 {
                    let ghost = "LAYOUT=IFDS_BEFORE_DATA\nBLOCK_ORDER=ROW_MAJOR\nBLOCK_LEADER=SIZE_AS_UINT4\nBLOCK_TRAILER=LAST_4_BYTES_REPEATED\nKNOWN_INCOMPATIBLE_EDITION=NO\n";
                    directory.write_data(format!("GDAL_STRUCTURAL_METADATA_SIZE={:06} bytes\n{ghost}", ghost.len()).as_bytes())?;
                    directory.write_tag(Tag::ModelPixelScaleTag, [2.0_f64, 2.0, 0.0])?;
                    directory.write_tag(Tag::ModelTiepointTag, [0.0_f64, 0.0, 0.0, 10.0, 20.0, 0.0])?;
                }
                let width = 17_u32 >> level;
                let height = 9_u32 >> level;
                directory.write_tag(Tag::ImageWidth, width)?;
                directory.write_tag(Tag::ImageLength, height)?;
                directory.write_tag(Tag::NewSubfileType, u32::from(level > 0))?;
                directory.write_tag(Tag::SamplesPerPixel, 1_u16)?;
                directory.write_tag(Tag::BitsPerSample, 8_u16)?;
                directory.write_tag(Tag::SampleFormat, 1_u16)?;
                directory.write_tag(Tag::Compression, 1_u16)?;
                directory.write_tag(Tag::PhotometricInterpretation, 1_u16)?;
                directory.write_tag(Tag::TileWidth, 16_u32)?;
                directory.write_tag(Tag::TileLength, 16_u32)?;
                let chunks = vec![0_u32; (width.div_ceil(16) * height.div_ceil(16)) as usize];
                directory.write_tag(Tag::TileOffsets, chunks.as_slice())?;
                directory.write_tag(Tag::TileByteCounts, chunks.as_slice())?;
                configure(level, &mut directory)?;
                directory.finish()?;
            }
        }
        Ok(stream.into_inner())
    }

    fn insert_entry<W: Write + Seek, K: TiffKind>(directory: &mut DirectoryEncoder<'_, W, K>, tag: Tag, entry: Entry) {
        let mut entries = Directory::empty();
        entries.extend([(tag, entry)]);
        directory.extend_from(&entries);
    }

    fn insert_ascii<W: Write + Seek, K: TiffKind>(directory: &mut DirectoryEncoder<'_, W, K>, tag: Tag, bytes: &[u8]) -> Result<()> {
        let entry = directory.write_entry_bytes(Type::ASCII, bytes)?;
        insert_entry(directory, tag, entry);
        Ok(())
    }

    fn assert_counted_ascii_is_preserved<K: TiffKind>() -> Result<()> {
        for text in [
            "",
            "\0",
            "a\0b",
            "a\0\0",
            "first\0second",
            "\0first\0second\0\0",
            "é",
            "€",
            "🙂",
            "ééé",
            "longer UTF-8: é\0€\0",
        ] {
            let mut counted = text.as_bytes().to_vec();
            counted.push(0);
            let bytes = test_tiff_kind::<K>(1, |_, directory| {
                for tag in [Tag::GdalNodata, Tag::Unknown(42112)] {
                    insert_ascii(directory, tag, &counted)?;
                }
                Ok(())
            })?;
            let mut decoder = Decoder::new(Cursor::new(bytes))?;
            for tag in [Tag::GdalNodata, Tag::Unknown(42112)] {
                assert_eq!(
                    optional_ascii(&mut decoder, tag)?.as_deref(),
                    Some(text),
                    "Counted ASCII bytes {counted:?}"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn reads_counted_utf8_and_internal_or_trailing_nuls_in_classic_tiff() -> Result<()> {
        assert_counted_ascii_is_preserved::<TiffKindStandard>()
    }

    #[test]
    fn reads_counted_utf8_and_internal_or_trailing_nuls_in_bigtiff() -> Result<()> {
        assert_counted_ascii_is_preserved::<TiffKindBig>()
    }

    fn assert_malformed_ascii_is_rejected<K: TiffKind>() -> Result<()> {
        for tag in [Tag::GdalNodata, Tag::Unknown(42112)] {
            for value in [
                b"".as_slice(),
                b"a",
                b"abcd",
                b"not terminated",
                b"first\0unterminated",
                b"\xff\0",
                b"longer \xff\0",
            ] {
                let bytes = test_tiff_kind::<K>(1, |_, directory| insert_ascii(directory, tag, value))?;
                assert!(
                    matches!(parse_geotiff_metadata(Cursor::new(bytes)), Err(Error::InvalidArgument(message)) if message.contains(&tag.to_u16().to_string())),
                    "Malformed ASCII {tag:?} bytes {value:?} must not be silently accepted"
                );
            }
        }
        Ok(())
    }

    #[test]
    fn rejects_nonterminated_or_invalid_utf8_optional_ascii() -> Result<()> {
        assert_malformed_ascii_is_rejected::<TiffKindStandard>()?;
        assert_malformed_ascii_is_rejected::<TiffKindBig>()
    }

    #[test]
    fn retained_nuls_are_rejected_by_nodata_and_gdal_xml_semantic_parsers() -> Result<()> {
        for (tag, value) in [
            (Tag::GdalNodata, b"255\0junk\0".as_slice()),
            (Tag::GdalNodata, b"255\0\0"),
            (Tag::Unknown(42112), b"<GDALMetadata/>\0junk\0"),
            (Tag::Unknown(42112), b"<GDALMetadata/>\0\0"),
        ] {
            let bytes = test_tiff(1, |_, directory| insert_ascii(directory, tag, value))?;
            assert!(
                parse_geotiff_metadata(Cursor::new(bytes)).is_err(),
                "Invalid semantic value for {tag:?}: {value:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn derives_high_level_metadata_from_main_image_only() -> Result<()> {
        let xml = "<GDALMetadata><Item name=\"DESCRIPTION\" sample=\"0\">é € 🙂</Item><Item name=\"SCALE\" sample=\"0\" role=\"scale\">2.5</Item><Item name=\"OFFSET\" sample=\"0\" role=\"offset\">10</Item><Item name=\"STATISTICS_MINIMUM\">1</Item><Item name=\"data_bounds\">[-10,-5,10,5]</Item></GDALMetadata>";
        let bytes = test_tiff(2, |level, directory| {
            if level == 0 {
                let mut counted_xml = xml.as_bytes().to_vec();
                counted_xml.push(0);
                insert_ascii(directory, Tag::Unknown(42112), &counted_xml)?;
                directory.write_tag(Tag::GeoKeyDirectoryTag, [1_u16, 1, 0, 2, 1024, 0, 1, 1, 3072, 0, 1, 3857])?;
                directory.write_tag(Tag::GdalNodata, "255")?;
            } else {
                directory.write_tag(Tag::BitsPerSample, 16_u16)?;
                directory.write_tag(Tag::SampleFormat, 2_u16)?;
                directory.write_tag(Tag::Compression, 5_u16)?;
                directory.write_tag(Tag::Predictor, 2_u16)?;
                directory.write_tag(
                    Tag::Unknown(42112),
                    "<GDALMetadata><Item name=\"SCALE\" sample=\"0\" role=\"scale\">99</Item></GDALMetadata>",
                )?;
            }
            Ok(())
        })?;
        let metadata = parse_geotiff_metadata(Cursor::new(bytes))?;
        assert_eq!(metadata.data_type, ArrayDataType::Uint8);
        assert_eq!(metadata.band_count, 1);
        assert_eq!(metadata.compression, None);
        assert_eq!(metadata.predictor, None);
        assert_eq!(metadata.band_metadata[0].description.as_deref(), Some("é € 🙂"));
        assert_eq!(metadata.band_metadata[0].scale, Some(2.5));
        assert_eq!(metadata.band_metadata[0].offset, Some(10.0));
        assert_eq!(metadata.geo_reference.scale(), Some(RasterScale { scale: 2.5, offset: 10.0 }));
        assert_eq!(metadata.geo_reference.nodata(), Some(255.0));
        assert_eq!(metadata.geo_reference.projection(), crs::epsg::WGS84_WEB_MERCATOR.to_string());
        assert_eq!(metadata.statistics.as_ref().expect("statistics").minimum_value, 1.0);
        assert_eq!(metadata.data_bounds, Some([-10.0, -5.0, 10.0, 5.0]));
        assert_eq!(metadata.overviews.len(), 2);
        assert!(metadata.overviews[0].chunk_locations.iter().all(TiffChunkLocation::is_sparse));
        assert_eq!(metadata.overviews[1].raster_size, RasterSize::with_rows_cols(Rows(4), Columns(8)));
        Ok(())
    }

    #[test]
    fn derives_uniform_sample_types_and_default_interleave() -> Result<()> {
        for (format, bits, expected) in [
            (1_u16, 8_u16, ArrayDataType::Uint8),
            (1, 16, ArrayDataType::Uint16),
            (1, 32, ArrayDataType::Uint32),
            (1, 64, ArrayDataType::Uint64),
            (2, 8, ArrayDataType::Int8),
            (2, 16, ArrayDataType::Int16),
            (2, 32, ArrayDataType::Int32),
            (2, 64, ArrayDataType::Int64),
            (3, 32, ArrayDataType::Float32),
            (3, 64, ArrayDataType::Float64),
        ] {
            let bytes = test_tiff(1, |_, directory| {
                directory.write_tag(Tag::SamplesPerPixel, 3_u16)?;
                directory.write_tag(Tag::BitsPerSample, [bits; 3])?;
                directory.write_tag(Tag::SampleFormat, [format; 3])?;
                Ok(())
            })?;
            let metadata = parse_geotiff_metadata(Cursor::new(bytes))?;
            assert_eq!(metadata.data_type, expected);
            assert_eq!(metadata.band_count, 3);
            assert_eq!(metadata.interleave, Interleave::Pixel);
        }
        Ok(())
    }

    #[test]
    fn derives_geographic_projection_from_inline_model_value() -> Result<()> {
        let bytes = test_tiff(1, |_, directory| {
            directory.write_tag(Tag::GeoKeyDirectoryTag, [1_u16, 1, 0, 2, 1024, 0, 1, 2, 2048, 0, 1, 4326])?;
            Ok(())
        })?;
        let metadata = parse_geotiff_metadata(Cursor::new(bytes))?;
        assert_eq!(metadata.geo_reference.projection(), crs::epsg::WGS84.to_string());
        Ok(())
    }

    #[test]
    fn parses_large_integer_nodata_as_high_level_float() -> Result<()> {
        let bytes = test_tiff(1, |_, directory| {
            directory.write_tag(Tag::BitsPerSample, 64_u16)?;
            directory.write_tag(Tag::GdalNodata, "18446744073709551615")?;
            Ok(())
        })?;
        let metadata = parse_geotiff_metadata(Cursor::new(bytes))?;
        assert_eq!(metadata.data_type, ArrayDataType::Uint64);
        assert_eq!(metadata.geo_reference.nodata(), Some(u64::MAX as f64));
        Ok(())
    }

    #[test]
    fn derives_compression_from_lerc_parameters() -> Result<()> {
        for (additional, expected) in [(0, Compression::Lerc), (1, Compression::LercDeflate), (2, Compression::LercZstd)] {
            let bytes = test_tiff(2, |level, directory| {
                directory.write_tag(Tag::Compression, 34887_u16)?;
                directory.write_tag(Tag::Unknown(50674), if level == 0 { [4_u32, additional] } else { [3, 0] })?;
                Ok(())
            })?;
            assert_eq!(parse_geotiff_metadata(Cursor::new(bytes))?.compression, Some(expected));
        }
        for parameters in [vec![4_u32], vec![4, 3], vec![4, 0, 1]] {
            let bytes = test_tiff(1, |_, directory| {
                directory.write_tag(Tag::Compression, 34887_u16)?;
                directory.write_tag(Tag::Unknown(50674), parameters.as_slice())?;
                Ok(())
            })?;
            assert!(parse_geotiff_metadata(Cursor::new(bytes)).is_err());
        }
        Ok(())
    }

    #[test]
    fn derives_transform_from_first_of_multiple_tiepoints() -> Result<()> {
        let bytes = test_tiff(1, |_, directory| {
            directory.write_tag(
                Tag::ModelTiepointTag,
                [1.0_f64, 2.0, 0.0, 12.0, 16.0, 0.0, 2.0, 3.0, 0.0, 14.0, 14.0, 0.0],
            )?;
            Ok(())
        })?;
        let metadata = parse_geotiff_metadata(Cursor::new(bytes))?;
        assert_eq!(metadata.geo_reference.geo_transform(), [10.0, 2.0, 0.0, 20.0, 0.0, -2.0].into());
        Ok(())
    }

    #[test]
    fn accepts_model_transformation_without_pixel_scale_or_tiepoints() -> Result<()> {
        let bytes = test_tiff(2, |_, directory| {
            directory.write_tag(
                Tag::ModelTransformationTag,
                [
                    2.0_f64, 0.5, 0.0, 10.0, 0.25, -2.0, 0.0, 20.0, 0.0, 0.0, 3.0, 4.0, 0.0, 0.0, 0.0, 1.0,
                ],
            )?;
            Ok(())
        })?;
        let mut decoder = Decoder::new(Cursor::new(bytes))?;
        decoder.next_image()?;
        let metadata = parse_cog_header(&mut decoder)?;
        assert_eq!(metadata.geo_reference.geo_transform(), [10.0, 2.0, 0.5, 20.0, 0.25, -2.0].into());
        Ok(())
    }

    #[test]
    fn rejects_malformed_present_optional_tags() -> Result<()> {
        for tag in [
            Tag::GdalNodata,
            Tag::Unknown(42112),
            Tag::ModelPixelScaleTag,
            Tag::ModelTiepointTag,
            Tag::ModelTransformationTag,
            Tag::GeoKeyDirectoryTag,
            Tag::Predictor,
        ] {
            let bytes = test_tiff(1, |_, directory| {
                directory.write_tag(tag, "invalid")?;
                Ok(())
            })?;
            assert!(
                parse_geotiff_metadata(Cursor::new(bytes)).is_err(),
                "Malformed optional {tag:?} was ignored"
            );
        }
        let bytes = test_tiff(1, |_, directory| {
            directory.write_tag(Tag::Unknown(42112), "<GDALMetadata>")?;
            Ok(())
        })?;
        assert!(parse_geotiff_metadata(Cursor::new(bytes)).is_err());
        Ok(())
    }

    #[test]
    fn rejects_truncated_geotiff_arrays() -> Result<()> {
        for (tag, count) in [
            (Tag::ModelPixelScaleTag, 2),
            (Tag::ModelTiepointTag, 5),
            (Tag::ModelTransformationTag, 8),
        ] {
            let bytes = test_tiff(1, |_, directory| {
                directory.write_tag(tag, vec![0.0_f64; count].as_slice())?;
                Ok(())
            })?;
            assert!(parse_geotiff_metadata(Cursor::new(bytes)).is_err(), "Malformed {tag:?} was ignored");
        }
        for keys in [vec![1_u16, 1, 0], vec![1, 1, 0, 1]] {
            let bytes = test_tiff(1, |_, directory| {
                directory.write_tag(Tag::GeoKeyDirectoryTag, keys.as_slice())?;
                Ok(())
            })?;
            assert!(parse_geotiff_metadata(Cursor::new(bytes)).is_err());
        }
        Ok(())
    }

    #[test]
    fn optional_metadata_outside_header_requests_more_data_and_from_file_retries() -> Result<()> {
        for (tag, value) in [
            (Tag::GdalNodata, "18446744073709551615"),
            (
                Tag::Unknown(42112),
                "<GDALMetadata><Item name=\"SCALE\" sample=\"0\" role=\"scale\">2.5</Item></GDALMetadata>",
            ),
        ] {
            let offset = u32::try_from(COG_HEADER_SIZE * 4 + 16).expect("test offset fits u32");
            let mut bytes = test_tiff(1, |_, directory| {
                insert_entry(
                    directory,
                    tag,
                    Entry::new(Type::ASCII, value.len() as u32 + 1, offset.to_ne_bytes()),
                );
                Ok(())
            })?;
            let header_len = bytes.len();
            bytes.resize(offset as usize, 0);
            bytes.extend_from_slice(value.as_bytes());
            bytes.push(0);
            assert!(
                matches!(
                    GeoTiffMetadata::from_buffer(bytes[..header_len].to_vec()),
                    Err(ParseFromBufferError::BufferTooSmall(_))
                ),
                "Truncated {tag:?} must request more data"
            );
            let expected = parse_geotiff_metadata(Cursor::new(&bytes))?;
            let check = |metadata: &GeoTiffMetadata| {
                assert_eq!(metadata.geo_reference, expected.geo_reference);
                assert_eq!(metadata.band_metadata.len(), expected.band_metadata.len());
            };
            let metadata = GeoTiffMetadata::from_buffer(bytes.clone()).map_err(|_| Error::Runtime("Full buffer failed to parse".into()))?;
            check(&metadata);
            let file = tempfile::NamedTempFile::new()?;
            std::fs::write(file.path(), &bytes)?;
            let metadata = GeoTiffMetadata::from_file(file.path())?;
            check(&metadata);
            assert!(metadata.gdal_ghost_data.as_ref().is_some_and(|ghost| ghost.is_cog()));
            std::fs::write(file.path(), &bytes[..header_len])?;
            assert!(GeoTiffMetadata::from_file(file.path()).is_err());
        }
        Ok(())
    }

    #[test]
    fn optional_georeferencing_arrays_outside_header_request_more_data() -> Result<()> {
        let scale = [2.0_f64, 2.0, 0.0];
        let point = [0.0_f64, 0.0, 0.0, 10.0, 20.0, 0.0];
        let matrix = [
            2.0_f64, 0.0, 0.0, 10.0, 0.0, -2.0, 0.0, 20.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 1.0,
        ];
        let keys = [1_u16, 1, 0, 0];
        for (tag, field_type, count, payload) in [
            (
                Tag::ModelPixelScaleTag,
                Type::DOUBLE,
                3,
                scale.into_iter().flat_map(f64::to_ne_bytes).collect::<Vec<_>>(),
            ),
            (
                Tag::ModelTiepointTag,
                Type::DOUBLE,
                6,
                point.into_iter().flat_map(f64::to_ne_bytes).collect(),
            ),
            (
                Tag::ModelTransformationTag,
                Type::DOUBLE,
                16,
                matrix.into_iter().flat_map(f64::to_ne_bytes).collect(),
            ),
            (
                Tag::GeoKeyDirectoryTag,
                Type::SHORT,
                4,
                keys.into_iter().flat_map(u16::to_ne_bytes).collect(),
            ),
        ] {
            let offset = u32::try_from(COG_HEADER_SIZE * 4 + 16).expect("test offset fits u32");
            let mut bytes = test_tiff(1, |_, directory| {
                insert_entry(directory, tag, Entry::new(field_type, count, offset.to_ne_bytes()));
                Ok(())
            })?;
            let header_len = bytes.len();
            bytes.resize(offset as usize, 0);
            bytes.extend_from_slice(&payload);
            assert!(
                matches!(
                    GeoTiffMetadata::from_buffer(bytes[..header_len].to_vec()),
                    Err(ParseFromBufferError::BufferTooSmall(_))
                ),
                "Truncated {tag:?} must request more data"
            );
            assert!(GeoTiffMetadata::from_buffer(bytes).is_ok(), "Complete {tag:?} must parse");
        }
        Ok(())
    }

    #[test]
    fn rejects_dimensions_outside_raster_size_range() -> Result<()> {
        for level_to_change in [0, 1] {
            for width in [0, i32::MAX as u32 + 1] {
                let bytes = test_tiff(2, |level, directory| {
                    if level == level_to_change {
                        directory.write_tag(Tag::ImageWidth, width)?;
                    }
                    Ok(())
                })?;
                assert!(parse_geotiff_metadata(Cursor::new(bytes)).is_err());
            }
        }
        Ok(())
    }

    #[test]
    fn rejects_mismatched_chunk_arrays_and_overflowing_ranges() -> Result<()> {
        for level_to_change in [0, 1] {
            let bytes = test_tiff(2, |level, directory| {
                if level == level_to_change {
                    directory.write_tag(Tag::TileByteCounts, [0_u32, 0, 0])?;
                }
                Ok(())
            })?;
            assert!(parse_geotiff_metadata(Cursor::new(bytes)).is_err());
            let bytes = test_tiff(2, |level, directory| {
                if level == level_to_change {
                    let offsets = vec![u64::MAX; if level == 0 { 2 } else { 1 }];
                    let counts = vec![4_u64; offsets.len()];
                    directory.write_tag(Tag::TileOffsets, offsets.as_slice())?;
                    directory.write_tag(Tag::TileByteCounts, counts.as_slice())?;
                }
                Ok(())
            })?;
            assert!(
                matches!(parse_geotiff_metadata(Cursor::new(bytes)), Err(Error::InvalidArgument(message)) if message.contains("overflows"))
            );
        }
        Ok(())
    }

    #[test]
    fn existing_tiled_and_striped_fixtures_keep_main_convenience_fields() -> Result<()> {
        for (filename, bands, layout, interleave) in [
            ("landusebyte.tif", 1, ChunkDataLayout::Striped(3), Interleave::Band),
            ("landusebyte_tiled.tif", 1, ChunkDataLayout::Tiled(256), Interleave::Band),
            ("multiband_cog.tif", 5, ChunkDataLayout::Tiled(512), Interleave::Tile),
            (
                "multiband_cog_interleave_band.tif",
                5,
                ChunkDataLayout::Tiled(512),
                Interleave::Band,
            ),
            (
                "multiband_cog_interleave_tile.tif",
                5,
                ChunkDataLayout::Tiled(512),
                Interleave::Tile,
            ),
        ] {
            let metadata = GeoTiffMetadata::from_file(&crate::testutils::workspace_test_data_dir().join(filename))?;
            assert_eq!(metadata.band_count, bands, "{filename}");
            assert_eq!(metadata.data_type, ArrayDataType::Uint8, "{filename}");
            assert_eq!(metadata.data_layout, layout, "{filename}");
            assert_eq!(metadata.interleave, interleave, "{filename}");
            assert!(!metadata.overviews[0].chunk_locations.is_empty());
        }
        Ok(())
    }
}

//! Assembly-only metadata for the decoder's current image directory.

use std::io::{Read, Seek};

use tiff::{
    decoder::{Decoder, ifd::Value},
    tags::{Tag, Type},
};

use crate::{Error, Result};

use super::gdal_metadata::{GdalMetadata, parse_gdal_metadata};

/// An unsigned TIFF rational, retained without floating-point conversion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Rational {
    pub(super) numerator: u32,
    pub(super) denominator: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LercParameters {
    pub(super) version: u32,
    /// TIFF LERC additional compression code: 0 = none, 1 = Deflate, 2 = Zstd.
    pub(super) additional_compression: u32,
}

/// Raw per-IFD metadata, independent of the shared reader's raster and chunk discovery.
///
/// Encoding codes retain their TIFF numeric values. Optional fields distinguish
/// absence from explicit values; sample vectors retain their source lengths.
#[derive(Debug, Clone)]
pub(super) struct ImageMetadata {
    pub(super) new_subfile_type: u32,
    pub(super) samples_per_pixel: u16,
    pub(super) bits_per_sample: Vec<u16>,
    pub(super) sample_format: Vec<u16>,
    pub(super) compression: u16,
    pub(super) predictor: u16,
    pub(super) photometric_interpretation: u16,
    pub(super) planar_configuration: u16,
    pub(super) tile_width: Option<u32>,
    pub(super) tile_length: Option<u32>,
    pub(super) rows_per_strip: Option<u32>,
    pub(super) lerc_parameters: Option<LercParameters>,
    pub(super) fill_order: Option<u16>,
    pub(super) orientation: Option<u16>,
    pub(super) x_resolution: Option<Rational>,
    pub(super) y_resolution: Option<Rational>,
    pub(super) resolution_unit: Option<u16>,
    pub(super) image_description: Option<String>,
    pub(super) software: Option<String>,
    pub(super) artist: Option<String>,
    pub(super) host_computer: Option<String>,
    pub(super) copyright: Option<String>,
    pub(super) icc_profile: Option<Vec<u8>>,
    pub(super) model_pixel_scale: Option<[f64; 3]>,
    pub(super) model_tiepoints: Option<Vec<[f64; 6]>>,
    pub(super) model_transformation: Option<[f64; 16]>,
    /// `GeoKeyDirectory` header and entries, including any inline SHORT parameters.
    pub(super) geo_key_directory: Option<Vec<u16>>,
    pub(super) geo_ascii_params: Option<String>,
    pub(super) geo_double_params: Option<Vec<f64>>,
    /// Original `GDAL_NODATA` text, without conversion through floating point.
    pub(super) nodata: Option<String>,
    pub(super) gdal_metadata: Option<GdalMetadata>,
    /// Unrepresented tags are recorded without reading or following their values.
    pub(super) unhandled_tag_ids: Vec<u16>,
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

    // Present metadata must not disappear when its value is outside a partial header.
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

fn optional_rational<R: Read + Seek>(decoder: &mut Decoder<R>, tag: Tag) -> Result<Option<Rational>> {
    optional_tag(decoder, tag, Type::RATIONAL, |value| match value {
        Value::Rational(numerator, denominator) if denominator != 0 => Ok(Rational { numerator, denominator }),
        _ => Err(tiff::TiffFormatError::InvalidTagValueType(tag).into()),
    })
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

/// Read only assembly metadata; the caller owns IFD traversal and chunk discovery.
pub(super) fn read_image_metadata<R: Read + Seek>(decoder: &mut Decoder<R>, tiled: bool) -> Result<ImageMetadata> {
    let model_tiepoints = optional_tag(decoder, Tag::ModelTiepointTag, Type::DOUBLE, Value::into_f64_vec)?
        .map(|values| {
            let (points, remainder) = values.as_chunks::<6>();
            if points.is_empty() || !remainder.is_empty() {
                return Err(Error::InvalidArgument(
                    "ModelTiepoint must contain one or more groups of 6 values".into(),
                ));
            }
            Ok(points.to_vec())
        })
        .transpose()?;
    let geo_key_directory = optional_tag(decoder, Tag::GeoKeyDirectoryTag, Type::SHORT, Value::into_u16_vec)?;
    if let Some(directory) = &geo_key_directory
        && (directory.len() < 4 || directory.len() < 4 + usize::from(directory[3]) * 4)
    {
        return Err(Error::InvalidArgument("GeoKeyDirectory has a truncated header or key list".into()));
    }
    let lerc_parameters = optional_tag(decoder, Tag::Unknown(50674), Type::LONG, Value::into_u32_vec)?
        .map(|values| match values.as_slice() {
            &[version, additional_compression] => Ok(LercParameters {
                version,
                additional_compression,
            }),
            _ => Err(Error::InvalidArgument("LercParameters must contain exactly 2 values".into())),
        })
        .transpose()?;
    let gdal_metadata = optional_ascii(decoder, Tag::Unknown(42112))?
        .map(|xml| parse_gdal_metadata(&xml))
        .transpose()?;

    let mut unhandled_tag_ids = Vec::new();
    for (tag, entry) in decoder.image_ifd().directory().iter() {
        if !is_handled_tag(tag, tiled) {
            unhandled_tag_ids.push(tag.to_u16());
        } else if matches!(entry.field_type(), Type::IFD | Type::IFD8) {
            return Err(Error::InvalidArgument(format!(
                "TIFF tag {} has an unexpected directory-pointer field type",
                tag.to_u16()
            )));
        }
    }

    let metadata = ImageMetadata {
        new_subfile_type: decoder.find_tag_unsigned(Tag::NewSubfileType)?.unwrap_or(0),
        samples_per_pixel: decoder.find_tag_unsigned(Tag::SamplesPerPixel)?.unwrap_or(1),
        bits_per_sample: decoder.find_tag_unsigned_vec(Tag::BitsPerSample)?.unwrap_or_else(|| vec![1]),
        sample_format: decoder.find_tag_unsigned_vec(Tag::SampleFormat)?.unwrap_or_else(|| vec![1]),
        compression: decoder.find_tag_unsigned(Tag::Compression)?.unwrap_or(1),
        predictor: decoder.find_tag_unsigned(Tag::Predictor)?.unwrap_or(1),
        photometric_interpretation: decoder.find_tag_unsigned(Tag::PhotometricInterpretation)?.unwrap_or(1),
        planar_configuration: decoder.find_tag_unsigned(Tag::PlanarConfiguration)?.unwrap_or(1),
        tile_width: decoder.find_tag_unsigned(Tag::TileWidth)?,
        tile_length: decoder.find_tag_unsigned(Tag::TileLength)?,
        rows_per_strip: decoder.find_tag_unsigned(Tag::RowsPerStrip)?,
        lerc_parameters,
        fill_order: optional_tag(decoder, Tag::FillOrder, Type::SHORT, Value::into_u16)?,
        orientation: optional_tag(decoder, Tag::Orientation, Type::SHORT, Value::into_u16)?,
        x_resolution: optional_rational(decoder, Tag::XResolution)?,
        y_resolution: optional_rational(decoder, Tag::YResolution)?,
        resolution_unit: optional_tag(decoder, Tag::ResolutionUnit, Type::SHORT, Value::into_u16)?,
        image_description: optional_ascii(decoder, Tag::ImageDescription)?,
        software: optional_ascii(decoder, Tag::Software)?,
        artist: optional_ascii(decoder, Tag::Artist)?,
        host_computer: optional_ascii(decoder, Tag::HostComputer)?,
        copyright: optional_ascii(decoder, Tag::Copyright)?,
        icc_profile: optional_tag(decoder, Tag::IccProfile, Type::UNDEFINED, Value::into_u8_vec)?,
        model_pixel_scale: optional_f64_array(decoder, Tag::ModelPixelScaleTag)?,
        model_tiepoints,
        model_transformation: optional_f64_array(decoder, Tag::ModelTransformationTag)?,
        geo_key_directory,
        geo_ascii_params: optional_ascii(decoder, Tag::GeoAsciiParamsTag)?,
        geo_double_params: optional_tag(decoder, Tag::GeoDoubleParamsTag, Type::DOUBLE, Value::into_f64_vec)?,
        nodata: optional_ascii(decoder, Tag::GdalNodata)?,
        gdal_metadata,
        unhandled_tag_ids,
    };
    if metadata.samples_per_pixel == 0
        || metadata.bits_per_sample.is_empty()
        || metadata.sample_format.is_empty()
        || metadata.tile_width == Some(0)
        || metadata.tile_length == Some(0)
        || metadata.rows_per_strip == Some(0)
    {
        return Err(Error::InvalidArgument(
            "TIFF image has an empty sample list or zero sample/tile/strip dimension".into(),
        ));
    }
    Ok(metadata)
}

fn is_handled_tag(tag: Tag, tiled: bool) -> bool {
    match tag {
        Tag::NewSubfileType
        | Tag::ImageWidth
        | Tag::ImageLength
        | Tag::BitsPerSample
        | Tag::Compression
        | Tag::PhotometricInterpretation
        | Tag::FillOrder
        | Tag::ImageDescription
        | Tag::Orientation
        | Tag::SamplesPerPixel
        | Tag::RowsPerStrip
        | Tag::XResolution
        | Tag::YResolution
        | Tag::PlanarConfiguration
        | Tag::ResolutionUnit
        | Tag::Software
        | Tag::Artist
        | Tag::HostComputer
        | Tag::Predictor
        | Tag::TileWidth
        | Tag::TileLength
        | Tag::Copyright
        | Tag::ModelPixelScaleTag
        | Tag::ModelTiepointTag
        | Tag::ModelTransformationTag
        | Tag::IccProfile
        | Tag::GeoKeyDirectoryTag
        | Tag::GeoDoubleParamsTag
        | Tag::GeoAsciiParamsTag
        | Tag::SampleFormat
        | Tag::GdalNodata
        | Tag::Unknown(42112 | 50674) => true,
        Tag::TileOffsets | Tag::TileByteCounts => tiled,
        Tag::StripOffsets | Tag::StripByteCounts => !tiled,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use tiff::{
        Directory,
        decoder::ifd::Entry,
        encoder::{DirectoryEncoder, Rational as EncodedRational, TiffEncoder, TiffKind, TiffKindBig, TiffKindStandard},
    };

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

    fn read_images(bytes: &[u8]) -> Result<Vec<ImageMetadata>> {
        let mut decoder = Decoder::new(Cursor::new(bytes))?;
        let mut images = vec![read_image_metadata(&mut decoder, true)?];
        while decoder.more_images() {
            decoder.next_image()?;
            images.push(read_image_metadata(&mut decoder, true)?);
        }
        Ok(images)
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
            let bytes = test_tiff_kind::<K>(2, |_, directory| {
                for tag in [
                    Tag::ImageDescription,
                    Tag::Software,
                    Tag::Artist,
                    Tag::HostComputer,
                    Tag::Copyright,
                    Tag::GeoAsciiParamsTag,
                ] {
                    insert_ascii(directory, tag, &counted)?;
                }
                Ok(())
            })?;
            for image in read_images(&bytes)? {
                for value in [
                    &image.image_description,
                    &image.software,
                    &image.artist,
                    &image.host_computer,
                    &image.copyright,
                    &image.geo_ascii_params,
                ] {
                    assert_eq!(value.as_deref(), Some(text), "Counted ASCII bytes {counted:?}");
                    let mut serialized = value.as_ref().expect("present ASCII value").as_bytes().to_vec();
                    serialized.push(0);
                    assert_eq!(serialized, counted, "One terminator must reproduce the complete source value");
                }
            }
        }
        Ok(())
    }

    #[test]
    fn retains_counted_utf8_and_internal_or_trailing_nuls_in_classic_tiff() -> Result<()> {
        assert_counted_ascii_is_preserved::<TiffKindStandard>()
    }

    #[test]
    fn retains_counted_utf8_and_internal_or_trailing_nuls_in_bigtiff() -> Result<()> {
        assert_counted_ascii_is_preserved::<TiffKindBig>()
    }

    fn assert_malformed_ascii_is_rejected<K: TiffKind>() -> Result<()> {
        for value in [
            b"".as_slice(),
            b"a",
            b"abcd",
            b"not terminated",
            b"first\0unterminated",
            b"\xff\0",
            b"longer \xff\0",
        ] {
            let bytes = test_tiff_kind::<K>(1, |_, directory| insert_ascii(directory, Tag::ImageDescription, value))?;
            assert!(
                matches!(read_images(&bytes), Err(Error::InvalidArgument(message)) if message.contains("270")),
                "Malformed ASCII bytes {value:?} must not be silently accepted"
            );
        }
        Ok(())
    }

    #[test]
    fn rejects_nonterminated_or_invalid_utf8_optional_ascii() -> Result<()> {
        assert_malformed_ascii_is_rejected::<TiffKindStandard>()?;
        assert_malformed_ascii_is_rejected::<TiffKindBig>()
    }

    #[test]
    fn retains_nodata_text_without_interpreting_or_truncating_it() -> Result<()> {
        for text in ["18446744073709551615", "255\0junk", "255\0"] {
            let bytes = test_tiff(1, |_, directory| {
                insert_ascii(directory, Tag::GdalNodata, format!("{text}\0").as_bytes())
            })?;
            assert_eq!(read_images(&bytes)?[0].nodata.as_deref(), Some(text));
        }
        Ok(())
    }

    #[test]
    fn rejects_retained_nuls_in_gdal_xml() -> Result<()> {
        for value in [b"<GDALMetadata/>\0junk\0".as_slice(), b"<GDALMetadata/>\0\0"] {
            let bytes = test_tiff(1, |_, directory| insert_ascii(directory, Tag::Unknown(42112), value))?;
            assert!(read_images(&bytes).is_err());
        }
        Ok(())
    }

    #[test]
    fn retains_rows_per_strip_without_defaulting_or_clamping_to_image_height() -> Result<()> {
        for rows in [None, Some(1_u32), Some(2), Some(3), Some(100), Some(u32::MAX)] {
            let mut stream = Cursor::new(Vec::new());
            {
                let mut encoder = TiffEncoder::new(&mut stream)?;
                let mut directory = encoder.image_directory()?;
                directory.write_tag(Tag::ImageWidth, 8_u32)?;
                directory.write_tag(Tag::ImageLength, 3_u32)?;
                directory.write_tag(Tag::BitsPerSample, 8_u16)?;
                directory.write_tag(Tag::PhotometricInterpretation, 1_u16)?;
                let strips = vec![0_u32; 3_u32.div_ceil(rows.unwrap_or(3).min(3)) as usize];
                directory.write_tag(Tag::StripOffsets, strips.as_slice())?;
                directory.write_tag(Tag::StripByteCounts, strips.as_slice())?;
                if let Some(rows) = rows {
                    directory.write_tag(Tag::RowsPerStrip, rows)?;
                }
                directory.finish()?;
            }
            let mut decoder = Decoder::new(Cursor::new(stream.into_inner()))?;
            let image = read_image_metadata(&mut decoder, false)?;
            assert_eq!(image.rows_per_strip, rows);
            assert!(image.unhandled_tag_ids.is_empty());
        }
        Ok(())
    }

    #[test]
    fn retains_per_ifd_metadata_without_inheriting_main_image_values() -> Result<()> {
        let xml = "<GDALMetadata><Item name=\"CUSTOM\" domain=\"private\" extra=\"kept\"> A &amp; B </Item><Item name=\"SCALE\" sample=\"0\" role=\"scale\">2.5</Item></GDALMetadata>";
        let overview_xml = "<GDALMetadata><Item name=\"CUSTOM_OVERVIEW\" sample=\"0\">overview</Item></GDALMetadata>";
        let keys = [1_u16, 1, 0, 3, 1024, 0, 1, 1, 1025, 0, 1, 2, 3072, 0, 1, 3857, 42];
        let bytes = test_tiff(2, |level, directory| {
            if level == 0 {
                directory.write_tag(Tag::Unknown(42112), xml)?;
                directory.write_tag(Tag::ModelPixelScaleTag, [2.0_f64, 2.0, 0.0])?;
                directory.write_tag(Tag::ModelTiepointTag, [0.0_f64, 0.0, 0.0, 10.0, 20.0, 0.0])?;
                directory.write_tag(Tag::GeoKeyDirectoryTag, keys)?;
                directory.write_tag(Tag::GeoAsciiParamsTag, "citation|")?;
                directory.write_tag(Tag::GeoDoubleParamsTag, [1.25_f64, 3.5])?;
                directory.write_tag(Tag::GdalNodata, "255")?;
                directory.write_tag(Tag::FillOrder, 1_u16)?;
                directory.write_tag(Tag::Orientation, 1_u16)?;
                directory.write_tag(Tag::ResolutionUnit, 2_u16)?;
                directory.write_tag(Tag::XResolution, EncodedRational { n: 300, d: 7 })?;
                directory.write_tag(Tag::YResolution, EncodedRational { n: 600, d: 11 })?;
                for (tag, value) in [
                    (Tag::ImageDescription, "image"),
                    (Tag::Software, "software"),
                    (Tag::Artist, "artist"),
                    (Tag::HostComputer, "host"),
                    (Tag::Copyright, "copyright"),
                ] {
                    directory.write_tag(tag, value)?;
                }
                insert_entry(directory, Tag::IccProfile, Entry::new(Type::UNDEFINED, 4, [1, 2, 3, 4]));
                insert_entry(directory, Tag::ExifDirectory, Entry::new(Type::IFD, 1, u32::MAX.to_ne_bytes()));
                directory.write_tag(Tag::Unknown(65000), "private TIFF metadata")?;
            } else {
                directory.write_tag(Tag::BitsPerSample, 16_u16)?;
                directory.write_tag(Tag::SampleFormat, 2_u16)?;
                directory.write_tag(Tag::Compression, 5_u16)?;
                directory.write_tag(Tag::Predictor, 2_u16)?;
                directory.write_tag(Tag::Unknown(42112), overview_xml)?;
            }
            Ok(())
        })?;
        let images = read_images(&bytes)?;
        assert_eq!(images.len(), 2);
        let main = &images[0];
        assert_eq!(main.new_subfile_type, 0);
        assert_eq!(main.samples_per_pixel, 1);
        assert_eq!(main.bits_per_sample, [8]);
        assert_eq!(main.sample_format, [1]);
        assert_eq!(
            (
                main.compression,
                main.predictor,
                main.photometric_interpretation,
                main.planar_configuration
            ),
            (1, 1, 1, 1)
        );
        assert_eq!((main.tile_width, main.tile_length, main.rows_per_strip), (Some(16), Some(16), None));
        assert_eq!(
            (main.fill_order, main.orientation, main.resolution_unit),
            (Some(1), Some(1), Some(2))
        );
        assert_eq!(
            main.x_resolution,
            Some(Rational {
                numerator: 300,
                denominator: 7
            })
        );
        assert_eq!(
            main.y_resolution,
            Some(Rational {
                numerator: 600,
                denominator: 11
            })
        );
        assert_eq!(main.image_description.as_deref(), Some("image"));
        assert_eq!(main.software.as_deref(), Some("software"));
        assert_eq!(main.artist.as_deref(), Some("artist"));
        assert_eq!(main.host_computer.as_deref(), Some("host"));
        assert_eq!(main.copyright.as_deref(), Some("copyright"));
        assert_eq!(main.icc_profile.as_deref(), Some([1_u8, 2, 3, 4].as_slice()));
        assert_eq!(main.geo_key_directory.as_deref(), Some(keys.as_slice()));
        assert_eq!(main.geo_ascii_params.as_deref(), Some("citation|"));
        assert_eq!(main.geo_double_params.as_deref(), Some([1.25_f64, 3.5].as_slice()));
        assert_eq!(main.model_pixel_scale, Some([2.0, 2.0, 0.0]));
        assert_eq!(main.model_tiepoints.as_deref(), Some([[0.0, 0.0, 0.0, 10.0, 20.0, 0.0]].as_slice()));
        assert_eq!(main.nodata.as_deref(), Some("255"));
        assert_eq!(
            main.gdal_metadata.as_ref().expect("main GDAL metadata").items,
            parse_gdal_metadata(xml)?.items
        );
        assert_eq!(main.unhandled_tag_ids, [Tag::ExifDirectory.to_u16(), 65000]);
        let overview = &images[1];
        assert_eq!(overview.new_subfile_type, 1);
        assert_eq!(overview.bits_per_sample, [16]);
        assert_eq!(overview.sample_format, [2]);
        assert_eq!((overview.compression, overview.predictor), (5, 2));
        assert_eq!(overview.model_pixel_scale, None);
        assert_eq!(overview.nodata, None);
        assert_eq!(overview.software, None);
        assert_eq!(overview.x_resolution, None);
        assert!(overview.unhandled_tag_ids.is_empty());
        assert_eq!(
            overview.gdal_metadata.as_ref().expect("overview GDAL metadata").items,
            parse_gdal_metadata(overview_xml)?.items
        );
        Ok(())
    }

    #[test]
    fn retains_both_lerc_parameters_for_each_ifd() -> Result<()> {
        let bytes = test_tiff(2, |level, directory| {
            directory.write_tag(Tag::Compression, 34887_u16)?;
            directory.write_tag(Tag::Unknown(50674), if level == 0 { [4_u32, 2] } else { [3, 0] })?;
            Ok(())
        })?;
        let images = read_images(&bytes)?;
        assert_eq!(images[0].compression, 34887);
        assert_eq!(
            images[0].lerc_parameters,
            Some(LercParameters {
                version: 4,
                additional_compression: 2
            })
        );
        assert_eq!(
            images[1].lerc_parameters,
            Some(LercParameters {
                version: 3,
                additional_compression: 0
            })
        );
        Ok(())
    }

    #[test]
    fn retains_full_model_transformation_and_multiple_tiepoints() -> Result<()> {
        let matrix = [
            2.0_f64, 0.0, 0.0, 10.0, 0.0, -2.0, 0.0, 20.0, 0.0, 0.0, 3.0, 4.0, 0.0, 0.0, 0.0, 1.0,
        ];
        let points = [0.0_f64, 0.0, 0.0, 10.0, 20.0, 0.0, 1.0, 1.0, 0.0, 12.0, 18.0, 0.0];
        let bytes = test_tiff(1, |_, directory| {
            directory.write_tag(Tag::ModelTransformationTag, matrix)?;
            directory.write_tag(Tag::ModelTiepointTag, points)?;
            Ok(())
        })?;
        let images = read_images(&bytes)?;
        assert_eq!(images[0].model_transformation, Some(matrix));
        assert_eq!(images[0].model_tiepoints.as_deref(), Some(points.as_chunks::<6>().0));
        Ok(())
    }

    #[test]
    fn rejects_malformed_present_optional_tags() -> Result<()> {
        for tag in [
            Tag::Software,
            Tag::GdalNodata,
            Tag::Unknown(42112),
            Tag::GeoAsciiParamsTag,
            Tag::FillOrder,
            Tag::Orientation,
            Tag::ResolutionUnit,
            Tag::XResolution,
            Tag::YResolution,
            Tag::IccProfile,
            Tag::ModelPixelScaleTag,
            Tag::ModelTiepointTag,
            Tag::ModelTransformationTag,
            Tag::GeoKeyDirectoryTag,
            Tag::GeoDoubleParamsTag,
            Tag::Unknown(50674),
        ] {
            let bytes = test_tiff(1, |_, directory| {
                directory.write_tag(tag, 42_u8)?;
                Ok(())
            })?;
            assert!(read_images(&bytes).is_err(), "Malformed optional {tag:?} was ignored");
        }
        let bytes = test_tiff(1, |_, directory| {
            directory.write_tag(Tag::Unknown(42112), "<GDALMetadata>")?;
            Ok(())
        })?;
        assert!(read_images(&bytes).is_err());
        Ok(())
    }

    #[test]
    fn rejects_invalid_rational_counts_and_zero_denominators() -> Result<()> {
        for values in [
            vec![EncodedRational { n: 300, d: 0 }],
            vec![EncodedRational { n: 300, d: 7 }, EncodedRational { n: 600, d: 11 }],
        ] {
            let bytes = test_tiff(1, |_, directory| {
                directory.write_tag(Tag::XResolution, values.as_slice())?;
                Ok(())
            })?;
            assert!(read_images(&bytes).is_err());
        }
        Ok(())
    }

    #[test]
    fn rejects_malformed_georeference_arrays_and_lerc_parameter_counts() -> Result<()> {
        for (tag, count) in [
            (Tag::ModelPixelScaleTag, 2),
            (Tag::ModelPixelScaleTag, 4),
            (Tag::ModelTiepointTag, 0),
            (Tag::ModelTiepointTag, 5),
            (Tag::ModelTiepointTag, 7),
            (Tag::ModelTransformationTag, 8),
            (Tag::ModelTransformationTag, 17),
        ] {
            let bytes = test_tiff(1, |_, directory| {
                directory.write_tag(tag, vec![0.0_f64; count].as_slice())?;
                Ok(())
            })?;
            assert!(read_images(&bytes).is_err(), "Malformed {tag:?} was ignored");
        }
        for keys in [vec![1_u16, 1, 0], vec![1, 1, 0, 1]] {
            let bytes = test_tiff(1, |_, directory| {
                directory.write_tag(Tag::GeoKeyDirectoryTag, keys.as_slice())?;
                Ok(())
            })?;
            assert!(read_images(&bytes).is_err());
        }
        for count in [0, 1, 3] {
            let bytes = test_tiff(1, |_, directory| {
                directory.write_tag(Tag::Unknown(50674), vec![0_u32; count].as_slice())?;
                Ok(())
            })?;
            assert!(read_images(&bytes).is_err());
        }
        Ok(())
    }

    #[test]
    fn optional_metadata_outside_available_bytes_propagates_unexpected_eof() -> Result<()> {
        for (tag, value) in [
            (Tag::Software, "metadata stored beyond the available header"),
            (Tag::GdalNodata, "18446744073709551615"),
            (
                Tag::Unknown(42112),
                "<GDALMetadata><Item name=\"CUSTOM\">outside header</Item></GDALMetadata>",
            ),
        ] {
            let offset = 65536_u32;
            let mut bytes = test_tiff(1, |_, directory| {
                insert_entry(
                    directory,
                    tag,
                    Entry::new(Type::ASCII, value.len() as u32 + 1, offset.to_ne_bytes()),
                );
                Ok(())
            })?;
            assert!(
                matches!(
                    read_images(&bytes),
                    Err(Error::IOError(error) | Error::TiffError(tiff::TiffError::IoError(error)))
                        if error.kind() == std::io::ErrorKind::UnexpectedEof
                ),
                "Truncated {tag:?} must not be treated as absent"
            );
            bytes.resize(offset as usize, 0);
            bytes.extend_from_slice(value.as_bytes());
            bytes.push(0);
            let images = read_images(&bytes)?;
            let image = &images[0];
            match tag {
                Tag::Software => assert_eq!(image.software.as_deref(), Some(value)),
                Tag::GdalNodata => assert_eq!(image.nodata.as_deref(), Some(value)),
                _ => assert_eq!(image.gdal_metadata.as_ref().unwrap().items, parse_gdal_metadata(value)?.items),
            }
        }
        Ok(())
    }

    #[test]
    fn rejects_directory_pointer_types_for_handled_tags() -> Result<()> {
        for field_type in [Type::IFD, Type::IFD8] {
            let bytes = test_tiff_kind::<TiffKindBig>(1, |_, directory| {
                let entry =
                    directory.write_entry_bytes(field_type, &u64::MAX.to_ne_bytes()[..if field_type == Type::IFD { 4 } else { 8 }])?;
                insert_entry(directory, Tag::NewSubfileType, entry);
                Ok(())
            })?;
            assert!(matches!(
                read_images(&bytes),
                Err(Error::InvalidArgument(message)) if message.contains("directory-pointer")
            ));
        }
        Ok(())
    }

    #[test]
    fn only_the_active_layouts_chunk_tags_are_handled() {
        for (tag, tiled) in [
            (Tag::TileOffsets, true),
            (Tag::TileByteCounts, true),
            (Tag::StripOffsets, false),
            (Tag::StripByteCounts, false),
        ] {
            assert!(is_handled_tag(tag, tiled));
            assert!(!is_handled_tag(tag, !tiled));
        }
        assert!(!is_handled_tag(Tag::SubIfd, true));
        assert!(!is_handled_tag(Tag::Unknown(65000), false));
    }
}

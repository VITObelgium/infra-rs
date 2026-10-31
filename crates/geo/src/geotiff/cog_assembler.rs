//! Raw assembly of compatible single-band COGs into a planar-separate multiband COG.
//!
//! The assembler deliberately does not decode or recompress imagery. It rebuilds the TIFF
//! directories and copies each compressed tile payload byte-for-byte.

use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};

use tiff::{
    decoder::Decoder,
    tags::{ByteOrder, Type},
};

use crate::{Error, RasterSize, Result};

mod gdal_metadata;
mod source_metadata;

use gdal_metadata::{GdalMetadataItem, serialize_gdal_metadata};
use source_metadata::{ImageMetadata, read_image_metadata};

use super::{GeoTiffMetadata, TiffChunkLocation, TiffOverview, gdalghostdata::GdalGhostData, io};

const TAG_NEW_SUBFILE_TYPE: u16 = 254;
const TAG_IMAGE_WIDTH: u16 = 256;
const TAG_IMAGE_LENGTH: u16 = 257;
const TAG_BITS_PER_SAMPLE: u16 = 258;
const TAG_COMPRESSION: u16 = 259;
const TAG_PHOTOMETRIC_INTERPRETATION: u16 = 262;
const TAG_FILL_ORDER: u16 = 266;
const TAG_IMAGE_DESCRIPTION: u16 = 270;
const TAG_ORIENTATION: u16 = 274;
const TAG_SAMPLES_PER_PIXEL: u16 = 277;
const TAG_X_RESOLUTION: u16 = 282;
const TAG_Y_RESOLUTION: u16 = 283;
const TAG_PLANAR_CONFIGURATION: u16 = 284;
const TAG_RESOLUTION_UNIT: u16 = 296;
const TAG_SOFTWARE: u16 = 305;
const TAG_ARTIST: u16 = 315;
const TAG_HOST_COMPUTER: u16 = 316;
const TAG_PREDICTOR: u16 = 317;
const TAG_TILE_WIDTH: u16 = 322;
const TAG_TILE_LENGTH: u16 = 323;
const TAG_TILE_OFFSETS: u16 = 324;
const TAG_TILE_BYTE_COUNTS: u16 = 325;
const TAG_EXTRA_SAMPLES: u16 = 338;
const TAG_COPYRIGHT: u16 = 33432;
const TAG_MODEL_PIXEL_SCALE: u16 = 33550;
const TAG_MODEL_TIEPOINT: u16 = 33922;
const TAG_MODEL_TRANSFORMATION: u16 = 34264;
const TAG_ICC_PROFILE: u16 = 34675;
const TAG_GEO_KEY_DIRECTORY: u16 = 34735;
const TAG_GEO_DOUBLE_PARAMS: u16 = 34736;
const TAG_GEO_ASCII_PARAMS: u16 = 34737;
const TAG_SAMPLE_FORMAT: u16 = 339;
const TAG_GDAL_METADATA: u16 = 42112;
const TAG_GDAL_NODATA: u16 = 42113;
const TAG_LERC_PARAMETERS: u16 = 50674;

const COMPRESSION_NONE: u16 = 1;
const COMPRESSION_LZW: u16 = 5;
const COMPRESSION_DEFLATE: u16 = 8;
const COMPRESSION_LERC: u16 = 34887;
const COMPRESSION_ZSTD: u16 = 50000;

const PHOTOMETRIC_BLACK_IS_ZERO: u16 = 1;
const PLANAR_SEPARATE: u16 = 2;
const REDUCED_IMAGE: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
struct EncodedTag {
    field_type: Type,
    count: u64,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
struct SourceLevel {
    raster_size: RasterSize,
    chunk_locations: Vec<TiffChunkLocation>,
    image_metadata: ImageMetadata,
}

#[derive(Debug)]
struct SourceCog {
    path: PathBuf,
    levels: Vec<SourceLevel>,
}

impl SourceCog {
    fn levels(&self) -> &[SourceLevel] {
        &self.levels
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TiffVariant {
    Classic,
    Big,
}

impl TiffVariant {
    fn align_metadata(self, value: u64) -> std::result::Result<u64, LayoutError> {
        let alignment = match self {
            Self::Classic => 2,
            Self::Big => 8,
        };
        checked_add(value, alignment - 1, "metadata alignment").map(|value| value & !(alignment - 1))
    }

    fn header_size(self) -> u64 {
        match self {
            Self::Classic => 8,
            Self::Big => 16,
        }
    }

    fn offset_size(self) -> usize {
        match self {
            Self::Classic => 4,
            Self::Big => 8,
        }
    }

    fn ifd_size(self, tag_count: usize) -> std::result::Result<u64, LayoutError> {
        let tag_count = u64::try_from(tag_count).map_err(|_| LayoutError::Overflow("IFD tag count does not fit in u64".into()))?;
        match self {
            Self::Classic => {
                if tag_count > u16::MAX as u64 {
                    return Err(LayoutError::Overflow(format!(
                        "Classic TIFF IFD has {tag_count} tags, exceeding u16::MAX"
                    )));
                }
                checked_add(
                    checked_add(2, checked_mul(tag_count, 12, "Classic TIFF IFD entries")?, "Classic TIFF IFD")?,
                    4,
                    "Classic TIFF next-IFD pointer",
                )
            }
            Self::Big => checked_add(
                checked_add(8, checked_mul(tag_count, 20, "BigTIFF IFD entries")?, "BigTIFF IFD")?,
                8,
                "BigTIFF next-IFD pointer",
            ),
        }
    }
}

#[derive(Debug)]
struct OutputIfd {
    tags: BTreeMap<u16, EncodedTag>,
}

#[derive(Debug)]
struct PlannedTag {
    value: EncodedTag,
    value_offset: Option<u64>,
}

#[derive(Debug)]
struct PlannedIfd {
    offset: u64,
    next_offset: u64,
    tags: BTreeMap<u16, PlannedTag>,
}

#[derive(Debug)]
struct PlannedBlock {
    source_index: usize,
    source: TiffChunkLocation,
    payload_offset: u64,
}

#[derive(Debug)]
struct LayoutPlan {
    variant: TiffVariant,
    ghost: Vec<u8>,
    first_ifd_offset: u64,
    ifds: Vec<PlannedIfd>,
    blocks: Vec<PlannedBlock>,
    final_size: u64,
}

#[derive(Debug)]
enum LayoutError {
    Overflow(String),
    Invalid(String),
}

/// Merge compatible staged single-band COGs into one tile-interleaved multiband
/// COG without decoding or recompressing tiles, reporting 0.0 at the start and
/// 1.0 after successful completion.
/// Inputs must be little-endian, tiled, single-band COGs with matching image and
/// overview layouts, data types, compression settings, georeferencing, and
/// dataset-level metadata. Unsupported pointer-bearing and image-layout tags are
/// rejected rather than copied unsafely.
/// Callbacks run synchronously, and 1.0 is reported only after syncing and
/// atomically publishing the output file.
/// Pass `&mut |_| {}` when progress reporting is not needed.
pub fn assemble_band_cogs(inputs: &[PathBuf], output: &Path, progress: &mut dyn FnMut(f64)) -> Result<()> {
    progress(0.0);
    if inputs.is_empty() {
        return Err(Error::InvalidArgument("At least one input band COG is required".into()));
    }
    if inputs.len() > u16::MAX as usize {
        return Err(Error::InvalidArgument(format!(
            "Band count {} exceeds the TIFF SamplesPerPixel limit",
            inputs.len()
        )));
    }

    let sources = read_sources(inputs)?;
    validate_compatibility(&sources)?;
    let plan = assembly_layout(&sources)?;
    write_output(&sources, output, &plan)?;
    progress(1.0);
    Ok(())
}

fn read_sources(inputs: &[PathBuf]) -> Result<Vec<SourceCog>> {
    inputs
        .iter()
        .enumerate()
        .map(|(index, path)| read_source_cog(index, path))
        .collect()
}

fn assembly_layout(sources: &[SourceCog]) -> Result<LayoutPlan> {
    let merged_gdal_metadata = merge_gdal_metadata(sources)?;
    let classic_ifds = build_output_ifds(sources, TiffVariant::Classic, &merged_gdal_metadata)?;
    let plan = match plan_layout(sources, classic_ifds, TiffVariant::Classic) {
        Ok(plan) => plan,
        Err(LayoutError::Overflow(_)) => {
            let big_ifds = build_output_ifds(sources, TiffVariant::Big, &merged_gdal_metadata)?;
            return plan_layout(sources, big_ifds, TiffVariant::Big).map_err(layout_error);
        }
        Err(error) => return Err(layout_error(error)),
    };
    Ok(plan)
}

fn read_source_cog(input_index: usize, path: &Path) -> Result<SourceCog> {
    let input_name = format!("Input band {} ({})", input_index + 1, path.display());
    let metadata = GeoTiffMetadata::from_file(path)?;
    if !metadata.gdal_ghost_data.as_ref().is_some_and(GdalGhostData::is_cog) {
        return Err(Error::InvalidArgument(format!(
            "{input_name} is not a compatible COG or is marked as modified"
        )));
    }
    if !metadata.is_tiled() {
        return Err(Error::InvalidArgument(format!("{input_name} must be a tiled COG")));
    }

    let file = File::open(path)?;
    let file_size = file.metadata()?.len();
    let levels = read_source_levels(file, metadata.overviews)?;
    for (level_index, level) in levels.iter().enumerate() {
        validate_source_level(&format!("{input_name}, IFD {level_index}"), level_index, file_size, level)?;
    }
    Ok(SourceCog {
        path: path.to_path_buf(),
        levels,
    })
}

fn read_source_levels(file: File, overviews: Vec<TiffOverview>) -> Result<Vec<SourceLevel>> {
    let mut decoder = Decoder::new(file)?.with_limits(tiff::decoder::Limits::unlimited());
    if decoder.byte_order() != ByteOrder::LittleEndian {
        return Err(Error::InvalidArgument("Only little-endian source COGs are supported".into()));
    }
    // The shared reader owns raster/chunk discovery. Only assembly-specific IFD
    // preservation and validation require this separate, private metadata pass.
    let levels = overviews
        .into_iter()
        .enumerate()
        .map(|(index, overview)| {
            if index > 0 {
                decoder.next_image()?;
            }
            let tiled = decoder.get_chunk_type() == tiff::decoder::ChunkType::Tile;
            let image_metadata = read_image_metadata(&mut decoder, tiled)?;
            Ok(SourceLevel {
                raster_size: overview.raster_size,
                chunk_locations: overview.chunk_locations,
                image_metadata,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if decoder.more_images() {
        return Err(Error::InvalidArgument(
            "Source image directories changed after reading COG metadata".into(),
        ));
    }
    Ok(levels)
}

fn validate_source_level(context: &str, level_index: usize, file_size: u64, overview: &SourceLevel) -> Result<()> {
    let image = &overview.image_metadata;
    if !image.unhandled_tag_ids.is_empty() {
        return Err(Error::InvalidArgument(format!(
            "{context} contains metadata the assembler cannot preserve: {:?}",
            image.unhandled_tag_ids
        )));
    }
    if image.samples_per_pixel != 1 || image.bits_per_sample.len() != 1 || image.sample_format.len() != 1 {
        return Err(Error::InvalidArgument(format!(
            "{context} must contain exactly one sample per pixel"
        )));
    }
    let expected_subfile_type = if level_index == 0 { 0 } else { REDUCED_IMAGE };
    if image.new_subfile_type != expected_subfile_type || (level_index > 0 && image.gdal_metadata.is_some()) {
        return Err(Error::InvalidArgument(format!(
            "{context} is not a supported base image or reduced-resolution overview"
        )));
    }
    let Some(tile_width) = image.tile_width.filter(|&width| width > 0 && Some(width) == image.tile_length) else {
        return Err(Error::InvalidArgument(format!("{context} must use nonzero square tiles")));
    };
    if image.rows_per_strip.is_some() {
        return Err(Error::InvalidArgument(format!("{context} contains strip-layout metadata")));
    }
    if image.photometric_interpretation != PHOTOMETRIC_BLACK_IS_ZERO || !matches!(image.planar_configuration, 1 | PLANAR_SEPARATE) {
        return Err(Error::InvalidArgument(format!(
            "{context} must use BlackIsZero samples and a supported planar configuration"
        )));
    }
    if image.fill_order.is_some_and(|value| value != 1) || image.orientation.is_some_and(|value| value != 1) {
        return Err(Error::InvalidArgument(format!(
            "{context} uses a fill order or orientation unsupported by the COG reader"
        )));
    }
    if !matches!(
        image.compression,
        COMPRESSION_NONE | COMPRESSION_LZW | COMPRESSION_DEFLATE | COMPRESSION_LERC | COMPRESSION_ZSTD
    ) || !matches!(image.predictor, 1..=3)
        || !matches!(
            (image.sample_format[0], image.bits_per_sample[0]),
            (1 | 2, 8 | 16 | 32 | 64) | (3, 32 | 64)
        )
    {
        return Err(Error::InvalidArgument(format!("{context} uses unsupported sample encoding")));
    }
    if (image.compression == COMPRESSION_LERC) != image.lerc_parameters.is_some()
        || image.lerc_parameters.is_some_and(|params| params.additional_compression > 2)
    {
        return Err(Error::InvalidArgument(format!("{context} has incompatible LERC parameters")));
    }
    let width = overview.raster_size.cols.count();
    let height = overview.raster_size.rows.count();
    if width <= 0 || height <= 0 {
        return Err(Error::InvalidArgument(format!("{context} contains invalid dimensions")));
    }
    let expected_tiles = u64::from(width as u32).div_ceil(u64::from(tile_width)) * u64::from(height as u32).div_ceil(u64::from(tile_width));
    if overview.chunk_locations.len() as u64 != expected_tiles {
        return Err(Error::InvalidArgument(format!(
            "{context} has {} tiles; dimensions require {expected_tiles}",
            overview.chunk_locations.len()
        )));
    }
    for (tile_index, &TiffChunkLocation { offset, size }) in overview.chunk_locations.iter().enumerate() {
        if (offset == 0) != (size == 0) || (size != 0 && !(4..=u64::from(u32::MAX)).contains(&size)) {
            return Err(Error::InvalidArgument(format!(
                "{context} tile {tile_index} has unsupported offset/size {offset}/{size}"
            )));
        }
        if offset.checked_add(size).is_none_or(|end| end > file_size) {
            return Err(Error::InvalidArgument(format!(
                "{context} tile {tile_index} exceeds source file size {file_size}"
            )));
        }
    }
    Ok(())
}

fn validate_compatibility(sources: &[SourceCog]) -> Result<()> {
    let reference = &sources[0];
    let reference_levels = reference.levels();
    for (band_index, source) in sources.iter().enumerate() {
        if source.levels().len() != reference_levels.len() {
            return Err(Error::InvalidArgument(format!(
                "Input band {} ({}) has a different number of overview levels",
                band_index + 1,
                source.path.display()
            )));
        }
        for (level_index, (level, expected)) in source.levels().iter().zip(reference_levels).enumerate() {
            let image = &level.image_metadata;
            let main = &source.levels()[0].image_metadata;
            let encoding = |image: &ImageMetadata| {
                (
                    image.bits_per_sample[0],
                    image.sample_format[0],
                    image.compression,
                    image.predictor,
                    image.tile_width,
                    image.tile_length,
                    image.lerc_parameters,
                )
            };
            if encoding(image) != encoding(main) || encoding(image) != encoding(&expected.image_metadata) {
                return Err(Error::InvalidArgument(format!(
                    "Input band {} ({}) IFD {level_index} has incompatible data encoding",
                    band_index + 1,
                    source.path.display()
                )));
            }
            if level.raster_size != expected.raster_size {
                return Err(Error::InvalidArgument(format!(
                    "Input band {} ({}) IFD {level_index} has incompatible dimensions",
                    band_index + 1,
                    source.path.display()
                )));
            }
            if level_index > 0 {
                let previous = &source.levels()[level_index - 1].raster_size;
                if level.raster_size.cols.count() > previous.cols.count()
                    || level.raster_size.rows.count() > previous.rows.count()
                    || level.raster_size == *previous
                {
                    return Err(Error::InvalidArgument(format!(
                        "Input band {} IFD {level_index} is not a reduced-resolution overview",
                        band_index + 1
                    )));
                }
            }
            if output_metadata_tags(image) != output_metadata_tags(&expected.image_metadata) {
                return Err(Error::InvalidArgument(format!(
                    "Input band {} ({}) IFD {level_index} has incompatible georeferencing or document metadata",
                    band_index + 1,
                    source.path.display()
                )));
            }
        }
    }
    Ok(())
}

// Serialize metadata retained privately by the assembler for the output.
fn output_metadata_tags(image: &ImageMetadata) -> BTreeMap<u16, EncodedTag> {
    let mut tags = BTreeMap::new();
    for (id, value) in [
        (TAG_FILL_ORDER, image.fill_order),
        (TAG_ORIENTATION, image.orientation),
        (TAG_RESOLUTION_UNIT, image.resolution_unit),
    ] {
        if let Some(value) = value {
            tags.insert(id, short_tag(&[value]));
        }
    }
    for (id, value) in [
        (TAG_IMAGE_DESCRIPTION, &image.image_description),
        (TAG_SOFTWARE, &image.software),
        (TAG_ARTIST, &image.artist),
        (TAG_HOST_COMPUTER, &image.host_computer),
        (TAG_COPYRIGHT, &image.copyright),
        (TAG_GEO_ASCII_PARAMS, &image.geo_ascii_params),
        (TAG_GDAL_NODATA, &image.nodata),
    ] {
        if let Some(value) = value {
            tags.insert(id, ascii_tag(value));
        }
    }
    for (id, value) in [(TAG_X_RESOLUTION, image.x_resolution), (TAG_Y_RESOLUTION, image.y_resolution)] {
        if let Some(value) = value {
            let mut tag = long_tag(&[value.numerator, value.denominator]);
            tag.field_type = Type::RATIONAL;
            tag.count = 1;
            tags.insert(id, tag);
        }
    }
    if let Some(profile) = &image.icc_profile {
        tags.insert(
            TAG_ICC_PROFILE,
            EncodedTag {
                field_type: Type::UNDEFINED,
                count: profile.len() as u64,
                bytes: profile.clone(),
            },
        );
    }
    if let Some(scale) = &image.model_pixel_scale {
        tags.insert(TAG_MODEL_PIXEL_SCALE, double_tag(scale));
    }
    if let Some(tiepoints) = &image.model_tiepoints {
        tags.insert(TAG_MODEL_TIEPOINT, double_tag(tiepoints.as_flattened()));
    }
    if let Some(transform) = &image.model_transformation {
        tags.insert(TAG_MODEL_TRANSFORMATION, double_tag(transform));
    }
    if let Some(directory) = &image.geo_key_directory {
        tags.insert(TAG_GEO_KEY_DIRECTORY, short_tag(directory));
    }
    if let Some(params) = &image.geo_double_params {
        tags.insert(TAG_GEO_DOUBLE_PARAMS, double_tag(params));
    }
    if let Some(params) = image.lerc_parameters {
        tags.insert(TAG_LERC_PARAMETERS, long_tag(&[params.version, params.additional_compression]));
    }
    tags
}

fn merge_gdal_metadata(sources: &[SourceCog]) -> Result<EncodedTag> {
    let mut common_items: Option<Vec<GdalMetadataItem>> = None;
    let mut output_items = Vec::new();
    for (band_index, source) in sources.iter().enumerate() {
        let metadata = &source.levels()[0].image_metadata.gdal_metadata;
        let mut globals = Vec::new();
        let mut keys = std::collections::BTreeSet::new();
        for mut item in metadata.iter().flat_map(|metadata| &metadata.items).cloned() {
            // Canonicalize attribute order, not the order or precedence of duplicate values.
            item.attributes.sort();
            let key = (
                item.attribute("name").map(str::to_owned),
                item.attribute("domain").map(str::to_owned),
                item.attribute("sample").map(str::to_owned),
                item.attribute("role").map(str::to_owned),
            );
            if !keys.insert(key) {
                return Err(Error::InvalidArgument(format!(
                    "Input band {} ({}) contains duplicate GDAL metadata keys",
                    band_index + 1,
                    source.path.display()
                )));
            }
            if item.attribute("name") == Some("INTERLEAVE") && item.attribute("domain") == Some("IMAGE_STRUCTURE") {
                continue;
            }
            match item.attribute("sample") {
                Some("0") => {
                    item.set_attribute("sample", band_index.to_string());
                    output_items.push(item);
                }
                Some(sample) => {
                    return Err(Error::InvalidArgument(format!(
                        "Input band {} GDAL metadata has sample={sample}; expected 0",
                        band_index + 1
                    )));
                }
                None => globals.push(item),
            }
        }
        globals.sort();
        match &common_items {
            None => common_items = Some(globals),
            Some(expected) if expected == &globals => {}
            Some(_) => {
                return Err(Error::InvalidArgument(format!(
                    "Input band {} ({}) has conflicting dataset-level GDAL metadata",
                    band_index + 1,
                    source.path.display()
                )));
            }
        }
    }
    let mut items = common_items.unwrap_or_default();
    items.extend(output_items);
    items.push(GdalMetadataItem {
        attributes: vec![("name".into(), "INTERLEAVE".into()), ("domain".into(), "IMAGE_STRUCTURE".into())],
        text: "TILE".into(),
    });
    Ok(ascii_tag(&serialize_gdal_metadata(&items)?))
}

fn build_output_ifds(sources: &[SourceCog], variant: TiffVariant, gdal_metadata: &EncodedTag) -> Result<Vec<OutputIfd>> {
    let band_count =
        u16::try_from(sources.len()).map_err(|_| Error::InvalidArgument(format!("Band count {} exceeds u16::MAX", sources.len())))?;
    let reference = &sources[0];
    let mut output = Vec::with_capacity(reference.levels().len());

    for (level_index, level) in reference.levels().iter().enumerate() {
        let image = &level.image_metadata;
        let mut tags = output_metadata_tags(image);

        if level_index == 0 {
            tags.insert(TAG_GDAL_METADATA, gdal_metadata.clone());
        }
        if level_index > 0 {
            tags.insert(TAG_NEW_SUBFILE_TYPE, long_tag(&[REDUCED_IMAGE]));
        }

        tags.insert(TAG_IMAGE_WIDTH, long_tag(&[level.raster_size.cols.count() as u32]));
        tags.insert(TAG_IMAGE_LENGTH, long_tag(&[level.raster_size.rows.count() as u32]));
        tags.insert(TAG_BITS_PER_SAMPLE, short_tag(&vec![image.bits_per_sample[0]; sources.len()]));
        tags.insert(TAG_COMPRESSION, short_tag(&[image.compression]));
        tags.insert(TAG_PHOTOMETRIC_INTERPRETATION, short_tag(&[PHOTOMETRIC_BLACK_IS_ZERO]));
        tags.insert(TAG_SAMPLES_PER_PIXEL, short_tag(&[band_count]));
        tags.insert(TAG_PLANAR_CONFIGURATION, short_tag(&[PLANAR_SEPARATE]));
        tags.insert(TAG_PREDICTOR, short_tag(&[image.predictor]));
        tags.insert(TAG_TILE_WIDTH, long_tag(&[image.tile_width.expect("validated tiled image")]));
        tags.insert(TAG_TILE_LENGTH, long_tag(&[image.tile_length.expect("validated tiled image")]));
        if sources.len() > 1 {
            // With BlackIsZero, the first sample is the grayscale/color sample and the
            // remaining samples are explicitly marked as unspecified extra channels.
            tags.insert(TAG_EXTRA_SAMPLES, short_tag(&vec![0; sources.len() - 1]));
        }
        tags.insert(TAG_SAMPLE_FORMAT, short_tag(&vec![image.sample_format[0]; sources.len()]));

        let array_count = level
            .chunk_locations
            .len()
            .checked_mul(sources.len())
            .ok_or_else(|| Error::InvalidArgument(format!("IFD {level_index} multiband tile array length overflows usize")))?;
        tags.insert(
            TAG_TILE_OFFSETS,
            match variant {
                TiffVariant::Classic => long_tag(&vec![0; array_count]),
                TiffVariant::Big => long8_tag(&vec![0; array_count]),
            },
        );
        tags.insert(TAG_TILE_BYTE_COUNTS, long_tag(&vec![0; array_count]));

        output.push(OutputIfd { tags });
    }

    Ok(output)
}

fn plan_layout(sources: &[SourceCog], output_ifds: Vec<OutputIfd>, variant: TiffVariant) -> std::result::Result<LayoutPlan, LayoutError> {
    let ghost = cog_ghost_metadata()?;
    let mut cursor = checked_add(variant.header_size(), ghost.len() as u64, "TIFF header and COG ghost area")?;
    cursor = variant.align_metadata(cursor)?;
    let first_ifd_offset = cursor;
    check_variant_offset(variant, first_ifd_offset, "first IFD")?;

    let mut ifds = Vec::with_capacity(output_ifds.len());
    for output_ifd in output_ifds {
        cursor = variant.align_metadata(cursor)?;
        let ifd_offset = cursor;
        check_variant_offset(variant, ifd_offset, "IFD")?;
        cursor = checked_add(cursor, variant.ifd_size(output_ifd.tags.len())?, "IFD extent")?;

        let mut tags = BTreeMap::new();
        for (tag_id, value) in output_ifd.tags {
            validate_encoded_tag(&value)?;
            check_variant_tag(variant, &value, tag_id)?;
            let is_tile_array = matches!(tag_id, TAG_TILE_OFFSETS | TAG_TILE_BYTE_COUNTS);
            let value_offset = if value.bytes.len() > variant.offset_size() && !is_tile_array {
                cursor = variant.align_metadata(cursor)?;
                let offset = cursor;
                check_variant_offset(variant, offset, &format!("TIFF tag {tag_id} value"))?;
                cursor = checked_add(cursor, value.bytes.len() as u64, &format!("TIFF tag {tag_id} value"))?;
                Some(offset)
            } else {
                None
            };
            tags.insert(tag_id, PlannedTag { value, value_offset });
        }

        ifds.push(PlannedIfd {
            offset: ifd_offset,
            next_offset: 0,
            tags,
        });
    }

    for index in 0..ifds.len().saturating_sub(1) {
        ifds[index].next_offset = ifds[index + 1].offset;
    }

    cursor = variant.align_metadata(cursor)?;
    for ifd in &mut ifds {
        for tag_id in [TAG_TILE_OFFSETS, TAG_TILE_BYTE_COUNTS] {
            let tag = ifd
                .tags
                .get_mut(&tag_id)
                .ok_or_else(|| LayoutError::Invalid(format!("Output IFD is missing tag {tag_id}")))?;
            if tag.value.bytes.len() > variant.offset_size() {
                cursor = variant.align_metadata(cursor)?;
                tag.value_offset = Some(cursor);
                check_variant_offset(variant, cursor, &format!("TIFF tag {tag_id} value"))?;
                cursor = checked_add(cursor, tag.value.bytes.len() as u64, &format!("TIFF tag {tag_id} value"))?;
            }
        }
    }

    cursor = variant.align_metadata(cursor)?;
    let mut blocks = Vec::new();
    for level_index in (0..ifds.len()).rev() {
        let tiles_per_band = sources[0].levels()[level_index].chunk_locations.len();
        let array_len = tiles_per_band
            .checked_mul(sources.len())
            .ok_or_else(|| LayoutError::Overflow(format!("IFD {level_index} tile array length overflows usize")))?;
        let mut offsets = vec![0u64; array_len];
        let mut counts = vec![0u32; array_len];

        for tile_index in 0..tiles_per_band {
            for (band_index, source) in sources.iter().enumerate() {
                let source_tile = source.levels()[level_index].chunk_locations[tile_index];
                if source_tile.size == 0 {
                    continue;
                }
                let size = u32::try_from(source_tile.size).map_err(|_| {
                    LayoutError::Invalid(format!(
                        "Input band {} IFD {level_index} tile {tile_index} exceeds the uint32 COG leader limit",
                        band_index + 1
                    ))
                })?;
                cursor = checked_add(cursor, 4, "COG block leader")?;
                let payload_offset = cursor;
                check_variant_offset(variant, payload_offset, "tile payload")?;
                offsets[band_index * tiles_per_band + tile_index] = payload_offset;
                counts[band_index * tiles_per_band + tile_index] = size;
                cursor = checked_add(cursor, source_tile.size, "tile payload")?;
                cursor = checked_add(cursor, 4, "COG block trailer")?;
                blocks.push(PlannedBlock {
                    source_index: band_index,
                    source: source_tile,
                    payload_offset,
                });
            }
        }

        let ifd = &mut ifds[level_index];
        ifd.tags.get_mut(&TAG_TILE_OFFSETS).expect("planned TileOffsets tag").value = match variant {
            TiffVariant::Classic => {
                let values = offsets
                    .iter()
                    .map(|&offset| {
                        u32::try_from(offset)
                            .map_err(|_| LayoutError::Overflow(format!("Classic TIFF tile offset {offset} exceeds u32::MAX")))
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                long_tag(&values)
            }
            TiffVariant::Big => long8_tag(&offsets),
        };
        ifd.tags.get_mut(&TAG_TILE_BYTE_COUNTS).expect("planned TileByteCounts tag").value = long_tag(&counts);
    }

    if variant == TiffVariant::Classic && cursor > u32::MAX as u64 {
        return Err(LayoutError::Overflow(format!("Classic TIFF output size {cursor} exceeds u32::MAX")));
    }

    Ok(LayoutPlan {
        variant,
        ghost,
        first_ifd_offset,
        ifds,
        blocks,
        final_size: cursor,
    })
}

fn write_output(sources: &[SourceCog], output: &Path, plan: &LayoutPlan) -> Result<()> {
    let parent = output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;

    {
        let mut writer = BufWriter::new(temporary.as_file_mut());
        let mut position = 0u64;
        write_header(&mut writer, plan.variant, plan.first_ifd_offset, &mut position)?;
        write_bytes(&mut writer, &plan.ghost, &mut position)?;
        pad_to(&mut writer, plan.first_ifd_offset, &mut position)?;

        for (ifd_index, ifd) in plan.ifds.iter().enumerate() {
            pad_to(&mut writer, ifd.offset, &mut position)?;
            write_ifd(&mut writer, ifd, plan.variant, &mut position)?;

            for (&tag_id, tag) in &ifd.tags {
                if matches!(tag_id, TAG_TILE_OFFSETS | TAG_TILE_BYTE_COUNTS) {
                    continue;
                }
                if let Some(value_offset) = tag.value_offset {
                    pad_to(&mut writer, value_offset, &mut position)?;
                    write_bytes(&mut writer, &tag.value.bytes, &mut position)?;
                }
            }

            if let Some(next_ifd) = plan.ifds.get(ifd_index + 1) {
                pad_to(&mut writer, next_ifd.offset, &mut position)?;
            }
        }

        for ifd in &plan.ifds {
            for tag_id in [TAG_TILE_OFFSETS, TAG_TILE_BYTE_COUNTS] {
                let tag = ifd.tags.get(&tag_id).expect("planned tile array tag");
                if let Some(value_offset) = tag.value_offset {
                    pad_to(&mut writer, value_offset, &mut position)?;
                    write_bytes(&mut writer, &tag.value.bytes, &mut position)?;
                }
            }
        }

        write_payloads(&mut writer, sources, plan, &mut position)?;

        if plan.blocks.is_empty() {
            pad_to(&mut writer, plan.final_size, &mut position)?;
        }
        if position != plan.final_size {
            return Err(Error::Runtime(format!(
                "COG layout mismatch: planned output size {}, wrote {position}",
                plan.final_size
            )));
        }

        writer.flush()?;
    }
    temporary.as_file().sync_all()?;
    temporary.persist(output).map_err(|error| Error::IOError(error.error))?;
    Ok(())
}

fn write_payloads(writer: &mut impl Write, sources: &[SourceCog], plan: &LayoutPlan, position: &mut u64) -> Result<()> {
    let mut source_files = VecDeque::new();
    let mut payload = Vec::new();

    for block in &plan.blocks {
        let source = source_file(&mut source_files, sources, block.source_index)?;
        copy_payload(writer, block, source, &mut payload, position)?;
    }
    Ok(())
}

fn copy_payload(writer: &mut impl Write, block: &PlannedBlock, source: &mut File, payload: &mut Vec<u8>, position: &mut u64) -> Result<()> {
    let leader = block
        .payload_offset
        .checked_sub(4)
        .ok_or_else(|| Error::Runtime("Planned tile payload offset is smaller than its leader".into()))?;
    pad_to(writer, leader, position)?;
    let size = u32::try_from(block.source.size)
        .map_err(|_| Error::Runtime(format!("Planned tile size {} exceeds u32::MAX", block.source.size)))?;
    write_bytes(writer, &size.to_le_bytes(), position)?;
    if *position != block.payload_offset {
        return Err(Error::Runtime(format!(
            "COG layout mismatch: tile payload planned at {}, writer is at {position}",
            block.payload_offset
        )));
    }
    read_source_payload(&block.source, source, payload)?;
    write_bytes(writer, payload, position)?;
    write_bytes(writer, &payload[payload.len() - 4..], position)?;
    Ok(())
}

fn read_source_payload(location: &TiffChunkLocation, source: &mut File, payload: &mut Vec<u8>) -> Result<()> {
    let size = usize::try_from(location.size).map_err(|_| Error::Runtime("Tile payload size exceeds addressable memory".into()))?;
    payload.clear();
    payload
        .try_reserve(size)
        .map_err(|error| Error::Runtime(format!("Cannot buffer tile payload: {error}")))?;
    payload.resize(size, 0);
    io::read_chunk(location, source, payload)?;
    Ok(())
}

fn write_header(writer: &mut impl Write, variant: TiffVariant, first_ifd: u64, position: &mut u64) -> Result<()> {
    write_bytes(writer, b"II", position)?;
    match variant {
        TiffVariant::Classic => {
            write_bytes(writer, &42u16.to_le_bytes(), position)?;
            let first_ifd = u32::try_from(first_ifd)
                .map_err(|_| Error::Runtime(format!("Classic TIFF first IFD offset {first_ifd} exceeds u32::MAX")))?;
            write_bytes(writer, &first_ifd.to_le_bytes(), position)?;
        }
        TiffVariant::Big => {
            write_bytes(writer, &43u16.to_le_bytes(), position)?;
            write_bytes(writer, &8u16.to_le_bytes(), position)?;
            write_bytes(writer, &0u16.to_le_bytes(), position)?;
            write_bytes(writer, &first_ifd.to_le_bytes(), position)?;
        }
    }
    Ok(())
}

fn write_ifd(writer: &mut impl Write, ifd: &PlannedIfd, variant: TiffVariant, position: &mut u64) -> Result<()> {
    match variant {
        TiffVariant::Classic => {
            let tag_count =
                u16::try_from(ifd.tags.len()).map_err(|_| Error::Runtime(format!("Classic TIFF IFD has {} tags", ifd.tags.len())))?;
            write_bytes(writer, &tag_count.to_le_bytes(), position)?;
        }
        TiffVariant::Big => write_bytes(writer, &(ifd.tags.len() as u64).to_le_bytes(), position)?,
    }

    let inline_size = variant.offset_size();
    for (&tag_id, tag) in &ifd.tags {
        write_bytes(writer, &tag_id.to_le_bytes(), position)?;
        write_bytes(writer, &tag.value.field_type.to_u16().to_le_bytes(), position)?;
        match variant {
            TiffVariant::Classic => {
                let count = u32::try_from(tag.value.count)
                    .map_err(|_| Error::Runtime(format!("TIFF tag {tag_id} count {} exceeds u32::MAX", tag.value.count)))?;
                write_bytes(writer, &count.to_le_bytes(), position)?;
            }
            TiffVariant::Big => write_bytes(writer, &tag.value.count.to_le_bytes(), position)?,
        }

        let mut slot = [0u8; 8];
        if tag.value.bytes.len() <= inline_size {
            slot[..tag.value.bytes.len()].copy_from_slice(&tag.value.bytes);
        } else {
            let offset = tag
                .value_offset
                .ok_or_else(|| Error::Runtime(format!("TIFF tag {tag_id} has no planned out-of-line value offset")))?;
            match variant {
                TiffVariant::Classic => {
                    slot[..4].copy_from_slice(
                        &u32::try_from(offset)
                            .map_err(|_| Error::Runtime(format!("Classic TIFF tag {tag_id} offset {offset} exceeds u32::MAX")))?
                            .to_le_bytes(),
                    );
                }
                TiffVariant::Big => slot.copy_from_slice(&offset.to_le_bytes()),
            }
        }
        write_bytes(writer, &slot[..inline_size], position)?;
    }

    match variant {
        TiffVariant::Classic => {
            let next = u32::try_from(ifd.next_offset)
                .map_err(|_| Error::Runtime(format!("Classic TIFF next IFD offset {} exceeds u32::MAX", ifd.next_offset)))?;
            write_bytes(writer, &next.to_le_bytes(), position)?;
        }
        TiffVariant::Big => write_bytes(writer, &ifd.next_offset.to_le_bytes(), position)?,
    }
    Ok(())
}

fn source_file<'a>(files: &'a mut VecDeque<(usize, File)>, sources: &[SourceCog], source_index: usize) -> Result<&'a mut File> {
    // A TIFF can have far more bands than the process has available file descriptors.
    const MAX_OPEN_SOURCES: usize = 32;
    let entry = if let Some(index) = files.iter().position(|(index, _)| *index == source_index) {
        files.remove(index).expect("located source file")
    } else {
        if files.len() == MAX_OPEN_SOURCES {
            files.pop_front();
        }
        (source_index, File::open(&sources[source_index].path)?)
    };
    files.push_back(entry);
    Ok(&mut files.back_mut().expect("source file was inserted").1)
}

fn write_bytes(writer: &mut impl Write, bytes: &[u8], position: &mut u64) -> Result<()> {
    writer.write_all(bytes)?;
    *position = position
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| Error::Runtime("Output position overflow".into()))?;
    Ok(())
}

fn pad_to(writer: &mut impl Write, target: u64, position: &mut u64) -> Result<()> {
    if *position > target {
        return Err(Error::Runtime(format!(
            "COG layout overlap: writer position {} exceeds planned offset {target}",
            *position
        )));
    }
    let mut remaining = target - *position;
    const ZEROES: [u8; 16] = [0; 16];
    while remaining > 0 {
        let count = usize::try_from(remaining.min(ZEROES.len() as u64)).expect("bounded by padding buffer");
        write_bytes(writer, &ZEROES[..count], position)?;
        remaining -= count as u64;
    }
    Ok(())
}

fn cog_ghost_metadata() -> std::result::Result<Vec<u8>, LayoutError> {
    let body = concat!(
        "LAYOUT=IFDS_BEFORE_DATA\n",
        "BLOCK_ORDER=ROW_MAJOR\n",
        "BLOCK_LEADER=SIZE_AS_UINT4\n",
        "BLOCK_TRAILER=LAST_4_BYTES_REPEATED\n",
        "INTERLEAVE=TILE\n",
        "KNOWN_INCOMPATIBLE_EDITION=NO\n "
    );
    if body.len() > 999_999 {
        return Err(LayoutError::Invalid(format!(
            "COG ghost metadata has {} bytes, exceeding its six-digit size field",
            body.len()
        )));
    }
    let header = format!("GDAL_STRUCTURAL_METADATA_SIZE={:06} bytes\n", body.len());
    debug_assert_eq!(header.len(), 43);
    let mut result = header.into_bytes();
    result.extend_from_slice(body.as_bytes());
    Ok(result)
}

fn validate_encoded_tag(tag: &EncodedTag) -> std::result::Result<(), LayoutError> {
    let width =
        type_width(tag.field_type).ok_or_else(|| LayoutError::Invalid(format!("Unsupported TIFF field type {:?}", tag.field_type)))?;
    let expected = tag
        .count
        .checked_mul(width as u64)
        .ok_or_else(|| LayoutError::Overflow(format!("TIFF value length overflows for count {}", tag.count)))?;
    if expected != tag.bytes.len() as u64 {
        return Err(LayoutError::Invalid(format!(
            "TIFF value type {:?} and count {} require {expected} bytes, found {}",
            tag.field_type,
            tag.count,
            tag.bytes.len()
        )));
    }
    Ok(())
}

fn type_width(field_type: Type) -> Option<usize> {
    match field_type {
        Type::BYTE | Type::ASCII | Type::SBYTE | Type::UNDEFINED => Some(1),
        Type::SHORT | Type::SSHORT => Some(2),
        Type::LONG | Type::SLONG | Type::FLOAT | Type::IFD => Some(4),
        Type::RATIONAL | Type::SRATIONAL | Type::DOUBLE | Type::LONG8 | Type::SLONG8 | Type::IFD8 => Some(8),
        _ => None,
    }
}

fn check_variant_tag(variant: TiffVariant, tag: &EncodedTag, tag_id: u16) -> std::result::Result<(), LayoutError> {
    if variant == TiffVariant::Classic && tag.count > u32::MAX as u64 {
        return Err(LayoutError::Overflow(format!(
            "Classic TIFF tag {tag_id} count {} exceeds u32::MAX",
            tag.count
        )));
    }
    if variant == TiffVariant::Classic && matches!(tag.field_type, Type::LONG8 | Type::SLONG8 | Type::IFD8) {
        return Err(LayoutError::Overflow(format!(
            "TIFF tag {tag_id} uses BigTIFF-only field type {:?}",
            tag.field_type
        )));
    }
    Ok(())
}

fn check_variant_offset(variant: TiffVariant, offset: u64, description: &str) -> std::result::Result<(), LayoutError> {
    if variant == TiffVariant::Classic && offset > u32::MAX as u64 {
        return Err(LayoutError::Overflow(format!(
            "Classic TIFF {description} offset {offset} exceeds u32::MAX"
        )));
    }
    Ok(())
}

fn checked_add(left: u64, right: u64, description: &str) -> std::result::Result<u64, LayoutError> {
    left.checked_add(right)
        .ok_or_else(|| LayoutError::Overflow(format!("{description} overflows u64")))
}

fn checked_mul(left: u64, right: u64, description: &str) -> std::result::Result<u64, LayoutError> {
    left.checked_mul(right)
        .ok_or_else(|| LayoutError::Overflow(format!("{description} overflows u64")))
}

fn layout_error(error: LayoutError) -> Error {
    match error {
        LayoutError::Overflow(message) | LayoutError::Invalid(message) => Error::InvalidArgument(message),
    }
}

fn ascii_tag(value: &str) -> EncodedTag {
    let mut bytes = value.as_bytes().to_vec();
    bytes.push(0);
    EncodedTag {
        field_type: Type::ASCII,
        count: bytes.len() as u64,
        bytes,
    }
}

fn double_tag(values: &[f64]) -> EncodedTag {
    EncodedTag {
        field_type: Type::DOUBLE,
        count: values.len() as u64,
        bytes: values.iter().flat_map(|value| value.to_le_bytes()).collect(),
    }
}

fn short_tag(values: &[u16]) -> EncodedTag {
    EncodedTag {
        field_type: Type::SHORT,
        count: values.len() as u64,
        bytes: values.iter().flat_map(|value| value.to_le_bytes()).collect(),
    }
}

fn long_tag(values: &[u32]) -> EncodedTag {
    EncodedTag {
        field_type: Type::LONG,
        count: values.len() as u64,
        bytes: values.iter().flat_map(|value| value.to_le_bytes()).collect(),
    }
}

fn long8_tag(values: &[u64]) -> EncodedTag {
    EncodedTag {
        field_type: Type::LONG8,
        count: values.len() as u64,
        bytes: values.iter().flat_map(|value| value.to_le_bytes()).collect(),
    }
}

#[cfg(test)]
mod layout_tests {
    use std::io::{Read, Seek, SeekFrom};

    use crate::{Columns, testutils};

    use super::*;

    fn source_fixture(change: impl FnOnce(&mut Vec<SourceLevel>)) -> Result<SourceCog> {
        let path = testutils::workspace_test_data_dir().join("multiband_cog_interleave_tile_google_maps_compatible.tif");
        let metadata = GeoTiffMetadata::from_file(&path)?;
        let bands = metadata.band_count as usize;
        let mut levels = read_source_levels(File::open(&path)?, metadata.overviews)?;
        for level in &mut levels {
            level.chunk_locations.truncate(level.chunk_locations.len() / bands);
            let image = &mut level.image_metadata;
            image.samples_per_pixel = 1;
            image.bits_per_sample.truncate(1);
            image.sample_format.truncate(1);
            image.gdal_metadata = None;
        }
        change(&mut levels);
        Ok(SourceCog { path, levels })
    }

    fn one_tile(levels: &mut Vec<SourceLevel>) {
        let size = levels[0].image_metadata.tile_width.expect("tiled fixture") as i32;
        levels.truncate(1);
        levels[0].raster_size = RasterSize::square(size);
        levels[0].chunk_locations.truncate(1);
    }

    fn layout(sources: &[SourceCog], variant: TiffVariant) -> Result<LayoutPlan> {
        let metadata = merge_gdal_metadata(sources)?;
        plan_layout(sources, build_output_ifds(sources, variant, &metadata)?, variant).map_err(layout_error)
    }

    #[test]
    fn bigtiff_aligns_all_directories_and_indirect_values() -> Result<()> {
        let sources = [source_fixture(|levels| {
            for overview in levels {
                overview.image_metadata.software = Some("odd-sized-value!".into());
            }
        })?];
        let plan = layout(&sources, TiffVariant::Big)?;
        for ifd in &plan.ifds {
            assert_eq!(ifd.offset % 8, 0);
            for tag in ifd.tags.values() {
                if let Some(offset) = tag.value_offset {
                    assert_eq!(offset % 8, 0);
                }
            }
        }
        let directory = tempfile::tempdir()?;
        let output = directory.path().join("big.tif");
        write_output(&sources, &output, &plan)?;
        let metadata = GeoTiffMetadata::from_file(&output)?;
        assert_eq!(metadata.overviews.len(), sources[0].levels().len());
        for (actual, expected) in metadata.overviews.iter().zip(sources[0].levels()) {
            assert_eq!(actual.raster_size, expected.raster_size);
            let mut input = File::open(&sources[0].path)?;
            let mut output_file = File::open(&output)?;
            for (actual, expected) in actual.chunk_locations.iter().zip(&expected.chunk_locations) {
                assert_eq!(actual.size, expected.size);
                if !actual.is_sparse() {
                    let mut a = vec![0; actual.size as usize];
                    let mut b = vec![0; expected.size as usize];
                    io::read_chunk(actual, &mut output_file, &mut a)?;
                    io::read_chunk(expected, &mut input, &mut b)?;
                    assert_eq!(a, b);
                }
            }
        }
        #[cfg(feature = "gdal")]
        {
            let input = crate::raster::formats::gdal::open_dataset_read_only(&sources[0].path)?;
            let output = crate::raster::formats::gdal::open_dataset_read_only(&output)?;
            let input_band = input.rasterband(1)?;
            let output_band = output.rasterband(1)?;
            for level in 0..sources[0].levels().len() {
                let expected = if level == 0 {
                    input.rasterband(1)?
                } else {
                    input_band.overview(level - 1)?
                };
                let actual = if level == 0 {
                    output.rasterband(1)?
                } else {
                    output_band.overview(level - 1)?
                };
                let expected = expected.read_band_as::<f64>()?;
                let actual = actual.read_band_as::<f64>()?;
                assert_eq!(actual.shape(), expected.shape());
                assert!(actual.data().iter().zip(expected.data()).all(|(a, b)| a.to_bits() == b.to_bits()));
            }
        }
        Ok(())
    }

    #[test]
    fn all_sparse_single_tile_outputs_include_final_padding() -> Result<()> {
        for variant in [TiffVariant::Classic, TiffVariant::Big] {
            for nodata in ["-1", "-12"] {
                let sources = [source_fixture(|levels| {
                    one_tile(levels);
                    levels[0].chunk_locations.fill(TiffChunkLocation { offset: 0, size: 0 });
                    levels[0].image_metadata.nodata = Some(nodata.into());
                })?];
                let plan = layout(&sources, variant)?;
                assert!(plan.blocks.is_empty());
                let directory = tempfile::tempdir()?;
                let output = directory.path().join("sparse.tif");
                write_output(&sources, &output, &plan)?;
                assert_eq!(std::fs::metadata(&output)?.len(), plan.final_size);
                let metadata = GeoTiffMetadata::from_file(&output)?;
                assert!(metadata.overviews[0].chunk_locations[0].is_sparse());
                let assembled = directory.path().join("assembled-sparse.tif");
                let mut fractions = Vec::new();
                assemble_band_cogs(std::slice::from_ref(&output), &assembled, &mut |fraction| {
                    if fraction == 1.0 {
                        assert!(GeoTiffMetadata::from_file(&assembled).is_ok());
                    }
                    fractions.push(fraction);
                })?;
                assert_eq!(fractions, [0.0, 1.0]);
                assert_eq!(
                    read_source_cog(0, &output)?.levels()[0].image_metadata.nodata.as_deref(),
                    Some(nodata)
                );
            }
        }
        Ok(())
    }

    #[test]
    fn classic_overflow_promotes_to_64_bit_tile_offsets_without_writing_large_payloads() -> Result<()> {
        let sources = [source_fixture(|levels| {
            one_tile(levels);
            let width = levels[0].image_metadata.tile_width.expect("tiled fixture") as i32 * 2;
            let overview = &mut levels[0];
            overview.raster_size.cols = Columns(width);
            overview.chunk_locations = vec![
                TiffChunkLocation {
                    offset: 8,
                    size: u64::from(u32::MAX),
                },
                TiffChunkLocation { offset: 8, size: 4 },
            ];
        })?];
        let metadata = merge_gdal_metadata(&sources)?;
        let classic = build_output_ifds(&sources, TiffVariant::Classic, &metadata)?;
        assert!(matches!(
            plan_layout(&sources, classic, TiffVariant::Classic),
            Err(LayoutError::Overflow(_))
        ));
        let plan = layout(&sources, TiffVariant::Big)?;
        assert!(plan.final_size > u64::from(u32::MAX));
        assert!(plan.blocks[1].payload_offset > u64::from(u32::MAX));
        assert_eq!(plan.ifds[0].tags[&TAG_TILE_OFFSETS].value.field_type, Type::LONG8);
        Ok(())
    }

    #[test]
    fn leaders_and_trailers_frame_exact_native_payloads() -> Result<()> {
        for bytes in [b"abcd".as_slice(), b"abcde".as_slice()] {
            let directory = tempfile::tempdir()?;
            let input = directory.path().join("payload.bin");
            let mut contents = vec![0; 8];
            contents.extend_from_slice(bytes);
            std::fs::write(&input, contents)?;
            let mut source = source_fixture(|levels| {
                one_tile(levels);
                levels[0].chunk_locations[0] = TiffChunkLocation {
                    offset: 8,
                    size: bytes.len() as u64,
                };
            })?;
            source.path = input;
            let sources = [source];
            let plan = layout(&sources, TiffVariant::Classic)?;
            let output = directory.path().join("framed.tif");
            write_output(&sources, &output, &plan)?;
            let mut file = File::open(output)?;
            file.seek(SeekFrom::Start(plan.blocks[0].payload_offset - 4))?;
            let mut framed = vec![0; bytes.len() + 8];
            file.read_exact(&mut framed)?;
            assert_eq!(&framed[..4], &(bytes.len() as u32).to_le_bytes());
            assert_eq!(&framed[4..4 + bytes.len()], bytes);
            assert_eq!(&framed[4 + bytes.len()..], &bytes[bytes.len() - 4..]);
        }
        Ok(())
    }

    #[test]
    fn truncated_payload_does_not_replace_destination_or_leave_temporary_files() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let input = directory.path().join("truncated.bin");
        std::fs::write(&input, [0; 8])?;
        let mut source = source_fixture(|levels| {
            one_tile(levels);
            levels[0].chunk_locations[0] = TiffChunkLocation { offset: 8, size: 4 };
        })?;
        source.path = input;
        let sources = [source];
        let plan = layout(&sources, TiffVariant::Classic)?;
        let output = directory.path().join("existing.tif");
        std::fs::write(&output, b"existing destination")?;
        assert!(write_output(&sources, &output, &plan).is_err());
        assert_eq!(std::fs::read(&output)?, b"existing destination");
        assert_eq!(std::fs::read_dir(directory.path())?.count(), 2);
        Ok(())
    }

    #[test]
    fn assembly_progress_does_not_complete_when_persist_fails() -> Result<()> {
        let sources = [source_fixture(one_tile)?];
        let directory = tempfile::tempdir()?;
        let input = directory.path().join("input.tif");
        write_output(&sources, &input, &layout(&sources, TiffVariant::Classic)?)?;
        let output = directory.path().join("destination-directory");
        std::fs::create_dir(&output)?;
        let mut fractions = Vec::new();
        assert!(assemble_band_cogs(&[input], &output, &mut |fraction| fractions.push(fraction)).is_err());
        assert_eq!(fractions, [0.0]);
        assert!(output.is_dir());
        assert_eq!(std::fs::read_dir(directory.path())?.count(), 2);
        Ok(())
    }

    #[test]
    fn writes_all_tile_payloads_without_progress_callbacks() -> Result<()> {
        let source = source_fixture(one_tile)?;
        let sources = (0..257)
            .map(|_| SourceCog {
                path: source.path.clone(),
                levels: source.levels.clone(),
            })
            .collect::<Vec<_>>();
        let directory = tempfile::tempdir()?;
        let input = directory.path().join("input.tif");
        let plan = layout(&sources, TiffVariant::Classic)?;
        assert_eq!(plan.blocks.len(), 257);
        write_output(&sources, &input, &plan)?;
        assert_eq!(std::fs::metadata(&input)?.len(), plan.final_size);
        Ok(())
    }

    #[test]
    fn assembly_preserves_native_non_xyz_resolution_and_counted_ascii() -> Result<()> {
        let sources = [source_fixture(|levels| {
            let image = &mut levels[0].image_metadata;
            image.model_pixel_scale = Some([30.0, 30.0, 0.0]);
            image.image_description = Some("first\0second".into());
            image.software = Some("é".into());
        })?];
        let directory = tempfile::tempdir()?;
        let input = directory.path().join("non-xyz.tif");
        write_output(&sources, &input, &layout(&sources, TiffVariant::Classic)?)?;
        let output = directory.path().join("assembled.tif");
        assemble_band_cogs(&[input], &output, &mut |_| {})?;
        let metadata = GeoTiffMetadata::from_file(&output)?;
        assert_eq!(metadata.geo_reference.cell_size_x(), 30.0);
        assert_eq!(metadata.overviews.len(), sources[0].levels().len());
        assert_eq!(
            read_source_cog(0, &output)?.levels()[0].image_metadata.image_description.as_deref(),
            Some("first\0second")
        );
        assert_eq!(
            read_source_cog(0, &output)?.levels()[0].image_metadata.software.as_deref(),
            Some("é")
        );
        Ok(())
    }

    #[test]
    fn metadata_compatibility_checks_text_after_embedded_nuls() -> Result<()> {
        let sources = [
            source_fixture(|levels| levels[0].image_metadata.image_description = Some("first\0a".into()))?,
            source_fixture(|levels| levels[0].image_metadata.image_description = Some("first\0b".into()))?,
        ];
        assert!(validate_compatibility(&sources).is_err());
        Ok(())
    }

    #[test]
    fn source_file_cache_is_bounded_and_reopens_evicted_sources() -> Result<()> {
        let source = source_fixture(one_tile)?;
        let sources = (0..128)
            .map(|_| SourceCog {
                path: source.path.clone(),
                levels: source.levels.clone(),
            })
            .collect::<Vec<_>>();
        let mut cache = VecDeque::new();
        for index in (0..sources.len()).chain([0, 127, 0]) {
            source_file(&mut cache, &sources, index)?;
            assert!(cache.len() <= 32);
            assert_eq!(cache.back().map(|entry| entry.0), Some(index));
        }
        Ok(())
    }
}

#[cfg(all(test, feature = "gdal"))]
mod tests {
    use std::fs::File;

    use crate::{
        BandIndex, GeoReference,
        cog::{CogCreationOptions, create_temporary_band_cogs},
        geotiff::{GeoTiffReader, metadata::Interleave},
        raster, testutils,
    };

    use super::*;

    #[test]
    fn assembly_progress_is_monotonic_and_completes_after_publishing() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("multiband_cog.tif");
        let (staging, bands) = create_temporary_band_cogs(&input.to_string_lossy(), CogCreationOptions::default(), None, None)?;
        let output = staging.path().join("with-progress.tif");
        let mut events = Vec::new();
        assemble_band_cogs(&bands, &output, &mut |fraction| {
            if fraction == 1.0 {
                let metadata = GeoTiffMetadata::from_file(&output).expect("Output must be readable at completion");
                assert_eq!(metadata.band_count as usize, bands.len());
            } else {
                assert!(!output.exists());
            }
            events.push(fraction);
        })?;
        assert_eq!(events.first(), Some(&0.0));
        assert_eq!(events.last(), Some(&1.0));
        assert!(events.iter().all(|fraction| fraction.is_finite() && (0.0..=1.0).contains(fraction)));
        assert!(events.windows(2).all(|pair| pair[0] <= pair[1]));
        assert_eq!(events.iter().filter(|&&fraction| fraction == 1.0).count(), 1);
        Ok(())
    }

    #[test_log::test]
    fn source_levels_match_cog_metadata() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("multiband_cog.tif");
        let options = CogCreationOptions {
            tile_size: 128,
            ..Default::default()
        };
        let (_staging, band_cogs) = create_temporary_band_cogs(&input.to_string_lossy(), options, None, None)?;
        let metadata = GeoTiffMetadata::from_file(&band_cogs[0])?;
        let source = read_source_cog(0, &band_cogs[0])?;

        assert!(
            metadata.overviews.len() > 1,
            "Expected a base image and reduced-resolution overviews"
        );
        assert_eq!(source.levels().len(), metadata.overviews.len());
        for (level, overview) in source.levels().iter().zip(&metadata.overviews) {
            assert_eq!(level.raster_size.cols, overview.raster_size.cols);
            assert_eq!(level.raster_size.rows, overview.raster_size.rows);
            assert_eq!(level.chunk_locations, overview.chunk_locations);
        }
        Ok(())
    }

    #[test_log::test]
    fn assembles_staged_band_cogs_without_changing_pixels() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("multiband_cog.tif");
        let (staging, band_cogs) = create_temporary_band_cogs(&input.to_string_lossy(), CogCreationOptions::default(), None, None)?;
        let output = staging.path().join("assembled.cog.tif");

        assemble_band_cogs(&band_cogs, &output, &mut |_| {})?;

        let mut assembled = GeoTiffReader::from_file(&output)?;
        let assembled_metadata = assembled.metadata().clone();
        assert_eq!(assembled_metadata.band_count as usize, band_cogs.len());
        assert_eq!(assembled_metadata.interleave, Interleave::Tile);
        assert!(assembled_metadata.gdal_ghost_data.as_ref().is_some_and(GdalGhostData::is_cog));
        assert_eq!(
            raster::formats::gdal::open_dataset_read_only(&output)?.raster_count(),
            band_cogs.len()
        );

        let source_metadata = band_cogs
            .iter()
            .map(|path| super::super::GeoTiffMetadata::from_file(path))
            .collect::<Result<Vec<_>>>()?;
        let mut output_file = File::open(&output)?;
        for (band_index, band_path) in band_cogs.iter().enumerate() {
            let mut source_file = File::open(band_path)?;
            for (overview_index, source_overview) in source_metadata[band_index].overviews.iter().enumerate() {
                let output_overview = &assembled_metadata.overviews[overview_index];
                let tile_count = source_overview.chunk_locations.len();
                for (tile_index, source_location) in source_overview.chunk_locations.iter().enumerate() {
                    let output_location = output_overview.chunk_locations[band_index * tile_count + tile_index];
                    assert_eq!(
                        read_payload(&mut output_file, output_location)?,
                        read_payload(&mut source_file, *source_location)?
                    );
                }
            }
        }

        let band_count = band_cogs.len();
        let physical_offsets = (0..assembled_metadata.overviews.len())
            .rev()
            .flat_map(|overview_index| {
                let overview = &assembled_metadata.overviews[overview_index];
                let tiles_per_band = overview.chunk_locations.len() / band_count;
                (0..tiles_per_band).flat_map(move |tile_index| {
                    (0..band_count).filter_map(move |band_index| {
                        let location = overview.chunk_locations[band_index * tiles_per_band + tile_index];
                        (!location.is_sparse()).then_some(location.offset)
                    })
                })
            })
            .collect::<Vec<_>>();
        assert!(physical_offsets.windows(2).all(|offsets| offsets[0] < offsets[1]));

        for (band_index, band_path) in band_cogs.iter().enumerate() {
            let mut source = GeoTiffReader::from_file(band_path)?;
            assert_eq!(source.metadata().overviews.len(), assembled_metadata.overviews.len());
            for overview_index in 0..source.metadata().overviews.len() {
                let expected = source.read_overview_band_as::<u8, GeoReference>(overview_index, super::super::FIRST_BAND)?;
                let actual = assembled.read_overview_band_as::<u8, GeoReference>(
                    overview_index,
                    BandIndex::new(band_index + 1).expect("one-based band index"),
                )?;
                assert_eq!(actual, expected);
            }
        }

        Ok(())
    }

    fn read_payload(file: &mut File, location: TiffChunkLocation) -> Result<Vec<u8>> {
        if location.is_sparse() {
            return Ok(Vec::new());
        }

        let mut payload = vec![0; location.size as usize];
        io::read_chunk(&location, file, &mut payload)?;
        Ok(payload)
    }
}

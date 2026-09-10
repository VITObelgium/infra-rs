//! Raw assembly of compatible single-band COGs into a planar-separate multiband COG.
//!
//! The assembler deliberately does not decode or recompress imagery. It rebuilds the TIFF
//! directories and copies each compressed tile payload byte-for-byte.

use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use tiff::{
    decoder::Decoder,
    tags::{ByteOrder, Type, ValueBuffer},
};
use xml::reader::{EventReader, XmlEvent};

use crate::{Error, Result};

use super::{TiffChunkLocation, gdalghostdata::GdalGhostData, io::COG_HEADER_SIZE};

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
const TAG_ROWS_PER_STRIP: u16 = 278;
const TAG_STRIP_BYTE_COUNTS: u16 = 279;
const TAG_X_RESOLUTION: u16 = 282;
const TAG_Y_RESOLUTION: u16 = 283;
const TAG_PLANAR_CONFIGURATION: u16 = 284;
const TAG_FREE_OFFSETS: u16 = 288;
const TAG_FREE_BYTE_COUNTS: u16 = 289;
const TAG_RESOLUTION_UNIT: u16 = 296;
const TAG_SOFTWARE: u16 = 305;
const TAG_ARTIST: u16 = 315;
const TAG_HOST_COMPUTER: u16 = 316;
const TAG_PREDICTOR: u16 = 317;
const TAG_TILE_WIDTH: u16 = 322;
const TAG_TILE_LENGTH: u16 = 323;
const TAG_TILE_OFFSETS: u16 = 324;
const TAG_TILE_BYTE_COUNTS: u16 = 325;
const TAG_SUB_IFD: u16 = 330;
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
const TAG_EXIF_DIRECTORY: u16 = 0x8769;
const TAG_GPS_DIRECTORY: u16 = 0x8825;
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
struct RawTag {
    field_type: Type,
    count: u64,
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct SourceLevel {
    width: u32,
    height: u32,
    tile_width: u32,
    tile_length: u32,
    bits_per_sample: u16,
    compression: u16,
    photometric: u16,
    predictor: u16,
    sample_format: u16,
    tile_locations: Vec<TiffChunkLocation>,
    tags: BTreeMap<u16, RawTag>,
}

#[derive(Debug)]
struct SourceCog {
    path: PathBuf,
    levels: Vec<SourceLevel>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TiffVariant {
    Classic,
    Big,
}

impl TiffVariant {
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
    tags: BTreeMap<u16, RawTag>,
}

#[derive(Debug)]
struct PlannedTag {
    value: RawTag,
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct XmlItem {
    attributes: Vec<(String, String)>,
    text: String,
}

/// Merge compatible staged single-band COGs into one planar-separate,
/// tile-interleaved multiband COG without decoding or recompressing tiles.
///
/// Inputs must be little-endian, tiled, single-band COGs with matching image and
/// overview layouts, data types, compression settings, georeferencing, and
/// dataset-level metadata. Unsupported pointer-bearing and image-layout tags are
/// rejected rather than copied unsafely.
pub fn assemble_band_cogs(inputs: &[PathBuf], output: &Path) -> Result<()> {
    if inputs.is_empty() {
        return Err(Error::InvalidArgument("At least one input band COG is required".into()));
    }
    if inputs.len() > u16::MAX as usize {
        return Err(Error::InvalidArgument(format!(
            "Band count {} exceeds the TIFF SamplesPerPixel limit",
            inputs.len()
        )));
    }

    for input in inputs {
        if input == output {
            return Err(Error::InvalidArgument(format!(
                "Output path must not equal an input path: {}",
                input.display()
            )));
        }
    }

    let sources = inputs
        .iter()
        .enumerate()
        .map(|(index, path)| read_source_cog(index, path))
        .collect::<Result<Vec<_>>>()?;

    validate_compatibility(&sources)?;
    let merged_gdal_metadata = merge_gdal_metadata(&sources)?;

    let classic_ifds = build_output_ifds(&sources, TiffVariant::Classic, &merged_gdal_metadata)?;
    let plan = match plan_layout(&sources, classic_ifds, TiffVariant::Classic) {
        Ok(plan) => plan,
        Err(LayoutError::Overflow(_)) => {
            let big_ifds = build_output_ifds(&sources, TiffVariant::Big, &merged_gdal_metadata)?;
            plan_layout(&sources, big_ifds, TiffVariant::Big).map_err(layout_error)?
        }
        Err(error) => return Err(layout_error(error)),
    };

    write_output(&sources, output, &plan)
}

fn read_source_cog(input_index: usize, path: &Path) -> Result<SourceCog> {
    let input_name = || format!("Input band {} ({})", input_index + 1, path.display());
    let file_size = std::fs::metadata(path)
        .map_err(|error| Error::Runtime(format!("{} cannot be inspected: {error}", input_name())))?
        .len();

    let mut header_file = File::open(path)?;
    let mut header = Vec::with_capacity(COG_HEADER_SIZE);
    Read::by_ref(&mut header_file)
        .take(COG_HEADER_SIZE as u64)
        .read_to_end(&mut header)?;
    if header.len() < 59 {
        return Err(Error::InvalidArgument(format!(
            "{} is too small to contain TIFF and COG headers",
            input_name()
        )));
    }

    let ghost = GdalGhostData::from_tiff_header_buffer(&header)
        .ok_or_else(|| Error::InvalidArgument(format!("{} has no valid GDAL COG structural metadata", input_name())))?;
    if !ghost.is_cog() {
        return Err(Error::InvalidArgument(format!(
            "{} is not a compatible COG or is marked as modified",
            input_name()
        )));
    }

    let file = File::open(path)?;
    let mut decoder = Decoder::new(file)?;
    if decoder.byte_order() != ByteOrder::LittleEndian {
        return Err(Error::InvalidArgument(format!(
            "{} is big-endian; only little-endian source COGs are supported",
            input_name()
        )));
    }

    let mut levels = Vec::new();
    loop {
        let tags = read_current_ifd_tags(&mut decoder)?;
        let level_index = levels.len();
        levels.push(parse_source_level(input_index, path, level_index, file_size, tags)?);

        if !decoder.more_images() {
            break;
        }
        decoder.next_image()?;
    }

    if levels.is_empty() {
        return Err(Error::InvalidArgument(format!("{} contains no image directories", input_name())));
    }

    Ok(SourceCog {
        path: path.to_path_buf(),
        levels,
    })
}

fn read_current_ifd_tags(decoder: &mut Decoder<File>) -> Result<BTreeMap<u16, RawTag>> {
    let entries = {
        let ifd = decoder.image_ifd();
        ifd.directory()
            .iter()
            .map(|(tag, entry)| (tag, entry.field_type()))
            .collect::<Vec<_>>()
    };

    let mut tags = BTreeMap::new();
    for (tag, field_type) in entries {
        let mut value = ValueBuffer::empty(field_type);
        let found = decoder.image_ifd().find_tag_buf(tag, &mut value)?;
        if found.is_none() {
            return Err(Error::Runtime(format!(
                "TIFF tag {} disappeared while reading its IFD",
                tag.to_u16()
            )));
        }
        value.set_byte_order(ByteOrder::LittleEndian);
        let raw_tag = RawTag {
            field_type: value.data_type(),
            count: value.count(),
            bytes: value.as_bytes().to_vec(),
        };
        validate_raw_tag(&raw_tag).map_err(layout_error)?;
        tags.insert(tag.to_u16(), raw_tag);
    }
    Ok(tags)
}

fn parse_source_level(
    input_index: usize,
    path: &Path,
    level_index: usize,
    file_size: u64,
    tags: BTreeMap<u16, RawTag>,
) -> Result<SourceLevel> {
    let context = || format!("Input band {} ({}), IFD {level_index}", input_index + 1, path.display());

    validate_supported_tags(&tags, &context())?;
    validate_copied_tags(&tags, &context())?;
    if level_index > 0 && tags.contains_key(&TAG_GDAL_METADATA) {
        return Err(Error::InvalidArgument(format!(
            "{} contains unsupported overview-level GDAL metadata",
            context()
        )));
    }

    let samples_per_pixel = required_scalar_u16(&tags, TAG_SAMPLES_PER_PIXEL, &context())?;
    if samples_per_pixel != 1 {
        return Err(Error::InvalidArgument(format!(
            "{} has SamplesPerPixel={samples_per_pixel}; expected one",
            context()
        )));
    }

    let new_subfile_type = optional_scalar_u32(&tags, TAG_NEW_SUBFILE_TYPE, &context())?.unwrap_or(0);
    let expected_subfile_type = if level_index == 0 { 0 } else { REDUCED_IMAGE };
    if new_subfile_type != expected_subfile_type {
        return Err(Error::InvalidArgument(format!(
            "{} has NewSubfileType={new_subfile_type}; expected {expected_subfile_type}",
            context()
        )));
    }

    let width = required_scalar_u32(&tags, TAG_IMAGE_WIDTH, &context())?;
    let height = required_scalar_u32(&tags, TAG_IMAGE_LENGTH, &context())?;
    let tile_width = required_scalar_u32(&tags, TAG_TILE_WIDTH, &context())?;
    let tile_length = required_scalar_u32(&tags, TAG_TILE_LENGTH, &context())?;
    if width == 0 || height == 0 || tile_width == 0 || tile_length == 0 {
        return Err(Error::InvalidArgument(format!(
            "{} contains a zero image or tile dimension",
            context()
        )));
    }
    if tile_width != tile_length {
        return Err(Error::InvalidArgument(format!(
            "{} uses non-square tiles ({tile_width}x{tile_length}), which are not supported",
            context()
        )));
    }

    let bits_per_sample = required_scalar_u16(&tags, TAG_BITS_PER_SAMPLE, &context())?;
    let compression = required_scalar_u16(&tags, TAG_COMPRESSION, &context())?;
    let photometric = required_scalar_u16(&tags, TAG_PHOTOMETRIC_INTERPRETATION, &context())?;
    let predictor = optional_scalar_u16(&tags, TAG_PREDICTOR, &context())?.unwrap_or(1);
    let sample_format = required_scalar_u16(&tags, TAG_SAMPLE_FORMAT, &context())?;
    let planar_configuration = optional_scalar_u16(&tags, TAG_PLANAR_CONFIGURATION, &context())?.unwrap_or(1);

    if !matches!(
        compression,
        COMPRESSION_NONE | COMPRESSION_LZW | COMPRESSION_DEFLATE | COMPRESSION_LERC | COMPRESSION_ZSTD
    ) {
        return Err(Error::InvalidArgument(format!(
            "{} uses unsupported compression {compression}",
            context()
        )));
    }
    if photometric != PHOTOMETRIC_BLACK_IS_ZERO {
        return Err(Error::InvalidArgument(format!(
            "{} uses PhotometricInterpretation={photometric}; only BlackIsZero single-band inputs are supported",
            context()
        )));
    }
    if !matches!(planar_configuration, 1 | PLANAR_SEPARATE) {
        return Err(Error::InvalidArgument(format!(
            "{} has invalid PlanarConfiguration={planar_configuration}",
            context()
        )));
    }
    if !matches!(predictor, 1..=3) {
        return Err(Error::InvalidArgument(format!(
            "{} uses unsupported Predictor={predictor}",
            context()
        )));
    }
    if !matches!((sample_format, bits_per_sample), (1 | 2, 8 | 16 | 32 | 64) | (3, 32 | 64)) {
        return Err(Error::InvalidArgument(format!(
            "{} uses unsupported SampleFormat/BitsPerSample combination {sample_format}/{bits_per_sample}",
            context()
        )));
    }
    if compression == COMPRESSION_LERC && !tags.contains_key(&TAG_LERC_PARAMETERS) {
        return Err(Error::InvalidArgument(format!(
            "{} uses LERC without a LercParameters tag",
            context()
        )));
    }
    if compression != COMPRESSION_LERC && tags.contains_key(&TAG_LERC_PARAMETERS) {
        return Err(Error::InvalidArgument(format!(
            "{} has a LercParameters tag without LERC compression",
            context()
        )));
    }

    let offsets = required_unsigned_vec(&tags, TAG_TILE_OFFSETS, &context())?;
    let byte_counts = required_unsigned_vec(&tags, TAG_TILE_BYTE_COUNTS, &context())?;
    if offsets.len() != byte_counts.len() {
        return Err(Error::InvalidArgument(format!(
            "{} has {} tile offsets but {} tile byte counts",
            context(),
            offsets.len(),
            byte_counts.len()
        )));
    }

    let tiles_across = u64::from(width).div_ceil(u64::from(tile_width));
    let tiles_down = u64::from(height).div_ceil(u64::from(tile_length));
    let expected_tile_count = tiles_across
        .checked_mul(tiles_down)
        .ok_or_else(|| Error::InvalidArgument(format!("{} tile count overflows u64", context())))?;
    if offsets.len() as u64 != expected_tile_count {
        return Err(Error::InvalidArgument(format!(
            "{} has {} tiles; dimensions require {expected_tile_count}",
            context(),
            offsets.len()
        )));
    }

    let mut tile_locations = Vec::with_capacity(offsets.len());
    for (tile_index, (&offset, &size)) in offsets.iter().zip(&byte_counts).enumerate() {
        if (offset == 0) != (size == 0) {
            return Err(Error::InvalidArgument(format!(
                "{} tile {tile_index} has inconsistent sparse offset/count ({offset}/{size})",
                context()
            )));
        }
        if size > u32::MAX as u64 {
            return Err(Error::InvalidArgument(format!(
                "{} tile {tile_index} has {size} payload bytes, exceeding the COG uint32 leader limit",
                context()
            )));
        }
        if size != 0 && size < 4 {
            return Err(Error::InvalidArgument(format!(
                "{} tile {tile_index} is only {size} bytes; a four-byte COG trailer cannot be generated",
                context()
            )));
        }
        if size != 0 {
            let end = offset
                .checked_add(size)
                .ok_or_else(|| Error::InvalidArgument(format!("{} tile {tile_index} range overflows u64", context())))?;
            if end > file_size {
                return Err(Error::InvalidArgument(format!(
                    "{} tile {tile_index} range {offset}..{end} exceeds file size {file_size}",
                    context()
                )));
            }
        }
        tile_locations.push(TiffChunkLocation { offset, size });
    }

    Ok(SourceLevel {
        width,
        height,
        tile_width,
        tile_length,
        bits_per_sample,
        compression,
        photometric,
        predictor,
        sample_format,
        tile_locations,
        tags,
    })
}

fn validate_supported_tags(tags: &BTreeMap<u16, RawTag>, context: &str) -> Result<()> {
    for &tag in tags.keys() {
        if !is_supported_input_tag(tag) {
            let reason = if matches!(
                tag,
                TAG_ROWS_PER_STRIP
                    | TAG_STRIP_BYTE_COUNTS
                    | TAG_FREE_OFFSETS
                    | TAG_FREE_BYTE_COUNTS
                    | TAG_SUB_IFD
                    | TAG_EXIF_DIRECTORY
                    | TAG_GPS_DIRECTORY
            ) {
                "pointer-bearing or non-tiled layout tag"
            } else {
                "tag is not on the assembler preservation allowlist"
            };
            return Err(Error::InvalidArgument(format!(
                "{context} contains unsupported TIFF tag {tag}: {reason}"
            )));
        }
    }
    Ok(())
}

fn validate_copied_tags(tags: &BTreeMap<u16, RawTag>, context: &str) -> Result<()> {
    for (&tag_id, tag) in tags {
        let valid = match tag_id {
            TAG_FILL_ORDER | TAG_ORIENTATION | TAG_RESOLUTION_UNIT => tag.field_type == Type::SHORT && tag.count == 1,
            TAG_IMAGE_DESCRIPTION
            | TAG_SOFTWARE
            | TAG_ARTIST
            | TAG_HOST_COMPUTER
            | TAG_COPYRIGHT
            | TAG_GEO_ASCII_PARAMS
            | TAG_GDAL_METADATA
            | TAG_GDAL_NODATA => tag.field_type == Type::ASCII && tag.count > 0,
            TAG_X_RESOLUTION | TAG_Y_RESOLUTION => tag.field_type == Type::RATIONAL && tag.count == 1,
            TAG_MODEL_PIXEL_SCALE => tag.field_type == Type::DOUBLE && tag.count == 3,
            TAG_MODEL_TIEPOINT => tag.field_type == Type::DOUBLE && tag.count >= 6 && tag.count.is_multiple_of(6),
            TAG_MODEL_TRANSFORMATION => tag.field_type == Type::DOUBLE && tag.count == 16,
            TAG_ICC_PROFILE => tag.field_type == Type::UNDEFINED && tag.count > 0,
            TAG_GEO_KEY_DIRECTORY => tag.field_type == Type::SHORT && tag.count >= 4 && (tag.count - 4).is_multiple_of(4),
            TAG_GEO_DOUBLE_PARAMS => tag.field_type == Type::DOUBLE,
            TAG_LERC_PARAMETERS => tag.field_type == Type::LONG && tag.count == 2,
            _ => true,
        };

        if !valid {
            return Err(Error::InvalidArgument(format!(
                "{context} TIFF tag {tag_id} has unsupported type/count {:?}[{}]",
                tag.field_type, tag.count
            )));
        }
        if matches!(tag.field_type, Type::IFD | Type::IFD8) {
            return Err(Error::InvalidArgument(format!(
                "{context} TIFF tag {tag_id} contains an unsupported directory pointer"
            )));
        }
    }

    Ok(())
}

fn is_supported_input_tag(tag: u16) -> bool {
    matches!(
        tag,
        TAG_NEW_SUBFILE_TYPE
            | TAG_IMAGE_WIDTH
            | TAG_IMAGE_LENGTH
            | TAG_BITS_PER_SAMPLE
            | TAG_COMPRESSION
            | TAG_PHOTOMETRIC_INTERPRETATION
            | TAG_FILL_ORDER
            | TAG_IMAGE_DESCRIPTION
            | TAG_ORIENTATION
            | TAG_SAMPLES_PER_PIXEL
            | TAG_X_RESOLUTION
            | TAG_Y_RESOLUTION
            | TAG_PLANAR_CONFIGURATION
            | TAG_RESOLUTION_UNIT
            | TAG_SOFTWARE
            | TAG_ARTIST
            | TAG_HOST_COMPUTER
            | TAG_PREDICTOR
            | TAG_TILE_WIDTH
            | TAG_TILE_LENGTH
            | TAG_TILE_OFFSETS
            | TAG_TILE_BYTE_COUNTS
            | TAG_SAMPLE_FORMAT
            | TAG_COPYRIGHT
            | TAG_MODEL_PIXEL_SCALE
            | TAG_MODEL_TIEPOINT
            | TAG_MODEL_TRANSFORMATION
            | TAG_ICC_PROFILE
            | TAG_GEO_KEY_DIRECTORY
            | TAG_GEO_DOUBLE_PARAMS
            | TAG_GEO_ASCII_PARAMS
            | TAG_GDAL_METADATA
            | TAG_GDAL_NODATA
            | TAG_LERC_PARAMETERS
    )
}

fn validate_compatibility(sources: &[SourceCog]) -> Result<()> {
    let reference = &sources[0];
    for (band_index, source) in sources.iter().enumerate().skip(1) {
        if source.levels.len() != reference.levels.len() {
            return Err(Error::InvalidArgument(format!(
                "Input band {} ({}) has {} IFD levels; input band 1 ({}) has {}",
                band_index + 1,
                source.path.display(),
                source.levels.len(),
                reference.path.display(),
                reference.levels.len()
            )));
        }
    }

    for level_index in 0..reference.levels.len() {
        let reference_level = &reference.levels[level_index];
        if level_index > 0 {
            let previous = &reference.levels[level_index - 1];
            if reference_level.width > previous.width
                || reference_level.height > previous.height
                || (reference_level.width == previous.width && reference_level.height == previous.height)
            {
                return Err(Error::InvalidArgument(format!(
                    "Input band 1 ({}) IFD {level_index} is not a reduced-resolution overview",
                    reference.path.display()
                )));
            }
        }

        for (band_index, source) in sources.iter().enumerate().skip(1) {
            let level = &source.levels[level_index];
            let mismatch = if level.width != reference_level.width || level.height != reference_level.height {
                Some(format!(
                    "dimensions {}x{} differ from {}x{}",
                    level.width, level.height, reference_level.width, reference_level.height
                ))
            } else if level.tile_width != reference_level.tile_width || level.tile_length != reference_level.tile_length {
                Some(format!(
                    "tile dimensions {}x{} differ from {}x{}",
                    level.tile_width, level.tile_length, reference_level.tile_width, reference_level.tile_length
                ))
            } else if level.tile_locations.len() != reference_level.tile_locations.len() {
                Some(format!(
                    "tile count {} differs from {}",
                    level.tile_locations.len(),
                    reference_level.tile_locations.len()
                ))
            } else if level.bits_per_sample != reference_level.bits_per_sample {
                Some(format!(
                    "BitsPerSample {} differs from {}",
                    level.bits_per_sample, reference_level.bits_per_sample
                ))
            } else if level.sample_format != reference_level.sample_format {
                Some(format!(
                    "SampleFormat {} differs from {}",
                    level.sample_format, reference_level.sample_format
                ))
            } else if level.compression != reference_level.compression {
                Some(format!(
                    "Compression {} differs from {}",
                    level.compression, reference_level.compression
                ))
            } else if level.predictor != reference_level.predictor {
                Some(format!("Predictor {} differs from {}", level.predictor, reference_level.predictor))
            } else if level.photometric != reference_level.photometric {
                Some(format!(
                    "PhotometricInterpretation {} differs from {}",
                    level.photometric, reference_level.photometric
                ))
            } else {
                None
            };

            if let Some(mismatch) = mismatch {
                return Err(Error::InvalidArgument(format!(
                    "Input band {} ({}), IFD {level_index} is incompatible with input band 1 ({}): {mismatch}",
                    band_index + 1,
                    source.path.display(),
                    reference.path.display()
                )));
            }
        }
    }

    // The current GeoTIFF reader stores these once and applies them to all overviews.
    for (level_index, level) in reference.levels.iter().enumerate().skip(1) {
        let main = &reference.levels[0];
        if (
            level.bits_per_sample,
            level.sample_format,
            level.compression,
            level.predictor,
            level.tile_width,
            level.tile_length,
        ) != (
            main.bits_per_sample,
            main.sample_format,
            main.compression,
            main.predictor,
            main.tile_width,
            main.tile_length,
        ) {
            return Err(Error::InvalidArgument(format!(
                "Input overviews use per-level data encoding at IFD {level_index}; the current GeoTIFF reader requires one data type, compression, predictor, and tile size for all levels"
            )));
        }
    }

    for level_index in 0..reference.levels.len() {
        for &tag in copied_tag_ids() {
            let expected = reference.levels[level_index].tags.get(&tag);
            for (band_index, source) in sources.iter().enumerate().skip(1) {
                let found = source.levels[level_index].tags.get(&tag);
                if found != expected {
                    return Err(Error::InvalidArgument(format!(
                        "Input band {} ({}), IFD {level_index} has incompatible TIFF tag {tag} compared with input band 1 ({})",
                        band_index + 1,
                        source.path.display(),
                        reference.path.display()
                    )));
                }
            }
        }
    }

    Ok(())
}

fn copied_tag_ids() -> &'static [u16] {
    &[
        TAG_FILL_ORDER,
        TAG_IMAGE_DESCRIPTION,
        TAG_ORIENTATION,
        TAG_X_RESOLUTION,
        TAG_Y_RESOLUTION,
        TAG_RESOLUTION_UNIT,
        TAG_SOFTWARE,
        TAG_ARTIST,
        TAG_HOST_COMPUTER,
        TAG_COPYRIGHT,
        TAG_MODEL_PIXEL_SCALE,
        TAG_MODEL_TIEPOINT,
        TAG_MODEL_TRANSFORMATION,
        TAG_ICC_PROFILE,
        TAG_GEO_KEY_DIRECTORY,
        TAG_GEO_DOUBLE_PARAMS,
        TAG_GEO_ASCII_PARAMS,
        TAG_GDAL_NODATA,
        TAG_LERC_PARAMETERS,
    ]
}

fn merge_gdal_metadata(sources: &[SourceCog]) -> Result<RawTag> {
    let mut common_items: Option<Vec<XmlItem>> = None;
    let mut output_items = Vec::new();

    for (band_index, source) in sources.iter().enumerate() {
        let items = match source.levels[0].tags.get(&TAG_GDAL_METADATA) {
            Some(tag) => parse_gdal_xml_tag(tag, band_index, &source.path)?,
            None => Vec::new(),
        };

        let mut globals = Vec::new();
        for mut item in items {
            if is_interleave_item(&item) {
                continue;
            }

            match item_attribute(&item, "sample") {
                Some(sample) => {
                    if sample != "0" {
                        return Err(Error::InvalidArgument(format!(
                            "Input band {} ({}) GDAL metadata contains sample={sample}; expected sample=0",
                            band_index + 1,
                            source.path.display()
                        )));
                    }
                    set_item_attribute(&mut item, "sample", band_index.to_string());
                    output_items.push(item);
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

    let mut all_items = common_items.unwrap_or_default();
    all_items.extend(output_items);
    all_items.push(XmlItem {
        attributes: vec![("name".into(), "INTERLEAVE".into()), ("domain".into(), "IMAGE_STRUCTURE".into())],
        text: "TILE".into(),
    });

    let mut xml = String::from("<GDALMetadata>\n");
    for item in all_items {
        xml.push_str("  <Item");
        for (name, value) in item.attributes {
            xml.push(' ');
            xml.push_str(&name);
            xml.push_str("=\"");
            push_xml_escaped(&mut xml, &value, true);
            xml.push('"');
        }
        xml.push('>');
        push_xml_escaped(&mut xml, &item.text, false);
        xml.push_str("</Item>\n");
    }
    xml.push_str("</GDALMetadata>\n");

    let mut bytes = xml.into_bytes();
    bytes.push(0);
    Ok(RawTag {
        field_type: Type::ASCII,
        count: bytes.len() as u64,
        bytes,
    })
}

fn parse_gdal_xml_tag(tag: &RawTag, band_index: usize, path: &Path) -> Result<Vec<XmlItem>> {
    if tag.field_type != Type::ASCII || tag.count as usize != tag.bytes.len() {
        return Err(Error::InvalidArgument(format!(
            "Input band {} ({}) has a malformed GDAL metadata tag",
            band_index + 1,
            path.display()
        )));
    }
    let xml_bytes = tag.bytes.strip_suffix(&[0]).ok_or_else(|| {
        Error::InvalidArgument(format!(
            "Input band {} ({}) GDAL metadata is not NUL-terminated",
            band_index + 1,
            path.display()
        ))
    })?;
    let xml = std::str::from_utf8(xml_bytes).map_err(|error| {
        Error::InvalidArgument(format!(
            "Input band {} ({}) GDAL metadata is not UTF-8: {error}",
            band_index + 1,
            path.display()
        ))
    })?;

    let mut items = Vec::new();
    let mut current: Option<XmlItem> = None;
    for event in EventReader::from_str(xml) {
        match event {
            Ok(XmlEvent::StartElement { name, attributes, .. }) if name.local_name == "Item" => {
                if current.is_some() {
                    return Err(Error::InvalidArgument(format!(
                        "Input band {} ({}) GDAL metadata contains nested Item elements",
                        band_index + 1,
                        path.display()
                    )));
                }
                current = Some(XmlItem {
                    attributes: attributes
                        .into_iter()
                        .map(|attribute| (attribute.name.local_name, attribute.value))
                        .collect(),
                    text: String::new(),
                });
            }
            Ok(XmlEvent::Characters(text) | XmlEvent::CData(text) | XmlEvent::Whitespace(text)) => {
                if let Some(item) = &mut current {
                    item.text.push_str(&text);
                }
            }
            Ok(XmlEvent::EndElement { name }) if name.local_name == "Item" => {
                let mut item = current.take().ok_or_else(|| {
                    Error::InvalidArgument(format!(
                        "Input band {} ({}) GDAL metadata closes an Item that was not opened",
                        band_index + 1,
                        path.display()
                    ))
                })?;
                item.attributes.sort();
                items.push(item);
            }
            Err(error) => {
                return Err(Error::InvalidArgument(format!(
                    "Input band {} ({}) has invalid GDAL metadata XML: {error}",
                    band_index + 1,
                    path.display()
                )));
            }
            _ => {}
        }
    }
    if current.is_some() {
        return Err(Error::InvalidArgument(format!(
            "Input band {} ({}) GDAL metadata has an unclosed Item",
            band_index + 1,
            path.display()
        )));
    }
    Ok(items)
}

fn item_attribute<'a>(item: &'a XmlItem, name: &str) -> Option<&'a str> {
    item.attributes
        .iter()
        .find_map(|(attribute_name, value)| (attribute_name == name).then_some(value.as_str()))
}

fn set_item_attribute(item: &mut XmlItem, name: &str, value: String) {
    if let Some((_, current)) = item.attributes.iter_mut().find(|(attribute_name, _)| attribute_name == name) {
        *current = value;
    } else {
        item.attributes.push((name.into(), value));
    }
    item.attributes.sort();
}

fn is_interleave_item(item: &XmlItem) -> bool {
    item_attribute(item, "name") == Some("INTERLEAVE") && item_attribute(item, "domain") == Some("IMAGE_STRUCTURE")
}

fn push_xml_escaped(output: &mut String, value: &str, attribute: bool) {
    for character in value.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' if attribute => output.push_str("&quot;"),
            '\'' if attribute => output.push_str("&apos;"),
            _ => output.push(character),
        }
    }
}

fn build_output_ifds(sources: &[SourceCog], variant: TiffVariant, gdal_metadata: &RawTag) -> Result<Vec<OutputIfd>> {
    let band_count =
        u16::try_from(sources.len()).map_err(|_| Error::InvalidArgument(format!("Band count {} exceeds u16::MAX", sources.len())))?;
    let reference = &sources[0];
    let mut output = Vec::with_capacity(reference.levels.len());

    for (level_index, level) in reference.levels.iter().enumerate() {
        let mut tags = BTreeMap::new();
        for &tag_id in copied_tag_ids() {
            if let Some(tag) = level.tags.get(&tag_id) {
                tags.insert(tag_id, tag.clone());
            }
        }

        if level_index == 0 {
            tags.insert(TAG_GDAL_METADATA, gdal_metadata.clone());
        }
        if level_index > 0 {
            tags.insert(TAG_NEW_SUBFILE_TYPE, long_tag(&[REDUCED_IMAGE]));
        }

        tags.insert(TAG_IMAGE_WIDTH, long_tag(&[level.width]));
        tags.insert(TAG_IMAGE_LENGTH, long_tag(&[level.height]));
        tags.insert(TAG_BITS_PER_SAMPLE, short_tag(&vec![level.bits_per_sample; sources.len()]));
        tags.insert(TAG_COMPRESSION, short_tag(&[level.compression]));
        tags.insert(TAG_PHOTOMETRIC_INTERPRETATION, short_tag(&[PHOTOMETRIC_BLACK_IS_ZERO]));
        tags.insert(TAG_SAMPLES_PER_PIXEL, short_tag(&[band_count]));
        tags.insert(TAG_PLANAR_CONFIGURATION, short_tag(&[PLANAR_SEPARATE]));
        tags.insert(TAG_PREDICTOR, short_tag(&[level.predictor]));
        tags.insert(TAG_TILE_WIDTH, long_tag(&[level.tile_width]));
        tags.insert(TAG_TILE_LENGTH, long_tag(&[level.tile_length]));
        if sources.len() > 1 {
            // With BlackIsZero, the first sample is the grayscale/color sample and the
            // remaining samples are explicitly marked as unspecified extra channels.
            tags.insert(TAG_EXTRA_SAMPLES, short_tag(&vec![0; sources.len() - 1]));
        }
        tags.insert(TAG_SAMPLE_FORMAT, short_tag(&vec![level.sample_format; sources.len()]));

        let array_count = level
            .tile_locations
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
    cursor = align_word(cursor)?;
    let first_ifd_offset = cursor;
    check_variant_offset(variant, first_ifd_offset, "first IFD")?;

    let mut ifds = Vec::with_capacity(output_ifds.len());
    for output_ifd in output_ifds {
        cursor = align_word(cursor)?;
        let ifd_offset = cursor;
        check_variant_offset(variant, ifd_offset, "IFD")?;
        cursor = checked_add(cursor, variant.ifd_size(output_ifd.tags.len())?, "IFD extent")?;

        let mut tags = BTreeMap::new();
        for (tag_id, value) in output_ifd.tags {
            validate_raw_tag(&value)?;
            check_variant_tag(variant, &value, tag_id)?;
            let is_tile_array = matches!(tag_id, TAG_TILE_OFFSETS | TAG_TILE_BYTE_COUNTS);
            let value_offset = if value.bytes.len() > variant.offset_size() && !is_tile_array {
                cursor = align_word(cursor)?;
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

    cursor = align_word(cursor)?;
    for ifd in &mut ifds {
        for tag_id in [TAG_TILE_OFFSETS, TAG_TILE_BYTE_COUNTS] {
            let tag = ifd
                .tags
                .get_mut(&tag_id)
                .ok_or_else(|| LayoutError::Invalid(format!("Output IFD is missing tag {tag_id}")))?;
            if tag.value.bytes.len() > variant.offset_size() {
                cursor = align_word(cursor)?;
                tag.value_offset = Some(cursor);
                check_variant_offset(variant, cursor, &format!("TIFF tag {tag_id} value"))?;
                cursor = checked_add(cursor, tag.value.bytes.len() as u64, &format!("TIFF tag {tag_id} value"))?;
            }
        }
    }

    cursor = align_word(cursor)?;
    let mut blocks = Vec::new();
    for level_index in (0..ifds.len()).rev() {
        let tiles_per_band = sources[0].levels[level_index].tile_locations.len();
        let array_len = tiles_per_band
            .checked_mul(sources.len())
            .ok_or_else(|| LayoutError::Overflow(format!("IFD {level_index} tile array length overflows usize")))?;
        let mut offsets = vec![0u64; array_len];
        let mut counts = vec![0u32; array_len];

        for tile_index in 0..tiles_per_band {
            for (band_index, source) in sources.iter().enumerate() {
                let source_tile = source.levels[level_index].tile_locations[tile_index];
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
    let source_files = sources
        .iter()
        .map(|source| File::open(&source.path))
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut source_files = source_files;

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

        for block in &plan.blocks {
            let expected_leader_offset = block
                .payload_offset
                .checked_sub(4)
                .ok_or_else(|| Error::Runtime("Planned tile payload offset is smaller than its leader".into()))?;
            pad_to(&mut writer, expected_leader_offset, &mut position)?;
            let payload_size = u32::try_from(block.source.size)
                .map_err(|_| Error::Runtime(format!("Planned tile size {} exceeds u32::MAX", block.source.size)))?;
            write_bytes(&mut writer, &payload_size.to_le_bytes(), &mut position)?;
            if position != block.payload_offset {
                return Err(Error::Runtime(format!(
                    "COG layout mismatch: tile payload planned at {}, writer is at {position}",
                    block.payload_offset
                )));
            }

            let source = &mut source_files[block.source_index];
            let trailer_offset = block
                .source
                .offset
                .checked_add(block.source.size - 4)
                .ok_or_else(|| Error::Runtime(format!("Source tile trailer offset overflows for band {}", block.source_index + 1)))?;
            source.seek(SeekFrom::Start(trailer_offset))?;
            let mut trailer = [0u8; 4];
            source.read_exact(&mut trailer)?;

            source.seek(SeekFrom::Start(block.source.offset))?;
            copy_exact(source, &mut writer, block.source.size)?;
            position = position
                .checked_add(block.source.size)
                .ok_or_else(|| Error::Runtime("Output position overflow while copying tile payload".into()))?;
            write_bytes(&mut writer, &trailer, &mut position)?;
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

fn copy_exact(reader: &mut impl Read, writer: &mut impl Write, mut remaining: u64) -> Result<()> {
    let mut buffer = vec![0u8; 64 * 1024];
    while remaining > 0 {
        let to_read = usize::try_from(remaining.min(buffer.len() as u64)).expect("bounded by buffer length");
        let read = reader.read(&mut buffer[..to_read])?;
        if read == 0 {
            return Err(Error::IOError(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("Source tile ended with {remaining} bytes remaining"),
            )));
        }
        writer.write_all(&buffer[..read])?;
        remaining -= read as u64;
    }
    Ok(())
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

fn validate_raw_tag(tag: &RawTag) -> std::result::Result<(), LayoutError> {
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

fn check_variant_tag(variant: TiffVariant, tag: &RawTag, tag_id: u16) -> std::result::Result<(), LayoutError> {
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

fn align_word(value: u64) -> std::result::Result<u64, LayoutError> {
    if value.is_multiple_of(2) {
        Ok(value)
    } else {
        checked_add(value, 1, "TIFF word alignment")
    }
}

fn layout_error(error: LayoutError) -> Error {
    match error {
        LayoutError::Overflow(message) | LayoutError::Invalid(message) => Error::InvalidArgument(message),
    }
}

fn short_tag(values: &[u16]) -> RawTag {
    RawTag {
        field_type: Type::SHORT,
        count: values.len() as u64,
        bytes: values.iter().flat_map(|value| value.to_le_bytes()).collect(),
    }
}

fn long_tag(values: &[u32]) -> RawTag {
    RawTag {
        field_type: Type::LONG,
        count: values.len() as u64,
        bytes: values.iter().flat_map(|value| value.to_le_bytes()).collect(),
    }
}

fn long8_tag(values: &[u64]) -> RawTag {
    RawTag {
        field_type: Type::LONG8,
        count: values.len() as u64,
        bytes: values.iter().flat_map(|value| value.to_le_bytes()).collect(),
    }
}

fn required_scalar_u16(tags: &BTreeMap<u16, RawTag>, tag_id: u16, context: &str) -> Result<u16> {
    optional_scalar_u16(tags, tag_id, context)?
        .ok_or_else(|| Error::InvalidArgument(format!("{context} is missing required TIFF tag {tag_id}")))
}

fn optional_scalar_u16(tags: &BTreeMap<u16, RawTag>, tag_id: u16, context: &str) -> Result<Option<u16>> {
    let Some(tag) = tags.get(&tag_id) else {
        return Ok(None);
    };
    if tag.count != 1 || tag.field_type != Type::SHORT || tag.bytes.len() != 2 {
        return Err(Error::InvalidArgument(format!(
            "{context} TIFF tag {tag_id} must be one SHORT, found {:?}[{}]",
            tag.field_type, tag.count
        )));
    }
    Ok(Some(u16::from_le_bytes(tag.bytes[..2].try_into().expect("two bytes"))))
}

fn required_scalar_u32(tags: &BTreeMap<u16, RawTag>, tag_id: u16, context: &str) -> Result<u32> {
    optional_scalar_u32(tags, tag_id, context)?
        .ok_or_else(|| Error::InvalidArgument(format!("{context} is missing required TIFF tag {tag_id}")))
}

fn optional_scalar_u32(tags: &BTreeMap<u16, RawTag>, tag_id: u16, context: &str) -> Result<Option<u32>> {
    let Some(tag) = tags.get(&tag_id) else {
        return Ok(None);
    };
    if tag.count != 1 {
        return Err(Error::InvalidArgument(format!(
            "{context} TIFF tag {tag_id} must contain one value, found {}",
            tag.count
        )));
    }
    match tag.field_type {
        Type::SHORT if tag.bytes.len() == 2 => Ok(Some(u16::from_le_bytes(tag.bytes[..2].try_into().expect("two bytes")) as u32)),
        Type::LONG if tag.bytes.len() == 4 => Ok(Some(u32::from_le_bytes(tag.bytes[..4].try_into().expect("four bytes")))),
        _ => Err(Error::InvalidArgument(format!(
            "{context} TIFF tag {tag_id} must be one SHORT or LONG, found {:?}[{}]",
            tag.field_type, tag.count
        ))),
    }
}

fn required_unsigned_vec(tags: &BTreeMap<u16, RawTag>, tag_id: u16, context: &str) -> Result<Vec<u64>> {
    let tag = tags
        .get(&tag_id)
        .ok_or_else(|| Error::InvalidArgument(format!("{context} is missing required TIFF tag {tag_id}")))?;
    match tag.field_type {
        Type::SHORT => tag
            .bytes
            .chunks_exact(2)
            .map(|bytes| Ok(u16::from_le_bytes(bytes.try_into().expect("two-byte chunk")) as u64))
            .collect(),
        Type::LONG => tag
            .bytes
            .chunks_exact(4)
            .map(|bytes| Ok(u32::from_le_bytes(bytes.try_into().expect("four-byte chunk")) as u64))
            .collect(),
        Type::LONG8 => tag
            .bytes
            .chunks_exact(8)
            .map(|bytes| Ok(u64::from_le_bytes(bytes.try_into().expect("eight-byte chunk"))))
            .collect(),
        _ => Err(Error::InvalidArgument(format!(
            "{context} TIFF tag {tag_id} must contain unsigned integers, found {:?}",
            tag.field_type
        ))),
    }
}

#[cfg(all(test, feature = "gdal"))]
mod tests {
    use std::{
        fs::File,
        io::{Read, Seek, SeekFrom},
        num::NonZeroUsize,
    };

    use crate::{
        GeoReference,
        cog::{CogCreationOptions, create_temporary_band_cogs},
        geotiff::{GeoTiffReader, metadata::Interleave},
        raster, testutils,
    };

    use super::*;

    #[test_log::test]
    fn assembles_staged_band_cogs_without_changing_pixels() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("multiband_cog.tif");
        let (staging, band_cogs) = create_temporary_band_cogs(&input.to_string_lossy(), CogCreationOptions::default())?;
        let output = staging.path().join("assembled.cog.tif");

        assemble_band_cogs(&band_cogs, &output)?;

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
                    NonZeroUsize::new(band_index + 1).expect("one-based band index"),
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

        file.seek(SeekFrom::Start(location.offset))?;
        let mut payload = vec![0; location.size as usize];
        file.read_exact(&mut payload)?;
        Ok(payload)
    }
}

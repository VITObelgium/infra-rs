use crate::{
    AnyDenseArray, ArrayDataType, ArrayNum, Columns, DenseArray, GeoReference, LatLonBounds, Point, RasterSize, Rows, Tile,
    ZoomLevelStrategy,
    cog::{
        CogStats, Compression, Predictor,
        io::{self, CogHeaderReader},
        stats,
        utils::HorizontalUnpredictable,
    },
    crs,
};
use simd_macro::simd_bounds;
use tiff::{decoder::ifd::Value, tags::Tag};

use crate::{Error, Result};
use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek},
    ops::Range,
    path::Path,
};

pub type TileOffsets = HashMap<Tile, CogTileLocation>;

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

fn verify_gdal_ghost_data(header: &[u8]) -> Result<()> {
    // Classic TIFF has magic number 42
    // BigTIFF has magic number 43
    let is_big_tiff = match header[0..4] {
        [0x43, 0x4f, 0x47, 0x00] => true,  // BigTIFF magic number
        [0x49, 0x49, 0x2a, 0x00] => false, // Classic TIFF magic number
        _ => return Err(Error::InvalidArgument("Not a valid COG file".into())),
    };

    let offset = if is_big_tiff { 16 } else { 8 };

    // GDAL_STRUCTURAL_METADATA_SIZE=XXXXXX bytes\n
    let first_line = std::str::from_utf8(&header[offset..offset + 43])
        .map_err(|e| Error::InvalidArgument(format!("Invalid UTF-8 in COG header: {e}")))?;
    if !first_line.starts_with("GDAL_STRUCTURAL_METADATA_SIZE=") {
        return Err(Error::InvalidArgument("COG not created with gdal".into()));
    }

    // // The header size is at bytes 30..36 (6 bytes)
    // let header_size_str = &first_line[30..36];
    // let header_size: usize = header_size_str
    //     .trim()
    //     .parse()
    //     .map_err(|e| Error::InvalidArgument(format!("Invalid header size: {e}")))?;

    // let header_str = String::from_utf8_lossy(&header[offset + 43..offset + 43 + header_size]);
    // log::debug!("Header: {header_str}");

    Ok(())
}

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
        } else {
            log::debug!("No model transformation info");
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

    fn generate_tiles_for_extent(geo_transform: [f64; 6], image_width: u32, image_height: u32, tile_size: u32, zoom: i32) -> Vec<Tile> {
        // Zoom levels are shifted by per tile_size factor above 256
        //debug_assert!(tile_size % 256 == 0);
        //zoom -= (tile_size / 256) as i32 - 1;

        let top_left = crs::web_mercator_to_lat_lon(Point::new(geo_transform[0], geo_transform[3]));
        let top_left_tile = Tile::for_coordinate(top_left, zoom);

        let tiles_wide = image_width.div_ceil(tile_size);
        let tiles_high = image_height.div_ceil(tile_size);

        let mut tiles = Vec::new();
        // Iteration has to be done in row-major order so the tiles match the order of the tile lists from the COG
        for ty in 0..tiles_high {
            for tx in 0..tiles_wide {
                tiles.push(Tile {
                    z: zoom,
                    x: top_left_tile.x + tx as i32,
                    y: top_left_tile.y + ty as i32,
                });
            }
        }

        tiles
    }

    fn read_nodata_value(&mut self) -> Result<Option<f64>> {
        if let Ok(nodata_str) = self.decoder.get_tag_ascii_string(Tag::GdalNodata) {
            Ok(nodata_str.parse::<f64>().ok())
        } else {
            Ok(None)
        }
    }

    pub fn parse_cog_header(&mut self) -> Result<CogMetadata> {
        let mut tile_inventory = HashMap::new();

        if !self.is_tiled()? {
            return Err(Error::InvalidArgument("Only tiled TIFFs are supported".into()));
        }

        let tile_size = self.decoder.get_tag_u32(Tag::TileWidth)? as i32;
        if tile_size != self.decoder.get_tag_u32(Tag::TileLength)? as i32 {
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

        // Now loop over the image directories to collect the tile offsets and sizes for the main raster image and all overviews.
        let max_zoom = Tile::zoom_level_for_pixel_size(geo_transform[1], ZoomLevelStrategy::Closest) - ((tile_size / 256) - 1);
        let mut current_zoom = max_zoom;

        loop {
            let tiles = Self::generate_tiles_for_extent(
                geo_transform,
                self.decoder.get_tag_u32(Tag::ImageWidth)?,
                self.decoder.get_tag_u32(Tag::ImageLength)?,
                self.decoder.get_tag_u32(Tag::TileWidth)?,
                current_zoom,
            );

            assert_eq!(self.decoder.tile_count()? as usize, tiles.len());

            let tile_offsets = self.decoder.get_tag_u64_vec(Tag::TileOffsets)?;
            let tile_byte_counts = self.decoder.get_tag_u64_vec(Tag::TileByteCounts)?;
            debug_assert_eq!(tile_offsets.len(), tile_byte_counts.len());

            itertools::izip!(tiles.iter(), tile_offsets.iter(), tile_byte_counts.iter()).for_each(|(tile, offset, byte_count)| {
                tile_inventory.insert(
                    *tile,
                    CogTileLocation {
                        offset: *offset,
                        size: *byte_count,
                    },
                );
            });

            if !self.decoder.more_images() {
                break;
            }

            current_zoom -= 1;
            self.decoder.next_image()?;
        }

        Ok(CogMetadata {
            min_zoom: current_zoom,
            max_zoom,
            tile_size,
            data_type,
            band_count: samples_per_pixel,
            compression,
            predictor,
            tile_offsets: tile_inventory,
            geo_reference: GeoReference::new("EPSG:3857", raster_size, geo_transform, nodata),
            statistics,
        })
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CogTileLocation {
    pub offset: u64,
    pub size: u64,
}

impl CogTileLocation {
    pub fn range_to_fetch(&self) -> Range<u64> {
        if self.size == 0 {
            return Range { start: 0, end: 0 };
        }

        Range {
            start: self.offset - 4,
            end: self.offset + self.size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CogMetadata {
    pub min_zoom: i32,
    pub max_zoom: i32,
    pub tile_size: i32,
    pub data_type: ArrayDataType,
    pub band_count: u32,
    pub geo_reference: GeoReference,
    pub compression: Option<Compression>,
    pub predictor: Option<Predictor>,
    pub statistics: Option<CogStats>,
    pub tile_offsets: TileOffsets,
}

impl CogMetadata {
    /// Returns the bounds of the tiles that contain data at the maximum zoom level.
    pub fn data_bounds(&self) -> LatLonBounds {
        let mut min_tile_x = i32::MAX;
        let mut max_tile_x = i32::MIN;
        let mut min_tile_y = i32::MAX;
        let mut max_tile_y = i32::MIN;

        for (tile, _) in self
            .tile_offsets
            .iter()
            .filter(|(tile, loc)| loc.size > 0 && tile.z == self.max_zoom)
        {
            min_tile_x = min_tile_x.min(tile.x);
            max_tile_x = max_tile_x.max(tile.x);
            min_tile_y = min_tile_y.min(tile.y);
            max_tile_y = max_tile_y.max(tile.y);
        }

        let min_tile = Tile {
            z: self.max_zoom,
            x: min_tile_x,
            y: min_tile_y,
        };

        let max_tile = Tile {
            z: self.max_zoom,
            x: max_tile_x,
            y: max_tile_y,
        };

        if min_tile_x == i32::MAX {
            // No tiles with data at the maximum zoom level
            return LatLonBounds::world();
        }

        LatLonBounds::hull(min_tile.upper_left(), max_tile.lower_right())
    }
}

#[derive(Debug, Clone)]
pub struct CogAccessor {
    meta: CogMetadata,
}

impl CogAccessor {
    pub fn is_cog(path: &Path) -> bool {
        let mut header = vec![0u8; io::COG_HEADER_SIZE];
        match File::open(path) {
            Ok(mut file) => match file.read_exact(&mut header) {
                Ok(()) => {}
                Err(_) => return false,
            },
            Err(_) => return false,
        };

        verify_gdal_ghost_data(&header).is_ok()
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        Self::new(CogHeaderReader::from_stream(File::open(path)?)?)
    }

    /// Create a `CogTileIndex` from a buffer containing the COG header the size of the buffer must match the `io::COG_HEADER_SIZE`.
    pub fn from_cog_header(buffer: Vec<u8>) -> Result<Self> {
        Self::new(CogHeaderReader::from_buffer(buffer)?)
    }

    fn new(reader: CogHeaderReader) -> Result<Self> {
        verify_gdal_ghost_data(reader.cog_header())?;
        let mut reader = CogDecoder::new(reader)?;
        let meta = reader.parse_cog_header()?;

        Ok(CogAccessor { meta })
    }

    pub fn meta_data(&self) -> &CogMetadata {
        &self.meta
    }

    pub fn tile_offsets(&self) -> &TileOffsets {
        &self.meta.tile_offsets
    }

    pub fn tile_offset(&self, tile: &Tile) -> Option<CogTileLocation> {
        self.meta.tile_offsets.get(tile).copied()
    }

    /// Read the tile data for the given tile using the provided reader.
    /// This method will return an error if the tile does not exist in the COG index
    /// If this is a COG with sparse tile support, for sparse tiles an empty array will be returned
    pub fn read_tile_data(&self, tile: &Tile, mut reader: impl Read + Seek) -> Result<AnyDenseArray> {
        Ok(match self.meta.data_type {
            ArrayDataType::Uint8 => AnyDenseArray::U8(self.read_tile_data_as::<u8>(tile, &mut reader)?),
            ArrayDataType::Uint16 => AnyDenseArray::U16(self.read_tile_data_as::<u16>(tile, &mut reader)?),
            ArrayDataType::Uint32 => AnyDenseArray::U32(self.read_tile_data_as::<u32>(tile, &mut reader)?),
            ArrayDataType::Uint64 => AnyDenseArray::U64(self.read_tile_data_as::<u64>(tile, &mut reader)?),
            ArrayDataType::Int8 => AnyDenseArray::I8(self.read_tile_data_as::<i8>(tile, &mut reader)?),
            ArrayDataType::Int16 => AnyDenseArray::I16(self.read_tile_data_as::<i16>(tile, &mut reader)?),
            ArrayDataType::Int32 => AnyDenseArray::I32(self.read_tile_data_as::<i32>(tile, &mut reader)?),
            ArrayDataType::Int64 => AnyDenseArray::I64(self.read_tile_data_as::<i64>(tile, &mut reader)?),
            ArrayDataType::Float32 => AnyDenseArray::F32(self.read_tile_data_as::<f32>(tile, &mut reader)?),
            ArrayDataType::Float64 => AnyDenseArray::F64(self.read_tile_data_as::<f64>(tile, &mut reader)?),
        })
    }

    pub fn parse_tile_data(&self, tile: &CogTileLocation, cog_chunk: &[u8]) -> Result<AnyDenseArray> {
        Ok(match self.meta.data_type {
            ArrayDataType::Uint8 => AnyDenseArray::U8(self.parse_tile_data_as::<u8>(tile, cog_chunk)?),
            ArrayDataType::Uint16 => AnyDenseArray::U16(self.parse_tile_data_as::<u16>(tile, cog_chunk)?),
            ArrayDataType::Uint32 => AnyDenseArray::U32(self.parse_tile_data_as::<u32>(tile, cog_chunk)?),
            ArrayDataType::Uint64 => AnyDenseArray::U64(self.parse_tile_data_as::<u64>(tile, cog_chunk)?),
            ArrayDataType::Int8 => AnyDenseArray::I8(self.parse_tile_data_as::<i8>(tile, cog_chunk)?),
            ArrayDataType::Int16 => AnyDenseArray::I16(self.parse_tile_data_as::<i16>(tile, cog_chunk)?),
            ArrayDataType::Int32 => AnyDenseArray::I32(self.parse_tile_data_as::<i32>(tile, cog_chunk)?),
            ArrayDataType::Int64 => AnyDenseArray::I64(self.parse_tile_data_as::<i64>(tile, cog_chunk)?),
            ArrayDataType::Float32 => AnyDenseArray::F32(self.parse_tile_data_as::<f32>(tile, cog_chunk)?),
            ArrayDataType::Float64 => AnyDenseArray::F64(self.parse_tile_data_as::<f64>(tile, cog_chunk)?),
        })
    }

    #[simd_bounds]
    pub fn read_tile_data_as<T: ArrayNum + HorizontalUnpredictable>(
        &self,
        tile: &Tile,
        mut reader: impl Read + Seek,
    ) -> Result<DenseArray<T>> {
        if T::TYPE != self.meta.data_type {
            return Err(Error::InvalidArgument(format!(
                "Tile data type mismatch: expected {:?}, got {:?}",
                self.meta.data_type,
                T::TYPE
            )));
        }

        if let Some(tile_location) = self.tile_offset(tile) {
            io::read_tile_data(
                &tile_location,
                self.meta.tile_size,
                self.meta.geo_reference.nodata(),
                self.meta.compression,
                self.meta.predictor,
                &mut reader,
            )
        } else {
            Err(Error::InvalidArgument(format!("{tile:?} not found in COG index")))
        }
    }

    #[simd_bounds]
    pub fn parse_tile_data_as<T: ArrayNum + HorizontalUnpredictable>(
        &self,
        tile: &CogTileLocation,
        cog_chunk: &[u8],
    ) -> Result<DenseArray<T>> {
        if T::TYPE != self.meta.data_type {
            return Err(Error::InvalidArgument(format!(
                "Tile data type mismatch: expected {:?}, got {:?}",
                self.meta.data_type,
                T::TYPE
            )));
        }

        let tile_data = io::parse_tile_data(
            tile,
            self.meta.tile_size,
            self.meta.geo_reference.nodata(),
            self.meta.compression,
            self.meta.predictor,
            cog_chunk,
        )?;

        Ok(tile_data)
    }
}

#[cfg(feature = "gdal")]
#[cfg(test)]
mod tests {
    use crate::{
        Array as _,
        cog::{CogCreationOptions, create_cog_tiles, creation::PredictorSelection},
        testutils,
    };

    use super::*;

    use approx::assert_relative_eq;

    const COG_TILE_SIZE: i32 = 256;

    fn create_test_cog(
        input_tif: &Path,
        output_tif: &Path,
        tile_size: i32,
        compression: Option<Compression>,
        predictor: Option<PredictorSelection>,
        output_type: Option<ArrayDataType>,
        allow_sparse: bool,
    ) -> Result<()> {
        let opts = CogCreationOptions {
            min_zoom: Some(7),
            zoom_level_strategy: ZoomLevelStrategy::Closest,
            tile_size: tile_size as u16,
            allow_sparse,
            compression,
            predictor,
            output_data_type: output_type,
        };
        create_cog_tiles(input_tif, output_tif, opts)?;

        Ok(())
    }

    #[test_log::test]
    fn data_bounds_sparse_tiles() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        {
            // Allow sparse tiles, this would reduce the size if the bounds
            create_test_cog(&input, &output, COG_TILE_SIZE, None, None, None, true)?;
            let cog = CogAccessor::from_file(&output)?;

            let data_bounds = cog.meta_data().data_bounds();
            assert_relative_eq!(data_bounds.northwest(), Tile { z: 10, x: 519, y: 340 }.upper_left());
            assert_relative_eq!(data_bounds.southeast(), Tile { z: 10, x: 528, y: 344 }.lower_right());
        }

        {
            // Don't allow sparse tiles, The bounds should now match the extent of the lowest zoom level
            create_test_cog(&input, &output, COG_TILE_SIZE, None, None, None, false)?;
            let cog = CogAccessor::from_file(&output)?;
            assert!(cog.meta_data().max_zoom == 10);

            let data_bounds = cog.meta_data().data_bounds();
            assert_relative_eq!(data_bounds.northwest(), Tile { z: 7, x: 64, y: 42 }.upper_left());
            assert_relative_eq!(data_bounds.southeast(), Tile { z: 7, x: 66, y: 43 }.lower_right());
        }

        Ok(())
    }

    #[test_log::test]
    fn cog_metadata() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        create_test_cog(&input, &output, COG_TILE_SIZE, None, None, None, true)?;
        let cog = CogAccessor::from_file(&output)?;

        let mut reader = File::open(&output)?;
        let meta = cog.meta_data();
        assert_eq!(meta.tile_size, COG_TILE_SIZE);
        assert_eq!(meta.data_type, ArrayDataType::Uint8);
        assert_eq!(meta.min_zoom, 7);
        assert_eq!(meta.max_zoom, 10);
        assert_eq!(meta.compression, None);
        assert_eq!(meta.predictor, None);
        assert_eq!(meta.geo_reference.nodata(), Some(255.0));

        assert!(!cog.tile_offsets().is_empty(), "Tile offsets should not be empty");
        // Decode all tiles
        for tile in cog.tile_offsets().keys() {
            let tile_data = cog.read_tile_data(tile, &mut reader)?;
            if tile_data.is_empty() {
                continue; // Skip empty tiles
            }

            assert_eq!(tile_data.len(), RasterSize::square(COG_TILE_SIZE).cell_count());
            assert_eq!(tile_data.data_type(), meta.data_type);

            let tile_data = cog.read_tile_data_as::<u8>(tile, &mut reader)?;
            assert_eq!(tile_data.size(), RasterSize::square(COG_TILE_SIZE));
        }

        Ok(())
    }

    #[test_log::test]
    fn read_test_cog() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        let reference_tile = Tile { z: 10, x: 524, y: 341 };
        let reference_tile_data = {
            // Create a test COG file without compression
            create_test_cog(&input, &output, COG_TILE_SIZE, None, None, None, true)?;
            let cog = CogAccessor::from_file(&output)?;

            let mut reader = File::open(&output)?;
            cog.read_tile_data_as::<u8>(&reference_tile, &mut reader).expect("None_u8")
        };

        {
            // Create a test COG file with LZW compression and no predictor
            create_test_cog(&input, &output, COG_TILE_SIZE, Some(Compression::Lzw), None, None, true)?;
            let cog = CogAccessor::from_file(&output)?;

            let mut reader = File::open(&output)?;
            let tile_data = cog.read_tile_data_as::<u8>(&reference_tile, &mut reader).expect("LZW_u8");
            assert_eq!(tile_data, reference_tile_data);
        }

        {
            // Create a test COG file with LZW compression and horizontal predictor
            create_test_cog(
                &input,
                &output,
                COG_TILE_SIZE,
                Some(Compression::Lzw),
                Some(PredictorSelection::Automatic),
                None,
                true,
            )?;
            let cog = CogAccessor::from_file(&output)?;
            assert_eq!(cog.meta_data().predictor, Some(Predictor::Horizontal));

            let mut reader = File::open(&output)?;
            let tile_data = cog.read_tile_data_as::<u8>(&reference_tile, &mut reader).expect("LZW_u8_predictor");
            assert_eq!(tile_data, reference_tile_data);
        }

        {
            // Create a test COG file as i32 with LZW compression and predictor
            create_test_cog(
                &input,
                &output,
                COG_TILE_SIZE,
                Some(Compression::Lzw),
                Some(PredictorSelection::Automatic),
                Some(ArrayDataType::Int32),
                true,
            )?;
            let cog = CogAccessor::from_file(&output)?;
            assert_eq!(cog.meta_data().predictor, Some(Predictor::Horizontal));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<i32>(&reference_tile, &mut reader)
                .expect("LZW_i32_predictor");

            assert_eq!(tile_data.cast_to::<u8>(), reference_tile_data);
        }

        {
            // Create a test COG file as f32 with LZW compression and no predictor
            create_test_cog(
                &input,
                &output,
                COG_TILE_SIZE,
                Some(Compression::Lzw),
                None,
                Some(ArrayDataType::Float32),
                true,
            )?;
            let cog = CogAccessor::from_file(&output)?;
            assert_eq!(cog.meta_data().predictor, None);
            assert_eq!(cog.meta_data().max_zoom, 10);

            let mut reader = File::open(&output)?;
            assert!(cog.read_tile_data_as::<f64>(&reference_tile, &mut reader).is_err());
            let tile_data = cog.read_tile_data_as::<f32>(&reference_tile, &mut reader).expect("LZW_f32");

            assert_eq!(tile_data.cast_to::<u8>(), reference_tile_data);
        }

        {
            // Create a test COG file as f64 with LZW compression and float predictor
            create_test_cog(
                &input,
                &output,
                COG_TILE_SIZE,
                Some(Compression::Lzw),
                Some(PredictorSelection::Automatic),
                Some(ArrayDataType::Float32),
                true,
            )?;
            let cog = CogAccessor::from_file(&output)?;
            assert_eq!(cog.meta_data().predictor, Some(Predictor::FloatingPoint));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<f32>(&reference_tile, &mut reader)
                .expect("LZW_f32_predictor");

            assert_eq!(tile_data.cast_to::<u8>(), reference_tile_data);
        }

        {
            // Create a test COG file as float with LZW compression and float predictor
            create_test_cog(
                &input,
                &output,
                COG_TILE_SIZE,
                Some(Compression::Lzw),
                Some(PredictorSelection::Automatic),
                Some(ArrayDataType::Float64),
                true,
            )?;
            let cog = CogAccessor::from_file(&output)?;
            assert_eq!(cog.meta_data().predictor, Some(Predictor::FloatingPoint));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<f64>(&reference_tile, &mut reader)
                .expect("LZW_f64_predictor");

            assert_eq!(tile_data.cast_to::<u8>(), reference_tile_data);
        }

        Ok(())
    }

    #[test_log::test]
    fn read_test_cog_512() -> Result<()> {
        const COG_TILE_SIZE: i32 = 512;
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        let reference_tile = Tile {
            z: 9,
            x: 524 / 2,
            y: 341 / 2,
        };
        let reference_tile_data = {
            // Create a test COG file without compression
            create_test_cog(&input, &output, COG_TILE_SIZE, None, None, None, true)?;
            let cog = CogAccessor::from_file(&output)?;

            let meta = cog.meta_data();
            assert_eq!(meta.tile_size, COG_TILE_SIZE);
            assert_eq!(meta.data_type, ArrayDataType::Uint8);
            assert_eq!(meta.min_zoom, 7);
            assert_eq!(meta.max_zoom, 9);

            let mut reader = File::open(&output)?;
            cog.read_tile_data_as::<u8>(&reference_tile, &mut reader)?
        };

        {
            // Create a test COG file with LZW compression and no predictor
            create_test_cog(&input, &output, COG_TILE_SIZE, Some(Compression::Lzw), None, None, true)?;
            let cog = CogAccessor::from_file(&output)?;

            let mut reader = File::open(&output)?;
            let tile_data = cog.read_tile_data_as::<u8>(&reference_tile, &mut reader)?;
            assert_eq!(tile_data, reference_tile_data);
        }

        Ok(())
    }

    #[test_log::test]
    fn compare_compression_results() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let no_compression_output = tmp.path().join("cog_no_compression.tif");
        create_test_cog(&input, &no_compression_output, COG_TILE_SIZE, None, None, None, true)?;

        let lzw_compression_output = tmp.path().join("cog_lzw_compression.tif");
        create_test_cog(
            &input,
            &lzw_compression_output,
            COG_TILE_SIZE,
            Some(Compression::Lzw),
            None,
            None,
            true,
        )?;

        let cog_no_compression = CogAccessor::from_file(&no_compression_output)?;
        let cog_lzw_compression = CogAccessor::from_file(&lzw_compression_output)?;

        assert!(cog_no_compression.tile_offsets().len() == cog_lzw_compression.tile_offsets().len());
        let mut no_compression_reader = File::open(&no_compression_output)?;
        let mut lzw_compression_reader = File::open(&lzw_compression_output)?;

        for tile in cog_no_compression.tile_offsets().keys() {
            let tile_data_no_compression = cog_no_compression.read_tile_data(tile, &mut no_compression_reader).unwrap();
            let tile_data_lzw_compression = cog_lzw_compression.read_tile_data(tile, &mut lzw_compression_reader).unwrap();

            assert_eq!(tile_data_no_compression, tile_data_lzw_compression);
        }

        Ok(())
    }
}

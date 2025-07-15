use crate::{
    AnyDenseArray, ArrayDataType, ArrayNum, DenseArray, GeoReference, LatLonBounds, Tile,
    cog::{
        CogStats, Compression, Predictor,
        decoder::CogDecoder,
        io::{self, CogHeaderReader},
        utils::HorizontalUnpredictable,
    },
};
use simd_macro::simd_bounds;

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
    pub tile_size: u16,
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
        Array as _, RasterSize, ZoomLevelStrategy,
        cog::{CogCreationOptions, create_cog_tiles, creation::PredictorSelection},
        crs, testutils,
    };

    use super::*;

    use approx::assert_relative_eq;

    const COG_TILE_SIZE: u16 = 256;

    fn create_test_cog(
        input_tif: &Path,
        output_tif: &Path,
        tile_size: u16,
        compression: Option<Compression>,
        predictor: Option<PredictorSelection>,
        output_type: Option<ArrayDataType>,
        allow_sparse: bool,
    ) -> Result<()> {
        let opts = CogCreationOptions {
            min_zoom: Some(7),
            zoom_level_strategy: ZoomLevelStrategy::Closest,
            tile_size,
            allow_sparse,
            compression,
            predictor,
            output_data_type: output_type,
            aligned_levels: None,
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
        assert_eq!(meta.geo_reference.projected_epsg(), Some(crs::epsg::WGS84_WEB_MERCATOR));

        assert!(!cog.tile_offsets().is_empty(), "Tile offsets should not be empty");
        // Decode all tiles
        for tile in cog.tile_offsets().keys() {
            let tile_data = cog.read_tile_data(tile, &mut reader)?;
            if tile_data.is_empty() {
                continue; // Skip empty tiles
            }

            assert_eq!(tile_data.len(), RasterSize::square(COG_TILE_SIZE as i32).cell_count());
            assert_eq!(tile_data.data_type(), meta.data_type);

            let tile_data = cog.read_tile_data_as::<u8>(tile, &mut reader)?;
            assert_eq!(tile_data.size(), RasterSize::square(COG_TILE_SIZE as i32));
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
    fn read_test_cog_unaligned_overviews() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        let opts = CogCreationOptions {
            min_zoom: Some(7),
            zoom_level_strategy: ZoomLevelStrategy::Closest,
            tile_size: COG_TILE_SIZE,
            allow_sparse: true,
            compression: None,
            predictor: None,
            output_data_type: None,
            aligned_levels: Some(2),
        };
        create_cog_tiles(&input, &output, opts)?;

        let cog = CogAccessor::from_file(&output)?;
        let meta = cog.meta_data();
        assert_eq!(meta.min_zoom, 9);
        assert_eq!(meta.max_zoom, 10);

        Ok(())
    }

    #[test_log::test]
    fn read_test_cog_512() -> Result<()> {
        const COG_TILE_SIZE: u16 = 512;
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

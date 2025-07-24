use crate::{
    AnyDenseArray, ArrayDataType, ArrayNum, DenseArray, GeoReference, RasterSize,
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
    fs::File,
    io::{Read, Seek},
    ops::Range,
    path::Path,
};

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

fn verify_gdal_ghost_data(header: &[u8]) -> Result<()> {
    // Classic TIFF has magic number 42
    // BigTIFF has magic number 43
    let is_big_tiff = match header[0..4] {
        [0x49, 0x49, 0x2a, 0x00] => false, // Classic TIFF magic number
        [0x49, 0x49, 0x2b, 0x00] => true,  // BigTIFF magic number
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
pub struct PyramidInfo {
    pub raster_size: RasterSize,
    pub tile_locations: Vec<CogTileLocation>,
}

#[derive(Debug, Clone)]
pub struct CogMetadata {
    pub tile_size: u32,
    pub data_type: ArrayDataType,
    pub band_count: u32,
    pub geo_reference: GeoReference,
    pub compression: Option<Compression>,
    pub predictor: Option<Predictor>,
    pub statistics: Option<CogStats>,
    pub pyramids: Vec<PyramidInfo>,
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
        let mut buffer_factor = 1;
        // This could be improved to reuse the existing buffer and append to it when the buffer is not large enough
        loop {
            let res = Self::new(CogHeaderReader::from_stream(
                File::open(path)?,
                io::COG_HEADER_SIZE * buffer_factor,
            )?);
            match res {
                Err(Error::IOError(io_err) | Error::TiffError(tiff::TiffError::IoError(io_err)))
                    if io_err.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    // If the error is an EOF, we need more data to parse the header
                    buffer_factor *= 2;
                }
                Ok(cog) => return Ok(cog),
                Err(e) => return Err(e),
            }
        }
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

    pub fn metadata(&self) -> &CogMetadata {
        &self.meta
    }

    pub fn pyramid_info(&self, index: usize) -> Option<&PyramidInfo> {
        self.meta.pyramids.get(index)
    }

    /// Read the tile data for the given tile using the provided reader.
    /// This method will return an error if the tile does not exist in the COG index
    /// If this is a COG with sparse tile support, for sparse tiles an empty array will be returned
    pub fn read_tile_data(&self, cog_tile: &CogTileLocation, mut reader: impl Read + Seek) -> Result<AnyDenseArray> {
        Ok(match self.meta.data_type {
            ArrayDataType::Uint8 => AnyDenseArray::U8(self.read_tile_data_as::<u8>(cog_tile, &mut reader)?),
            ArrayDataType::Uint16 => AnyDenseArray::U16(self.read_tile_data_as::<u16>(cog_tile, &mut reader)?),
            ArrayDataType::Uint32 => AnyDenseArray::U32(self.read_tile_data_as::<u32>(cog_tile, &mut reader)?),
            ArrayDataType::Uint64 => AnyDenseArray::U64(self.read_tile_data_as::<u64>(cog_tile, &mut reader)?),
            ArrayDataType::Int8 => AnyDenseArray::I8(self.read_tile_data_as::<i8>(cog_tile, &mut reader)?),
            ArrayDataType::Int16 => AnyDenseArray::I16(self.read_tile_data_as::<i16>(cog_tile, &mut reader)?),
            ArrayDataType::Int32 => AnyDenseArray::I32(self.read_tile_data_as::<i32>(cog_tile, &mut reader)?),
            ArrayDataType::Int64 => AnyDenseArray::I64(self.read_tile_data_as::<i64>(cog_tile, &mut reader)?),
            ArrayDataType::Float32 => AnyDenseArray::F32(self.read_tile_data_as::<f32>(cog_tile, &mut reader)?),
            ArrayDataType::Float64 => AnyDenseArray::F64(self.read_tile_data_as::<f64>(cog_tile, &mut reader)?),
        })
    }

    #[simd_bounds]
    pub fn read_tile_data_as<T: ArrayNum + HorizontalUnpredictable>(
        &self,
        cog_tile: &CogTileLocation,
        mut reader: impl Read + Seek,
    ) -> Result<DenseArray<T>> {
        if T::TYPE != self.meta.data_type {
            return Err(Error::InvalidArgument(format!(
                "Tile data type mismatch: expected {:?}, got {:?}",
                self.meta.data_type,
                T::TYPE
            )));
        }

        io::read_tile_data(
            cog_tile,
            self.meta.tile_size,
            self.meta.geo_reference.nodata(),
            self.meta.compression,
            self.meta.predictor,
            &mut reader,
        )
    }

    #[simd_bounds]
    pub fn parse_tile_data_as<T: ArrayNum + HorizontalUnpredictable>(
        &self,
        cog_tile: &CogTileLocation,
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
            cog_tile,
            self.meta.tile_size,
            self.meta.geo_reference.nodata(),
            self.meta.compression,
            self.meta.predictor,
            None,
            cog_chunk,
        )?;

        Ok(tile_data)
    }
}

#[cfg(feature = "gdal")]
#[cfg(test)]
mod tests {
    use crate::{
        Array as _, RasterSize, Tile, ZoomLevelStrategy,
        cog::{CogCreationOptions, create_cog_tiles, creation::PredictorSelection},
        crs, testutils,
    };

    use super::*;

    const COG_TILE_SIZE: u32 = 256;

    fn create_test_cog(
        input_tif: &Path,
        output_tif: &Path,
        tile_size: u32,
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
    fn cog_metadata_larger_then_default_header_size() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        let opts = CogCreationOptions {
            min_zoom: Some(2),
            zoom_level_strategy: ZoomLevelStrategy::PreferHigher,
            tile_size: Tile::TILE_SIZE,
            allow_sparse: false,
            compression: None,
            predictor: None,
            output_data_type: Some(ArrayDataType::Uint8),
            aligned_levels: None,
        };
        create_cog_tiles(&input, &output, opts)?;

        let cog = CogAccessor::from_file(&output)?;

        let meta = cog.metadata();
        assert_eq!(meta.tile_size, opts.tile_size);
        assert_eq!(meta.data_type, opts.output_data_type.unwrap());
        assert_eq!(meta.compression, None);
        assert_eq!(meta.predictor, None);
        assert_eq!(meta.geo_reference.nodata(), Some(255.0));
        assert_eq!(meta.geo_reference.projected_epsg(), Some(crs::epsg::WGS84_WEB_MERCATOR));
        assert_eq!(cog.metadata().pyramids.len(), 9); // zoom levels 2 to 10

        Ok(())
    }

    #[test_log::test]
    fn cog_metadata() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        create_test_cog(&input, &output, COG_TILE_SIZE, None, None, None, true)?;
        let cog = CogAccessor::from_file(&output)?;

        let meta = cog.metadata();
        assert_eq!(meta.tile_size, COG_TILE_SIZE);
        assert_eq!(meta.data_type, ArrayDataType::Uint8);
        assert_eq!(meta.compression, None);
        assert_eq!(meta.predictor, None);
        assert_eq!(meta.geo_reference.nodata(), Some(255.0));
        assert_eq!(meta.geo_reference.projected_epsg(), Some(crs::epsg::WGS84_WEB_MERCATOR));
        assert_eq!(cog.metadata().pyramids.len(), 4); // zoom levels 7 to 10

        // Decode all cog tile
        let mut reader = File::open(&output)?;
        for pyramid in cog.metadata().pyramids.iter() {
            assert!(!pyramid.tile_locations.is_empty(), "Pyramid tile locations should not be empty");

            for tile in &pyramid.tile_locations {
                if tile.size == 0 {
                    continue; // Skip empty tiles
                }

                let tile_data = cog.read_tile_data(tile, &mut reader)?;

                assert_eq!(tile_data.len(), RasterSize::square(COG_TILE_SIZE as i32).cell_count());
                assert_eq!(tile_data.data_type(), meta.data_type);

                let tile_data = cog.read_tile_data_as::<u8>(tile, &mut reader)?;
                assert_eq!(tile_data.size(), RasterSize::square(COG_TILE_SIZE as i32));
            }
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

        for (pyramid_no_compression, pyramid_lzw) in cog_no_compression
            .metadata()
            .pyramids
            .iter()
            .zip(cog_lzw_compression.metadata().pyramids.iter())
        {
            assert!(
                pyramid_no_compression.tile_locations.len() == pyramid_lzw.tile_locations.len(),
                "Pyramid tile locations should match in count"
            );

            let mut no_compression_reader = File::open(&no_compression_output)?;
            let mut lzw_compression_reader = File::open(&lzw_compression_output)?;

            for (tile, tile_lzw) in pyramid_no_compression.tile_locations.iter().zip(pyramid_lzw.tile_locations.iter()) {
                let tile_data_no_compression = cog_no_compression.read_tile_data(tile, &mut no_compression_reader).unwrap();
                let tile_data_lzw_compression = cog_lzw_compression.read_tile_data(tile_lzw, &mut lzw_compression_reader).unwrap();

                assert_eq!(tile_data_no_compression, tile_data_lzw_compression);
            }
        }

        Ok(())
    }
}

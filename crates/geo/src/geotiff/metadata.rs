use std::fs::File;
use std::io::Seek;
use std::path::Path;

use crate::geotiff::{
    ChunkDataLayout, TiffStats, decoder,
    gdalghostdata::GdalGhostData,
    gdalmetadata::BandMetadata,
    io::{self, CogHeaderReader},
    reader::TiffOverview,
};
use crate::raster::{Compression, Predictor};
use crate::{ArrayDataType, Error, GeoReference, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Interleave {
    /// One tiff chunk/strip contains the data for one band, chunks of band 1 are followed by the chunks of band 2 etc.
    Band,
    /// One tiff chunk/strip contains data for all bands interleaved per pixel
    Pixel,
    /// One tiff chunk/tile contains data for a single band but they are interleaved per band.
    /// So with one read the chunks for all the bands can be loaded
    Tile,
}

#[derive(Debug, Clone)]
pub struct GeoTiffMetadata {
    pub data_layout: ChunkDataLayout,
    pub band_count: u32,
    pub data_type: ArrayDataType,
    pub compression: Option<Compression>,
    pub predictor: Option<Predictor>,
    pub statistics: Option<TiffStats>,
    pub geo_reference: GeoReference,
    pub overviews: Vec<TiffOverview>,
    pub interleave: Interleave,
    pub gdal_ghost_data: Option<GdalGhostData>, // Additional GDAL ghost metadata if the file was created with GDAL
    /// Per-band metadata (scale/offset) from GDAL metadata XML
    pub band_metadata: Vec<BandMetadata>,
}

pub enum ParseFromBufferError {
    BufferTooSmall(Vec<u8>),
    Error(crate::Error),
}

impl GeoTiffMetadata {
    pub fn from_file(path: &Path) -> Result<Self> {
        let mut file_reader = File::open(path)?;

        let mut buffer = Vec::with_capacity(io::COG_HEADER_SIZE);
        io::append_from_stream_to_buffer(&mut buffer, &mut file_reader, io::COG_HEADER_SIZE)?;
        let ghost_data = GdalGhostData::from_tiff_header_buffer(&buffer);
        file_reader.seek(std::io::SeekFrom::Start(0))?;

        let mut cog_buffer_reader = CogHeaderReader::from_stream(&mut file_reader, io::COG_HEADER_SIZE)?;
        if ghost_data.as_ref().is_some_and(|ghost| ghost.is_cog()) {
            cog_buffer_reader.seek(std::io::SeekFrom::Start(0))?;

            // This is a COG, try to read the tiff metadata with as litte io calls as possible by using the `CogHeaderReader`.
            // This errors however if a read occurs that is larger than the default header size.
            // In that case we will increase the buffer size until we can read the header successfully.

            loop {
                let res = decoder::parse_geotiff_metadata(&mut cog_buffer_reader);

                match res {
                    Err(Error::IOError(io_err) | Error::TiffError(tiff::TiffError::IoError(io_err)))
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof =>
                    {
                        // If the error is an EOF, we need more data to parse the header
                        cog_buffer_reader.increase_buffer_size(&mut file_reader)?;
                        log::debug!("Cog header dit not fit in default header size, retry with increased buffer size");
                    }
                    Ok(mut meta) => {
                        meta.gdal_ghost_data = ghost_data;
                        return Ok(meta);
                    }
                    Err(e) => return Err(e),
                }
            }
        } else {
            file_reader.seek(std::io::SeekFrom::Start(0))?;
            decoder::parse_geotiff_metadata(&mut file_reader)
        }
    }

    pub fn from_buffer(buf: Vec<u8>) -> std::result::Result<Self, ParseFromBufferError> {
        let mut reader = CogHeaderReader::from_buffer(buf);
        match decoder::parse_geotiff_metadata(&mut reader) {
            Ok(meta) => Ok(meta),
            Err(Error::IOError(io_err) | Error::TiffError(tiff::TiffError::IoError(io_err)))
                if io_err.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                Err(ParseFromBufferError::BufferTooSmall(reader.into_buffer()))
            }

            Err(e) => Err(ParseFromBufferError::Error(e)),
        }
    }

    pub fn chunk_row_length(&self) -> u32 {
        match self.data_layout {
            ChunkDataLayout::Tiled(size) => size,
            ChunkDataLayout::Striped(_) => self.geo_reference.columns().count() as u32,
        }
    }

    pub fn is_tiled(&self) -> bool {
        matches!(self.data_layout, ChunkDataLayout::Tiled(_))
    }
}

#[cfg(test)]
#[cfg(feature = "gdal")]
mod tests {

    use crate::{
        Tile, ZoomLevelStrategy,
        cog::{CogCreationOptions, PredictorSelection, create_cog_tiles},
        crs,
        raster::{self, formats},
        testutils,
    };

    use super::*;

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
    fn cog_metadata() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        create_test_cog(&input, &output, Tile::TILE_SIZE, None, None, None, true)?;

        let meta = GeoTiffMetadata::from_file(&output)?;
        assert_eq!(meta.data_layout, ChunkDataLayout::Tiled(Tile::TILE_SIZE));
        assert_eq!(meta.data_type, ArrayDataType::Uint8);
        assert_eq!(meta.compression, None);
        assert_eq!(meta.predictor, None);
        assert_eq!(meta.geo_reference.nodata(), Some(255.0));
        assert_eq!(meta.geo_reference.projected_epsg(), Some(crs::epsg::WGS84_WEB_MERCATOR));
        assert_eq!(meta.overviews.len(), 4); // zoom levels 7 to 10

        Ok(())
    }

    #[test_log::test]
    fn cog_metadata_larger_then_default_header_size() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        let opts = CogCreationOptions {
            min_zoom: Some(4),
            zoom_level_strategy: ZoomLevelStrategy::PreferHigher,
            tile_size: Tile::TILE_SIZE,
            allow_sparse: false,
            compression: None,
            predictor: None,
            output_data_type: Some(ArrayDataType::Uint8),
            aligned_levels: None,
        };
        create_cog_tiles(&input, &output, opts)?;

        let meta = GeoTiffMetadata::from_file(&output)?;
        assert_eq!(meta.data_layout, ChunkDataLayout::Tiled(opts.tile_size));
        assert_eq!(meta.data_type, opts.output_data_type.unwrap());
        assert_eq!(meta.compression, None);
        assert_eq!(meta.predictor, None);
        assert_eq!(meta.geo_reference.nodata(), Some(255.0));
        assert_eq!(meta.geo_reference.projected_epsg(), Some(crs::epsg::WGS84_WEB_MERCATOR));
        assert_eq!(meta.overviews.len(), 7); // zoom levels 4 to 10

        Ok(())
    }

    #[test_log::test]
    fn geotiff_non_cog() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        let options = vec![
            "-f".to_string(),
            "GTiff".to_string(),
            "-co".to_string(),
            "NUM_THREADS=ALL_CPUS".to_string(),
        ];

        let creation_options: Vec<(String, String)> = vec![];

        let src_ds = formats::gdal::open_dataset_read_only(input)?;
        raster::algo::gdal::warp_to_disk_cli(&src_ds, &output, &options, &creation_options)?;

        let meta = GeoTiffMetadata::from_file(&output)?;
        assert_eq!(meta.data_layout, ChunkDataLayout::Striped(3));
        assert_eq!(meta.data_type, ArrayDataType::Uint8);
        assert_eq!(meta.compression, None);
        assert_eq!(meta.predictor, None);
        assert_eq!(meta.geo_reference.nodata(), Some(255.0));
        assert_eq!(meta.geo_reference.projected_epsg(), Some(crs::epsg::BELGIAN_LAMBERT72));
        assert_eq!(meta.overviews.len(), 1);

        Ok(())
    }
}

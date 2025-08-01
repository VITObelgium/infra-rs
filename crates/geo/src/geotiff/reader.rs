use crate::{
    ArrayInterop, ArrayMetadata, ArrayNum, DenseArray, GeoReference, RasterSize,
    geotiff::{GeoTiffMetadata, gdalghostdata::GdalGhostData, io, tileio, utils::HorizontalUnpredictable},
};

use inf::allocate;
use simd_macro::simd_bounds;

use crate::{Error, Result};
use std::{fs::File, io::Read, ops::Range, path::Path};

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

#[derive(Debug, Clone, Copy)]
pub struct TiffChunkLocation {
    pub offset: u64,
    pub size: u64,
}

impl TiffChunkLocation {
    pub fn is_sparse(&self) -> bool {
        self.offset == 0 && self.size == 0
    }

    pub fn range_to_fetch(&self) -> Range<u64> {
        if self.size == 0 {
            return Range { start: 0, end: 0 };
        }

        Range {
            start: self.offset,
            end: self.offset + self.size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PyramidInfo {
    pub raster_size: RasterSize,
    pub chunk_locations: Vec<TiffChunkLocation>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkDataLayout {
    Tiled(u32),   // Tile size in pixels
    Striped(u32), // Rows per strip
}

#[derive(Debug)]
pub struct GeoTiffReader {
    meta: GeoTiffMetadata,
    tiff_file: File,
}

impl GeoTiffReader {
    pub fn is_cog(path: &Path) -> bool {
        let mut header = vec![0u8; io::COG_HEADER_SIZE];
        match File::open(path) {
            Ok(mut file) => match file.read_exact(&mut header) {
                Ok(()) => {}
                Err(_) => return false,
            },
            Err(_) => return false,
        };

        GdalGhostData::from_tiff_header_buffer(&header).is_some_and(|ghost| ghost.is_cog())
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        Ok(GeoTiffReader {
            meta: GeoTiffMetadata::from_file(path)?,
            tiff_file: File::open(path)?,
        })
    }

    pub fn metadata(&self) -> &GeoTiffMetadata {
        &self.meta
    }

    pub fn pyramid_info(&self, index: usize) -> Option<&PyramidInfo> {
        self.meta.pyramids.get(index)
    }

    fn geo_ref(&self) -> &GeoReference {
        &self.meta.geo_reference
    }

    #[simd_bounds]
    fn read_tiled_raster_as<T: ArrayNum + HorizontalUnpredictable, M: ArrayMetadata>(
        &mut self,
        chunks: &[TiffChunkLocation],
        tile_size: u32,
    ) -> Result<DenseArray<T, M>> {
        let mut data = allocate::AlignedVecUnderConstruction::new(self.geo_ref().raster_size().cell_count());

        let right_edge_cols = self.geo_ref().columns().count() as usize % tile_size as usize;
        let tiles_per_row = (self.geo_ref().columns().count() as usize).div_ceil(tile_size as usize);

        let mut tile_buf = vec![T::NODATA; tile_size as usize * tile_size as usize];
        for (chunk_index, chunk_offset) in chunks.iter().enumerate() {
            let col_start = (chunk_index % tiles_per_row) * tile_size as usize;
            let row_start = chunk_index / tiles_per_row;
            let is_right_edge = (chunk_index + 1) % tiles_per_row == 0;
            let row_size = if is_right_edge { right_edge_cols } else { tile_size as usize };

            self.read_chunk_data_into_buffer_as(chunk_offset, &mut tile_buf)?;

            for (tile_row_index, tile_row_data) in tile_buf.chunks_mut(tile_size as usize).enumerate() {
                if row_start * tile_size as usize + tile_row_index >= self.geo_ref().rows().count() as usize {
                    break; // Skip rows that are outside the raster bounds
                }

                let index_start =
                    ((row_start * tile_size as usize + tile_row_index) * self.geo_ref().columns().count() as usize) + col_start;
                let data_slice = &mut unsafe { data.as_slice_mut() }[index_start..index_start + row_size];
                data_slice.copy_from_slice(&tile_row_data[0..row_size]);
            }
        }

        DenseArray::new_init_nodata(M::with_geo_reference(self.geo_ref().clone()), unsafe { data.assume_init() })
    }

    #[simd_bounds]
    fn read_striped_raster_as<T: ArrayNum + HorizontalUnpredictable, M: ArrayMetadata>(
        &mut self,
        chunks: &[TiffChunkLocation],
        rows_per_strip: u32,
    ) -> Result<DenseArray<T, M>> {
        let geo_ref = &self.meta.geo_reference;
        let mut data = allocate::AlignedVecUnderConstruction::new(geo_ref.raster_size().cell_count());

        let strip_size = self.meta.geo_reference.columns().count() as usize * rows_per_strip as usize;
        for (stripe_offset, stripe_buf) in chunks.iter().zip(unsafe { data.as_slice_mut() }.chunks_mut(strip_size)) {
            //debug_assert_eq!(stripe_offset.size as usize, stripe_buf.len());
            self.read_chunk_data_into_buffer_as(stripe_offset, stripe_buf)?;
        }

        DenseArray::new_init_nodata(M::with_geo_reference(self.geo_ref().clone()), unsafe { data.assume_init() })
    }

    #[simd_bounds]
    pub fn read_raster_as<T: ArrayNum + HorizontalUnpredictable, M: ArrayMetadata>(&mut self) -> Result<DenseArray<T, M>> {
        if let Some(pyramid) = self.meta.pyramids.first().cloned() {
            if pyramid.chunk_locations.is_empty() {
                return Err(Error::Runtime("No tiles available in the geotiff".into()));
            }

            match self.meta.data_layout {
                ChunkDataLayout::Tiled(tile_size) => {
                    return self.read_tiled_raster_as::<T, M>(&pyramid.chunk_locations, tile_size);
                }
                ChunkDataLayout::Striped(rows_per_strip) => {
                    return self.read_striped_raster_as::<T, M>(&pyramid.chunk_locations, rows_per_strip);
                }
            }
        }

        Err(Error::Runtime("No raster data available in the geotiff".into()))
    }

    #[simd_bounds]
    pub fn read_chunk_as<T: ArrayNum + HorizontalUnpredictable>(&mut self, chunk: &TiffChunkLocation) -> Result<DenseArray<T>> {
        let chunk_row_size = self.meta.chunk_row_length();

        if T::TYPE != self.meta.data_type {
            return Err(Error::InvalidArgument(format!(
                "Tile data type mismatch: expected {:?}, got {:?}",
                self.meta.data_type,
                T::TYPE
            )));
        }

        tileio::read_tile_data(
            chunk,
            chunk_row_size,
            self.meta.geo_reference.nodata(),
            self.meta.compression,
            self.meta.predictor,
            &mut self.tiff_file,
        )
    }

    #[simd_bounds]
    pub fn read_chunk_data_into_buffer_as<T: ArrayNum + HorizontalUnpredictable>(
        &mut self,
        chunk: &TiffChunkLocation,
        chunk_data: &mut [T],
    ) -> Result<()> {
        let row_length = self.meta.chunk_row_length();

        if T::TYPE != self.meta.data_type {
            return Err(Error::InvalidArgument(format!(
                "Tile data type mismatch: expected {:?}, got {:?}",
                self.meta.data_type,
                T::TYPE
            )));
        }

        io::read_chunk_data_into_buffer(
            chunk,
            row_length,
            self.meta.geo_reference.nodata(),
            self.meta.compression,
            self.meta.predictor,
            &mut self.tiff_file,
            chunk_data,
        )?;

        Ok(())
    }
}

#[cfg(feature = "gdal")]
#[cfg(test)]
mod tests {
    use crate::{
        Array as _, ArrayDataType, GeoReference, RasterSize, Tile, ZoomLevelStrategy,
        cog::{CogCreationOptions, PredictorSelection, create_cog_tiles},
        crs,
        geotiff::{Compression, Predictor},
        raster::{self, DenseRaster, GeoTiffWriteOptions, RasterIO, TiffChunkType, WriteRasterOptions},
        testutils,
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

        let src_ds = raster::io::dataset::open_read_only(input)?;
        raster::algo::warp_to_disk_cli(&src_ds, &output, &options, &creation_options)?;

        let tiff = GeoTiffReader::from_file(&output)?;
        let meta = tiff.metadata();
        assert_eq!(meta.data_layout, ChunkDataLayout::Striped(3));
        assert_eq!(meta.data_type, ArrayDataType::Uint8);
        assert_eq!(meta.compression, None);
        assert_eq!(meta.predictor, None);
        assert_eq!(meta.geo_reference.nodata(), Some(255.0));
        assert_eq!(meta.geo_reference.projected_epsg(), Some(crs::epsg::BELGIAN_LAMBERT72));
        assert_eq!(meta.pyramids.len(), 1);

        Ok(())
    }

    #[test_log::test]
    fn cog_metadata() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        create_test_cog(&input, &output, COG_TILE_SIZE, None, None, None, true)?;
        let mut cog = GeoTiffReader::from_file(&output)?;

        let meta = cog.metadata();
        assert_eq!(meta.data_layout, ChunkDataLayout::Tiled(COG_TILE_SIZE));
        assert_eq!(meta.data_type, ArrayDataType::Uint8);
        assert_eq!(meta.compression, None);
        assert_eq!(meta.predictor, None);
        assert_eq!(meta.geo_reference.nodata(), Some(255.0));
        assert_eq!(meta.geo_reference.projected_epsg(), Some(crs::epsg::WGS84_WEB_MERCATOR));
        assert_eq!(cog.metadata().pyramids.len(), 4); // zoom levels 7 to 10

        // Decode all cog tile
        for pyramid in cog.metadata().pyramids.clone().iter() {
            assert!(!pyramid.chunk_locations.is_empty(), "Pyramid tile locations should not be empty");

            for tile in &pyramid.chunk_locations {
                if tile.is_sparse() {
                    continue; // Skip empty tiles
                }

                let tile_data = cog.read_chunk_as::<u8>(tile)?;
                assert_eq!(tile_data.len(), RasterSize::square(COG_TILE_SIZE as i32).cell_count());
                let tile_data = cog.read_chunk_as::<u8>(tile)?;
                assert_eq!(tile_data.size(), RasterSize::square(COG_TILE_SIZE as i32));
            }
        }

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

        let cog = GeoTiffReader::from_file(&output)?;

        let meta = cog.metadata();
        assert_eq!(meta.data_layout, ChunkDataLayout::Tiled(opts.tile_size));
        assert_eq!(meta.data_type, opts.output_data_type.unwrap());
        assert_eq!(meta.compression, None);
        assert_eq!(meta.predictor, None);
        assert_eq!(meta.geo_reference.nodata(), Some(255.0));
        assert_eq!(meta.geo_reference.projected_epsg(), Some(crs::epsg::WGS84_WEB_MERCATOR));
        assert_eq!(meta.pyramids.len(), 7); // zoom levels 4 to 10

        Ok(())
    }

    #[test_log::test]
    fn read_striped_raster() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let mut geotiff = GeoTiffReader::from_file(&input)?;

        let raster = geotiff.read_raster_as::<u8, GeoReference>()?;
        let gdal_raster = DenseRaster::<u8>::read(input)?;

        assert_eq!(raster, gdal_raster);

        Ok(())
    }

    #[test_log::test]
    fn read_striped_raster_with_predictor() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let striped_geotiff = tmp.path().join("tiled_striped.tif");

        {
            // Create a copy of the landuse raster (striped) as a tiled GeoTIFF
            let mut ras = DenseRaster::<u8>::read(&input)?;
            let geo_tiff_options = GeoTiffWriteOptions {
                chunk_type: TiffChunkType::Striped,
                compression: Some(Compression::Lzw),
                predictor: Some(Predictor::Horizontal),
                ..Default::default()
            };

            ras.write_with_options(&striped_geotiff, WriteRasterOptions::GeoTiff(geo_tiff_options))?;
        }

        let mut geotiff = GeoTiffReader::from_file(&striped_geotiff)?;
        assert_eq!(geotiff.metadata().compression, Some(Compression::Lzw));
        assert_eq!(geotiff.metadata().predictor, Some(Predictor::Horizontal));

        let raster = geotiff.read_raster_as::<u8, GeoReference>()?;
        let gdal_raster = DenseRaster::<u8>::read(input)?;

        assert_eq!(raster, gdal_raster);

        Ok(())
    }

    #[test_log::test]
    fn read_tiled_raster() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let tiled_geotiff = tmp.path().join("tiled_geotiff.tif");

        {
            // Create a copy of the landuse raster (striped) as a tiled GeoTIFF
            let mut ras = DenseRaster::<u8>::read(&input)?;
            let geo_tiff_options = GeoTiffWriteOptions {
                chunk_type: TiffChunkType::Tiled,
                ..Default::default()
            };

            ras.write_with_options(&tiled_geotiff, WriteRasterOptions::GeoTiff(geo_tiff_options))?;
        }

        let mut geotiff = GeoTiffReader::from_file(&tiled_geotiff)?;
        assert_eq!(geotiff.metadata().data_layout, ChunkDataLayout::Tiled(256));

        let raster = geotiff.read_raster_as::<u8, GeoReference>()?;
        let gdal_raster = DenseRaster::<u8>::read(input)?;

        assert_eq!(raster, gdal_raster);

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

        let mut cog_no_compression = GeoTiffReader::from_file(&no_compression_output)?;
        let mut cog_lzw_compression = GeoTiffReader::from_file(&lzw_compression_output)?;

        for (pyramid_no_compression, pyramid_lzw) in cog_no_compression
            .metadata()
            .pyramids
            .clone()
            .iter()
            .zip(cog_lzw_compression.metadata().pyramids.clone().iter())
        {
            assert!(
                pyramid_no_compression.chunk_locations.len() == pyramid_lzw.chunk_locations.len(),
                "Pyramid tile locations should match in count"
            );

            for (tile, tile_lzw) in pyramid_no_compression
                .chunk_locations
                .iter()
                .zip(pyramid_lzw.chunk_locations.iter())
            {
                let tile_data_no_compression = cog_no_compression.read_chunk_as::<u8>(tile).unwrap();
                let tile_data_lzw_compression = cog_lzw_compression.read_chunk_as::<u8>(tile_lzw).unwrap();

                assert_eq!(tile_data_no_compression, tile_data_lzw_compression);
            }
        }

        Ok(())
    }
}

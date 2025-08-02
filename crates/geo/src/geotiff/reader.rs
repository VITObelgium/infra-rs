use crate::{
    ArrayInterop, ArrayMetadata, ArrayNum, DenseArray, GeoReference, RasterSize,
    geotiff::{GeoTiffMetadata, io, utils::HorizontalUnpredictable},
};

use inf::allocate;
use num::NumCast;
use simd_macro::simd_bounds;

use crate::{Error, Result};
use std::{fs::File, ops::Range, path::Path};

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
pub struct TiffOverview {
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
    pub fn from_file(path: &Path) -> Result<Self> {
        Ok(GeoTiffReader {
            meta: GeoTiffMetadata::from_file(path)?,
            tiff_file: File::open(path)?,
        })
    }

    pub fn metadata(&self) -> &GeoTiffMetadata {
        &self.meta
    }

    pub fn overview_info(&self, index: usize) -> Option<&TiffOverview> {
        self.meta.overviews.get(index)
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
        self.read_overview_as(0)
    }

    #[simd_bounds]
    /// Reads an overview raster at the specified index
    /// overview 0 is the full resolution raster, and each subsequent overview is a downsampled version.
    pub fn read_overview_as<T: ArrayNum + HorizontalUnpredictable, M: ArrayMetadata>(
        &mut self,
        overview_index: usize,
    ) -> Result<DenseArray<T, M>> {
        if let Some(overview) = self.meta.overviews.get(overview_index).cloned() {
            if overview.chunk_locations.is_empty() {
                return Err(Error::Runtime("No tiles available in the geotiff".into()));
            }

            match self.meta.data_layout {
                ChunkDataLayout::Tiled(tile_size) => {
                    return self.read_tiled_raster_as::<T, M>(&overview.chunk_locations, tile_size);
                }
                ChunkDataLayout::Striped(rows_per_strip) => {
                    return self.read_striped_raster_as::<T, M>(&overview.chunk_locations, rows_per_strip);
                }
            }
        }

        Err(Error::Runtime(format!("No overview available with index {overview_index}")))
    }

    #[simd_bounds]
    fn read_chunk_data_into_buffer_as<T: ArrayNum + HorizontalUnpredictable>(
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

        if chunk.is_sparse() {
            // Sparse tiles are filled with nodata value
            chunk_data.fill(self.meta.geo_reference.nodata().and_then(NumCast::from).unwrap_or(T::NODATA));
        } else {
            io::read_chunk_data_into_buffer(
                chunk,
                row_length,
                self.meta.geo_reference.nodata(),
                self.meta.compression,
                self.meta.predictor,
                &mut self.tiff_file,
                chunk_data,
            )?;
        }

        Ok(())
    }
}

#[cfg(feature = "gdal")]
#[cfg(test)]
mod tests {
    use crate::{
        ArrayDataType, GeoReference, RasterMetadata, ZoomLevelStrategy,
        cog::{CogCreationOptions, PredictorSelection, create_cog_tiles},
        geotiff::{Compression, Predictor},
        raster::{DenseRaster, GeoTiffWriteOptions, RasterIO, TiffChunkType, WriteRasterOptions},
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

        for overview_index in 0..cog_no_compression.metadata().overviews.len() {
            let overview_no_compression = cog_no_compression.read_overview_as::<u8, RasterMetadata>(overview_index)?;
            let overview_lzw = cog_lzw_compression.read_overview_as::<u8, RasterMetadata>(overview_index)?;

            assert_eq!(overview_no_compression, overview_lzw);
        }

        Ok(())
    }
}

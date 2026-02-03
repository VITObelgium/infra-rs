use crate::geotiff::utils;
use crate::raster::intersection::{CutOut, intersect_georeference};
use crate::{
    ArrayInterop, ArrayMetadata, ArrayNum, Cell, Columns, DenseArray, GeoReference, RasterSize, Rows,
    geotiff::{BandIndex, FIRST_BAND, GeoTiffMetadata, io},
};

use inf::{allocate, cast};
use num::NumCast;
use simd_macro::simd_bounds;

use crate::{Error, Result, raster};
use std::{fs::File, mem::MaybeUninit, ops::Range, path::Path};

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
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
    fn read_tiled_raster_band_as<T: ArrayNum, M: ArrayMetadata>(
        &mut self,
        overview: &TiffOverview,
        band_index: BandIndex,
        tile_size: u32,
    ) -> Result<DenseArray<T, M>> {
        let nodata = cast::option::<T>(self.geo_ref().nodata()).unwrap_or(T::NODATA);
        let mut data = allocate::aligned_vec_filled_with(nodata, overview.raster_size.cell_count());
        let georef = Self::read_tiled_raster_band_into_buffer(&self.meta, overview, band_index, tile_size, &mut self.tiff_file, &mut data)?;
        DenseArray::new_init_nodata(M::with_geo_reference(georef), data)
    }

    #[simd_bounds]
    fn read_tiled_raster_band_into_buffer<T: ArrayNum, M: ArrayMetadata>(
        meta: &GeoTiffMetadata,
        overview: &TiffOverview,
        band_index: BandIndex,
        tile_size: u32,
        tiff_file: &mut File,
        buffer: &mut [T],
    ) -> Result<M> {
        io::merge_overview_into_buffer(meta, overview, band_index, tile_size, buffer, |chunk| {
            let mut buf = vec![0; chunk.size as usize];
            io::read_chunk(&chunk, tiff_file, &mut buf)?;
            Ok(buf)
        })
    }

    #[simd_bounds]
    fn read_striped_raster_band_as<T: ArrayNum, M: ArrayMetadata>(
        &mut self,
        band_index: BandIndex,
        chunks: &[TiffChunkLocation],
        rows_per_strip: u32,
    ) -> Result<DenseArray<T, M>> {
        let raster_size = self.meta.geo_reference.raster_size();
        let mut data = allocate::AlignedVecUnderConstruction::new(raster_size.cell_count());
        let georef =
            self.read_striped_raster_band_into_buffer::<T, M>(band_index, chunks, rows_per_strip, unsafe { data.as_slice_mut() })?;
        DenseArray::new_init_nodata(georef, unsafe { data.assume_init() })
    }

    #[simd_bounds]
    fn read_striped_raster_band_into_buffer<T: ArrayNum, M: ArrayMetadata>(
        &mut self,
        band_index: BandIndex,
        chunks: &[TiffChunkLocation],
        rows_per_strip: u32,
        buffer: &mut [T],
    ) -> Result<M> {
        if band_index.get() > 1 {
            return Err(Error::Runtime(format!(
                "Multiband GeoTIFF with striped layout not implemented. Requested band index: {}",
                band_index.get()
            )));
        }

        let geo_ref = self.meta.geo_reference.clone();
        let strip_size = geo_ref.columns().count() as usize * rows_per_strip as usize;
        for (stripe_offset, stripe_buf) in chunks.iter().zip(buffer.chunks_mut(strip_size)) {
            Self::read_chunk_data_into_buffer_as(&self.meta, stripe_offset, &mut self.tiff_file, stripe_buf)?;
        }

        Ok(M::with_geo_reference(geo_ref))
    }

    #[simd_bounds]
    pub fn read_raster_as<T: ArrayNum, M: ArrayMetadata>(&mut self) -> Result<DenseArray<T, M>> {
        self.read_overview_band_as(0, FIRST_BAND)
    }

    #[simd_bounds]
    pub fn read_raster_band_as<T: ArrayNum, M: ArrayMetadata>(&mut self, band_index: BandIndex) -> Result<DenseArray<T, M>> {
        self.read_overview_band_as(0, band_index)
    }

    #[simd_bounds]
    pub fn read_raster_into_buffer<T: ArrayNum, M: ArrayMetadata>(&mut self, dst_data: &mut [std::mem::MaybeUninit<T>]) -> Result<M> {
        self.read_overview_band_into_buffer::<T, M>(0, FIRST_BAND, dst_data)
    }

    /// Reads a band from an overview raster at the specified index
    /// overview 0 is the full resolution raster, and each subsequent overview is a downsampled version.
    /// `band_index` is 1 based.
    #[simd_bounds]
    pub fn read_overview_band_as<T: ArrayNum, M: ArrayMetadata>(
        &mut self,
        overview_index: usize,
        band_index: BandIndex,
    ) -> Result<DenseArray<T, M>> {
        if let Some(overview) = self.meta.overviews.get(overview_index).cloned() {
            if overview.chunk_locations.is_empty() {
                return Err(Error::Runtime("No tiles available in the geotiff".into()));
            }

            match self.meta.data_layout {
                ChunkDataLayout::Tiled(tile_size) => {
                    return self.read_tiled_raster_band_as::<T, M>(&overview, band_index, tile_size);
                }
                ChunkDataLayout::Striped(rows_per_strip) => {
                    return self.read_striped_raster_band_as::<T, M>(band_index, &overview.chunk_locations, rows_per_strip);
                }
            }
        }

        Err(Error::Runtime(format!("No overview available with index {overview_index}")))
    }

    /// Reads an overview raster at the specified index
    /// overview 0 is the full resolution raster, and each subsequent overview is a downsampled version.
    #[simd_bounds]
    pub fn read_band_region_into_buffer<T: ArrayNum, M: ArrayMetadata>(
        &mut self,
        band_index: BandIndex,
        region: &GeoReference,
        buffer: &mut [MaybeUninit<T>],
    ) -> Result<M> {
        self.read_overview_region_into_buffer(0, band_index, region, buffer)
    }

    /// Reads an overview raster at the specified index
    /// overview 0 is the full resolution raster, and each subsequent overview is a downsampled version.
    #[simd_bounds]
    pub fn read_overview_band_into_buffer<T: ArrayNum, M: ArrayMetadata>(
        &mut self,
        overview_index: usize,
        band_index: BandIndex,
        buffer: &mut [MaybeUninit<T>],
    ) -> Result<M> {
        if let Some(overview) = self.meta.overviews.get(overview_index).cloned() {
            if overview.chunk_locations.is_empty() {
                return Err(Error::Runtime("No tiles available in the geotiff".into()));
            }

            // Cast away the maybe uninit - we will fill the entire buffer
            let buffer = raster::utils::cast_away_uninit_mut(buffer);

            match self.meta.data_layout {
                ChunkDataLayout::Tiled(tile_size) => {
                    return Self::read_tiled_raster_band_into_buffer(
                        &self.meta,
                        &overview,
                        band_index,
                        tile_size,
                        &mut self.tiff_file,
                        buffer,
                    );
                }
                ChunkDataLayout::Striped(rows_per_strip) => {
                    return self.read_striped_raster_band_into_buffer(band_index, &overview.chunk_locations, rows_per_strip, buffer);
                }
            }
        }

        Err(Error::Runtime(format!("No overview available with index {overview_index}")))
    }

    /// Reads an overview raster at the specified index
    /// overview 0 is the full resolution raster, and each subsequent overview is a downsampled version.
    #[simd_bounds]
    pub fn read_overview_region_into_buffer<T: ArrayNum, M: ArrayMetadata>(
        &mut self,
        overview_index: usize,
        band_index: BandIndex,
        extent: &crate::GeoReference,
        buffer: &mut [MaybeUninit<T>],
    ) -> Result<M> {
        let nodata = MaybeUninit::new(self.geo_ref().nodata().and_then(NumCast::from).unwrap_or(T::NODATA));

        if let Some(overview) = self.meta.overviews.get(overview_index).cloned() {
            if overview.chunk_locations.is_empty() {
                buffer.fill(nodata);
                return Ok(ArrayMetadata::with_geo_reference(extent.clone()));
            }

            let intersection = intersect_georeference(&self.metadata().geo_reference, extent)?;
            if intersection.dst_col_offset > 0
                || intersection.dst_row_offset > 0
                || intersection.cols + intersection.dst_col_offset < extent.columns().count()
                || intersection.rows + intersection.dst_row_offset < extent.rows().count()
            {
                // The requested extent is partially outside the raster bounds, fill with nodata first
                buffer.fill(nodata);
            }

            // Cast away the maybe uninit - we will fill the entire buffer
            let buffer = raster::utils::cast_away_uninit_mut(buffer);
            match self.meta.data_layout {
                ChunkDataLayout::Tiled(tile_size) => {
                    let chunk_tiles = Self::calculate_chunk_tiles_for_extent(
                        &overview,
                        overview_index,
                        band_index,
                        self.meta.band_count as usize,
                        self.geo_ref(),
                        extent,
                        tile_size,
                    )?;
                    utils::merge_tile_chunks_into_buffer(&self.meta, extent, tile_size, &chunk_tiles, &mut self.tiff_file, buffer)?;
                    return Ok(M::with_geo_reference(extent.clone()));
                }
                ChunkDataLayout::Striped(rows_per_strip) => {
                    let chunk_tiles =
                        Self::calculate_chunk_strips_for_extent(&overview, overview_index, self.geo_ref(), extent, rows_per_strip)?;
                    utils::merge_strip_chunks_into_buffer(&self.meta, extent, rows_per_strip, &chunk_tiles, &mut self.tiff_file, buffer)?;
                    return Ok(M::with_geo_reference(extent.clone()));
                    //return self.read_striped_raster_into_buffer(&overview.chunk_locations, rows_per_strip, buffer);
                }
            }
        }

        Err(Error::Runtime(format!("No overview available with index {overview_index}")))
    }

    #[simd_bounds]
    fn read_chunk_data_into_buffer_as<T: ArrayNum>(
        meta: &GeoTiffMetadata,
        chunk: &TiffChunkLocation,
        tiff_file: &mut File,
        chunk_data: &mut [T],
    ) -> Result<()> {
        let row_length = meta.chunk_row_length();

        if T::TYPE != meta.data_type {
            return Err(Error::InvalidArgument(format!(
                "Tile data type mismatch: expected {:?}, got {:?}",
                meta.data_type,
                T::TYPE
            )));
        }

        // io function handles the sparse check
        io::read_chunk_data_into_buffer(
            chunk,
            row_length,
            meta.geo_reference.nodata(),
            meta.compression,
            meta.predictor,
            tiff_file,
            chunk_data,
        )?;

        Ok(())
    }

    /// Calculates the chunks needed and their location in the cutout area
    fn calculate_chunk_tiles_for_extent(
        overview: &TiffOverview,
        overview_index: usize, // index of the overview to use, 0 is full resolution
        band_index: BandIndex,
        band_count: usize,
        geo_reference: &GeoReference, // georeference of the full cog image
        cutout: &GeoReference,        // georeference of the cutout area
        block_size: u32,
    ) -> Result<Vec<(TiffChunkLocation, CutOut)>> {
        let mut chunk_tiles = Vec::default();

        let cell_size = geo_reference.cell_size() / (overview_index as f64 + 1.0);
        let geo_ref_overview = utils::change_georef_cell_size(geo_reference, cell_size);

        let tiles_wide = (overview.raster_size.cols.count() as u32).div_ceil(block_size) as usize;
        let tiles_high = (overview.raster_size.rows.count() as u32).div_ceil(block_size) as usize;
        assert!(
            tiles_wide * tiles_high == overview.chunk_locations.len(),
            "Expected {} tiles, but got {}",
            tiles_wide * tiles_high,
            overview.chunk_locations.len()
        );

        assert!(overview.chunk_locations.len().is_multiple_of(band_count));
        let chunks_per_band = overview.chunk_locations.len() / band_count;

        let top_left_cell = geo_reference.point_to_cell(cutout.top_left());
        let bottom_right_cell = geo_reference.point_to_cell(cutout.bottom_right());

        let block_size = block_size as i32;
        let min_tile_x = (top_left_cell.col / block_size).clamp(0, tiles_wide as i32 - 1) as usize;
        let max_tile_x = (bottom_right_cell.col / block_size).clamp(0, tiles_wide as i32 - 1) as usize;
        let min_tile_y = (top_left_cell.row / block_size).clamp(0, tiles_high as i32 - 1) as usize;
        let max_tile_y = (bottom_right_cell.row / block_size).clamp(0, tiles_high as i32 - 1) as usize;

        for ty in min_tile_y..=max_tile_y {
            let mut current_source_cell = Cell::from_row_col(ty as i32 * block_size, 0);
            // Calculate the actual height of this chunk (may be smaller at the edges)
            let chunk_height = Rows(if current_source_cell.row + block_size > overview.raster_size.rows.count() {
                debug_assert!(ty + 1 == tiles_high);
                overview.raster_size.rows.count() - current_source_cell.row
            } else {
                block_size
            });

            for tx in min_tile_x..=max_tile_x {
                current_source_cell.col = tx as i32 * block_size;
                let chunk_width = Columns(if current_source_cell.col + block_size > overview.raster_size.cols.count() {
                    debug_assert!(tx + 1 == tiles_wide);
                    overview.raster_size.cols.count() - current_source_cell.col
                } else {
                    block_size
                });

                let lower_left_cell = Cell::from_row_col(current_source_cell.row + chunk_height.count() - 1, current_source_cell.col);

                let chunk_geo_ref = GeoReference::with_bottom_left_origin(
                    String::default(),
                    RasterSize::with_rows_cols(chunk_height, chunk_width),
                    geo_ref_overview.cell_lower_left(lower_left_cell),
                    cell_size,
                    Option::<f64>::None,
                );

                let band_index0 = band_index.get() - 1; // to 0-based index
                let tiff_chunk = &overview.chunk_locations[(ty * tiles_wide + tx) + (band_index0 * chunks_per_band)];
                let cutout_offsets = intersect_georeference(&chunk_geo_ref, cutout)?;
                debug_assert!(cutout_offsets.cols > 0 && cutout_offsets.rows > 0);

                chunk_tiles.push((*tiff_chunk, cutout_offsets));
            }
        }

        Ok(chunk_tiles)
    }

    /// Calculates the chunks needed and their location in the cutout area
    fn calculate_chunk_strips_for_extent(
        overview: &TiffOverview,
        overview_index: usize,        // index of the overview to use, 0 is full resolution
        geo_reference: &GeoReference, // georeference of the full cog image
        cutout: &GeoReference,        // georeference of the cutout area
        rows_per_strip: u32,
    ) -> Result<Vec<(TiffChunkLocation, GeoReference, bool)>> {
        let mut chunk_tiles = Vec::default();

        let cell_size = geo_reference.cell_size() / (overview_index as f64 + 1.0);
        let geo_ref_overview = utils::change_georef_cell_size(geo_reference, cell_size);

        let number_of_strips = overview.chunk_locations.len();

        let top_left_cell = geo_reference.point_to_cell(cutout.top_left());
        let bottom_right_cell = geo_reference.point_to_cell(cutout.bottom_right());

        let rows_per_strip = rows_per_strip as i32;
        let min_strip = (top_left_cell.row / rows_per_strip).clamp(0, number_of_strips as i32 - 1);
        let max_strip = (bottom_right_cell.row / rows_per_strip).clamp(0, number_of_strips as i32 - 1);

        for strip in min_strip..=max_strip {
            let current_source_cell = Cell::from_row_col(strip * rows_per_strip, 0);
            // Calculate the actual height of this chunk (may be smaller at the edges)
            let chunk_height = Rows(if current_source_cell.row + rows_per_strip > overview.raster_size.rows.count() {
                debug_assert!(strip + 1 == number_of_strips as i32);
                overview.raster_size.rows.count() - current_source_cell.row
            } else {
                rows_per_strip
            });

            let lower_left_cell = Cell::from_row_col(current_source_cell.row + chunk_height.count() - 1, current_source_cell.col);

            let chunk_geo_ref = GeoReference::with_bottom_left_origin(
                String::default(),
                RasterSize::with_rows_cols(chunk_height, geo_reference.columns()),
                geo_ref_overview.cell_lower_left(lower_left_cell),
                cell_size,
                Option::<f64>::None,
            );

            chunk_tiles.push((
                overview.chunk_locations[strip as usize],
                chunk_geo_ref,
                strip == number_of_strips as i32 - 1,
            ));
        }

        Ok(chunk_tiles)
    }
}

#[cfg(feature = "gdal")]
#[cfg(test)]
mod tests {
    use crate::{
        ArrayDataType, GeoReference, RasterMetadata, ZoomLevelStrategy,
        cog::{CogCreationOptions, PredictorSelection, create_cog_tiles},
        geotiff::gdalghostdata::Interleave,
        raster::{Compression, DenseRaster, GeoTiffWriteOptions, Predictor, RasterReadWrite, TiffChunkType, WriteRasterOptions},
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
    fn read_multiband_raster_tiled_interleave_band() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("multiband_cog_interleave_band.tif");

        let mut geotiff = GeoTiffReader::from_file(&input)?;
        assert_eq!(5, geotiff.metadata().band_count);
        assert_eq!(geotiff.metadata().data_layout, ChunkDataLayout::Tiled(512));
        assert_eq!(geotiff.metadata().interleave, Interleave::Band);

        for band_index in 1..=5 {
            let band = BandIndex::new(band_index).expect("band indices are 1-based");
            let raster_band = geotiff.read_raster_band_as::<u8, GeoReference>(band)?;
            let gdal_band = DenseRaster::<u8>::read_band(&input, band_index)?;
            assert_eq!(raster_band, gdal_band);
        }

        Ok(())
    }

    #[test_log::test]
    fn read_multiband_raster_tiled_interleave_tile() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("multiband_cog_interleave_tile.tif");

        let mut geotiff = GeoTiffReader::from_file(&input)?;
        assert_eq!(5, geotiff.metadata().band_count);
        assert_eq!(geotiff.metadata().data_layout, ChunkDataLayout::Tiled(512));
        assert_eq!(geotiff.metadata().interleave, Interleave::Tile);

        for band_index in 1..=5 {
            let band = BandIndex::new(band_index).expect("band indices are 1-based");
            let raster_band = geotiff.read_raster_band_as::<u8, GeoReference>(band)?;
            let gdal_band = DenseRaster::<u8>::read_band(&input, band_index)?;
            assert_eq!(raster_band, gdal_band);
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

        let zstd_compression_output = tmp.path().join("cog_zstd_compression.tif");
        create_test_cog(
            &input,
            &zstd_compression_output,
            COG_TILE_SIZE,
            Some(Compression::Zstd),
            None,
            None,
            true,
        )?;

        #[cfg(feature = "deflate")]
        let deflate_compression_output = tmp.path().join("cog_deflate_compression.tif");
        #[cfg(feature = "deflate")]
        create_test_cog(
            &input,
            &deflate_compression_output,
            COG_TILE_SIZE,
            Some(Compression::Deflate),
            None,
            None,
            true,
        )?;

        let mut cog_no_compression = GeoTiffReader::from_file(&no_compression_output)?;
        let mut cog_lzw_compression = GeoTiffReader::from_file(&lzw_compression_output)?;
        let mut cog_zstd_compression = GeoTiffReader::from_file(&zstd_compression_output)?;
        #[cfg(feature = "deflate")]
        let mut cog_deflate_compression = GeoTiffReader::from_file(&deflate_compression_output)?;

        let band_index = FIRST_BAND;
        for overview_index in 0..cog_no_compression.metadata().overviews.len() {
            let overview_no_compression = cog_no_compression.read_overview_band_as::<u8, RasterMetadata>(overview_index, band_index)?;
            let overview_lzw = cog_lzw_compression.read_overview_band_as::<u8, RasterMetadata>(overview_index, band_index)?;
            let overview_zstd = cog_zstd_compression.read_overview_band_as::<u8, RasterMetadata>(overview_index, band_index)?;

            assert_eq!(overview_no_compression, overview_lzw);
            assert_eq!(overview_no_compression, overview_zstd);

            #[cfg(feature = "deflate")]
            {
                let overview_deflate = cog_deflate_compression.read_overview_band_as::<u8, RasterMetadata>(overview_index, band_index)?;
                assert_eq!(overview_no_compression, overview_deflate);
            }
        }

        Ok(())
    }
}

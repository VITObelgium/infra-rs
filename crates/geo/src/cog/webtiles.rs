use crate::{
    AnyDenseArray, Array as _, ArrayDataType, ArrayMetadata as _, ArrayNum, Cell, CellSize, Columns, DenseArray, Error, GeoReference,
    RasterMetadata, Rect, Result, Rows, Window,
    cog::{CogTileLocation, HorizontalUnpredictable, io, reader::PyramidInfo},
    raster::io::{CutOut, dataset::intersect_metadata},
};
use std::{
    collections::HashMap,
    io::{Read, Seek},
};

use crate::{
    LatLonBounds, Point, RasterSize, Tile,
    cog::{CogAccessor, CogMetadata},
    crs,
};

use inf::allocate;
use num::NumCast;
use simd_macro::simd_bounds;

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

#[derive(Debug, Clone)]
pub enum TileSource {
    Aligned(CogTileLocation),
    Unaligned(Vec<(CogTileLocation, CutOut)>),
}

#[derive(Debug, Clone)]
pub struct WebTiles {
    zoom_levels: Vec<HashMap<Tile, TileSource>>,
}

impl WebTiles {
    pub fn from_cog_metadata(meta: &CogMetadata) -> Self {
        let mut zoom_levels = vec![HashMap::default(); 22];

        for pyramid in &meta.pyramids {
            // log::info!("Aligned {current_zoom} {}x{}", image_width, image_height);

            let tile_aligned = pyramid.raster_size.cols.count() % meta.tile_size as i32 == 0
                && pyramid.raster_size.rows.count() % meta.tile_size as i32 == 0;

            if tile_aligned {
                let tiles = generate_tiles_for_extent(
                    meta.geo_reference.geo_transform(),
                    pyramid.raster_size,
                    meta.tile_size,
                    pyramid.zoom_level,
                );
                tiles.into_iter().zip(&pyramid.tile_locations).for_each(|(web_tile, cog_tile)| {
                    zoom_levels[web_tile.z as usize].insert(web_tile, TileSource::Aligned(*cog_tile));
                });
            } else {
                let tiles = generate_tiles_for_extent_unaligned(&meta.geo_reference, pyramid, meta.tile_size);
                let cog_tile_bounds = create_cog_tile_web_mercator_bounds(pyramid, &meta.geo_reference, meta.tile_size).unwrap();

                log::info!(
                    "Unaligned zoom level: {} Web tiles {} Cog tiles {}",
                    pyramid.zoom_level,
                    tiles.len(),
                    pyramid.tile_locations.len(),
                );

                let pixel_size = Tile::pixel_size_at_zoom_level(pyramid.zoom_level);

                for tile in &tiles {
                    let mut tile_sources = Vec::new();

                    let web_tile_georef = GeoReference::from_tile(tile, meta.tile_size as usize, 1);

                    for (cog_tile, bounds) in &cog_tile_bounds {
                        let cog_tile_georef = GeoReference::with_origin(
                            "",
                            RasterSize::square(meta.tile_size as i32),
                            bounds.bottom_left(),
                            CellSize::square(pixel_size),
                            Option::<f64>::None,
                        );

                        if tile.web_mercator_bounds().intersects(bounds) {
                            if let Ok(cutout) = intersect_metadata(&cog_tile_georef, &web_tile_georef) {
                                //log::info!("Tile {tile:?} Cutout {cutout:?}");
                                tile_sources.push((*cog_tile, cutout));
                            }
                            // } else {
                            //     log::error!("---- Failed Tile intersection ----");
                            // }

                            // let intersection = tile.web_mercator_bounds().intersection(bounds);
                            // let cols = (intersection.width() / pixel_size).round() as u32;
                            // let rows = (intersection.height() / pixel_size).round() as u32;
                        }
                    }

                    if !tile_sources.is_empty() {
                        zoom_levels[tile.z as usize].insert(*tile, TileSource::Unaligned(tile_sources));
                    }
                }
            }
        }

        trim_empty_zoom_levels(&mut zoom_levels);

        WebTiles { zoom_levels }
    }

    pub fn tile_source(&self, tile: &Tile) -> Option<&TileSource> {
        self.zoom_levels.get(tile.z as usize).and_then(|level| level.get(tile))
    }

    pub fn zoom_level_tile_sources(&self, zoom_level: u8) -> Option<&HashMap<Tile, TileSource>> {
        if zoom_level as usize >= self.zoom_levels.len() {
            return None;
        }

        Some(&self.zoom_levels[zoom_level as usize])
    }

    pub fn min_zoom(&self) -> i32 {
        let mut min_zoom = 0;
        for zoom_level in &self.zoom_levels {
            if zoom_level.is_empty() {
                min_zoom += 1;
            } else {
                break;
            }
        }

        min_zoom
    }

    pub fn max_zoom(&self) -> i32 {
        (self.zoom_levels.len() - 1) as i32
    }

    /// Returns the bounds of the tiles that contain data at the maximum zoom level.
    pub fn data_bounds(&self) -> LatLonBounds {
        let mut min_tile_x = i32::MAX;
        let mut max_tile_x = i32::MIN;
        let mut min_tile_y = i32::MAX;
        let mut max_tile_y = i32::MIN;

        if let Some(last_zoom_level) = self.zoom_levels.last() {
            for (tile, _) in last_zoom_level.iter().filter(|(_, loc)| match loc {
                TileSource::Aligned(loc) => loc.size > 0,
                TileSource::Unaligned(_) => false, // Max zoom levels should not have unaligned tiles
            }) {
                min_tile_x = min_tile_x.min(tile.x);
                max_tile_x = max_tile_x.max(tile.x);
                min_tile_y = min_tile_y.min(tile.y);
                max_tile_y = max_tile_y.max(tile.y);
            }
        }

        let max_zoom = self.max_zoom();

        let min_tile = Tile {
            z: max_zoom,
            x: min_tile_x,
            y: min_tile_y,
        };

        let max_tile = Tile {
            z: max_zoom,
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

fn trim_empty_zoom_levels(zoom_levels: &mut Vec<HashMap<Tile, TileSource>>) {
    // Remove empty zoom levels from the end
    while let Some(last) = zoom_levels.last() {
        if last.is_empty() {
            zoom_levels.pop();
        } else {
            break;
        }
    }
}

fn generate_tiles_for_extent(geo_transform: [f64; 6], raster_size: RasterSize, tile_size: u16, zoom: i32) -> Vec<Tile> {
    let top_left = crs::web_mercator_to_lat_lon(Point::new(geo_transform[0], geo_transform[3]));
    let top_left_tile = Tile::for_coordinate(top_left, zoom);

    let tiles_wide = (raster_size.cols.count() as u16).div_ceil(tile_size);
    let tiles_high = (raster_size.rows.count() as u16).div_ceil(tile_size);

    let mut tiles = Vec::new();
    // Iteration has to be done in row-major order so the tiles match the order of the tile lists from the COG
    for ty in 0..tiles_high {
        for tx in 0..tiles_wide {
            let tile = Tile {
                z: zoom,
                x: top_left_tile.x + tx as i32,
                y: top_left_tile.y + ty as i32,
            };

            tiles.push(tile);
        }
    }

    tiles
}

fn generate_tiles_for_extent_unaligned(geo_ref: &GeoReference, pyramid: &PyramidInfo, tile_size: u16) -> Vec<Tile> {
    // The geo_transform is from the highest zoom level, the origin does not match unaligned zoom levels

    let top_left = crs::web_mercator_to_lat_lon(geo_ref.top_left());
    let bottom_right = crs::web_mercator_to_lat_lon(geo_ref.bottom_right());

    let top_left_tile = Tile::for_coordinate(top_left, pyramid.zoom_level);
    let bottom_right_tile = Tile::for_coordinate(bottom_right, pyramid.zoom_level);

    let tiles_wide = bottom_right_tile.x - top_left_tile.x + 1;
    let tiles_high = bottom_right_tile.y - top_left_tile.y + 1;

    let mut tiles = Vec::new();
    // Iteration has to be done in row-major order so the tiles match the order of the tile lists from the COG
    for ty in 0..tiles_high {
        for tx in 0..tiles_wide {
            let tile = Tile {
                z: pyramid.zoom_level,
                x: top_left_tile.x + tx,
                y: top_left_tile.y + ty,
            };

            tiles.push(tile);
        }
    }

    tiles
}

fn create_cog_tile_web_mercator_bounds(
    pyramid: &PyramidInfo,
    geo_reference: &GeoReference,
    tile_size: u16,
) -> Result<Vec<(CogTileLocation, Rect<f64>)>> {
    let mut web_tiles = Vec::with_capacity(pyramid.tile_locations.len());

    let tiles_wide = (pyramid.raster_size.cols.count() as u16).div_ceil(tile_size) as usize;
    let tiles_high = (pyramid.raster_size.rows.count() as u16).div_ceil(tile_size) as usize;

    if tiles_wide * tiles_high != pyramid.tile_locations.len() {
        return Err(Error::InvalidArgument(format!(
            "Expected {} tiles, but got {}",
            tiles_wide * tiles_high,
            pyramid.tile_locations.len()
        )));
    }

    let tile_x_offset = tile_size as f64 * Tile::pixel_size_at_zoom_level(pyramid.zoom_level);
    let tile_y_offset = -tile_x_offset; // Y coordinates decrease from top to bottom in web mercator

    for ty in 0..tiles_high {
        let y_offset = ty as f64 * tile_y_offset;
        for tx in 0..tiles_wide {
            let cog_tile = &pyramid.tile_locations[ty * tiles_wide + tx];
            let x_offset = tx as f64 * tile_x_offset;

            let offset = Point::new(x_offset, y_offset);
            let top_left = geo_reference.top_left() + offset;
            let bottom_right = top_left + Point::new(tile_x_offset, tile_y_offset);

            web_tiles.push((*cog_tile, Rect::from_nw_se(top_left, bottom_right)));
        }
    }

    Ok(web_tiles)
}

pub struct WebTileInfo {
    pub min_zoom: i32,
    pub max_zoom: i32,
    pub bounds: LatLonBounds,
}

#[derive(Debug, Clone)]
pub struct WebTilesReader {
    web_tiles: WebTiles,
    cog: CogAccessor,
}

impl WebTilesReader {
    pub fn data_type(&self) -> ArrayDataType {
        self.cog.metadata().data_type
    }

    pub fn from_cog(cog: CogAccessor) -> Result<Self> {
        let web_tiles = WebTiles::from_cog_metadata(cog.metadata());

        Ok(Self { web_tiles, cog })
    }

    pub fn tile_info(&self) -> WebTileInfo {
        WebTileInfo {
            min_zoom: self.web_tiles.min_zoom(),
            max_zoom: self.web_tiles.max_zoom(),
            bounds: self.data_bounds(),
        }
    }

    pub fn data_bounds(&self) -> LatLonBounds {
        self.web_tiles.data_bounds()
    }

    pub fn cog_metadata(&self) -> &CogMetadata {
        self.cog.metadata()
    }

    fn tile_source(&self, tile: &Tile) -> Option<&TileSource> {
        self.web_tiles.tile_source(tile)
    }

    pub fn zoom_level_tile_sources(&self, zoom_level: u8) -> Option<&HashMap<Tile, TileSource>> {
        self.web_tiles.zoom_level_tile_sources(zoom_level)
    }

    pub fn pyramid_info(&self, zoom_level: u8) -> Option<&PyramidInfo> {
        self.cog_metadata().pyramids.iter().find(|p| p.zoom_level == zoom_level as i32)
    }

    /// Read the tile data for the given tile using the provided reader.
    /// This method will return an error if the tile does not exist in the COG index
    /// If this is a COG with sparse tile support, for sparse tiles an empty array will be returned
    pub fn read_tile_data(&self, tile: &Tile, mut reader: impl Read + Seek) -> Result<Option<AnyDenseArray>> {
        Ok(match self.data_type() {
            ArrayDataType::Uint8 => self.read_tile_data_as::<u8>(tile, &mut reader)?.map(AnyDenseArray::U8),
            ArrayDataType::Uint16 => self.read_tile_data_as::<u16>(tile, &mut reader)?.map(AnyDenseArray::U16),
            ArrayDataType::Uint32 => self.read_tile_data_as::<u32>(tile, &mut reader)?.map(AnyDenseArray::U32),
            ArrayDataType::Uint64 => self.read_tile_data_as::<u64>(tile, &mut reader)?.map(AnyDenseArray::U64),
            ArrayDataType::Int8 => self.read_tile_data_as::<i8>(tile, &mut reader)?.map(AnyDenseArray::I8),
            ArrayDataType::Int16 => self.read_tile_data_as::<i16>(tile, &mut reader)?.map(AnyDenseArray::I16),
            ArrayDataType::Int32 => self.read_tile_data_as::<i32>(tile, &mut reader)?.map(AnyDenseArray::I32),
            ArrayDataType::Int64 => self.read_tile_data_as::<i64>(tile, &mut reader)?.map(AnyDenseArray::I64),
            ArrayDataType::Float32 => self.read_tile_data_as::<f32>(tile, &mut reader)?.map(AnyDenseArray::F32),
            ArrayDataType::Float64 => self.read_tile_data_as::<f64>(tile, &mut reader)?.map(AnyDenseArray::F64),
        })
    }

    pub fn parse_tile_data(&self, tile_source: &TileSource, cog_chunk: &[u8]) -> Result<AnyDenseArray> {
        match tile_source {
            TileSource::Aligned(cog_tile) => Ok(match self.data_type() {
                ArrayDataType::Uint8 => AnyDenseArray::U8(self.cog.parse_tile_data_as::<u8>(cog_tile, cog_chunk)?),
                ArrayDataType::Uint16 => AnyDenseArray::U16(self.cog.parse_tile_data_as::<u16>(cog_tile, cog_chunk)?),
                ArrayDataType::Uint32 => AnyDenseArray::U32(self.cog.parse_tile_data_as::<u32>(cog_tile, cog_chunk)?),
                ArrayDataType::Uint64 => AnyDenseArray::U64(self.cog.parse_tile_data_as::<u64>(cog_tile, cog_chunk)?),
                ArrayDataType::Int8 => AnyDenseArray::I8(self.cog.parse_tile_data_as::<i8>(cog_tile, cog_chunk)?),
                ArrayDataType::Int16 => AnyDenseArray::I16(self.cog.parse_tile_data_as::<i16>(cog_tile, cog_chunk)?),
                ArrayDataType::Int32 => AnyDenseArray::I32(self.cog.parse_tile_data_as::<i32>(cog_tile, cog_chunk)?),
                ArrayDataType::Int64 => AnyDenseArray::I64(self.cog.parse_tile_data_as::<i64>(cog_tile, cog_chunk)?),
                ArrayDataType::Float32 => AnyDenseArray::F32(self.cog.parse_tile_data_as::<f32>(cog_tile, cog_chunk)?),
                ArrayDataType::Float64 => AnyDenseArray::F64(self.cog.parse_tile_data_as::<f64>(cog_tile, cog_chunk)?),
            }),
            TileSource::Unaligned(tile_sources) => todo!(),
        }
    }

    fn merge_tile_sources<T: ArrayNum + HorizontalUnpredictable>(
        &self,
        tile_sources: &[(CogTileLocation, CutOut)],
        cog_chunks: &[&[u8]],
    ) -> Result<DenseArray<T>> {
        let tile_size = self.cog_metadata().tile_size as usize;

        let mut arr = DenseArray::new(
            RasterMetadata::sized_with_nodata(RasterSize::square(tile_size as i32), NumCast::from(T::NODATA)),
            allocate::aligned_vec_filled_with(T::NODATA, tile_size * tile_size),
        )?;

        for ((cog_location, cutout), cog_chunck) in tile_sources.iter().zip(cog_chunks) {
            let chunk_range = cog_location.range_to_fetch();
            if chunk_range.start == chunk_range.end {
                return Ok(DenseArray::empty());
            }

            let tile_cutout = self.cog.parse_tile_data_as::<T>(cog_location, cog_chunck)?;

            let dest_window = Window::new(
                Cell::from_row_col(cutout.dst_row_offset, cutout.dst_col_offset),
                RasterSize::with_rows_cols(Rows(cutout.rows), Columns(cutout.cols)),
            );

            let src_window = Window::new(
                Cell::from_row_col(cutout.src_row_offset, cutout.src_col_offset),
                RasterSize::with_rows_cols(Rows(cutout.rows), Columns(cutout.cols)),
            );

            for (dest, source) in arr.iter_window_mut(dest_window).zip(tile_cutout.iter_window(src_window)) {
                *dest = source;
            }
        }

        Ok(arr)
    }

    #[simd_bounds]
    pub fn read_tile_data_as<T: ArrayNum + HorizontalUnpredictable>(
        &self,
        tile: &Tile,
        mut reader: impl Read + Seek,
    ) -> Result<Option<DenseArray<T>>> {
        if let Some(tile_source) = self.tile_source(tile) {
            match tile_source {
                TileSource::Aligned(cog_tile) => Ok(Some(self.cog.read_tile_data_as::<T>(cog_tile, &mut reader)?)),
                TileSource::Unaligned(tile_sources) => {
                    let cog_chunks: Vec<Vec<u8>> = tile_sources
                        .iter()
                        .flat_map(|(cog_tile_offset, _)| io::read_cog_chunk(cog_tile_offset, &mut reader))
                        .collect();

                    let cog_chunk_refs: Vec<&[u8]> = cog_chunks.iter().map(|chunk| chunk.as_slice()).collect();
                    Ok(Some(self.merge_tile_sources(tile_sources, &cog_chunk_refs)?))
                }
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, path::Path};

    use approx::assert_relative_eq;

    use crate::{
        ZoomLevelStrategy,
        cog::{CogCreationOptions, Compression, Predictor, PredictorSelection, create_cog_tiles},
        testutils,
    };

    use super::*;

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
            let reader = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;

            let data_bounds = reader.data_bounds();
            assert_relative_eq!(data_bounds.northwest(), Tile { z: 10, x: 519, y: 340 }.upper_left());
            assert_relative_eq!(data_bounds.southeast(), Tile { z: 10, x: 528, y: 344 }.lower_right());
        }

        {
            // Don't allow sparse tiles, The bounds should now match the extent of the lowest zoom level
            create_test_cog(&input, &output, COG_TILE_SIZE, None, None, None, false)?;
            let reader = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;
            assert!(reader.tile_info().max_zoom == 10);

            let data_bounds = reader.data_bounds();
            assert_relative_eq!(data_bounds.northwest(), Tile { z: 7, x: 64, y: 42 }.upper_left());
            assert_relative_eq!(data_bounds.southeast(), Tile { z: 7, x: 66, y: 43 }.lower_right());
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
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;

            let mut reader = File::open(&output)?;
            cog.read_tile_data_as::<u8>(&reference_tile, &mut reader).expect("None_u8").unwrap()
        };

        {
            // Create a test COG file with LZW compression and no predictor
            create_test_cog(&input, &output, COG_TILE_SIZE, Some(Compression::Lzw), None, None, true)?;
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;

            let mut reader = File::open(&output)?;
            let tile_data = cog.read_tile_data_as::<u8>(&reference_tile, &mut reader).expect("LZW_u8").unwrap();
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
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::Horizontal));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<u8>(&reference_tile, &mut reader)
                .expect("LZW_u8_predictor")
                .unwrap();
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
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::Horizontal));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<i32>(&reference_tile, &mut reader)
                .expect("LZW_i32_predictor")
                .unwrap();

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
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, None);
            assert_eq!(cog.cog_metadata().max_zoom, 10);

            let mut reader = File::open(&output)?;
            assert!(cog.read_tile_data_as::<f64>(&reference_tile, &mut reader).is_err());
            let tile_data = cog
                .read_tile_data_as::<f32>(&reference_tile, &mut reader)
                .expect("LZW_f32")
                .unwrap();

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
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::FloatingPoint));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<f32>(&reference_tile, &mut reader)
                .expect("LZW_f32_predictor")
                .unwrap();

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
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::FloatingPoint));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<f64>(&reference_tile, &mut reader)
                .expect("LZW_f64_predictor")
                .unwrap();

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

        let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;
        let meta = cog.cog_metadata();
        assert_eq!(meta.min_zoom, 7);
        assert_eq!(meta.max_zoom, 10);

        // let zoom_level_7 = meta.pyramids.iter().find(|p| p.zoom_level == 7).expect("Zoom level 7 not found");

        // for cog_tile in &zoom_level_7.tile_locations {
        //     let reader = File::open(&output).unwrap();
        //     let mut tile_arr = io::read_tile_data::<u8>(
        //         cog_tile,
        //         meta.tile_size,
        //         meta.geo_reference.nodata(),
        //         meta.compression,
        //         meta.predictor,
        //         reader,
        //     )?;

        //     tile_arr.write(&PathBuf::from(format!("/Users/dirk/cog/cog_tile_{:?}.tif", cog_tile.offset)))?;
        // }

        // Decode all tiles
        let mut reader = File::open(&output)?;

        // let tile = Tile { z: 7, x: 65, y: 42 };

        // let tile_source = cog.tile_source(&tile).unwrap();
        // match tile_source {
        //     TileSource::Aligned(_) => {
        //         panic!("Expected unaligned tile source for tile {tile:?}");
        //     }
        //     TileSource::Unaligned(tile_sources) => {
        //         assert_eq!(2, tile_sources.len());
        //     }
        // }

        //let tile_data = cog.read_tile_data(&tile, &mut reader)?.unwrap();

        for tile in cog.zoom_level_tile_sources(7).unwrap().keys() {
            if let Some(tile_data) = cog.read_tile_data(tile, &mut reader)? {
                if tile_data.is_empty() {
                    continue; // Skip empty tiles
                }

                assert_eq!(tile_data.len(), RasterSize::square(COG_TILE_SIZE as i32).cell_count());
                assert_eq!(tile_data.data_type(), meta.data_type);

                let tile_data = cog.read_tile_data_as::<u8>(tile, &mut reader)?.unwrap();
                assert_eq!(tile_data.size(), RasterSize::square(COG_TILE_SIZE as i32));
            }
        }

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
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;

            let meta = cog.cog_metadata();
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
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;

            let mut reader = File::open(&output)?;
            let tile_data = cog.read_tile_data_as::<u8>(&reference_tile, &mut reader)?;
            assert_eq!(tile_data, reference_tile_data);
        }

        Ok(())
    }

    #[test_log::test]
    fn generate_tiles_for_extent_unaligned() -> Result<()> {
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

        let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;
        let pyramid = cog.pyramid_info(7).expect("Zoom level 7 not found");

        let tiles = super::generate_tiles_for_extent_unaligned(&cog.cog_metadata().geo_reference, pyramid, COG_TILE_SIZE);
        assert_eq!(tiles.len(), 6);
        assert_eq!(
            tiles,
            vec![
                Tile { z: 7, x: 64, y: 42 },
                Tile { z: 7, x: 65, y: 42 },
                Tile { z: 7, x: 66, y: 42 },
                Tile { z: 7, x: 64, y: 43 },
                Tile { z: 7, x: 65, y: 43 },
                Tile { z: 7, x: 66, y: 43 },
            ]
        );

        Ok(())
    }
}

use crate::{
    AnyDenseArray, Array as _, ArrayDataType, ArrayMetadata as _, ArrayNum, Cell, CellSize, Columns, DenseArray, Error, GeoReference,
    RasterMetadata, Result, Rows, Window, ZoomLevelStrategy,
    cog::{CogStats, CogTileLocation, HorizontalUnpredictable, io, reader::PyramidInfo},
    raster::intersection::{CutOut, intersect_georeference},
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
    pub fn from_cog_metadata(meta: &CogMetadata) -> Result<Self> {
        let mut zoom_levels = vec![HashMap::default(); 22];

        let mut zoom_level = Tile::zoom_level_for_pixel_size(meta.geo_reference.cell_size_x(), ZoomLevelStrategy::Closest, meta.tile_size);
        if (Tile::pixel_size_at_zoom_level(zoom_level, meta.tile_size) - meta.geo_reference.cell_size_x()).abs() > 1e-6 {
            return Err(Error::Runtime(format!(
                "The main COG file content is not scaled to match a web zoom level, COG pixel size {}, zoom level {zoom_level} pixel size {}",
                Tile::pixel_size_at_zoom_level(zoom_level, meta.tile_size),
                meta.geo_reference.cell_size_x()
            )));
        }

        for pyramid in &meta.pyramids {
            let top_left_coordinate = crs::web_mercator_to_lat_lon(meta.geo_reference.top_left());
            let top_left_tile = Tile::for_coordinate(top_left_coordinate, zoom_level);
            let tile_aligned = top_left_tile.coordinate_pixel_offset(top_left_coordinate, meta.tile_size) == Some((0, 0));

            if tile_aligned {
                let tiles = generate_tiles_for_extent(meta.geo_reference.geo_transform(), pyramid.raster_size, meta.tile_size, zoom_level);
                tiles.into_iter().zip(&pyramid.tile_locations).for_each(|(web_tile, cog_tile)| {
                    zoom_levels[web_tile.z as usize].insert(web_tile, TileSource::Aligned(*cog_tile));
                });
            } else {
                let pyramid_geo_ref = GeoReference::with_origin(
                    "EPSG:3857",
                    pyramid.raster_size,
                    meta.geo_reference.bottom_left(),
                    CellSize::square(Tile::pixel_size_at_zoom_level(zoom_level, meta.tile_size)),
                    Option::<f64>::None,
                );

                let tiles = generate_tiles_for_extent_unaligned(&meta.geo_reference, zoom_level, meta.tile_size);
                let cog_tile_bounds = create_cog_tile_web_mercator_bounds(pyramid, &pyramid_geo_ref, zoom_level, meta.tile_size).unwrap();

                log::info!(
                    "Unaligned zoom level: {} Web tiles {} Cog tiles {}",
                    zoom_level,
                    tiles.len(),
                    pyramid.tile_locations.len(),
                );

                for tile in &tiles {
                    let mut tile_sources = Vec::new();
                    let web_tile_georef = GeoReference::from_tile(tile, meta.tile_size as usize, 1);

                    for (cog_tile, bounds) in &cog_tile_bounds {
                        if web_tile_georef.intersects(bounds).unwrap() {
                            if let Ok(cutout) = intersect_georeference(bounds, &web_tile_georef) {
                                tile_sources.push((*cog_tile, cutout));
                            }
                        }
                    }

                    if !tile_sources.is_empty() {
                        zoom_levels[tile.z as usize].insert(*tile, TileSource::Unaligned(tile_sources));
                    }
                }
            }

            zoom_level -= 1;
        }

        trim_empty_zoom_levels(&mut zoom_levels);

        Ok(WebTiles { zoom_levels })
    }

    pub fn tile_source(&self, tile: &Tile) -> Option<&TileSource> {
        self.zoom_levels.get(tile.z as usize).and_then(|level| level.get(tile))
    }

    pub fn zoom_level_tile_sources(&self, zoom_level: i32) -> Option<&HashMap<Tile, TileSource>> {
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

fn generate_tiles_for_extent(geo_transform: [f64; 6], raster_size: RasterSize, tile_size: u32, zoom: i32) -> Vec<Tile> {
    let top_left = crs::web_mercator_to_lat_lon(Point::new(geo_transform[0], geo_transform[3]));
    let top_left_tile = Tile::for_coordinate(top_left, zoom);

    let tiles_wide = (raster_size.cols.count() as u32).div_ceil(tile_size);
    let tiles_high = (raster_size.rows.count() as u32).div_ceil(tile_size);

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

fn generate_tiles_for_extent_unaligned(geo_ref: &GeoReference, zoom_level: i32, tile_size: u32) -> Vec<Tile> {
    // The geo_transform is from the highest zoom level, the origin does not match unaligned zoom levels

    let top_left = crs::web_mercator_to_lat_lon(geo_ref.top_left());
    let bottom_right = crs::web_mercator_to_lat_lon(geo_ref.bottom_right());

    let top_left_tile = Tile::for_coordinate(top_left, zoom_level);
    let bottom_right_tile = Tile::for_coordinate(bottom_right, zoom_level);

    assert!(
        tile_size % Tile::TILE_SIZE == 0,
        "Tile size must be a factor of {}",
        Tile::TILE_SIZE
    );

    let tiles_wide = bottom_right_tile.x - top_left_tile.x + 1;
    let tiles_high = bottom_right_tile.y - top_left_tile.y + 1;

    let mut tiles = Vec::new();
    // Iteration has to be done in row-major order so the tiles match the order of the tile lists from the COG
    for ty in 0..tiles_high {
        for tx in 0..tiles_wide {
            let tile = Tile {
                z: zoom_level,
                x: top_left_tile.x + tx,
                y: top_left_tile.y + ty,
            };

            tiles.push(tile);
        }
    }

    tiles
}

fn change_georef_cell_size(geo_reference: &GeoReference, cell_size: CellSize) -> GeoReference {
    let mut result = geo_reference.clone();
    let x_factor = cell_size.x() / geo_reference.cell_size_x();
    let y_factor = cell_size.y() / geo_reference.cell_size_y();
    result.set_cell_size(cell_size);

    let raster_size = geo_reference.raster_size();
    let new_rows = Rows((raster_size.rows.count() as f64 * y_factor).round() as i32);
    let new_cols = Columns((raster_size.cols.count() as f64 * x_factor).round() as i32);
    result.set_rows(new_rows);
    result.set_columns(new_cols);

    result
}

fn create_cog_tile_web_mercator_bounds(
    pyramid: &PyramidInfo,
    geo_reference: &GeoReference, // georeference of the full cog image
    zoom_level: i32,
    tile_size: u32,
) -> Result<Vec<(CogTileLocation, GeoReference)>> {
    let mut web_tiles = Vec::with_capacity(pyramid.tile_locations.len());

    let cell_size = CellSize::square(Tile::pixel_size_at_zoom_level(zoom_level, tile_size));
    let geo_ref_zoom_level = change_georef_cell_size(geo_reference, cell_size);

    let tiles_wide = (pyramid.raster_size.cols.count() as u32).div_ceil(tile_size) as usize;
    let tiles_high = (pyramid.raster_size.rows.count() as u32).div_ceil(tile_size) as usize;

    if tiles_wide * tiles_high != pyramid.tile_locations.len() {
        return Err(Error::InvalidArgument(format!(
            "Expected {} tiles, but got {}",
            tiles_wide * tiles_high,
            pyramid.tile_locations.len()
        )));
    }

    let tile_size = tile_size as i32;
    for ty in 0..tiles_high {
        let mut current_source_cell = Cell::from_row_col(ty as i32 * tile_size, 0);
        let tile_height = Rows(if current_source_cell.row + tile_size > pyramid.raster_size.rows.count() {
            debug_assert!(ty + 1 == tiles_high);
            pyramid.raster_size.rows.count() - current_source_cell.row
        } else {
            tile_size
        });

        for tx in 0..tiles_wide {
            current_source_cell.col = tx as i32 * tile_size;
            let tile_width = Columns(if current_source_cell.col + tile_size > pyramid.raster_size.cols.count() {
                debug_assert!(tx + 1 == tiles_wide);
                pyramid.raster_size.cols.count() - current_source_cell.col
            } else {
                tile_size
            });

            let lower_left_cell = Cell::from_row_col(current_source_cell.row + tile_height.count() - 1, current_source_cell.col);

            let cog_tile_geo_ref = GeoReference::with_origin(
                "EPSG:3857",
                RasterSize::with_rows_cols(tile_height, tile_width),
                geo_ref_zoom_level.cell_lower_left(lower_left_cell),
                cell_size,
                Option::<f64>::None,
            );

            let cog_tile = &pyramid.tile_locations[ty * tiles_wide + tx];
            web_tiles.push((*cog_tile, cog_tile_geo_ref));
        }
    }

    Ok(web_tiles)
}

pub struct WebTileInfo {
    pub min_zoom: i32,
    pub max_zoom: i32,
    pub tile_size: u32,
    pub data_type: ArrayDataType,
    pub bounds: LatLonBounds,
    pub statistics: Option<CogStats>,
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
        let web_tiles = WebTiles::from_cog_metadata(cog.metadata())?;

        Ok(Self { web_tiles, cog })
    }

    pub fn tile_info(&self) -> WebTileInfo {
        WebTileInfo {
            min_zoom: self.web_tiles.min_zoom(),
            max_zoom: self.web_tiles.max_zoom(),
            tile_size: self.cog.metadata().tile_size,
            data_type: self.data_type(),
            bounds: self.data_bounds(),
            statistics: self.cog.metadata().statistics.clone(),
        }
    }

    pub fn data_bounds(&self) -> LatLonBounds {
        self.web_tiles.data_bounds()
    }

    pub fn cog_metadata(&self) -> &CogMetadata {
        self.cog.metadata()
    }

    /// For a given tile, returns which cog tiles are used to construct the tile data.
    /// In case of an aligned overview, this will be a single cog tile.
    /// In case of an unaligned overview, this will be a list of cog tiles with their cutout information.
    pub fn tile_source(&self, tile: &Tile) -> Option<&TileSource> {
        self.web_tiles.tile_source(tile)
    }

    pub fn zoom_level_tile_sources(&self, zoom_level: i32) -> Option<&HashMap<Tile, TileSource>> {
        self.web_tiles.zoom_level_tile_sources(zoom_level)
    }

    pub fn pyramid_info(&self, zoom_level: i32) -> Option<&PyramidInfo> {
        let pyramid_index = (self.web_tiles.max_zoom() - zoom_level) as usize;
        self.cog_metadata().pyramids.get(pyramid_index)
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

    pub fn parse_tile_data(&self, tile_source: &TileSource, cog_chunks: &[&[u8]]) -> Result<AnyDenseArray> {
        match tile_source {
            TileSource::Aligned(cog_tile) => Ok(match self.data_type() {
                ArrayDataType::Uint8 => AnyDenseArray::U8(self.cog.parse_tile_data_as::<u8>(cog_tile, cog_chunks[0])?),
                ArrayDataType::Uint16 => AnyDenseArray::U16(self.cog.parse_tile_data_as::<u16>(cog_tile, cog_chunks[0])?),
                ArrayDataType::Uint32 => AnyDenseArray::U32(self.cog.parse_tile_data_as::<u32>(cog_tile, cog_chunks[0])?),
                ArrayDataType::Uint64 => AnyDenseArray::U64(self.cog.parse_tile_data_as::<u64>(cog_tile, cog_chunks[0])?),
                ArrayDataType::Int8 => AnyDenseArray::I8(self.cog.parse_tile_data_as::<i8>(cog_tile, cog_chunks[0])?),
                ArrayDataType::Int16 => AnyDenseArray::I16(self.cog.parse_tile_data_as::<i16>(cog_tile, cog_chunks[0])?),
                ArrayDataType::Int32 => AnyDenseArray::I32(self.cog.parse_tile_data_as::<i32>(cog_tile, cog_chunks[0])?),
                ArrayDataType::Int64 => AnyDenseArray::I64(self.cog.parse_tile_data_as::<i64>(cog_tile, cog_chunks[0])?),
                ArrayDataType::Float32 => AnyDenseArray::F32(self.cog.parse_tile_data_as::<f32>(cog_tile, cog_chunks[0])?),
                ArrayDataType::Float64 => AnyDenseArray::F64(self.cog.parse_tile_data_as::<f64>(cog_tile, cog_chunks[0])?),
            }),
            TileSource::Unaligned(tile_sources) => Ok(match self.data_type() {
                ArrayDataType::Uint8 => AnyDenseArray::U8(self.merge_tile_sources(tile_sources, cog_chunks)?),
                ArrayDataType::Uint16 => AnyDenseArray::U16(self.merge_tile_sources(tile_sources, cog_chunks)?),
                ArrayDataType::Uint32 => AnyDenseArray::U32(self.merge_tile_sources(tile_sources, cog_chunks)?),
                ArrayDataType::Uint64 => AnyDenseArray::U64(self.merge_tile_sources(tile_sources, cog_chunks)?),
                ArrayDataType::Int8 => AnyDenseArray::I8(self.merge_tile_sources(tile_sources, cog_chunks)?),
                ArrayDataType::Int16 => AnyDenseArray::I16(self.merge_tile_sources(tile_sources, cog_chunks)?),
                ArrayDataType::Int32 => AnyDenseArray::I32(self.merge_tile_sources(tile_sources, cog_chunks)?),
                ArrayDataType::Int64 => AnyDenseArray::I64(self.merge_tile_sources(tile_sources, cog_chunks)?),
                ArrayDataType::Float32 => AnyDenseArray::F32(self.merge_tile_sources(tile_sources, cog_chunks)?),
                ArrayDataType::Float64 => AnyDenseArray::F64(self.merge_tile_sources(tile_sources, cog_chunks)?),
            }),
        }
    }

    #[simd_bounds]
    fn merge_tile_sources<T: ArrayNum + HorizontalUnpredictable>(
        &self,
        tile_sources: &[(CogTileLocation, CutOut)],
        cog_chunks: &[&[u8]],
    ) -> Result<DenseArray<T>> {
        let tile_size = self.cog_metadata().tile_size as usize;

        let mut arr = DenseArray::filled_with_nodata(RasterMetadata::sized_with_nodata(
            RasterSize::square(tile_size as i32),
            NumCast::from(T::NODATA),
        ));

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
    use std::{
        fs::File,
        path::{Path, PathBuf},
    };

    use approx::assert_relative_eq;
    use path_macro::path;

    use crate::{
        Nodata as _, ZoomLevelStrategy,
        cog::{CogCreationOptions, Compression, Predictor, PredictorSelection, create_cog_tiles, debug},
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
            assert_eq!(cog.tile_info().max_zoom, 10);

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

        let cog_path = create_unaligned_test_cog(tmp.path(), COG_TILE_SIZE)?;
        let cog = WebTilesReader::from_cog(CogAccessor::from_file(&cog_path)?)?;

        let meta = cog.tile_info();
        assert_eq!(meta.min_zoom, 7);
        assert_eq!(meta.max_zoom, 10);

        // Decode all tiles
        let mut reader = File::open(&cog_path)?;

        {
            let tile = Tile { z: 7, x: 66, y: 42 };
            let tile_source = cog.tile_source(&tile).unwrap();
            match tile_source {
                TileSource::Aligned(_) => {
                    panic!("Expected unaligned tile source for tile {tile:?}");
                }
                TileSource::Unaligned(tile_sources) => {
                    assert_eq!(1, tile_sources.len());
                }
            }

            let tile_data = cog.read_tile_data_as::<u8>(&tile, &mut reader).unwrap().unwrap();
            let mut first_row_value_count = 0;
            for row in 0..tile_data.rows().count() {
                // This tile only contains a bit of data in the lower left corner, the rest is nodata
                // Verify the zero padding values that are present in the cog for the unaligned overviews are not present in the tile data

                // Last row should be nodata
                let last_col_value = tile_data[Cell::from_row_col(row, COG_TILE_SIZE as i32 - 1)];
                assert!(
                    last_col_value.is_nodata(),
                    "Last cell at row {row} is not nodata but {last_col_value}"
                );

                // Middle row should be nodata
                let middle_col_value = tile_data[Cell::from_row_col(row, (COG_TILE_SIZE / 2) as i32 - 1)];
                assert!(
                    middle_col_value.is_nodata(),
                    "Middle cell at row {row} is not nodata but {middle_col_value}"
                );

                // First row should not all be nodata
                let first_col_value = tile_data[Cell::from_row_col(row, 0)];
                if !first_col_value.is_nodata() {
                    first_row_value_count += 1;
                }
            }

            assert_ne!(first_row_value_count, 0);
            assert!(tile_data.as_slice()[0..(COG_TILE_SIZE / 2) as usize].iter().all(|&v| v.is_nodata()));
        }

        {
            let tile = Tile { z: 8, x: 131, y: 85 };
            let tile_source = cog.tile_source(&tile).unwrap();
            match tile_source {
                TileSource::Aligned(_) => {
                    panic!("Expected unaligned tile source for tile {tile:?}");
                }
                TileSource::Unaligned(tile_sources) => {
                    assert_eq!(2, tile_sources.len());
                }
            }

            let tile_data = cog.read_tile_data_as::<u8>(&tile, &mut reader).unwrap().unwrap();
            assert!(
                !tile_data.as_slice()
                    [COG_TILE_SIZE as usize * (COG_TILE_SIZE as usize - 1)..COG_TILE_SIZE as usize * COG_TILE_SIZE as usize]
                    .iter()
                    .all(|&v| v == 0)
            );
        }

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
        const COG_TILE_SIZE: u32 = 512;
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
            assert_eq!(cog.tile_info().min_zoom, 7);
            assert_eq!(cog.tile_info().max_zoom, 9);

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
    fn generate_tiles_for_extent_unaligned_256px() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let cog_path = create_unaligned_test_cog(tmp.path(), COG_TILE_SIZE)?;
        let cog = WebTilesReader::from_cog(CogAccessor::from_file(&cog_path)?)?;

        let tiles = super::generate_tiles_for_extent_unaligned(&cog.cog_metadata().geo_reference, 7, COG_TILE_SIZE);
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

    #[test_log::test]
    fn generate_tiles_for_extent_unaligned_512px() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let cog_path = create_unaligned_test_cog(tmp.path(), COG_TILE_SIZE * 2)?;
        let cog = WebTilesReader::from_cog(CogAccessor::from_file(&cog_path)?)?;

        let tiles = super::generate_tiles_for_extent_unaligned(&cog.cog_metadata().geo_reference, 7, COG_TILE_SIZE * 2);
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

    #[test_log::test]
    fn create_cog_tile_web_mercator_bounds() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let cog_path = create_unaligned_test_cog(tmp.path(), COG_TILE_SIZE)?;

        let cog_accessor = CogAccessor::from_file(&cog_path)?;
        let zoom_level_8_index = 2;
        let cog_tiles = cog_accessor.pyramid_info(zoom_level_8_index).unwrap().tile_locations.clone();

        let cog = WebTilesReader::from_cog(cog_accessor)?;
        let bounds = super::create_cog_tile_web_mercator_bounds(
            cog.pyramid_info(8).unwrap(),
            &cog.cog_metadata().geo_reference,
            8,
            cog.cog_metadata().tile_size,
        )?;

        {
            // Top right tile
            let (cog_tile_location, geo_ref) = &bounds[2];
            assert_eq!(cog_tile_location.offset, cog_tiles[2].offset);
            assert_relative_eq!(geo_ref.top_left(), Point::new(547900.6187481433, 6731350.458905762), epsilon = 1e-6);
        }

        {
            // Bottom right tile
            let (cog_tile_location, geo_ref) = &bounds[5];
            assert_eq!(cog_tile_location.offset, cog_tiles[5].offset);
            assert_relative_eq!(geo_ref.top_left(), Point::new(547900.6187481433, 6574807.424977721), epsilon = 1e-6);
        }

        Ok(())
    }

    #[test_log::test]
    fn generate_tile_sources_256px() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let cog_path = create_unaligned_test_cog(tmp.path(), COG_TILE_SIZE)?;
        let cog = WebTilesReader::from_cog(CogAccessor::from_file(&cog_path)?)?;

        let tile_sources = cog.zoom_level_tile_sources(8).expect("Zoom level 8 not found");
        let web_tile = Tile { z: 8, x: 132, y: 85 };

        let tile = tile_sources.get(&web_tile).unwrap();
        match tile {
            TileSource::Aligned(_) => panic!("Expected unaligned tile source for tile {tile:?}"),
            TileSource::Unaligned(items) => {
                assert_eq!(1, items.len());
                let (tile_location, cutout) = items.first().unwrap();

                assert_eq!(
                    tile_location.offset,
                    264600 /* offset of the cog tile at 0 based index 2 (top right cog tile)*/
                );

                assert_eq!(cutout.cols, 128);
                assert_eq!(cutout.rows, 256);
                assert_eq!(cutout.src_col_offset, 128);
                assert_eq!(cutout.src_row_offset, 0);
                assert_eq!(cutout.dst_col_offset, 0);
                assert_eq!(cutout.dst_row_offset, 0);
            }
        }

        Ok(())
    }

    #[test_log::test]
    fn generate_tile_sources_512px() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let cog_path = create_unaligned_test_cog(tmp.path(), COG_TILE_SIZE * 2)?;
        let cog_accessor = CogAccessor::from_file(&cog_path)?;
        let zoom_level_7_index = 2;
        let cog_tiles = cog_accessor.pyramid_info(zoom_level_7_index).unwrap().tile_locations.clone();
        let cog = WebTilesReader::from_cog(cog_accessor)?;

        assert_eq!(cog.tile_info().max_zoom, 9);

        let tile_sources = cog.zoom_level_tile_sources(7).expect("Zoom level 7 not found");
        let web_tile = Tile { z: 7, x: 65, y: 42 };

        let tile = tile_sources.get(&web_tile).unwrap();
        match tile {
            TileSource::Aligned(_) => panic!("Expected unaligned tile source for tile {tile:?}"),
            TileSource::Unaligned(items) => {
                assert_eq!(2, items.len());
                let (tile_location, cutout) = &items[0];
                assert_eq!(tile_location.offset, cog_tiles[0].offset);

                assert_eq!(cutout.cols, 256);
                assert_eq!(cutout.rows, 256);
                assert_eq!(cutout.src_row_offset, 0);
                assert_eq!(cutout.src_col_offset, 256);
                assert_eq!(cutout.dst_row_offset, 256);
                assert_eq!(cutout.dst_col_offset, 0);

                let (tile_location, cutout) = &items[1];
                assert_eq!(tile_location.offset, cog_tiles[1].offset);

                assert_eq!(cutout.cols, 256);
                assert_eq!(cutout.rows, 256);
                assert_eq!(cutout.src_col_offset, 0);
                assert_eq!(cutout.src_row_offset, 0);
                assert_eq!(cutout.dst_col_offset, 256);
                assert_eq!(cutout.dst_row_offset, 256);
            }
        }

        Ok(())
    }

    #[test_log::test]
    fn create_cog_tiles_for_debugging() -> Result<()> {
        // This test generates COG tiles from a test TIFF file and dumps the web tiles and COG tiles for zoom levels 7 and 8.
        // The qgis project in tests/data/cog_debug can be used to visually inspect the generated web tiles with resprect to the cog tiles.

        let output_dir = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "cog_debug");
        for tile_size in [256, 512] {
            let cog_path = create_unaligned_test_cog(&output_dir, tile_size)?;

            for zoom_level in 7..=8 {
                debug::dump_cog_tiles(&cog_path, zoom_level, &output_dir.join("cog_tile").join(format!("{tile_size}px")))?;
                debug::dump_web_tiles(&cog_path, zoom_level, &output_dir.join("web_tile").join(format!("{tile_size}px")))?;
            }
        }

        Ok(())
    }

    fn create_unaligned_test_cog(dir: &Path, tile_size: u32) -> Result<PathBuf> {
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = dir.join(format!("cog_{tile_size}px.tif"));

        let opts = CogCreationOptions {
            min_zoom: Some(7),
            zoom_level_strategy: ZoomLevelStrategy::Closest,
            tile_size,
            allow_sparse: true,
            compression: None,
            predictor: None,
            output_data_type: None,
            aligned_levels: Some(2),
        };
        create_cog_tiles(&input, &output, opts)?;

        Ok(output)
    }
}

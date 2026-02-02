use crate::{
    AnyDenseArray, Array as _, ArrayDataType, ArrayInterop, ArrayMetadata as _, ArrayNum, Cell, CellSize, Columns, DenseArray, Error,
    GeoReference, GeoTransform, Point, RasterMetadata, Result, Rows, ZoomLevelStrategy,
    geotiff::{
        self, GeoTiffMetadata, TiffChunkLocation, TiffOverview, TiffStats, io,
        tileio::{self},
        utils,
    },
    raster::intersection::{CutOut, intersect_georeference},
};
use std::{
    collections::HashMap,
    io::{Read, Seek},
};

use crate::{LatLonBounds, RasterSize, Tile, crs};

use inf::allocate::AlignedVecUnderConstruction;
use num::NumCast;
use simd_macro::simd_bounds;

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

#[derive(Debug, Clone)]
pub enum TileSource {
    Aligned(TiffChunkLocation),
    Unaligned(Vec<(TiffChunkLocation, CutOut)>),
    MultiBandAligned(Vec<TiffChunkLocation>),
    MultiBandUnaligned(Vec<(Vec<TiffChunkLocation>, CutOut)>),
}

#[derive(Debug, Clone, Default)]
struct ZoomLevelInfo {
    tile_aligned: bool, // Web tiles are aligned with COG tiles at this zoom level
    tiles: HashMap<Tile, TileSource>,
}

#[derive(Debug, Clone)]
/// `WebTiles` is a structure that holds the necessary information to read Web xyz tiles from a cog.
/// It is constructed from the metadata of a COG file, which contains information about the tiff tile layout and the locations of the tiff tiles in the COG.
/// Tiff tiles don't always have a one-to-one mapping to web tiles, so this structure contains the necessary information to create web tiles from 1 or more tiff tiles.
pub struct WebTiles {
    zoom_levels: Vec<ZoomLevelInfo>,
}

/// Iterates over a slice in stride-based groups.
///
/// Each group yields elements starting at an offset and
/// stepping by `stride` (i.e. grouped by index modulo `stride`).
///
/// Example with `stride = 3`:
/// Input slice: `[a, b, c, d, e, f, g, h, i]`
/// Output tuples: `(a, d, g)`, `(b, e, h)`, `(c, f, i)`
pub fn stride_groups<T>(slice: &[T], stride: usize) -> impl Iterator<Item = impl Iterator<Item = &T>> {
    debug_assert!(slice.len().is_multiple_of(stride));
    (0..stride).map(move |offset| slice.iter().skip(offset).step_by(stride))
}

impl WebTiles {
    pub fn from_cog_metadata(meta: &GeoTiffMetadata) -> Result<Self> {
        let mut zoom_levels = vec![ZoomLevelInfo::default(); 22];

        let tile_size = meta.chunk_row_length();
        let mut zoom_level = Tile::zoom_level_for_pixel_size(meta.geo_reference.cell_size_x(), ZoomLevelStrategy::Closest, tile_size);
        if (Tile::pixel_size_at_zoom_level(zoom_level, tile_size) - meta.geo_reference.cell_size_x()).abs() > 1e-6 {
            return Err(Error::Runtime(format!(
                "The main COG file content is not scaled to match a web zoom level, COG pixel size {}, zoom level {zoom_level} pixel size {}",
                Tile::pixel_size_at_zoom_level(zoom_level, tile_size),
                meta.geo_reference.cell_size_x()
            )));
        }

        let cell_size = meta.geo_reference.cell_size();

        // Offset to get the center of the bottom right pixel
        // Otherwise the bottom right coordinate is exactly at the edge of the tile and the next one will be taken
        let offset = Point::new(cell_size.x() / 2.0, cell_size.y() / 2.0);

        for overview in &meta.overviews {
            let top_left_coordinate = crs::web_mercator_to_lat_lon(meta.geo_reference.top_left());
            let bottom_right_center_coordinate = crs::web_mercator_to_lat_lon(meta.geo_reference.bottom_right() - offset);
            let top_left_tile = Tile::for_coordinate(top_left_coordinate, zoom_level);
            let bottom_right_tile = Tile::for_coordinate(bottom_right_center_coordinate, zoom_level);

            let tl_diff = meta.geo_reference.top_left() - crs::lat_lon_to_web_mercator(top_left_tile.upper_left());
            let br_diff = meta.geo_reference.bottom_right() - crs::lat_lon_to_web_mercator(bottom_right_tile.lower_right());

            let top_left_aligned = tl_diff.x().abs() < 1e-6 && tl_diff.y().abs() < 1e-6;
            let bottom_right_aligned = br_diff.x().abs() < 1e-6 && br_diff.y().abs() < 1e-6;
            let tile_aligned = top_left_aligned && bottom_right_aligned;

            if zoom_level as usize >= zoom_levels.len() {
                zoom_levels.resize(zoom_level as usize + 1, ZoomLevelInfo::default());
            }

            zoom_levels[zoom_level as usize].tile_aligned = tile_aligned;

            if tile_aligned {
                let tiles = generate_tiles_for_extent(meta.geo_reference.geo_transform(), overview.raster_size, tile_size, zoom_level);
                if meta.band_count == 1 {
                    tiles.into_iter().zip(&overview.chunk_locations).for_each(|(web_tile, cog_tile)| {
                        zoom_levels[web_tile.z as usize]
                            .tiles
                            .insert(web_tile, TileSource::Aligned(*cog_tile));
                    });
                } else {
                    // The chunk_locations list is always ordered by band first: tile0_band0, tile1_band0, ..., tile0_band1, tile1_band1, ...
                    // So we iterate strided to combine the chunks of all the bands per tile
                    debug_assert!(overview.chunk_locations.len().is_multiple_of(meta.band_count as usize));
                    let chunks_per_band = overview.chunk_locations.len() / meta.band_count as usize;
                    tiles
                        .into_iter()
                        .zip(stride_groups(&overview.chunk_locations, chunks_per_band))
                        .for_each(|(web_tile, cog_tiles)| {
                            zoom_levels[web_tile.z as usize]
                                .tiles
                                .insert(web_tile, TileSource::MultiBandAligned(cog_tiles.copied().collect()));
                        });
                }
            } else {
                let overview_geo_ref = GeoReference::with_bottom_left_origin(
                    "EPSG:3857",
                    overview.raster_size,
                    meta.geo_reference.bottom_left(),
                    CellSize::square(Tile::pixel_size_at_zoom_level(zoom_level, tile_size)),
                    Option::<f64>::None,
                );

                let tiles = generate_tiles_for_extent_unaligned(&meta.geo_reference, zoom_level, tile_size);
                if let Ok(cog_tile_bounds) =
                    create_cog_tile_web_mercator_bounds(overview, &overview_geo_ref, zoom_level, tile_size, meta.band_count)
                {
                    dbg!(zoom_level, &cog_tile_bounds, &tiles);
                    for tile in &tiles {
                        let mut tile_sources = Vec::new();
                        let web_tile_georef = GeoReference::from_tile(tile, tile_size as usize, 1);

                        for (cog_tiles, bounds) in &cog_tile_bounds {
                            if web_tile_georef.intersects(bounds)?
                                && let Ok(cutout) = intersect_georeference(bounds, &web_tile_georef)
                            {
                                tile_sources.push((cog_tiles.clone(), cutout));
                            }
                        }

                        if !tile_sources.is_empty() {
                            if meta.band_count == 1 {
                                let tile_sources = tile_sources.into_iter().map(|(ts, cutout)| (ts[0], cutout)).collect();
                                zoom_levels[tile.z as usize]
                                    .tiles
                                    .insert(*tile, TileSource::Unaligned(tile_sources));
                            } else {
                                zoom_levels[tile.z as usize]
                                    .tiles
                                    .insert(*tile, TileSource::MultiBandUnaligned(tile_sources));
                            }
                        }
                    }
                }
            }

            zoom_level -= 1;
        }

        trim_empty_zoom_levels(&mut zoom_levels);

        Ok(WebTiles { zoom_levels })
    }

    pub fn tile_source(&self, tile: &Tile) -> Option<&TileSource> {
        self.zoom_levels.get(tile.z as usize).and_then(|level| level.tiles.get(tile))
    }

    pub fn zoom_level_tile_sources(&self, zoom_level: i32) -> Option<&HashMap<Tile, TileSource>> {
        if zoom_level as usize >= self.zoom_levels.len() {
            return None;
        }

        Some(&self.zoom_levels[zoom_level as usize].tiles)
    }

    pub fn min_zoom(&self) -> i32 {
        let mut min_zoom = 0;
        for zoom_level in &self.zoom_levels {
            if zoom_level.tiles.is_empty() {
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
            for (tile, _) in last_zoom_level.tiles.iter().filter(|(_, loc)| match loc {
                TileSource::Aligned(loc) => loc.size > 0,
                TileSource::Unaligned(_) | TileSource::MultiBandUnaligned(_) => false, // Max zoom level should be aligned
                TileSource::MultiBandAligned(locs) => locs.iter().all(|loc| loc.size > 0),
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

fn trim_empty_zoom_levels(zoom_levels: &mut Vec<ZoomLevelInfo>) {
    // Remove empty zoom levels from the end
    while let Some(last) = zoom_levels.last() {
        if last.tiles.is_empty() {
            zoom_levels.pop();
        } else {
            break;
        }
    }
}

fn generate_tiles_for_extent(geo_transform: GeoTransform, raster_size: RasterSize, tile_size: u32, zoom: i32) -> Vec<Tile> {
    let top_left = crs::web_mercator_to_lat_lon(geo_transform.top_left());
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
        tile_size.is_multiple_of(Tile::TILE_SIZE),
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

// For the given overview, create the web mercator bounds of each tiff tile at the given zoom level paired with the chunk locations for each band
// Returns a vector of (chunk locations, geo reference) tuples
// The geo reference is the bounding box of the COG tile in web mercator coordinates
// The chunk locations is a list of chunk locations for each band of the COG tile
fn create_cog_tile_web_mercator_bounds(
    overview: &TiffOverview,
    geo_reference: &GeoReference, // georeference of the full cog image
    zoom_level: i32,
    tile_size: u32,
    band_count: u32,
) -> Result<Vec<(Vec<TiffChunkLocation>, GeoReference)>> {
    let mut web_tiles = Vec::with_capacity(overview.chunk_locations.len());

    let cell_size = CellSize::square(Tile::pixel_size_at_zoom_level(zoom_level, tile_size));
    let geo_ref_zoom_level = geotiff::utils::change_georef_cell_size(geo_reference, cell_size);

    let tiles_wide = (overview.raster_size.cols.count() as u32).div_ceil(tile_size) as usize;
    let tiles_high = (overview.raster_size.rows.count() as u32).div_ceil(tile_size) as usize;
    log::error!("Overview: {tiles_wide}x{tiles_high} tiles");

    if tiles_wide * tiles_high * band_count as usize != overview.chunk_locations.len() {
        return Err(Error::InvalidArgument(format!(
            "Expected {} tiles, but got {}",
            tiles_wide * tiles_high * band_count as usize,
            overview.chunk_locations.len()
        )));
    }

    let chunks_per_band = overview.chunk_locations.len() / band_count as usize;
    let tile_size = tile_size as i32;
    for ty in 0..tiles_high {
        let mut current_source_cell = Cell::from_row_col(ty as i32 * tile_size, 0);
        let tile_height = Rows(if current_source_cell.row + tile_size > overview.raster_size.rows.count() {
            debug_assert!(ty + 1 == tiles_high);
            overview.raster_size.rows.count() - current_source_cell.row
        } else {
            tile_size
        });

        for tx in 0..tiles_wide {
            current_source_cell.col = tx as i32 * tile_size;
            let tile_width = Columns(if current_source_cell.col + tile_size > overview.raster_size.cols.count() {
                debug_assert!(tx + 1 == tiles_wide);
                overview.raster_size.cols.count() - current_source_cell.col
            } else {
                tile_size
            });

            let lower_left_cell = Cell::from_row_col(current_source_cell.row + tile_height.count() - 1, current_source_cell.col);

            let cog_tile_geo_ref = GeoReference::with_bottom_left_origin(
                crs::epsg::WGS84_WEB_MERCATOR.to_string(),
                RasterSize::with_rows_cols(tile_height, tile_width),
                geo_ref_zoom_level.cell_lower_left(lower_left_cell),
                cell_size,
                Option::<f64>::None,
            );

            let mut cog_tiles = Vec::with_capacity(band_count as usize);
            for band in 0..band_count {
                cog_tiles.push(overview.chunk_locations[(ty * tiles_wide + tx) + (band as usize * chunks_per_band)]);
            }

            web_tiles.push((cog_tiles, cog_tile_geo_ref));
        }
    }

    Ok(web_tiles)
}

#[derive(Debug, Clone)]
pub struct WebTileInfo {
    pub min_zoom: i32,
    pub max_zoom: i32,
    pub tile_size: u32,
    pub band_count: u32,
    pub data_type: ArrayDataType,
    pub bounds: LatLonBounds,
    pub statistics: Option<TiffStats>,
}

#[derive(Debug, Clone)]
pub struct WebTilesReader {
    web_tiles: WebTiles,
    cog_meta: GeoTiffMetadata,
}

impl WebTilesReader {
    pub fn data_type(&self) -> ArrayDataType {
        self.cog_meta.data_type
    }

    pub fn new(cog_meta: GeoTiffMetadata) -> Result<Self> {
        let web_tiles = WebTiles::from_cog_metadata(&cog_meta)?;

        Ok(Self { web_tiles, cog_meta })
    }

    pub fn tile_info(&self) -> WebTileInfo {
        WebTileInfo {
            min_zoom: self.web_tiles.min_zoom(),
            max_zoom: self.web_tiles.max_zoom(),
            tile_size: self.cog_meta.chunk_row_length(),
            band_count: self.cog_meta.band_count,
            data_type: self.data_type(),
            bounds: self.data_bounds(),
            statistics: self.cog_meta.statistics.clone(),
        }
    }

    pub fn data_bounds(&self) -> LatLonBounds {
        self.web_tiles.data_bounds()
    }

    pub fn cog_metadata(&self) -> &GeoTiffMetadata {
        &self.cog_meta
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

    pub fn overview(&self, zoom_level: i32) -> Option<&TiffOverview> {
        let overview_index = (self.web_tiles.max_zoom() - zoom_level) as usize;
        self.cog_metadata().overviews.get(overview_index)
    }

    /// Returns the most suitable overview for the given square area in pixels.
    pub fn overview_for_display_size(&self, display_size: usize) -> Option<&TiffOverview> {
        let display_size = display_size as i32;

        self.cog_meta.overviews.iter().min_by(|&lhs, &rhs| {
            let left_diff = lhs.raster_size.max_dimension().abs_diff(display_size);
            let right_diff = rhs.raster_size.max_dimension().abs_diff(display_size);

            left_diff.cmp(&right_diff)
        })
    }

    pub fn read_overview(
        &self,
        overview: &TiffOverview,
        band_index: usize,
        chunk_cb: impl FnMut(TiffChunkLocation) -> Result<Vec<u8>>,
    ) -> Result<AnyDenseArray> {
        Ok(match self.data_type() {
            ArrayDataType::Uint8 => AnyDenseArray::U8(self.read_overview_as::<u8>(overview, band_index, chunk_cb)?),
            ArrayDataType::Uint16 => AnyDenseArray::U16(self.read_overview_as::<u16>(overview, band_index, chunk_cb)?),
            ArrayDataType::Uint32 => AnyDenseArray::U32(self.read_overview_as::<u32>(overview, band_index, chunk_cb)?),
            ArrayDataType::Uint64 => AnyDenseArray::U64(self.read_overview_as::<u64>(overview, band_index, chunk_cb)?),
            ArrayDataType::Int8 => AnyDenseArray::I8(self.read_overview_as::<i8>(overview, band_index, chunk_cb)?),
            ArrayDataType::Int16 => AnyDenseArray::I16(self.read_overview_as::<i16>(overview, band_index, chunk_cb)?),
            ArrayDataType::Int32 => AnyDenseArray::I32(self.read_overview_as::<i32>(overview, band_index, chunk_cb)?),
            ArrayDataType::Int64 => AnyDenseArray::I64(self.read_overview_as::<i64>(overview, band_index, chunk_cb)?),
            ArrayDataType::Float32 => AnyDenseArray::F32(self.read_overview_as::<f32>(overview, band_index, chunk_cb)?),
            ArrayDataType::Float64 => AnyDenseArray::F64(self.read_overview_as::<f64>(overview, band_index, chunk_cb)?),
        })
    }

    #[simd_bounds]
    pub fn read_overview_as<T: ArrayNum>(
        &self,
        overview: &TiffOverview,
        band_index: usize,
        chunk_cb: impl FnMut(TiffChunkLocation) -> Result<Vec<u8>>,
    ) -> Result<DenseArray<T>> {
        if T::TYPE != self.cog_meta.data_type {
            return Err(Error::InvalidArgument(format!(
                "Overview data type mismatch: expected {:?}, got {:?}",
                self.cog_meta.data_type,
                T::TYPE
            )));
        }

        let mut buffer = AlignedVecUnderConstruction::new(overview.raster_size.cell_count());
        io::merge_overview_into_buffer::<T, RasterMetadata>(
            &self.cog_meta,
            overview,
            band_index,
            self.cog_meta.chunk_row_length(),
            unsafe { buffer.as_slice_mut() },
            chunk_cb,
        )?;

        DenseArray::<T>::new_init_nodata(
            RasterMetadata::sized_with_nodata(overview.raster_size, self.cog_meta.geo_reference.nodata()),
            unsafe { buffer.assume_init() },
        )
    }

    /// Read the tile data for the given tile using the provided reader.
    /// This method will return an error if the tile does not exist in the COG index
    /// If this is a COG with sparse tile support, for sparse tiles an empty array will be returned
    pub fn read_tile_data(&self, tile: &Tile, band: usize, mut reader: impl Read + Seek) -> Result<Option<AnyDenseArray>> {
        Ok(match self.data_type() {
            ArrayDataType::Uint8 => self.read_tile_data_as::<u8>(tile, band, &mut reader)?.map(AnyDenseArray::U8),
            ArrayDataType::Uint16 => self.read_tile_data_as::<u16>(tile, band, &mut reader)?.map(AnyDenseArray::U16),
            ArrayDataType::Uint32 => self.read_tile_data_as::<u32>(tile, band, &mut reader)?.map(AnyDenseArray::U32),
            ArrayDataType::Uint64 => self.read_tile_data_as::<u64>(tile, band, &mut reader)?.map(AnyDenseArray::U64),
            ArrayDataType::Int8 => self.read_tile_data_as::<i8>(tile, band, &mut reader)?.map(AnyDenseArray::I8),
            ArrayDataType::Int16 => self.read_tile_data_as::<i16>(tile, band, &mut reader)?.map(AnyDenseArray::I16),
            ArrayDataType::Int32 => self.read_tile_data_as::<i32>(tile, band, &mut reader)?.map(AnyDenseArray::I32),
            ArrayDataType::Int64 => self.read_tile_data_as::<i64>(tile, band, &mut reader)?.map(AnyDenseArray::I64),
            ArrayDataType::Float32 => self.read_tile_data_as::<f32>(tile, band, &mut reader)?.map(AnyDenseArray::F32),
            ArrayDataType::Float64 => self.read_tile_data_as::<f64>(tile, band, &mut reader)?.map(AnyDenseArray::F64),
        })
    }

    pub fn parse_tile_data(&self, tile_source: &TileSource, band: usize, cog_chunks: &[&[u8]]) -> Result<AnyDenseArray> {
        if band < 1 || band > self.cog_meta.band_count as usize {
            return Err(Error::InvalidArgument(format!(
                "Band index out of range: requested band {}, but COG has {} bands (bands are 1-indexed)",
                band, self.cog_meta.band_count
            )));
        }
        assert!(band <= cog_chunks.len());

        match tile_source {
            TileSource::Aligned(_) => Ok(match self.data_type() {
                ArrayDataType::Uint8 => AnyDenseArray::U8(self.parse_tile_data_as::<u8>(cog_chunks[0])?),
                ArrayDataType::Uint16 => AnyDenseArray::U16(self.parse_tile_data_as::<u16>(cog_chunks[0])?),
                ArrayDataType::Uint32 => AnyDenseArray::U32(self.parse_tile_data_as::<u32>(cog_chunks[0])?),
                ArrayDataType::Uint64 => AnyDenseArray::U64(self.parse_tile_data_as::<u64>(cog_chunks[0])?),
                ArrayDataType::Int8 => AnyDenseArray::I8(self.parse_tile_data_as::<i8>(cog_chunks[0])?),
                ArrayDataType::Int16 => AnyDenseArray::I16(self.parse_tile_data_as::<i16>(cog_chunks[0])?),
                ArrayDataType::Int32 => AnyDenseArray::I32(self.parse_tile_data_as::<i32>(cog_chunks[0])?),
                ArrayDataType::Int64 => AnyDenseArray::I64(self.parse_tile_data_as::<i64>(cog_chunks[0])?),
                ArrayDataType::Float32 => AnyDenseArray::F32(self.parse_tile_data_as::<f32>(cog_chunks[0])?),
                ArrayDataType::Float64 => AnyDenseArray::F64(self.parse_tile_data_as::<f64>(cog_chunks[0])?),
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
            TileSource::MultiBandAligned(_) => {
                let band_index = band - 1; // to 0-based index
                Ok(match self.data_type() {
                    ArrayDataType::Uint8 => AnyDenseArray::U8(self.parse_tile_data_as::<u8>(cog_chunks[band_index])?),
                    ArrayDataType::Uint16 => AnyDenseArray::U16(self.parse_tile_data_as::<u16>(cog_chunks[band_index])?),
                    ArrayDataType::Uint32 => AnyDenseArray::U32(self.parse_tile_data_as::<u32>(cog_chunks[band_index])?),
                    ArrayDataType::Uint64 => AnyDenseArray::U64(self.parse_tile_data_as::<u64>(cog_chunks[band_index])?),
                    ArrayDataType::Int8 => AnyDenseArray::I8(self.parse_tile_data_as::<i8>(cog_chunks[band_index])?),
                    ArrayDataType::Int16 => AnyDenseArray::I16(self.parse_tile_data_as::<i16>(cog_chunks[band_index])?),
                    ArrayDataType::Int32 => AnyDenseArray::I32(self.parse_tile_data_as::<i32>(cog_chunks[band_index])?),
                    ArrayDataType::Int64 => AnyDenseArray::I64(self.parse_tile_data_as::<i64>(cog_chunks[band_index])?),
                    ArrayDataType::Float32 => AnyDenseArray::F32(self.parse_tile_data_as::<f32>(cog_chunks[band_index])?),
                    ArrayDataType::Float64 => AnyDenseArray::F64(self.parse_tile_data_as::<f64>(cog_chunks[band_index])?),
                })
            }
            TileSource::MultiBandUnaligned(band_tile_sources) => {
                // For unaligned multiband tiles, we need to find the tile sources for the requested band
                let band_index = band - 1;

                // Extract the tile sources for this specific band
                let (tile_sources_for_band, _cutout) = &band_tile_sources[band_index];

                // Calculate the offset into cog_chunks for this band's chunks
                let chunks_before: usize = band_tile_sources[..band_index].iter().map(|(sources, _)| sources.len()).sum();

                let chunks_for_band = tile_sources_for_band.len();
                let band_cog_chunks: Vec<(TiffChunkLocation, CutOut)> =
                    tile_sources_for_band.iter().map(|loc| (*loc, _cutout.clone())).collect();

                let band_chunk_refs: Vec<&[u8]> = cog_chunks[chunks_before..chunks_before + chunks_for_band].to_vec();

                Ok(match self.data_type() {
                    ArrayDataType::Uint8 => AnyDenseArray::U8(self.merge_tile_sources(&band_cog_chunks, &band_chunk_refs)?),
                    ArrayDataType::Uint16 => AnyDenseArray::U16(self.merge_tile_sources(&band_cog_chunks, &band_chunk_refs)?),
                    ArrayDataType::Uint32 => AnyDenseArray::U32(self.merge_tile_sources(&band_cog_chunks, &band_chunk_refs)?),
                    ArrayDataType::Uint64 => AnyDenseArray::U64(self.merge_tile_sources(&band_cog_chunks, &band_chunk_refs)?),
                    ArrayDataType::Int8 => AnyDenseArray::I8(self.merge_tile_sources(&band_cog_chunks, &band_chunk_refs)?),
                    ArrayDataType::Int16 => AnyDenseArray::I16(self.merge_tile_sources(&band_cog_chunks, &band_chunk_refs)?),
                    ArrayDataType::Int32 => AnyDenseArray::I32(self.merge_tile_sources(&band_cog_chunks, &band_chunk_refs)?),
                    ArrayDataType::Int64 => AnyDenseArray::I64(self.merge_tile_sources(&band_cog_chunks, &band_chunk_refs)?),
                    ArrayDataType::Float32 => AnyDenseArray::F32(self.merge_tile_sources(&band_cog_chunks, &band_chunk_refs)?),
                    ArrayDataType::Float64 => AnyDenseArray::F64(self.merge_tile_sources(&band_cog_chunks, &band_chunk_refs)?),
                })
            }
        }
    }

    pub fn parse_multi_band_tile_data(&self, tile_source: &TileSource, cog_chunks: &[&[u8]]) -> Result<Vec<AnyDenseArray>> {
        match tile_source {
            TileSource::MultiBandAligned(cog_tiles) => {
                let tiles = cog_tiles
                    .iter()
                    .zip(cog_chunks.iter())
                    .map(|(chunk, chunk_bytes)| self.parse_tile_data(&TileSource::Aligned(*chunk), 1, &[chunk_bytes]))
                    .collect::<Result<Vec<AnyDenseArray>>>()?;
                Ok(tiles)
            }
            TileSource::MultiBandUnaligned(tile_sources) => {
                let tiles = tile_sources
                    .iter()
                    .zip(cog_chunks.iter())
                    .map(|((chunks, cutout), chunk_bytes)| {
                        self.parse_tile_data(&TileSource::Unaligned(vec![(chunks[0], cutout.clone())]), 1, &[chunk_bytes])
                    })
                    .collect::<Result<Vec<AnyDenseArray>>>()?;
                Ok(tiles)
            }
            _ => Err(Error::InvalidArgument("Multi band tile request on single band raster".into())),
        }
    }

    #[simd_bounds]
    pub fn read_tile_data_as<T: ArrayNum>(
        &self,
        tile: &Tile,
        band: usize,
        reader: &mut (impl Read + Seek),
    ) -> Result<Option<DenseArray<T>>> {
        if T::TYPE != self.cog_meta.data_type {
            return Err(Error::InvalidArgument(format!(
                "Tile data type mismatch: expected {:?}, got {:?}",
                self.cog_meta.data_type,
                T::TYPE
            )));
        }

        if band < 1 || band > self.cog_meta.band_count as usize {
            return Err(Error::InvalidArgument(format!(
                "Band index out of range: requested band {}, but COG has {} bands (bands are 1-indexed)",
                band, self.cog_meta.band_count
            )));
        }

        if let Some(tile_source) = self.tile_source(tile) {
            match tile_source {
                TileSource::Aligned(cog_tile) => Ok(Some(tileio::read_tile_data(
                    cog_tile,
                    self.cog_meta.chunk_row_length(),
                    self.cog_meta.geo_reference.nodata(),
                    self.cog_meta.compression,
                    self.cog_meta.predictor,
                    reader,
                )?)),
                TileSource::Unaligned(tile_sources) => {
                    let cog_chunks: Vec<Vec<u8>> = tile_sources
                        .iter()
                        .flat_map(|(cog_tile_offset, _)| -> Result<Vec<u8>> {
                            if cog_tile_offset.is_sparse() {
                                return Ok(vec![]);
                            }

                            let mut chunk = vec![0; cog_tile_offset.size as usize];
                            io::read_chunk(cog_tile_offset, reader, &mut chunk)?;
                            Ok(chunk)
                        })
                        .collect();

                    let cog_chunk_refs: Vec<&[u8]> = cog_chunks.iter().map(|chunk| chunk.as_slice()).collect();
                    Ok(Some(self.merge_tile_sources(tile_sources, &cog_chunk_refs)?))
                }
                TileSource::MultiBandAligned(band_locations) => {
                    let band_index = band - 1;
                    if band_index >= band_locations.len() {
                        return Err(Error::InvalidArgument(format!(
                            "Band index out of range: requested band {}, but only {} bands available",
                            band,
                            band_locations.len()
                        )));
                    }

                    let cog_tile = &band_locations[band_index];
                    Ok(Some(tileio::read_tile_data(
                        cog_tile,
                        self.cog_meta.chunk_row_length(),
                        self.cog_meta.geo_reference.nodata(),
                        self.cog_meta.compression,
                        self.cog_meta.predictor,
                        reader,
                    )?))
                }
                TileSource::MultiBandUnaligned(band_tile_sources) => {
                    let band_index = band - 1;
                    let tile_sources = band_tile_sources
                        .iter()
                        .map(|(band_chunks, cutout)| (band_chunks[band_index], cutout.clone()))
                        .collect::<Vec<_>>();

                    let cog_chunks: Vec<Vec<u8>> = tile_sources
                        .iter()
                        .flat_map(|(cog_tile_offset, _)| -> Result<Vec<u8>> {
                            if cog_tile_offset.is_sparse() {
                                return Ok(vec![]);
                            }

                            let mut chunk = vec![0; cog_tile_offset.size as usize];
                            io::read_chunk(cog_tile_offset, reader, &mut chunk)?;
                            Ok(chunk)
                        })
                        .collect();

                    let cog_chunk_refs: Vec<&[u8]> = cog_chunks.iter().map(|chunk| chunk.as_slice()).collect();
                    Ok(Some(self.merge_tile_sources(&tile_sources, &cog_chunk_refs)?))
                }
            }
        } else {
            Ok(None)
        }
    }

    #[simd_bounds]
    /// Parses the tile data from a byte slice into a `DenseArray<T>`.
    /// Only call this for parsing tiled data layout.
    fn parse_tile_data_as<T: ArrayNum>(&self, tile_data: &[u8]) -> Result<DenseArray<T>> {
        assert!(self.cog_meta.is_tiled(), "expected tiled data layout");
        let tile_size = self.cog_meta.chunk_row_length();

        if tile_data.is_empty() {
            // Empty tile data means this is a sparse tile, return a nodata array
            return Ok(DenseArray::filled_with_nodata(RasterMetadata::sized(
                RasterSize::square(tile_size as i32),
                T::TYPE,
            )));
        }

        if T::TYPE != self.cog_meta.data_type {
            return Err(Error::InvalidArgument(format!(
                "Tile data type mismatch: expected {:?}, got {:?}",
                self.cog_meta.data_type,
                T::TYPE
            )));
        }

        let tile_data = tileio::parse_tile_data(
            tile_size,
            self.cog_meta.geo_reference.nodata(),
            self.cog_meta.compression,
            self.cog_meta.predictor,
            None,
            tile_data,
        )?;

        Ok(tile_data)
    }

    #[simd_bounds]
    fn merge_tile_sources<T: ArrayNum>(&self, tile_sources: &[(TiffChunkLocation, CutOut)], cog_chunks: &[&[u8]]) -> Result<DenseArray<T>> {
        let tile_size = self.cog_metadata().chunk_row_length() as usize;
        let tile_raster_size = RasterSize::square(tile_size as i32);

        let mut arr = DenseArray::filled_with_nodata(RasterMetadata::sized_with_nodata(tile_raster_size, NumCast::from(T::NODATA)));

        for ((cog_location, cutout), cog_chunck) in tile_sources.iter().zip(cog_chunks) {
            if cog_location.is_sparse() {
                continue; // Skip sparse tiles, they are already filled with nodata
            }

            let tile_cutout = self.parse_tile_data_as::<T>(cog_chunck)?;
            utils::merge_tile_chunk_into_buffer(cutout, &tile_cutout, arr.as_mut_slice(), tile_raster_size);
        }

        Ok(arr)
    }
}

#[cfg(test)]
#[cfg(feature = "gdal")]
mod tests {
    use std::{
        fs::File,
        path::{Path, PathBuf},
    };

    use approx::assert_relative_eq;
    use path_macro::path;

    use crate::{
        Array, Nodata as _, Point, ZoomLevelStrategy,
        cog::{CogCreationOptions, PredictorSelection, create_cog_tiles, debug},
        raster::{Compression, Predictor, RasterReadWrite},
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
            let reader = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;

            let data_bounds = reader.data_bounds();
            assert_relative_eq!(data_bounds.northwest(), Tile { z: 10, x: 519, y: 340 }.upper_left());
            assert_relative_eq!(data_bounds.southeast(), Tile { z: 10, x: 528, y: 344 }.lower_right());
        }

        {
            // Don't allow sparse tiles, The bounds should now match the extent of the lowest zoom level
            create_test_cog(&input, &output, COG_TILE_SIZE, None, None, None, false)?;
            let reader = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
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
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;

            let mut reader = File::open(&output)?;
            cog.read_tile_data_as::<u8>(&reference_tile, 1, &mut reader)
                .expect("None_u8")
                .unwrap()
        };

        {
            // Create a test COG file with LZW compression and no predictor
            create_test_cog(&input, &output, COG_TILE_SIZE, Some(Compression::Lzw), None, None, true)?;
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<u8>(&reference_tile, 1, &mut reader)
                .expect("LZW_u8")
                .unwrap();
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
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::Horizontal));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<u8>(&reference_tile, 1, &mut reader)
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
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::Horizontal));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<i32>(&reference_tile, 1, &mut reader)
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
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, None);
            assert_eq!(cog.tile_info().max_zoom, 10);

            let mut reader = File::open(&output)?;
            assert!(cog.read_tile_data_as::<f64>(&reference_tile, 1, &mut reader).is_err());
            let tile_data = cog
                .read_tile_data_as::<f32>(&reference_tile, 1, &mut reader)
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
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::FloatingPoint));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<f32>(&reference_tile, 1, &mut reader)
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
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::FloatingPoint));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<f64>(&reference_tile, 1, &mut reader)
                .expect("LZW_f64_predictor")
                .unwrap();

            assert_eq!(tile_data.cast_to::<u8>(), reference_tile_data);
        }

        {
            // Create a test COG file as float with Zstď compression and float predictor
            create_test_cog(
                &input,
                &output,
                COG_TILE_SIZE,
                Some(Compression::Zstd),
                Some(PredictorSelection::Automatic),
                Some(ArrayDataType::Float64),
                true,
            )?;
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::FloatingPoint));
            assert_eq!(cog.cog_metadata().compression, Some(Compression::Zstd));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<f64>(&reference_tile, 1, &mut reader)
                .expect("ZSTD_f64_predictor")
                .unwrap();

            assert_eq!(tile_data.cast_to::<u8>(), reference_tile_data);
        }

        Ok(())
    }

    #[test_log::test]
    fn read_test_cog_unaligned_overviews() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let cog_path = create_unaligned_test_cog(tmp.path(), COG_TILE_SIZE)?;
        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&cog_path)?)?;

        let meta = cog.tile_info();
        assert_eq!(meta.min_zoom, 7);
        assert_eq!(meta.max_zoom, 10);

        // Decode all tiles
        let mut reader = File::open(&cog_path)?;

        {
            let tile = Tile { z: 7, x: 66, y: 42 };
            let tile_source = cog.tile_source(&tile).unwrap();
            match tile_source {
                TileSource::Unaligned(tile_sources) => {
                    assert_eq!(1, tile_sources.len());
                }
                _ => {
                    panic!("Expected unaligned single band tile source for tile {tile:?}");
                }
            }

            let tile_data = cog.read_tile_data_as::<u8>(&tile, 1, &mut reader).unwrap().unwrap();
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
                TileSource::Unaligned(tile_sources) => {
                    assert_eq!(2, tile_sources.len());
                }
                _ => {
                    panic!("Expected unaligned single band tile source for tile {tile:?}");
                }
            }

            let tile_data = cog.read_tile_data_as::<u8>(&tile, 1, &mut reader).unwrap().unwrap();
            assert!(
                !tile_data.as_slice()
                    [COG_TILE_SIZE as usize * (COG_TILE_SIZE as usize - 1)..COG_TILE_SIZE as usize * COG_TILE_SIZE as usize]
                    .iter()
                    .all(|&v| v == 0)
            );
        }

        for tile in cog.zoom_level_tile_sources(7).unwrap().keys() {
            if let Some(tile_data) = cog.read_tile_data(tile, 1, &mut reader)? {
                if tile_data.is_empty() {
                    continue; // Skip empty tiles
                }

                assert_eq!(tile_data.len(), RasterSize::square(COG_TILE_SIZE as i32).cell_count());
                assert_eq!(tile_data.data_type(), meta.data_type);

                let tile_data = cog.read_tile_data_as::<u8>(tile, 1, &mut reader)?.unwrap();
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
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;

            let meta = cog.cog_metadata();
            assert_eq!(meta.chunk_row_length(), COG_TILE_SIZE);
            assert_eq!(meta.data_type, ArrayDataType::Uint8);
            assert_eq!(cog.tile_info().min_zoom, 7);
            assert_eq!(cog.tile_info().max_zoom, 9);

            let mut reader = File::open(&output)?;
            cog.read_tile_data_as::<u8>(&reference_tile, 1, &mut reader)?
        };

        {
            // Create a test COG file with LZW compression and no predictor
            create_test_cog(&input, &output, COG_TILE_SIZE, Some(Compression::Lzw), None, None, true)?;
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;

            let mut reader = File::open(&output)?;
            let tile_data = cog.read_tile_data_as::<u8>(&reference_tile, 1, &mut reader)?;
            assert_eq!(tile_data, reference_tile_data);
        }

        Ok(())
    }

    #[test_log::test]
    fn generate_tiles_for_extent_unaligned_256px() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let cog_path = create_unaligned_test_cog(tmp.path(), COG_TILE_SIZE)?;
        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&cog_path)?)?;

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
        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&cog_path)?)?;

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

        let meta = GeoTiffMetadata::from_file(&cog_path)?;
        let zoom_level_8_index = 2;
        let cog_tiles = meta.overviews.get(zoom_level_8_index).unwrap().chunk_locations.clone();

        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&cog_path)?)?;
        let bounds = super::create_cog_tile_web_mercator_bounds(
            cog.overview(8).unwrap(),
            &cog.cog_metadata().geo_reference,
            8,
            cog.cog_metadata().chunk_row_length(),
            meta.band_count,
        )?;

        {
            // Top right tile
            let (cog_tile_location, geo_ref) = &bounds[2];
            assert_eq!(cog_tile_location[0].offset, cog_tiles[2].offset);
            assert_relative_eq!(geo_ref.top_left(), Point::new(547900.6187481433, 6731350.458905762), epsilon = 1e-6);
        }

        {
            // Bottom right tile
            let (cog_tile_location, geo_ref) = &bounds[5];
            assert_eq!(cog_tile_location[0].offset, cog_tiles[5].offset);
            assert_relative_eq!(geo_ref.top_left(), Point::new(547900.6187481433, 6574807.424977721), epsilon = 1e-6);
        }

        Ok(())
    }

    #[test_log::test]
    fn generate_tile_sources_256px() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let cog_path = create_unaligned_test_cog(tmp.path(), COG_TILE_SIZE)?;
        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&cog_path)?)?;

        let tile_sources = cog.zoom_level_tile_sources(8).expect("Zoom level 8 not found");
        let web_tile = Tile { z: 8, x: 132, y: 85 };

        let tile = tile_sources.get(&web_tile).unwrap();
        match tile {
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
            _ => {
                panic!("Expected unaligned single band tile source for tile {tile:?}");
            }
        }

        Ok(())
    }

    #[test_log::test]
    fn generate_tile_sources_512px() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let cog_path = create_unaligned_test_cog(tmp.path(), COG_TILE_SIZE * 2)?;
        let meta = GeoTiffMetadata::from_file(&cog_path)?;
        let zoom_level_7_index = 2;
        let cog_tiles = meta.overviews.get(zoom_level_7_index).unwrap().chunk_locations.clone();
        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&cog_path)?)?;

        assert_eq!(cog.tile_info().max_zoom, 9);

        let tile_sources = cog.zoom_level_tile_sources(7).expect("Zoom level 7 not found");
        let web_tile = Tile { z: 7, x: 65, y: 42 };

        let tile = tile_sources.get(&web_tile).unwrap();
        match tile {
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
            _ => {
                panic!("Expected unaligned single band tile source for tile {tile:?}");
            }
        }

        Ok(())
    }

    #[test_log::test]
    fn overview_for_display_size() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let cog_path = create_unaligned_test_cog(tmp.path(), COG_TILE_SIZE * 2)?;
        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&cog_path)?)?;

        let overview = cog.overview_for_display_size(512).unwrap();

        let mut reader = File::open(&cog_path)?;
        let band_index = 1;
        assert_eq!(RasterSize::with_rows_cols(Rows(512), Columns(1024)), overview.raster_size);
        let actual = cog.read_overview_as::<u8>(overview, band_index, |chunk: TiffChunkLocation| {
            let mut buf = vec![0; chunk.size as usize];
            reader.seek(std::io::SeekFrom::Start(chunk.offset)).unwrap();
            reader.read_exact(&mut buf)?;
            Ok(buf)
        })?;

        let reference = DenseArray::<u8>::read(testutils::geo_test_data_dir().join("reference").join("raster_overview.tif"))?;
        assert_eq!(reference, actual);

        Ok(())
    }

    #[test_log::test]
    fn prefer_lower_zoom_level_outside_of_raster_range_data_handling() -> Result<()> {
        // Test case create because of issue where the prefer lower zoom level strategy would cause zoom level 6
        // to be detected as aligned. Because of this no cutouts would be created and the tiles at zoom level 6 would show
        // 0 values outside of the raster bounds instead of nodata.
        // This was caused by the fact the alignment check only looked at the top left corner of the raster and not the full extent.
        // So if the top left corner was aligned it was assumed that no tile processing was needed.

        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        let tile_size = COG_TILE_SIZE * 2;

        let opts = CogCreationOptions {
            min_zoom: Some(6),
            zoom_level_strategy: ZoomLevelStrategy::PreferLower,
            tile_size,
            allow_sparse: true,
            compression: None,
            predictor: None,
            output_data_type: None,
            aligned_levels: Some(2),
        };
        create_cog_tiles(&input, &output, opts)?;

        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
        assert_eq!(cog.tile_info().min_zoom, 6);
        assert_eq!(cog.tile_info().max_zoom, 8);
        //assert_eq!(cog.cog_metadata().overviews[2]., 8);

        let mut reader = File::open(&output)?;
        let tile = cog.read_tile_data_as::<u8>(&Tile { z: 6, x: 33, y: 21 }, 1, &mut reader)?.unwrap();

        // The upper right corner of this tile is padding outside of the geotiff raster bounds, should be nodata
        assert!(tile.cell_is_nodata(Cell::from_row_col(0, tile_size as i32 - 1)));

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
                debug::dump_tiff_tiles(
                    &cog_path,
                    1,
                    zoom_level,
                    &output_dir.join("cog_tile").join(format!("{tile_size}px")),
                )?;
                debug::dump_web_tiles(
                    &cog_path,
                    1,
                    zoom_level,
                    &output_dir.join("web_tile").join(format!("{tile_size}px")),
                )?;
            }
        }

        Ok(())
    }

    #[test_log::test]
    fn create_multiband_cog_tiles_for_debugging() -> Result<()> {
        // This test generates COG tiles from a test TIFF file and dumps the web tiles and COG tiles for zoom levels 7 and 8.
        // The qgis project in tests/data/cog_debug can be used to visually inspect the generated web tiles with resprect to the cog tiles.

        let output_dir = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "cog_debug_multiband");
        let cog_path = testutils::workspace_test_data_dir().join("multiband_cog_interleave_tile_google_maps_compatible.tif");
        for zoom_level in 15..=17 {
            for band in 1..=5 {
                debug::dump_tiff_tiles(
                    &cog_path,
                    band,
                    zoom_level,
                    &output_dir.join("cog_tile").join(format!("band_{band}")),
                )?;
                debug::dump_web_tiles(
                    &cog_path,
                    band,
                    zoom_level,
                    &output_dir.join("web_tile").join(format!("band_{band}")),
                )?;
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

    #[test_log::test]
    fn read_multiband_unaligned_cog() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("multiband_cog_interleave_tile.tif");
        let meta = GeoTiffMetadata::from_file(&input)?;
        let web_tiles = WebTiles::from_cog_metadata(&meta)?;

        assert_eq!(web_tiles.min_zoom(), 14);
        assert_eq!(web_tiles.max_zoom(), 16);
        assert_eq!(web_tiles.zoom_levels.len(), 17);

        for tile_source in web_tiles.zoom_levels[16].tiles.values() {
            assert!(
                matches!(tile_source, TileSource::MultiBandUnaligned(_)),
                "Expected MultiBandUnaligned tile source"
            );
        }

        Ok(())
    }

    #[test_log::test]
    fn read_multiband_cog_aligned() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("multiband_cog_interleave_tile_google_maps_compatible.tif");
        let meta = GeoTiffMetadata::from_file(&input)?;
        let web_tiles = WebTiles::from_cog_metadata(&meta)?;

        assert_eq!(meta.data_type, ArrayDataType::Float32);
        assert_eq!(web_tiles.min_zoom(), 15);
        assert_eq!(web_tiles.max_zoom(), 17);
        assert_eq!(web_tiles.zoom_levels.len(), 18);

        for zoom_level in 0..=14 {
            assert_eq!(web_tiles.zoom_levels[zoom_level].tiles.len(), 0);
        }

        assert!(!web_tiles.zoom_levels[15].tile_aligned);
        assert!(!web_tiles.zoom_levels[16].tile_aligned);
        assert!(web_tiles.zoom_levels[17].tile_aligned);

        for tile_source in web_tiles.zoom_levels[15].tiles.values() {
            assert!(
                matches!(tile_source, TileSource::MultiBandUnaligned(_)),
                "Expected MultiBandUnaligned tile source"
            );
        }
        for tile_source in web_tiles.zoom_levels[17].tiles.values() {
            assert!(
                matches!(tile_source, TileSource::MultiBandAligned(_)),
                "Expected MultiBandUnaligned tile source"
            );
        }
        Ok(())
    }
}

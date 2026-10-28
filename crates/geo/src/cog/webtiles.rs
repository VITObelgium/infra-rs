use crate::{
    AnyDenseArray, Array as _, ArrayDataType, ArrayInterop, ArrayMetadata as _, ArrayNum, Cell, CellSize, Columns, DenseArray, Error,
    GeoReference, GeoTransform, Point, RasterMetadata, Result, Rows, ZoomLevelStrategy,
    geotiff::{
        self, BandIndex, BandMetadata, GeoTiffMetadata, TiffChunkLocation, TiffOverview, TiffStats, io,
        tileio::{self},
        utils,
    },
    raster::intersection::{CutOut, intersect_georeference},
};
use std::{
    collections::HashMap,
    io::{Read, Seek},
    ops::{Bound, Range, RangeBounds},
};

use crate::{LatLonBounds, RasterSize, Tile, crs};

use inf::allocate::AlignedVecUnderConstruction;

pub(super) fn check_cancelled(cancelled: &impl Fn() -> bool) -> Result<()> {
    if cancelled() { Err(Error::Cancelled) } else { Ok(()) }
}

#[derive(Debug, Clone)]
pub enum TileSource {
    Aligned(TiffChunkLocation),
    Unaligned(Vec<(TiffChunkLocation, CutOut)>),
    // Each element of the vector corresponds to the chunk offset of that band
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
    geo_reference: GeoReference,
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

fn warp_web_mercator_bounds_to_lat_lon(bounds: &GeoReference) -> LatLonBounds {
    let top_left = crs::web_mercator_to_lat_lon(bounds.top_left());
    let bottom_right = crs::web_mercator_to_lat_lon(bounds.bottom_right());

    LatLonBounds::hull(top_left, bottom_right)
}

// Scale the standard resolution directly: the shared tile-size offset assumes
// tiles of at least 256 pixels and underflows for 128-pixel tiles.
fn web_tile_pixel_size(zoom_level: i32, tile_size: u32) -> f64 {
    Tile::pixel_size_at_zoom_level(zoom_level, Tile::TILE_SIZE) * f64::from(Tile::TILE_SIZE) / f64::from(tile_size)
}

fn native_tile_grid_size(overview: &TiffOverview, tile_size: u32, band_count: u32) -> Result<(usize, usize)> {
    if tile_size == 0 || tile_size > i32::MAX as u32 || band_count == 0 {
        return Err(Error::InvalidArgument(
            "COG tile size and band count must be positive and tile size must fit in i32".into(),
        ));
    }
    let rows = overview.raster_size.rows.count();
    let cols = overview.raster_size.cols.count();
    if rows <= 0 || cols <= 0 {
        return Err(Error::InvalidArgument(format!("Invalid COG overview dimensions {cols}x{rows}")));
    }
    let tiles_wide = (cols as u32).div_ceil(tile_size) as usize;
    let tiles_high = (rows as u32).div_ceil(tile_size) as usize;
    let expected_chunks = tiles_wide
        .checked_mul(tiles_high)
        .and_then(|count| count.checked_mul(band_count as usize))
        .ok_or_else(|| Error::InvalidArgument("COG overview chunk count overflows usize".into()))?;
    if overview.chunk_locations.len() != expected_chunks {
        return Err(Error::InvalidArgument(format!(
            "Expected {expected_chunks} chunks for {tiles_wide}x{tiles_high} tiles and {band_count} bands, got {}",
            overview.chunk_locations.len()
        )));
    }
    Ok((tiles_wide, tiles_high))
}

impl WebTiles {
    pub fn from_cog_metadata(meta: &GeoTiffMetadata) -> Result<Self> {
        let tile_size = meta.chunk_row_length();
        if tile_size == 0 || tile_size > i32::MAX as u32 || meta.band_count == 0 || meta.overviews.is_empty() {
            return Err(Error::InvalidArgument(
                "COG requires a positive tile size, band count, and at least one image level".into(),
            ));
        }
        let raster_size = meta.geo_reference.raster_size();
        if raster_size.rows.count() <= 0 || raster_size.cols.count() <= 0 {
            return Err(Error::InvalidArgument("COG raster dimensions must be positive".into()));
        }
        let cell_size = meta.geo_reference.cell_size();
        if !cell_size.x().is_finite() || !cell_size.y().is_finite() || cell_size.x() <= 0.0 || cell_size.y() >= 0.0 {
            return Err(Error::InvalidArgument(
                "COG requires finite, positive-width, north-up pixel sizes".into(),
            ));
        }
        let bottom_right = meta.geo_reference.bottom_right();
        if !meta.geo_reference.geo_transform().coefficients().into_iter().all(f64::is_finite)
            || !bottom_right.x().is_finite()
            || !bottom_right.y().is_finite()
        {
            return Err(Error::InvalidArgument("COG georeferencing must be finite".into()));
        }
        let standard_pixel_size = cell_size.x() * f64::from(tile_size) / f64::from(Tile::TILE_SIZE);
        if !standard_pixel_size.is_finite() || standard_pixel_size <= 0.0 {
            return Err(Error::InvalidArgument(
                "COG pixel size cannot be represented at the requested tile size".into(),
            ));
        }
        let max_zoom = Tile::zoom_level_for_pixel_size(standard_pixel_size, ZoomLevelStrategy::Closest, Tile::TILE_SIZE);
        // Tile bounds add one to signed 32-bit XYZ coordinates.
        if !(0..=30).contains(&max_zoom) {
            return Err(Error::InvalidArgument(format!(
                "COG zoom level {max_zoom} is outside the supported range 0..=30"
            )));
        }
        if meta.overviews.len() > max_zoom as usize + 1 {
            return Err(Error::InvalidArgument(format!(
                "COG has {} image levels starting at zoom {max_zoom}; overview zoom levels would become negative",
                meta.overviews.len()
            )));
        }
        let expected_pixel_size = web_tile_pixel_size(max_zoom, tile_size);
        if (expected_pixel_size - cell_size.x()).abs() > 1e-6 {
            return Err(Error::Runtime(format!(
                "The main COG file content is not scaled to match a web zoom level, COG pixel size {expected_pixel_size}, zoom level {max_zoom} pixel size {}",
                cell_size.x()
            )));
        }
        let mut zoom_levels = vec![ZoomLevelInfo::default(); max_zoom as usize + 1];

        // Offset to get the center of the bottom right pixel
        // Otherwise the bottom right coordinate is exactly at the edge of the tile and the next one will be taken
        let offset = Point::new(cell_size.x() / 2.0, cell_size.y() / 2.0);

        for (level_index, overview) in meta.overviews.iter().enumerate() {
            native_tile_grid_size(overview, tile_size, meta.band_count)?;
            let zoom_level = max_zoom - level_index as i32;
            let top_left_coordinate = crs::web_mercator_to_lat_lon(meta.geo_reference.top_left());
            let bottom_right_center_coordinate = crs::web_mercator_to_lat_lon(bottom_right - offset);
            let top_left_tile = Tile::for_coordinate(top_left_coordinate, zoom_level);
            let bottom_right_tile = Tile::for_coordinate(bottom_right_center_coordinate, zoom_level);

            let tl_diff = meta.geo_reference.top_left() - crs::lat_lon_to_web_mercator(top_left_tile.upper_left());
            let br_diff = bottom_right - crs::lat_lon_to_web_mercator(bottom_right_tile.lower_right());

            let top_left_aligned = tl_diff.x().abs() < 1e-6 && tl_diff.y().abs() < 1e-6;
            let bottom_right_aligned = br_diff.x().abs() < 1e-6 && br_diff.y().abs() < 1e-6;
            let tile_aligned = top_left_aligned && bottom_right_aligned;

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
                let overview_cell_size = CellSize::square(web_tile_pixel_size(zoom_level, tile_size));
                let overview_geo_ref = GeoReference::with_bottom_left_origin(
                    "EPSG:3857",
                    overview.raster_size,
                    meta.geo_reference.bottom_left(),
                    overview_cell_size,
                    Option::<f64>::None,
                );

                let tiles = generate_tiles_for_extent_unaligned(&meta.geo_reference, zoom_level);
                let cog_tile_bounds =
                    create_cog_tile_web_mercator_bounds(overview, &overview_geo_ref, zoom_level, tile_size, meta.band_count)?;
                for tile in &tiles {
                    let mut tile_sources = Vec::new();
                    let web_tile_georef = GeoReference::with_bottom_left_origin(
                        "EPSG:3857",
                        RasterSize::square(tile_size as i32),
                        crs::lat_lon_to_web_mercator(tile.bounds().southwest()),
                        overview_cell_size,
                        Some(f64::NAN),
                    );

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

        trim_empty_zoom_levels(&mut zoom_levels);
        if zoom_levels.is_empty() {
            return Err(Error::InvalidArgument("COG extent does not map to any web tiles".into()));
        }

        Ok(WebTiles {
            zoom_levels,
            geo_reference: meta.geo_reference.clone(),
        })
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
                TileSource::MultiBandAligned(locs) => locs.iter().any(|loc| loc.size > 0),
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
            // No tiles with aligned data at the maximum zoom level
            // Fall back to the tiff georeference
            return warp_web_mercator_bounds_to_lat_lon(&self.geo_reference);
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

fn generate_tiles_for_extent_unaligned(geo_ref: &GeoReference, zoom_level: i32) -> Vec<Tile> {
    // The geo_transform is from the highest zoom level, the origin does not match unaligned zoom levels

    let top_left = crs::web_mercator_to_lat_lon(geo_ref.top_left());
    let bottom_right = crs::web_mercator_to_lat_lon(geo_ref.bottom_right());

    let top_left_tile = Tile::for_coordinate(top_left, zoom_level);
    let bottom_right_tile = Tile::for_coordinate(bottom_right, zoom_level);

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
    let (tiles_wide, tiles_high) = native_tile_grid_size(overview, tile_size, band_count)?;
    let mut web_tiles = Vec::with_capacity(tiles_wide * tiles_high);

    let cell_size = CellSize::square(web_tile_pixel_size(zoom_level, tile_size));
    let geo_ref_zoom_level = geotiff::utils::change_georef_cell_size(geo_reference, cell_size);

    let chunks_per_band = overview.chunk_locations.len() / band_count as usize;
    let tile_size = tile_size as i32;
    for ty in 0..tiles_high {
        let mut current_source_cell = Cell::from_row_col(ty as i32 * tile_size, 0);
        let tile_height = Rows((overview.raster_size.rows.count() - current_source_cell.row).min(tile_size));

        for tx in 0..tiles_wide {
            current_source_cell.col = tx as i32 * tile_size;
            let tile_width = Columns((overview.raster_size.cols.count() - current_source_cell.col).min(tile_size));

            let lower_left_cell = Cell::from_row_col(current_source_cell.row + tile_height.count() - 1, current_source_cell.col);

            let cog_tile_geo_ref = GeoReference::with_bottom_left_origin(
                crs::epsg::WGS84_WEB_MERCATOR.to_string(),
                RasterSize::with_rows_cols(tile_height, tile_width),
                geo_ref_zoom_level.cell_lower_left(lower_left_cell),
                cell_size,
                Option::<f64>::None,
            );

            let cog_tiles: Vec<_> = (0..band_count as usize)
                .map(|band| overview.chunk_locations[(ty * tiles_wide + tx) + (band * chunks_per_band)])
                .collect();

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
    /// The bounding box of the webtiles that contain raster data
    pub tile_bounds: LatLonBounds,
    /// The bounding box on pixel level that contains raster data (typically used to configure a map view camera to show the data)
    pub data_bounds: Option<LatLonBounds>,
    /// Per-band metadata (description, scale, offset), indexed by zero-based band.
    /// Always contains `band_count` entries, with defaults for bands without metadata.
    pub band_metadata: Vec<BandMetadata>,
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

    pub fn cog_metadata(&self) -> &GeoTiffMetadata {
        &self.cog_meta
    }

    /// Returns native TIFF image levels in IFD order, with the base image first.
    ///
    /// The returned slice borrows the high-level overview dimensions and original
    /// chunk locations. An unaligned native chunk can contribute to several XYZ
    /// tiles, so traversing the web tile mappings would duplicate payloads.
    pub fn native_overviews(&self) -> &[TiffOverview] {
        &self.cog_meta.overviews
    }

    pub fn new(mut cog_meta: GeoTiffMetadata) -> Result<Self> {
        let web_tiles = WebTiles::from_cog_metadata(&cog_meta)?;
        cog_meta.normalize_band_metadata()?;

        Ok(Self { web_tiles, cog_meta })
    }

    pub fn tile_info(&self) -> WebTileInfo {
        WebTileInfo {
            min_zoom: self.web_tiles.min_zoom(),
            max_zoom: self.web_tiles.max_zoom(),
            tile_size: self.cog_meta.chunk_row_length(),
            band_count: self.cog_meta.band_count,
            data_type: self.data_type(),
            tile_bounds: self.data_bounds(),
            data_bounds: self.cog_meta.data_bounds.map(LatLonBounds::from),
            band_metadata: self.cog_meta.band_metadata.clone(),
            statistics: self.cog_meta.statistics.clone(),
        }
    }

    pub fn data_bounds(&self) -> LatLonBounds {
        self.web_tiles.data_bounds()
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
        let overview_index = usize::try_from(self.web_tiles.max_zoom().checked_sub(zoom_level)?).ok()?;
        self.native_overviews().get(overview_index)
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
        band_index: BandIndex,
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

    pub fn read_overview_as<T: ArrayNum>(
        &self,
        overview: &TiffOverview,
        band_index: BandIndex,
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
    pub fn read_tile_data(&self, tile: &Tile, band: BandIndex, mut reader: impl Read + Seek) -> Result<Option<AnyDenseArray>> {
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

    // Version of the tiledata parsing that takes pre-fetched cog chunks as input
    // Used for contexts where only asynchronous chunk fetching is possible
    // `cog_chunks` must contain only the chunks needed to reconstruct the requested `band` for this tile:
    // - `TileSource::Aligned` and `TileSource::MultiBandAligned`: exactly one chunk (the selected band)
    // - `TileSource::Unaligned` and `TileSource::MultiBandUnaligned`: one chunk per tile source for the selected band
    pub fn parse_tile_data(&self, tile_source: &TileSource, band: BandIndex, cog_chunks: &[&[u8]]) -> Result<AnyDenseArray> {
        self.parse_tile_data_cancellable(tile_source, band, cog_chunks, &|| false)
    }

    pub(super) fn parse_tile_data_cancellable(
        &self,
        tile_source: &TileSource,
        band: BandIndex,
        cog_chunks: &[&[u8]],
        cancelled: &impl Fn() -> bool,
    ) -> Result<AnyDenseArray> {
        if band.get() > self.cog_meta.band_count as usize {
            return Err(Error::InvalidArgument(format!(
                "Band index out of range: requested band {}, but COG has {} bands (bands are 1-indexed)",
                band.get(),
                self.cog_meta.band_count
            )));
        }

        check_cancelled(cancelled)?;
        let array = match tile_source {
            TileSource::Aligned(_) | TileSource::MultiBandAligned(_) => {
                assert_eq!(
                    cog_chunks.len(),
                    1,
                    "Aligned tile parsing requires exactly one selected-band chunk, got {}",
                    cog_chunks.len()
                );
                crate::dispatch_datatype!(self.data_type(), T, self.parse_tile_data_as::<T>(cog_chunks[0])?)
            }
            TileSource::Unaligned(tile_sources) => {
                assert_eq!(
                    tile_sources.len(),
                    cog_chunks.len(),
                    "Unaligned tile parsing requires one chunk per tile source, expected {}, got {}",
                    tile_sources.len(),
                    cog_chunks.len()
                );
                crate::dispatch_datatype!(
                    self.data_type(),
                    T,
                    self.merge_tile_sources_cancellable::<T>(tile_sources, cog_chunks, cancelled)?
                )
            }
            TileSource::MultiBandUnaligned(band_tile_sources) => {
                let band_index = band.get() - 1;
                assert_eq!(
                    band_tile_sources.len(),
                    cog_chunks.len(),
                    "Unaligned multiband tile parsing requires one selected-band chunk per tile source, expected {}, got {}",
                    band_tile_sources.len(),
                    cog_chunks.len()
                );
                let band_cog_chunks = band_tile_sources
                    .iter()
                    .map(|(locations, cutout)| (locations[band_index], cutout.clone()))
                    .collect::<Vec<_>>();
                crate::dispatch_datatype!(
                    self.data_type(),
                    T,
                    self.merge_tile_sources_cancellable::<T>(&band_cog_chunks, cog_chunks, cancelled)?
                )
            }
        };
        check_cancelled(cancelled)?;
        Ok(array)
    }

    /// Normalizes a nonempty, 1-indexed band selection to an exclusive range.
    pub(super) fn normalize_band_range(&self, band_range: impl RangeBounds<usize>) -> Result<Range<usize>> {
        let invalid_range = || Error::InvalidArgument("Band range must be nonempty, 1-indexed, and within the COG band count".into());
        let band_limit = (self.cog_meta.band_count as usize).checked_add(1).ok_or_else(invalid_range)?;
        let band_start = match band_range.start_bound() {
            Bound::Included(&start) => Some(start),
            Bound::Excluded(&start) => start.checked_add(1),
            Bound::Unbounded => Some(1),
        }
        .ok_or_else(invalid_range)?;
        let band_end = match band_range.end_bound() {
            Bound::Included(&end) => end.checked_add(1),
            Bound::Excluded(&end) => Some(end),
            Bound::Unbounded => Some(band_limit),
        }
        .ok_or_else(invalid_range)?;
        if !(1..band_end).contains(&band_start) || band_end > band_limit {
            return Err(invalid_range());
        }
        Ok(band_start..band_end)
    }

    // `band_range` supports all `RangeBounds<usize>` forms, including inclusive ranges.
    // This API only accepts multiband tile sources.
    // `cog_chunks` must contain only the chunks needed to reconstruct the requested bands for this tile:
    // - `TileSource::MultiBandAligned`: one chunk per requested band (band_range.len() chunks total)
    // - `TileSource::MultiBandUnaligned`: for each tile source, the requested bands in order
    //   (tile_source_count * band_range.len() chunks total, grouped per tile source)
    pub fn parse_multi_band_tile_data<R: std::ops::RangeBounds<usize>>(
        &self,
        band_range: R,
        tile_source: &TileSource,
        cog_chunks: &[&[u8]],
    ) -> Result<Vec<AnyDenseArray>> {
        self.parse_multi_band_tile_data_cancellable(band_range, tile_source, cog_chunks, || false)
    }

    /// Parses selected bands using the same decoder as [`Self::parse_tile_data`].
    ///
    /// Cancellation is checked before and after each band decode and between
    /// native chunk decodes. Cancellation discards all partially decoded output.
    /// Invalid or overflowing band ranges return [`Error::InvalidArgument`].
    pub fn parse_multi_band_tile_data_cancellable<R: RangeBounds<usize>>(
        &self,
        band_range: R,
        tile_source: &TileSource,
        cog_chunks: &[&[u8]],
        cancelled: impl Fn() -> bool,
    ) -> Result<Vec<AnyDenseArray>> {
        if !matches!(tile_source, TileSource::MultiBandAligned(_) | TileSource::MultiBandUnaligned(_)) {
            return Err(Error::InvalidArgument(
                "parse_multi_band_tile_data requires a multiband tile source".to_string(),
            ));
        }

        let band_range = self.normalize_band_range(band_range)?;
        let band_start = band_range.start;
        let requested_band_count = band_range.len();
        let chunks_valid = match tile_source {
            TileSource::MultiBandAligned(_) => cog_chunks.len() == requested_band_count,
            TileSource::MultiBandUnaligned(band_tile_sources) => cog_chunks.len() >= band_tile_sources.len() * requested_band_count,
            _ => unreachable!("non-multiband tile sources are rejected above"),
        };
        assert!(
            chunks_valid,
            "Invalid multiband tile parse precondition: band_range={:?}, band_count={}, chunk_count={}",
            band_range,
            self.cog_meta.band_count,
            cog_chunks.len()
        );

        match tile_source {
            TileSource::MultiBandAligned(_) => band_range
                .zip(cog_chunks.iter())
                .map(|(band, chunk)| {
                    let band = BandIndex::new(band).unwrap();
                    self.parse_tile_data_cancellable(tile_source, band, &[*chunk], &cancelled)
                })
                .collect::<Result<Vec<_>>>(),
            TileSource::MultiBandUnaligned(band_tile_sources) => {
                let tile_count = band_tile_sources.len();
                band_range
                    .map(|band| {
                        let band = BandIndex::new(band).unwrap();
                        let band_offset = band.get() - band_start;
                        let band_chunks = cog_chunks
                            .iter()
                            .skip(band_offset)
                            .step_by(requested_band_count)
                            .take(tile_count)
                            .copied()
                            .collect::<Vec<_>>();
                        self.parse_tile_data_cancellable(tile_source, band, &band_chunks, &cancelled)
                    })
                    .collect::<Result<Vec<_>>>()
            }
            _ => unreachable!("non-multiband tile sources are rejected above"),
        }
    }

    pub fn read_tile_data_as<T: ArrayNum>(
        &self,
        tile: &Tile,
        band: BandIndex,
        reader: &mut (impl Read + Seek),
    ) -> Result<Option<DenseArray<T>>> {
        if T::TYPE != self.cog_meta.data_type {
            return Err(Error::InvalidArgument(format!(
                "Tile data type mismatch: expected {:?}, got {:?}",
                self.cog_meta.data_type,
                T::TYPE
            )));
        }

        if band.get() > self.cog_meta.band_count as usize {
            return Err(Error::InvalidArgument(format!(
                "Band index out of range: requested band {}, but COG has {} bands (bands are 1-indexed)",
                band.get(),
                self.cog_meta.band_count
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
                        .map(|(cog_tile_offset, _)| -> Result<Vec<u8>> {
                            if cog_tile_offset.is_sparse() {
                                return Ok(vec![]);
                            }

                            let mut chunk = vec![0; cog_tile_offset.size as usize];
                            io::read_chunk(cog_tile_offset, reader, &mut chunk)?;
                            Ok(chunk)
                        })
                        .collect::<Result<_>>()?;

                    let cog_chunk_refs: Vec<&[u8]> = cog_chunks.iter().map(|chunk| chunk.as_slice()).collect();
                    Ok(Some(self.merge_tile_sources(tile_sources, &cog_chunk_refs)?))
                }
                TileSource::MultiBandAligned(band_locations) => {
                    let band_index = band.get() - 1; // to 0-based index
                    if band_index >= band_locations.len() {
                        return Err(Error::InvalidArgument(format!(
                            "Band index out of range: requested band {}, but only {} bands available",
                            band.get(),
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
                    let band_index = band.get() - 1; // to 0-based index
                    let tile_sources = band_tile_sources
                        .iter()
                        .map(|(band_chunks, cutout)| (band_chunks[band_index], cutout.clone()))
                        .collect::<Vec<_>>();

                    let cog_chunks: Vec<Vec<u8>> = tile_sources
                        .iter()
                        .map(|(cog_tile_offset, _)| -> Result<Vec<u8>> {
                            if cog_tile_offset.is_sparse() {
                                return Ok(vec![]);
                            }

                            let mut chunk = vec![0; cog_tile_offset.size as usize];
                            io::read_chunk(cog_tile_offset, reader, &mut chunk)?;
                            Ok(chunk)
                        })
                        .collect::<Result<_>>()?;

                    let cog_chunk_refs: Vec<&[u8]> = cog_chunks.iter().map(|chunk| chunk.as_slice()).collect();
                    Ok(Some(self.merge_tile_sources(&tile_sources, &cog_chunk_refs)?))
                }
            }
        } else {
            Ok(None)
        }
    }

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

    fn merge_tile_sources<T: ArrayNum>(&self, tile_sources: &[(TiffChunkLocation, CutOut)], cog_chunks: &[&[u8]]) -> Result<DenseArray<T>> {
        self.merge_tile_sources_cancellable(tile_sources, cog_chunks, &|| false)
    }

    fn merge_tile_sources_cancellable<T: ArrayNum>(
        &self,
        tile_sources: &[(TiffChunkLocation, CutOut)],
        cog_chunks: &[&[u8]],
        cancelled: &impl Fn() -> bool,
    ) -> Result<DenseArray<T>> {
        let tile_size = self.cog_metadata().chunk_row_length() as usize;
        let tile_raster_size = RasterSize::square(tile_size as i32);

        let mut arr = DenseArray::filled_with_nodata(RasterMetadata::sized_with_nodata(tile_raster_size, num::NumCast::from(T::NODATA)));

        for ((cog_location, cutout), cog_chunck) in tile_sources.iter().zip(cog_chunks) {
            check_cancelled(cancelled)?;
            if cog_location.is_sparse() {
                continue; // Skip sparse tiles, they are already filled with nodata
            }

            let tile_cutout = self.parse_tile_data_as::<T>(cog_chunck)?;
            check_cancelled(cancelled)?;
            utils::merge_tile_chunk_into_buffer(cutout, &tile_cutout, arr.as_mut_slice(), tile_raster_size);
        }

        Ok(arr)
    }
}

#[cfg(test)]
mod mapping_tests {
    use std::io::{Cursor, ErrorKind, SeekFrom};

    use crate::{FIRST_BAND, geotiff::ChunkDataLayout, testutils};

    use super::*;

    fn mapping_metadata(tile_size: u32, band_count: u32, unaligned: bool) -> Result<GeoTiffMetadata> {
        let input = testutils::workspace_test_data_dir().join("multiband_cog_interleave_tile_google_maps_compatible.tif");
        let mut meta = GeoTiffMetadata::from_file(&input)?;
        let tile = Tile { z: 2, x: 1, y: 1 };
        let pixel_size = web_tile_pixel_size(tile.z, tile_size);
        let mut top_left = crs::lat_lon_to_web_mercator(tile.upper_left());
        if unaligned {
            let shift = pixel_size * f64::from(tile_size) / 2.0;
            top_left += Point::new(shift, -shift);
        }
        let size = RasterSize::square(tile_size as i32);
        meta.geo_reference = GeoReference::with_top_left_origin("EPSG:3857", size, top_left, CellSize::square(pixel_size), Some(255_u8));
        meta.data_layout = ChunkDataLayout::Tiled(tile_size);
        meta.band_count = band_count;
        meta.band_metadata.resize_with(band_count as usize, BandMetadata::default);
        meta.data_type = ArrayDataType::Uint8;
        meta.compression = None;
        meta.predictor = None;
        meta.overviews.truncate(1);
        meta.overviews[0].raster_size = size;
        let chunk_size = u64::from(tile_size).pow(2);
        meta.overviews[0].chunk_locations = (0..band_count)
            .map(|band| TiffChunkLocation {
                offset: 1 + u64::from(band) * chunk_size,
                size: chunk_size,
            })
            .collect();
        Ok(meta)
    }

    #[test]
    fn normalize_band_range_supports_all_bound_forms() -> Result<()> {
        let reader = WebTilesReader::new(mapping_metadata(256, 3, false)?)?;
        for (bounds, expected) in [
            ((Bound::Unbounded, Bound::Unbounded), 1..4),
            ((Bound::Included(1), Bound::Included(3)), 1..4),
            ((Bound::Excluded(1), Bound::Excluded(4)), 2..4),
            ((Bound::Included(2), Bound::Unbounded), 2..4),
            ((Bound::Unbounded, Bound::Excluded(2)), 1..2),
        ] {
            assert_eq!(reader.normalize_band_range(bounds)?, expected);
        }
        Ok(())
    }

    #[test]
    fn normalize_band_range_rejects_empty_out_of_bounds_and_overflow() -> Result<()> {
        let reader = WebTilesReader::new(mapping_metadata(256, 3, false)?)?;
        for bounds in [
            (Bound::Included(0), Bound::Excluded(2)),
            (Bound::Included(2), Bound::Excluded(2)),
            (Bound::Included(3), Bound::Excluded(1)),
            (Bound::Included(1), Bound::Included(4)),
            (Bound::Included(4), Bound::Unbounded),
            (Bound::Unbounded, Bound::Excluded(0)),
            (Bound::Excluded(usize::MAX), Bound::Unbounded),
            (Bound::Unbounded, Bound::Included(usize::MAX)),
        ] {
            assert!(matches!(reader.normalize_band_range(bounds), Err(Error::InvalidArgument(_))));
        }
        Ok(())
    }

    #[test]
    fn multiband_parser_rejects_invalid_range_before_decoding() -> Result<()> {
        let reader = WebTilesReader::new(mapping_metadata(256, 3, false)?)?;
        let source = TileSource::MultiBandAligned(reader.cog_metadata().overviews[0].chunk_locations.clone());
        assert!(matches!(
            reader.parse_multi_band_tile_data(0..2, &source, &[&[1], &[2]]),
            Err(Error::InvalidArgument(_))
        ));
        Ok(())
    }

    #[test]
    fn parser_checks_cancellation_before_decoding_truncated_band() -> Result<()> {
        let reader = WebTilesReader::new(mapping_metadata(256, 1, false)?)?;
        let source = TileSource::Aligned(reader.cog_metadata().overviews[0].chunk_locations[0]);
        assert!(matches!(
            reader.parse_tile_data_cancellable(&source, FIRST_BAND, &[&[1]], &|| true),
            Err(Error::Cancelled)
        ));
        Ok(())
    }

    #[test]
    fn parser_discards_decoded_band_when_cancelled_after_decode() -> Result<()> {
        let reader = WebTilesReader::new(mapping_metadata(256, 1, false)?)?;
        let source = TileSource::Aligned(reader.cog_metadata().overviews[0].chunk_locations[0]);
        let checks = std::cell::Cell::new(0);
        let result = reader.parse_tile_data_cancellable(&source, FIRST_BAND, &[&[]], &|| {
            checks.set(checks.get() + 1);
            checks.get() == 2
        });
        assert!(matches!(result, Err(Error::Cancelled)));
        assert_eq!(checks.get(), 2);
        Ok(())
    }

    #[test]
    fn multiband_parser_cancels_after_first_band_or_before_truncated_second_band() -> Result<()> {
        let reader = WebTilesReader::new(mapping_metadata(256, 3, false)?)?;
        let source = TileSource::MultiBandAligned(reader.cog_metadata().overviews[0].chunk_locations.clone());
        let first_chunk = vec![11; 256 * 256];
        for cancel_at in [2, 3] {
            let checks = std::cell::Cell::new(0);
            let result = reader.parse_multi_band_tile_data_cancellable(1..3, &source, &[&first_chunk, &[1]], || {
                checks.set(checks.get() + 1);
                checks.get() == cancel_at
            });
            assert!(matches!(result, Err(Error::Cancelled)));
            assert_eq!(checks.get(), cancel_at);
        }
        Ok(())
    }

    #[test]
    fn unaligned_parser_checks_cancellation_between_native_chunk_decodes() -> Result<()> {
        let reader = WebTilesReader::new(mapping_metadata(128, 1, true)?)?;
        let location = reader.cog_metadata().overviews[0].chunk_locations[0];
        let cutout = CutOut {
            rows: 128,
            cols: 64,
            ..Default::default()
        };
        let source = TileSource::Unaligned(vec![(location, cutout.clone()), (location, cutout)]);
        let first_chunk = vec![11; 128 * 128];
        let checks = std::cell::Cell::new(0);
        let result = reader.parse_tile_data_cancellable(&source, FIRST_BAND, &[&first_chunk, &[1]], &|| {
            checks.set(checks.get() + 1);
            checks.get() == 4
        });
        assert!(matches!(result, Err(Error::Cancelled)));
        assert_eq!(checks.get(), 4);
        Ok(())
    }

    #[test]
    fn reader_pads_incomplete_band_metadata() -> Result<()> {
        for existing_bands in [0, 1] {
            let mut meta = mapping_metadata(128, 3, false)?;
            meta.band_metadata = vec![
                BandMetadata {
                    description: Some("Red".into()),
                    scale: Some(crate::RasterScale { scale: 0.5, offset: 10.0 }),
                };
                existing_bands
            ];
            let reader = WebTilesReader::new(meta)?;
            let metadata = reader.cog_metadata();
            assert_eq!(metadata.band_metadata.len(), metadata.band_count as usize);
            if existing_bands == 1 {
                assert_eq!(metadata.band_metadata[0].description.as_deref(), Some("Red"));
                assert_eq!(
                    metadata.band_metadata[0].scale,
                    Some(crate::RasterScale { scale: 0.5, offset: 10.0 })
                );
            }
            assert!(
                metadata.band_metadata[existing_bands..]
                    .iter()
                    .all(|band| band.description.is_none() && band.scale.is_none())
            );
            let tile_info = reader.tile_info();
            assert_eq!(tile_info.band_metadata.len(), tile_info.band_count as usize);
        }
        Ok(())
    }

    #[test]
    fn reader_rejects_excess_band_metadata() -> Result<()> {
        let mut meta = mapping_metadata(128, 1, false)?;
        meta.band_metadata.push(BandMetadata::default());
        assert!(matches!(WebTilesReader::new(meta), Err(Error::InvalidArgument(message)) if message.contains("Band metadata")));
        Ok(())
    }

    #[test]
    fn rejects_zero_tile_size_band_count_and_missing_levels() -> Result<()> {
        let meta = mapping_metadata(256, 1, false)?;
        let mut invalid = meta.clone();
        invalid.data_layout = ChunkDataLayout::Tiled(0);
        assert!(matches!(WebTilesReader::new(invalid), Err(Error::InvalidArgument(_))));
        let mut invalid = meta.clone();
        invalid.band_count = 0;
        assert!(matches!(WebTilesReader::new(invalid), Err(Error::InvalidArgument(_))));
        let mut invalid = meta;
        invalid.overviews.clear();
        assert!(matches!(WebTilesReader::new(invalid), Err(Error::InvalidArgument(_))));
        Ok(())
    }

    #[test]
    fn rejects_nonpositive_raster_and_overview_dimensions() -> Result<()> {
        let meta = mapping_metadata(256, 1, false)?;
        for (rows, cols) in [(0, 256), (-1, 256), (256, 0), (256, -1)] {
            let size = RasterSize::with_rows_cols(Rows(rows), Columns(cols));
            let mut invalid = meta.clone();
            invalid.geo_reference = GeoReference::new("EPSG:3857", size, meta.geo_reference.geo_transform(), None, None);
            assert!(matches!(WebTilesReader::new(invalid), Err(Error::InvalidArgument(_))));
            let mut invalid = meta.clone();
            invalid.overviews[0].raster_size = size;
            assert!(matches!(WebTilesReader::new(invalid), Err(Error::InvalidArgument(_))));
        }
        Ok(())
    }

    #[test]
    fn rejects_invalid_pixel_sizes() -> Result<()> {
        let meta = mapping_metadata(256, 1, false)?;
        for size in [0.0, -1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut invalid = meta.clone();
            invalid.geo_reference.set_cell_size_x(size);
            assert!(
                matches!(WebTilesReader::new(invalid), Err(Error::InvalidArgument(_))),
                "pixel width {size}"
            );
        }
        for size in [0.0, 1.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut invalid = meta.clone();
            invalid.geo_reference.set_cell_size_y(size);
            assert!(
                matches!(WebTilesReader::new(invalid), Err(Error::InvalidArgument(_))),
                "pixel height {size}"
            );
        }
        Ok(())
    }

    #[test]
    fn rejects_nonfinite_georeferencing() -> Result<()> {
        let meta = mapping_metadata(256, 1, false)?;
        for index in [0, 2, 3, 4] {
            let mut coefficients = meta.geo_reference.geo_transform().coefficients();
            coefficients[index] = f64::NAN;
            let mut invalid = meta.clone();
            invalid.geo_reference = GeoReference::new("EPSG:3857", meta.geo_reference.raster_size(), coefficients.into(), None, None);
            assert!(matches!(WebTilesReader::new(invalid), Err(Error::InvalidArgument(_))));
        }
        Ok(())
    }

    #[test]
    fn rejects_unrepresentable_zoom_and_overview_underflow() -> Result<()> {
        let meta = mapping_metadata(256, 1, false)?;
        for zoom in [-1, 31] {
            let mut invalid = meta.clone();
            invalid
                .geo_reference
                .set_cell_size(CellSize::square(web_tile_pixel_size(zoom, 256)));
            let error = WebTilesReader::new(invalid).unwrap_err();
            assert!(matches!(error, Error::InvalidArgument(message) if message.contains("zoom level")));
        }
        let mut invalid = meta;
        invalid.geo_reference.set_cell_size(CellSize::square(web_tile_pixel_size(0, 256)));
        invalid.overviews.push(invalid.overviews[0].clone());
        let error = WebTilesReader::new(invalid).unwrap_err();
        assert!(matches!(error, Error::InvalidArgument(message) if message.contains("would become negative")));
        Ok(())
    }

    #[test]
    fn rejects_aligned_chunk_count_mismatches_without_zip_truncation() -> Result<()> {
        for band_count in [1, 3] {
            let meta = mapping_metadata(256, band_count, false)?;
            let valid = WebTilesReader::new(meta.clone())?;
            assert!(valid.web_tiles.zoom_levels[2].tile_aligned);
            for extra in [false, true] {
                let mut invalid = meta.clone();
                if extra {
                    invalid.overviews[0]
                        .chunk_locations
                        .extend_from_slice(&meta.overviews[0].chunk_locations);
                } else {
                    invalid.overviews[0].chunk_locations.clear();
                }
                let error = WebTilesReader::new(invalid).unwrap_err();
                assert!(matches!(error, Error::InvalidArgument(message) if message.contains("Expected")));
            }
        }
        Ok(())
    }

    #[test]
    fn rejects_unaligned_chunk_count_mismatches() -> Result<()> {
        for band_count in [1, 3] {
            let mut meta = mapping_metadata(256, band_count, true)?;
            meta.overviews[0].chunk_locations.clear();
            let error = WebTilesReader::new(meta).unwrap_err();
            assert!(matches!(error, Error::InvalidArgument(message) if message.contains("Expected")));
        }
        Ok(())
    }

    #[test]
    fn native_overviews_preserve_ifd_order_and_metadata() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("multiband_cog_interleave_tile.tif");
        let meta = GeoTiffMetadata::from_file(&input)?;
        let expected = meta.overviews.clone();
        let reader = WebTilesReader::new(meta)?;
        assert!(std::ptr::eq(reader.native_overviews(), reader.cog_metadata().overviews.as_slice()));
        assert_eq!(reader.native_overviews().len(), expected.len());
        for (actual, expected) in reader.native_overviews().iter().zip(expected) {
            assert_eq!(actual.raster_size, expected.raster_size);
            assert_eq!(actual.chunk_locations, expected.chunk_locations);
        }
        assert!(reader.overview(i32::MIN).is_none());
        assert!(reader.overview(i32::MAX).is_none());
        Ok(())
    }

    #[test]
    fn rejects_non_xyz_resolution() -> Result<()> {
        let mut meta = mapping_metadata(256, 1, false)?;
        meta.geo_reference.set_cell_size(CellSize::square(30.0));
        let error = WebTilesReader::new(meta).unwrap_err();
        assert!(matches!(error, Error::Runtime(message) if message.contains("not scaled to match a web zoom level")));
        Ok(())
    }

    #[test]
    fn native_overviews_do_not_duplicate_unaligned_128_pixel_chunks() -> Result<()> {
        for band_count in [1, 3] {
            let reader = WebTilesReader::new(mapping_metadata(128, band_count, true)?)?;
            let native_chunks = &reader.native_overviews()[0].chunk_locations;
            assert_eq!(native_chunks.len(), band_count as usize);
            let sources = reader.zoom_level_tile_sources(2).unwrap();
            assert!(sources.len() > 1);
            for source in sources.values() {
                match source {
                    TileSource::Unaligned(chunks) => assert!(chunks.iter().all(|(chunk, _)| chunk == &native_chunks[0])),
                    TileSource::MultiBandUnaligned(chunks) => assert!(chunks.iter().all(|(chunk, _)| chunk == native_chunks)),
                    other => panic!("Expected an unaligned tile source, got {other:?}"),
                }
            }
        }
        Ok(())
    }

    #[test]
    fn unaligned_reads_propagate_truncated_chunks() -> Result<()> {
        for band_count in [1, 3] {
            let reader = WebTilesReader::new(mapping_metadata(128, band_count, true)?)?;
            let tile = reader.zoom_level_tile_sources(2).unwrap().keys().next().unwrap();
            let band = BandIndex::new(band_count as usize).unwrap();
            let error = reader
                .read_tile_data_as::<u8>(tile, band, &mut Cursor::new(Vec::<u8>::new()))
                .unwrap_err();
            assert!(matches!(error, Error::IOError(error) if error.kind() == ErrorKind::UnexpectedEof));
        }
        Ok(())
    }

    struct FailedSeek;

    impl Read for FailedSeek {
        fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
            panic!("Read must not occur after a failed seek");
        }
    }

    impl Seek for FailedSeek {
        fn seek(&mut self, _: SeekFrom) -> std::io::Result<u64> {
            Err(ErrorKind::PermissionDenied.into())
        }
    }

    #[test]
    fn unaligned_reads_propagate_seek_errors() -> Result<()> {
        for band_count in [1, 3] {
            let reader = WebTilesReader::new(mapping_metadata(128, band_count, true)?)?;
            let tile = reader.zoom_level_tile_sources(2).unwrap().keys().next().unwrap();
            let band = BandIndex::new(band_count as usize).unwrap();
            let error = reader.read_tile_data_as::<u8>(tile, band, &mut FailedSeek).unwrap_err();
            assert!(matches!(error, Error::IOError(error) if error.kind() == ErrorKind::PermissionDenied));
        }
        Ok(())
    }

    #[test]
    fn unaligned_reads_keep_sparse_chunks_and_cutouts_in_order() -> Result<()> {
        let tile_size = 128;
        let chunk_size = u64::from(tile_size).pow(2);
        let chunks = [
            TiffChunkLocation { offset: 0, size: 0 },
            TiffChunkLocation {
                offset: 1,
                size: chunk_size,
            },
            TiffChunkLocation {
                offset: 1 + chunk_size,
                size: chunk_size,
            },
        ];
        let cutouts = [
            CutOut::default(),
            CutOut {
                rows: 128,
                cols: 64,
                ..Default::default()
            },
            CutOut {
                rows: 128,
                cols: 64,
                dst_col_offset: 64,
                ..Default::default()
            },
        ];
        let mut data = vec![0];
        data.extend(vec![11; chunk_size as usize]);
        data.extend(vec![22; chunk_size as usize]);
        let expected = (0..128)
            .flat_map(|_| (0..128).map(|col| if col < 64 { 11 } else { 22 }))
            .collect::<Vec<_>>();
        for band_count in [1, 3] {
            let mut reader = WebTilesReader::new(mapping_metadata(tile_size, band_count, true)?)?;
            let tile = *reader.zoom_level_tile_sources(2).unwrap().keys().next().unwrap();
            let source = if band_count == 1 {
                TileSource::Unaligned(chunks.into_iter().zip(cutouts.iter().cloned()).collect())
            } else {
                TileSource::MultiBandUnaligned(
                    chunks
                        .iter()
                        .zip(&cutouts)
                        .map(|(chunk, cutout)| {
                            let mut bands = vec![chunks[0]; band_count as usize];
                            bands[band_count as usize - 1] = *chunk;
                            (bands, cutout.clone())
                        })
                        .collect(),
                )
            };
            reader.web_tiles.zoom_levels[2].tiles.insert(tile, source);
            let band = if band_count == 1 {
                FIRST_BAND
            } else {
                BandIndex::new(band_count as usize).unwrap()
            };
            let actual = reader.read_tile_data_as::<u8>(&tile, band, &mut Cursor::new(&data))?.unwrap();
            assert_eq!(actual.as_slice(), expected);
        }
        Ok(())
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
        Array, FIRST_BAND, Nodata as _, Point, ZoomLevelStrategy,
        cog::{CogCreationOptions, PredictorSelection, create_cog_tiles, debug},
        raster::{Compression, DenseRaster, Predictor, RasterReadWrite},
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
            scale: false,
        };
        create_cog_tiles(input_tif, output_tif, opts)?;

        Ok(())
    }

    #[test_log::test]
    fn lerc_compression_metadata() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        for compression in [Compression::Lerc, Compression::LercDeflate, Compression::LercZstd] {
            create_test_cog(&input, &output, COG_TILE_SIZE, Some(compression), None, None, true)?;
            let metadata = GeoTiffMetadata::from_file(&output)?;
            assert_eq!(metadata.compression, Some(compression));
        }

        Ok(())
    }

    fn compare_lerc_compressed_cog(compression: Compression) -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");
        let reference_tile = Tile { z: 10, x: 524, y: 341 };

        create_test_cog(&input, &output, COG_TILE_SIZE, None, None, None, true)?;
        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
        let mut reader = File::open(&output)?;
        let reference_tile_data = cog.read_tile_data_as::<u8>(&reference_tile, FIRST_BAND, &mut reader)?;

        create_test_cog(&input, &output, COG_TILE_SIZE, Some(compression), None, None, true)?;
        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
        assert_eq!(cog.cog_metadata().compression, Some(compression));
        let mut reader = File::open(&output)?;
        let tile_data = cog.read_tile_data_as::<u8>(&reference_tile, FIRST_BAND, &mut reader)?;

        assert_eq!(tile_data, reference_tile_data);
        Ok(())
    }

    #[cfg(feature = "deflate")]
    #[test_log::test]
    fn read_lerc_deflate_test_cog() -> Result<()> {
        compare_lerc_compressed_cog(Compression::LercDeflate)
    }

    #[test_log::test]
    fn read_lerc_zstd_test_cog() -> Result<()> {
        compare_lerc_compressed_cog(Compression::LercZstd)
    }

    #[test_log::test]
    fn read_lerc_nodata_test_cog() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let source = tmp.path().join("source.tif");
        let output = tmp.path().join("cog.tif");
        let reference_tile = Tile { z: 10, x: 524, y: 341 };

        let mut geo_reference = GeoReference::from_file(&testutils::workspace_test_data_dir().join("landusebyte.tif"))?;
        geo_reference.set_nodata(Some(f64::NAN));
        let mut raster = DenseRaster::<f32>::filled_with(Some(1.0), geo_reference);
        for (index, value) in raster.as_mut_slice().iter_mut().enumerate() {
            if index.is_multiple_of(2) {
                *value = f32::NODATA;
            }
        }
        raster.write(&source)?;

        create_test_cog(&source, &output, COG_TILE_SIZE, None, None, Some(ArrayDataType::Float32), true)?;
        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
        let mut reader = File::open(&output)?;
        let reference_tile_data = cog
            .read_tile_data_as::<f32>(&reference_tile, FIRST_BAND, &mut reader)?
            .expect("reference tile");

        create_test_cog(
            &source,
            &output,
            COG_TILE_SIZE,
            Some(Compression::Lerc),
            None,
            Some(ArrayDataType::Float32),
            true,
        )?;
        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
        let mut reader = File::open(&output)?;
        let tile_data = cog
            .read_tile_data_as::<f32>(&reference_tile, FIRST_BAND, &mut reader)?
            .expect("LERC tile");

        for (actual, expected) in tile_data.iter().zip(reference_tile_data.iter()) {
            assert!((actual.is_nodata() && expected.is_nodata()) || actual == expected);
        }

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
            cog.read_tile_data_as::<u8>(&reference_tile, FIRST_BAND, &mut reader)
                .expect("None_u8")
                .unwrap()
        };

        {
            // Assert that all the tiles in the zoom levels are of the correct type
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
            let min_zoom = cog.tile_info().min_zoom;
            let max_zoom = cog.tile_info().max_zoom;
            for zoom in min_zoom..=max_zoom {
                let zoom_level_info = &cog.web_tiles.zoom_levels[zoom as usize];
                let zoom_level_tiles = cog.zoom_level_tile_sources(zoom).unwrap();

                if zoom_level_info.tile_aligned {
                    assert!(
                        zoom_level_tiles
                            .values()
                            .all(|tile_source| matches!(tile_source, TileSource::Aligned(_))),
                        "Expected all tiles at aligned zoom level {zoom} to be Aligned"
                    );
                } else {
                    assert!(
                        zoom_level_tiles
                            .values()
                            .all(|tile_source| matches!(tile_source, TileSource::Unaligned(_))),
                        "Expected all tiles at unaligned zoom level {zoom} to be Unaligned"
                    );
                }
            }
        }

        {
            // Create a test COG file with LZW compression and no predictor
            create_test_cog(&input, &output, COG_TILE_SIZE, Some(Compression::Lzw), None, None, true)?;
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<u8>(&reference_tile, FIRST_BAND, &mut reader)
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
                .read_tile_data_as::<u8>(&reference_tile, FIRST_BAND, &mut reader)
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
                .read_tile_data_as::<i32>(&reference_tile, FIRST_BAND, &mut reader)
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
            assert!(cog.read_tile_data_as::<f64>(&reference_tile, FIRST_BAND, &mut reader).is_err());
            let tile_data = cog
                .read_tile_data_as::<f32>(&reference_tile, FIRST_BAND, &mut reader)
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
                .read_tile_data_as::<f32>(&reference_tile, FIRST_BAND, &mut reader)
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
                .read_tile_data_as::<f64>(&reference_tile, FIRST_BAND, &mut reader)
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
                .read_tile_data_as::<f64>(&reference_tile, FIRST_BAND, &mut reader)
                .expect("ZSTD_f64_predictor")
                .unwrap();

            assert_eq!(tile_data.cast_to::<u8>(), reference_tile_data);
        }

        {
            // Create a test COG file with LERC compression and decode it with our own decoder
            create_test_cog(&input, &output, COG_TILE_SIZE, Some(Compression::Lerc), None, None, true)?;
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().compression, Some(Compression::Lerc));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<u8>(&reference_tile, FIRST_BAND, &mut reader)
                .expect("LERC_u8")
                .unwrap();
            assert_eq!(tile_data, reference_tile_data);
        }

        #[cfg(feature = "deflate")]
        {
            // Create a test COG file with Deflate compression and no predictor
            create_test_cog(&input, &output, COG_TILE_SIZE, Some(Compression::Deflate), None, None, true)?;
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().compression, Some(Compression::Deflate));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<u8>(&reference_tile, FIRST_BAND, &mut reader)
                .expect("Deflate_u8")
                .unwrap();
            assert_eq!(tile_data, reference_tile_data);
        }

        #[cfg(feature = "deflate")]
        {
            // Create a test COG file with Deflate compression and horizontal predictor
            create_test_cog(
                &input,
                &output,
                COG_TILE_SIZE,
                Some(Compression::Deflate),
                Some(PredictorSelection::Automatic),
                None,
                true,
            )?;
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::Horizontal));
            assert_eq!(cog.cog_metadata().compression, Some(Compression::Deflate));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<u8>(&reference_tile, FIRST_BAND, &mut reader)
                .expect("Deflate_u8_predictor")
                .unwrap();
            assert_eq!(tile_data, reference_tile_data);
        }

        #[cfg(feature = "deflate")]
        {
            // Create a test COG file as i32 with Deflate compression and horizontal predictor
            create_test_cog(
                &input,
                &output,
                COG_TILE_SIZE,
                Some(Compression::Deflate),
                Some(PredictorSelection::Automatic),
                Some(ArrayDataType::Int32),
                true,
            )?;
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::Horizontal));
            assert_eq!(cog.cog_metadata().compression, Some(Compression::Deflate));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<i32>(&reference_tile, FIRST_BAND, &mut reader)
                .expect("Deflate_i32_predictor")
                .unwrap();
            assert_eq!(tile_data.cast_to::<u8>(), reference_tile_data);
        }

        #[cfg(feature = "deflate")]
        {
            // Create a test COG file as f32 with Deflate compression and float predictor
            create_test_cog(
                &input,
                &output,
                COG_TILE_SIZE,
                Some(Compression::Deflate),
                Some(PredictorSelection::Automatic),
                Some(ArrayDataType::Float32),
                true,
            )?;
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::FloatingPoint));
            assert_eq!(cog.cog_metadata().compression, Some(Compression::Deflate));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<f32>(&reference_tile, FIRST_BAND, &mut reader)
                .expect("Deflate_f32_predictor")
                .unwrap();
            assert_eq!(tile_data.cast_to::<u8>(), reference_tile_data);
        }

        #[cfg(feature = "deflate")]
        {
            // Create a test COG file as f64 with Deflate compression and float predictor
            create_test_cog(
                &input,
                &output,
                COG_TILE_SIZE,
                Some(Compression::Deflate),
                Some(PredictorSelection::Automatic),
                Some(ArrayDataType::Float64),
                true,
            )?;
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::FloatingPoint));
            assert_eq!(cog.cog_metadata().compression, Some(Compression::Deflate));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<f64>(&reference_tile, FIRST_BAND, &mut reader)
                .expect("Deflate_f64_predictor")
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

            let tile_data = cog.read_tile_data_as::<u8>(&tile, FIRST_BAND, &mut reader).unwrap().unwrap();
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

            let tile_data = cog.read_tile_data_as::<u8>(&tile, FIRST_BAND, &mut reader).unwrap().unwrap();
            assert!(
                !tile_data.as_slice()
                    [COG_TILE_SIZE as usize * (COG_TILE_SIZE as usize - 1)..COG_TILE_SIZE as usize * COG_TILE_SIZE as usize]
                    .iter()
                    .all(|&v| v == 0)
            );
        }

        for tile in cog.zoom_level_tile_sources(7).unwrap().keys() {
            if let Some(tile_data) = cog.read_tile_data(tile, FIRST_BAND, &mut reader)? {
                if tile_data.is_empty() {
                    continue; // Skip empty tiles
                }

                assert_eq!(tile_data.len(), RasterSize::square(COG_TILE_SIZE as i32).cell_count());
                assert_eq!(tile_data.data_type(), meta.data_type);

                let tile_data = cog.read_tile_data_as::<u8>(tile, FIRST_BAND, &mut reader)?.unwrap();
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
            cog.read_tile_data_as::<u8>(&reference_tile, FIRST_BAND, &mut reader)?
        };

        {
            // Create a test COG file with LZW compression and no predictor
            create_test_cog(&input, &output, COG_TILE_SIZE, Some(Compression::Lzw), None, None, true)?;
            let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;

            let mut reader = File::open(&output)?;
            let tile_data = cog.read_tile_data_as::<u8>(&reference_tile, FIRST_BAND, &mut reader)?;
            assert_eq!(tile_data, reference_tile_data);
        }

        Ok(())
    }

    #[test_log::test]
    fn generate_tiles_for_extent_unaligned_256px() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let cog_path = create_unaligned_test_cog(tmp.path(), COG_TILE_SIZE)?;
        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&cog_path)?)?;

        let tiles = super::generate_tiles_for_extent_unaligned(&cog.cog_metadata().geo_reference, 7);
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

        let tiles = super::generate_tiles_for_extent_unaligned(&cog.cog_metadata().geo_reference, 7);
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
        let meta = GeoTiffMetadata::from_file(&cog_path)?;
        let zoom_level_8_index = 2;
        let cog_tiles = meta.overviews.get(zoom_level_8_index).unwrap().chunk_locations.clone();
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
                    cog_tiles[2].offset /* offset of the cog tile at 0 based index 2 (top right cog tile)*/
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
        let band_index = FIRST_BAND;
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
            scale: false,
        };
        create_cog_tiles(&input, &output, opts)?;

        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&output)?)?;
        assert_eq!(cog.tile_info().min_zoom, 6);
        assert_eq!(cog.tile_info().max_zoom, 8);
        //assert_eq!(cog.cog_metadata().overviews[2]., 8);

        let mut reader = File::open(&output)?;
        let tile = cog
            .read_tile_data_as::<u8>(&Tile { z: 6, x: 33, y: 21 }, FIRST_BAND, &mut reader)?
            .unwrap();

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
                    FIRST_BAND,
                    zoom_level,
                    &output_dir.join("cog_tile").join(format!("{tile_size}px")),
                )?;
                debug::dump_web_tiles(
                    &cog_path,
                    FIRST_BAND,
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
                let band_index = BandIndex::new(band).expect("band indices are 1-based");
                debug::dump_tiff_tiles(
                    &cog_path,
                    band_index,
                    zoom_level,
                    &output_dir.join("cog_tile").join(format!("band_{band}")),
                )?;
                debug::dump_web_tiles(
                    &cog_path,
                    band_index,
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
            scale: false,
        };
        create_cog_tiles(&input, &output, opts)?;

        Ok(output)
    }

    #[test_log::test]
    fn tile_info_includes_band_metadata_from_cog_metadata() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("multiband_cog_interleave_tile.tif");
        let mut metadata = GeoTiffMetadata::from_file(&input)?;
        let band_count = metadata.band_count;
        metadata.band_metadata = vec![
            crate::geotiff::BandMetadata {
                description: Some("Red".to_string()),
                scale: Some(crate::RasterScale { scale: 0.5, offset: 10.0 }),
            },
            crate::geotiff::BandMetadata::default(),
            crate::geotiff::BandMetadata {
                description: Some("Blue".to_string()),
                scale: Some(crate::RasterScale { scale: 2.0, offset: -5.0 }),
            },
        ];
        let data_bounds = [4.66, 52.22, 5.13, 52.51];
        metadata.data_bounds = Some(data_bounds);

        let tile_info = WebTilesReader::new(metadata)?.tile_info();

        assert_eq!(tile_info.band_count, band_count);
        assert_eq!(tile_info.band_metadata.len(), band_count as usize);
        assert_eq!(tile_info.band_metadata[0].description.as_deref(), Some("Red"));
        assert_eq!(
            tile_info.band_metadata[0].scale,
            Some(crate::RasterScale { scale: 0.5, offset: 10.0 })
        );
        assert_eq!(tile_info.band_metadata[1].description, None);
        assert_eq!(tile_info.band_metadata[1].scale, None);
        assert_eq!(tile_info.band_metadata[2].description.as_deref(), Some("Blue"));
        assert_eq!(
            tile_info.band_metadata[2].scale,
            Some(crate::RasterScale { scale: 2.0, offset: -5.0 })
        );
        assert_eq!(tile_info.data_bounds, Some(data_bounds.into()));

        Ok(())
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

        assert_eq!(meta.data_type, ArrayDataType::Uint8);
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

    #[test_log::test]
    fn parse_multi_band_tile_data() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("multiband_cog_interleave_tile_google_maps_compatible.tif");
        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&input)?)?;
        let band_count = cog.cog_metadata().band_count as usize;

        let (tile, tile_source) = cog
            .zoom_level_tile_sources(17)
            .expect("expected zoom level 17 to exist")
            .iter()
            .find(|(_, source)| matches!(source, TileSource::MultiBandAligned(_)))
            .expect("expected at least one aligned multiband tile at zoom level 17");

        let band_locations = match tile_source {
            TileSource::MultiBandAligned(locations) => locations,
            _ => unreachable!("tile source should be multiband aligned"),
        };

        let mut reader = File::open(&input)?;
        let cog_chunks: Vec<Vec<u8>> = band_locations
            .iter()
            .map(|chunk_location| {
                if chunk_location.is_sparse() {
                    return Ok(vec![]);
                }

                let mut chunk = vec![0; chunk_location.size as usize];
                io::read_chunk(chunk_location, &mut reader, &mut chunk)?;
                Ok(chunk)
            })
            .collect::<Result<_>>()?;

        let cog_chunk_refs: Vec<&[u8]> = cog_chunks.iter().map(|chunk| chunk.as_slice()).collect();
        let parsed = cog.parse_multi_band_tile_data(1..=band_count, tile_source, &cog_chunk_refs)?;

        assert_eq!(parsed.len(), band_count);

        std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(0))?;
        for band_nr in 1..=band_count {
            let band = BandIndex::new(band_nr).expect("band indices are 1-based");
            let expected = AnyDenseArray::U8(cog.read_tile_data_as::<u8>(tile, band, &mut reader)?.unwrap());
            assert_eq!(parsed[band_nr - 1], expected);
        }

        let subset_range = 2..3;
        let subset_chunk_refs = cog_chunk_refs[(subset_range.start - 1)..(subset_range.end - 1)].to_vec();
        let parsed_subset = cog.parse_multi_band_tile_data(subset_range.clone(), tile_source, &subset_chunk_refs)?;

        assert_eq!(parsed_subset.len(), subset_range.len());

        std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(0))?;
        for (index, band_nr) in subset_range.enumerate() {
            let band = BandIndex::new(band_nr).expect("band indices are 1-based");
            let expected = AnyDenseArray::U8(cog.read_tile_data_as::<u8>(tile, band, &mut reader)?.unwrap());
            assert_eq!(parsed_subset[index], expected);
        }

        let parsed_subset_inclusive = cog.parse_multi_band_tile_data(2..=2, tile_source, &cog_chunk_refs[1..2])?;
        assert_eq!(parsed_subset_inclusive.len(), 1);
        std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(0))?;
        let expected = AnyDenseArray::U8(
            cog.read_tile_data_as::<u8>(tile, BandIndex::new(2).expect("band indices are 1-based"), &mut reader)?
                .expect("tile should be readable for band 2"),
        );
        assert_eq!(parsed_subset_inclusive[0], expected);

        Ok(())
    }

    #[test_log::test]
    fn parse_multi_band_tile_data_lerc_zstd() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let input = testutils::workspace_test_data_dir().join("multiband_cog_interleave_tile_google_maps_compatible.tif");
        let reference_path = tmp.path().join("reference.tif");
        let lerc_zstd_path = tmp.path().join("lerc_zstd.tif");

        for (output, compression) in [(&reference_path, None), (&lerc_zstd_path, Some(Compression::LercZstd))] {
            create_cog_tiles(
                &input,
                output,
                CogCreationOptions {
                    min_zoom: Some(15),
                    zoom_level_strategy: ZoomLevelStrategy::Closest,
                    tile_size: COG_TILE_SIZE,
                    compression,
                    predictor: None,
                    allow_sparse: true,
                    output_data_type: None,
                    aligned_levels: None,
                    scale: false,
                },
            )?;
        }

        let reference = WebTilesReader::new(GeoTiffMetadata::from_file(&reference_path)?)?;
        let lerc_zstd = WebTilesReader::new(GeoTiffMetadata::from_file(&lerc_zstd_path)?)?;
        assert_eq!(lerc_zstd.cog_metadata().compression, Some(Compression::LercZstd));
        assert_eq!(lerc_zstd.cog_metadata().band_count, reference.cog_metadata().band_count);

        let band_count = lerc_zstd.cog_metadata().band_count as usize;
        let max_zoom = lerc_zstd.tile_info().max_zoom;
        let (tile, tile_source) = lerc_zstd
            .zoom_level_tile_sources(max_zoom)
            .expect("expected maximum zoom level to exist")
            .iter()
            .find(|(_, source)| matches!(source, TileSource::MultiBandAligned(locations) if locations.iter().all(|location| !location.is_sparse())))
            .expect("expected at least one non-sparse aligned multiband tile");
        let band_locations = match tile_source {
            TileSource::MultiBandAligned(locations) => locations,
            _ => unreachable!("tile source should be aligned multiband"),
        };

        let mut lerc_zstd_reader = File::open(&lerc_zstd_path)?;
        let cog_chunks = band_locations
            .iter()
            .map(|location| {
                let mut chunk = vec![0; location.size as usize];
                io::read_chunk(location, &mut lerc_zstd_reader, &mut chunk)?;
                Ok(chunk)
            })
            .collect::<Result<Vec<_>>>()?;
        let cog_chunk_refs = cog_chunks.iter().map(Vec::as_slice).collect::<Vec<_>>();
        let parsed = lerc_zstd.parse_multi_band_tile_data(1..=band_count, tile_source, &cog_chunk_refs)?;

        let mut reference_reader = File::open(&reference_path)?;
        for band_nr in 1..=band_count {
            let band = BandIndex::new(band_nr).expect("band indices are 1-based");
            let expected = AnyDenseArray::U8(
                reference
                    .read_tile_data_as::<u8>(tile, band, &mut reference_reader)?
                    .expect("reference tile should be readable"),
            );
            assert_eq!(parsed[band_nr - 1], expected);
        }

        Ok(())
    }

    #[test_log::test]
    fn parse_multi_band_tile_data_unaligned_multiband() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("multiband_cog_interleave_tile_google_maps_compatible.tif");
        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&input)?)?;
        let band_count = cog.cog_metadata().band_count as usize;

        let (tile, tile_source) = cog
            .zoom_level_tile_sources(15)
            .expect("expected zoom level 15 to exist")
            .iter()
            .find(|(_, source)| matches!(source, TileSource::MultiBandUnaligned(_)))
            .expect("expected at least one unaligned multiband tile at zoom level 15");

        let band_tile_sources = match tile_source {
            TileSource::MultiBandUnaligned(sources) => sources,
            _ => unreachable!("tile source should be unaligned multiband"),
        };

        let mut reader = File::open(&input)?;
        let mut cog_chunks: Vec<Vec<u8>> = Vec::new();
        for (tile_band_locations, _) in band_tile_sources {
            for chunk_location in tile_band_locations {
                if chunk_location.is_sparse() {
                    cog_chunks.push(vec![]);
                    continue;
                }

                let mut chunk = vec![0; chunk_location.size as usize];
                io::read_chunk(chunk_location, &mut reader, &mut chunk)?;
                cog_chunks.push(chunk);
            }
        }

        let cog_chunk_refs: Vec<&[u8]> = cog_chunks.iter().map(|chunk| chunk.as_slice()).collect();
        let parsed = cog.parse_multi_band_tile_data(1..=band_count, tile_source, &cog_chunk_refs)?;

        assert_eq!(parsed.len(), band_count);

        std::io::Seek::seek(&mut reader, std::io::SeekFrom::Start(0))?;
        for band_nr in 1..=band_count {
            let band = BandIndex::new(band_nr).expect("band indices are 1-based");
            let expected = cog
                .read_tile_data(tile, band, &mut reader)?
                .expect("tile should be readable for all bands in this test");
            assert_eq!(parsed[band_nr - 1], expected);
        }

        Ok(())
    }

    #[test_log::test]
    fn parse_multi_band_tile_data_rejects_non_multiband_tile_source() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let cog_path = create_unaligned_test_cog(tmp.path(), COG_TILE_SIZE)?;
        let cog = WebTilesReader::new(GeoTiffMetadata::from_file(&cog_path)?)?;

        let max_zoom = cog.tile_info().max_zoom;
        let tile_source = cog
            .zoom_level_tile_sources(max_zoom)
            .expect("expected max zoom level to exist")
            .values()
            .next()
            .expect("expected at least one tile source at max zoom");

        assert!(
            matches!(tile_source, TileSource::Aligned(_) | TileSource::Unaligned(_)),
            "expected a non-multiband tile source for single-band COG"
        );

        match cog.parse_multi_band_tile_data(1..=1, tile_source, &[]) {
            Err(Error::InvalidArgument(message)) => {
                assert!(message.contains("multiband tile source"), "unexpected error message: {message}");
            }
            other => panic!("expected InvalidArgument for non-multiband tile source, got {other:?}"),
        }

        Ok(())
    }
}

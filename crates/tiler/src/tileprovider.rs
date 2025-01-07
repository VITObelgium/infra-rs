use geo::{Coordinate, LatLonBounds, Tile};
use inf::Legend;
use std::{ops::Range, sync::atomic::AtomicU64};

use crate::{
    layermetadata::{LayerId, LayerMetadata},
    tiledata::TileData,
    PixelFormat, Result,
};

#[derive(Debug, Clone)]
pub struct TileRequest {
    pub tile: Tile,
    pub dpi_ratio: u8,
    pub pixel_format: PixelFormat,
}

#[derive(Debug, Clone)]
pub struct ColorMappedTileRequest<'a> {
    pub tile: Tile,
    pub dpi_ratio: u8,
    pub legend: &'a Legend,
}

/// All tile providers must implement this trait.
pub trait TileProvider {
    /// Returns the metadata of all available layers
    fn layers(&self) -> Vec<LayerMetadata>;
    /// Returns the metadata of a single layer with the given id
    fn layer(&self, id: LayerId) -> Result<LayerMetadata>;
    /// For a given layer, returns the range of values in the provided bounding box
    /// If zoom is provided, the range is calculated for the given zoom level (can improve performance for large rasters)
    fn extent_value_range(&self, id: LayerId, extent: LatLonBounds, zoom: Option<i32>) -> Result<Range<f64>>;
    /// For a given layer, returns the pixel vlalue at the provided coordinate
    fn get_raster_value(&self, id: LayerId, coord: Coordinate) -> Result<Option<f32>>;
    /// For a given layer, get the raw tile data
    fn get_tile(&self, id: LayerId, req: &TileRequest) -> Result<TileData>;
    /// For a given layer, get the tile data, the coloring is done using the provided legend
    fn get_tile_color_mapped(&self, id: LayerId, req: &ColorMappedTileRequest) -> Result<TileData>;
}

static LAYER_ID: AtomicU64 = AtomicU64::new(0);

pub fn unique_layer_id() -> LayerId {
    LayerId::from(LAYER_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1)
}

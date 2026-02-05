//! Web Tiles tab - browse web tile mappings and visualize tile data.

use geo::Tile;
use image::DynamicImage;
use ratatui::widgets::ListState;
use ratatui_image::protocol::StatefulProtocol;

/// View state for navigating the tile hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TileViewState {
    /// Browsing the list of bands (for multiband COGs).
    #[default]
    BrowsingBands,
    /// Browsing the list of zoom levels.
    BrowsingZoomLevels,
    /// Browsing tiles within a selected zoom level.
    BrowsingTiles,
    /// Viewing a specific tile's data.
    ViewingTile,
}

/// Cached tile data for visualization.
pub struct TileData {
    /// The tile coordinates.
    pub tile: Tile,
    /// Pixel data as grayscale values (0-255).
    pub pixels: Vec<u8>,
    /// Tile width in pixels.
    pub width: u32,
    /// Tile height in pixels.
    pub height: u32,
    /// Band index this data was read from.
    pub band_index: usize,
    /// The image for high-res rendering.
    pub image: Option<DynamicImage>,
    /// Image protocol state for high-res rendering.
    pub image_state: Option<StatefulProtocol>,
    /// Whether to show hi-res rendering (triggered by Space key).
    pub show_hires: bool,
}

/// State for the Web Tiles tab.
#[derive(Default)]
pub struct WebTilesTabState {
    /// Number of bands in the COG.
    pub band_count: u32,
    /// Currently selected band (1-based).
    pub selected_band: Option<usize>,
    /// Currently selected zoom level.
    pub selected_zoom: Option<i32>,
    /// Currently selected tile within the zoom level.
    pub selected_tile: Option<Tile>,
    /// Current view state in the hierarchy.
    pub view_state: TileViewState,
    /// Cached tile data for visualization.
    pub tile_data: Option<TileData>,
    /// List state for band selection.
    pub band_list_state: ListState,
    /// List state for zoom level selection.
    pub zoom_list_state: ListState,
    /// List state for tile selection.
    pub tile_list_state: ListState,
    /// Minimum zoom level.
    pub min_zoom: i32,
    /// Maximum zoom level.
    pub max_zoom: i32,
    /// Total number of tiles at current zoom level.
    pub tile_count: usize,
    /// Currently selected tile index (for list navigation).
    pub selected_tile_index: Option<usize>,
    /// Cached list of tiles for the current zoom level.
    pub tiles_at_zoom: Vec<Tile>,
}

impl WebTilesTabState {
    /// Create a new web tiles tab state with the given zoom range and band count.
    pub fn new(min_zoom: i32, max_zoom: i32, band_count: u32) -> Self {
        let mut state = Self {
            min_zoom,
            max_zoom,
            band_count,
            ..Default::default()
        };

        // For single band, skip band selection and go straight to zoom levels
        if band_count == 1 {
            state.selected_band = Some(1);
            state.view_state = TileViewState::BrowsingZoomLevels;
            if min_zoom <= max_zoom {
                state.selected_zoom = Some(min_zoom);
                state.zoom_list_state.select(Some(0));
            }
        } else {
            // For multiband, start with band selection
            state.selected_band = Some(1);
            state.band_list_state.select(Some(0));
            state.view_state = TileViewState::BrowsingBands;
        }

        state
    }

    /// Check if this is a multiband COG.
    pub fn is_multiband(&self) -> bool {
        self.band_count > 1
    }

    /// Get the number of zoom levels.
    pub fn zoom_level_count(&self) -> usize {
        if self.max_zoom >= self.min_zoom {
            (self.max_zoom - self.min_zoom + 1) as usize
        } else {
            0
        }
    }

    /// Select the next band.
    pub fn select_next_band(&mut self) {
        if self.band_count == 0 {
            return;
        }
        let next = match self.selected_band {
            Some(idx) => (idx + 1).min(self.band_count as usize),
            None => 1,
        };
        self.selected_band = Some(next);
        self.band_list_state.select(Some(next - 1));
    }

    /// Select the previous band.
    pub fn select_previous_band(&mut self) {
        if self.band_count == 0 {
            return;
        }
        let prev = match self.selected_band {
            Some(idx) => idx.saturating_sub(1).max(1),
            None => 1,
        };
        self.selected_band = Some(prev);
        self.band_list_state.select(Some(prev - 1));
    }

    /// Enter the zoom levels view for the currently selected band.
    pub fn enter_zoom_levels_view(&mut self) {
        if self.selected_band.is_some() {
            self.view_state = TileViewState::BrowsingZoomLevels;
            // Select first zoom level if available
            if self.min_zoom <= self.max_zoom {
                self.selected_zoom = Some(self.min_zoom);
                self.zoom_list_state.select(Some(0));
            }
        }
    }

    /// Exit to the bands view.
    pub fn exit_to_bands(&mut self) {
        self.view_state = TileViewState::BrowsingBands;
        self.selected_zoom = None;
        self.zoom_list_state.select(None);
        self.tile_data = None;
        self.tiles_at_zoom.clear();
    }

    /// Select the next zoom level.
    pub fn select_next_zoom(&mut self) {
        if let Some(zoom) = self.selected_zoom {
            if zoom < self.max_zoom {
                self.selected_zoom = Some(zoom + 1);
                let idx = (zoom + 1 - self.min_zoom) as usize;
                self.zoom_list_state.select(Some(idx));
            }
        } else if self.min_zoom <= self.max_zoom {
            self.selected_zoom = Some(self.min_zoom);
            self.zoom_list_state.select(Some(0));
        }
    }

    /// Select the previous zoom level.
    pub fn select_previous_zoom(&mut self) {
        if let Some(zoom) = self.selected_zoom {
            if zoom > self.min_zoom {
                self.selected_zoom = Some(zoom - 1);
                let idx = (zoom - 1 - self.min_zoom) as usize;
                self.zoom_list_state.select(Some(idx));
            }
        } else if self.min_zoom <= self.max_zoom {
            self.selected_zoom = Some(self.min_zoom);
            self.zoom_list_state.select(Some(0));
        }
    }

    /// Enter the tiles view for the currently selected zoom level.
    pub fn enter_tiles_view(&mut self, tile_count: usize) {
        self.view_state = TileViewState::BrowsingTiles;
        self.tile_count = tile_count;
        // Select first tile if available
        if tile_count > 0 {
            self.selected_tile_index = Some(0);
            self.tile_list_state.select(Some(0));
            if !self.tiles_at_zoom.is_empty() {
                self.selected_tile = Some(self.tiles_at_zoom[0]);
            }
        } else {
            self.selected_tile = None;
            self.selected_tile_index = None;
            self.tile_list_state.select(None);
        }
    }

    /// Exit to the zoom levels view.
    pub fn exit_to_zoom_levels(&mut self) {
        self.view_state = TileViewState::BrowsingZoomLevels;
        self.selected_tile = None;
        self.selected_tile_index = None;
        self.tile_list_state.select(None);
        self.tile_data = None;
        self.tiles_at_zoom.clear();
    }

    /// Select the next tile.
    pub fn select_next_tile(&mut self) {
        if self.tile_count == 0 {
            return;
        }
        let next = match self.selected_tile_index {
            Some(idx) => (idx + 1).min(self.tile_count - 1),
            None => 0,
        };
        self.selected_tile_index = Some(next);
        self.tile_list_state.select(Some(next));
        if next < self.tiles_at_zoom.len() {
            self.selected_tile = Some(self.tiles_at_zoom[next]);
        }
    }

    /// Select the previous tile.
    pub fn select_previous_tile(&mut self) {
        if self.tile_count == 0 {
            return;
        }
        let prev = match self.selected_tile_index {
            Some(idx) => idx.saturating_sub(1),
            None => 0,
        };
        self.selected_tile_index = Some(prev);
        self.tile_list_state.select(Some(prev));
        if prev < self.tiles_at_zoom.len() {
            self.selected_tile = Some(self.tiles_at_zoom[prev]);
        }
    }

    /// Scroll down by half a page (Ctrl+D).
    pub fn scroll_down_half_page(&mut self, viewport_height: usize) {
        if self.tile_count == 0 {
            return;
        }
        let half_page = viewport_height / 2;
        let next = match self.selected_tile_index {
            Some(idx) => (idx + half_page).min(self.tile_count - 1),
            None => 0,
        };
        self.selected_tile_index = Some(next);
        self.tile_list_state.select(Some(next));
        if next < self.tiles_at_zoom.len() {
            self.selected_tile = Some(self.tiles_at_zoom[next]);
        }
    }

    /// Scroll up by half a page (Ctrl+U).
    pub fn scroll_up_half_page(&mut self, viewport_height: usize) {
        if self.tile_count == 0 {
            return;
        }
        let half_page = viewport_height / 2;
        let prev = match self.selected_tile_index {
            Some(idx) => idx.saturating_sub(half_page),
            None => 0,
        };
        self.selected_tile_index = Some(prev);
        self.tile_list_state.select(Some(prev));
        if prev < self.tiles_at_zoom.len() {
            self.selected_tile = Some(self.tiles_at_zoom[prev]);
        }
    }

    /// Enter the tile viewing mode.
    pub fn enter_tile_view(&mut self) {
        if self.selected_tile.is_some() {
            self.view_state = TileViewState::ViewingTile;
        }
    }

    /// Exit to the tiles list view.
    pub fn exit_to_tiles(&mut self) {
        self.view_state = TileViewState::BrowsingTiles;
        self.tile_data = None;
    }

    /// Clear cached tile data.
    pub fn clear_tile_data(&mut self) {
        self.tile_data = None;
    }

    /// Set the tile data.
    pub fn set_tile_data(&mut self, data: TileData) {
        self.tile_data = Some(data);
    }

    /// Update the cached tiles for a zoom level.
    pub fn set_tiles_at_zoom(&mut self, tiles: Vec<Tile>) {
        self.tiles_at_zoom = tiles;
        self.tile_count = self.tiles_at_zoom.len();
    }

    /// Check if tile data needs to be loaded.
    pub fn needs_tile_data(&self, tile: &Tile, band_idx: usize) -> bool {
        match &self.tile_data {
            Some(data) => data.tile != *tile || data.band_index != band_idx,
            None => true,
        }
    }

    /// Get the currently selected band index (1-based).
    pub fn get_selected_band(&self) -> usize {
        self.selected_band.unwrap_or(1)
    }
}

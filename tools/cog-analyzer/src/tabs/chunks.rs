//! Raw Chunks tab - browse COG internal structure and visualize chunk data.

use image::DynamicImage;
use ratatui::widgets::ListState;
use ratatui_image::protocol::StatefulProtocol;

/// View state for navigating the chunk hierarchy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChunkViewState {
    /// Browsing the list of overviews.
    #[default]
    BrowsingOverviews,
    /// Browsing chunks within a selected overview.
    BrowsingChunks,
    /// Viewing a specific chunk's data.
    ViewingChunk,
}

/// Cached chunk data for visualization.
pub struct ChunkData {
    /// Overview index this chunk belongs to.
    pub overview_index: usize,
    /// Chunk index within the overview.
    pub chunk_index: usize,
    /// Pixel data as grayscale values (0-255).
    pub pixels: Vec<u8>,
    /// Chunk width in pixels.
    pub width: u32,
    /// Chunk height in pixels.
    pub height: u32,
    /// Band index this data was read from.
    pub band_index: usize,
    /// Whether this is a sparse (empty) chunk.
    pub is_sparse: bool,
    /// The image for high-res rendering.
    pub image: Option<DynamicImage>,
    /// Image protocol state for high-res rendering.
    pub image_state: Option<StatefulProtocol>,
    /// Whether to show hi-res rendering (triggered by Space key).
    pub show_hires: bool,
}

/// State for the Raw Chunks tab.
#[derive(Default)]
pub struct ChunksTabState {
    /// Currently selected overview index.
    pub selected_overview: Option<usize>,
    /// Currently selected chunk index within the overview.
    pub selected_chunk: Option<usize>,
    /// Current view state in the hierarchy.
    pub view_state: ChunkViewState,
    /// Cached chunk data for visualization.
    pub chunk_data: Option<ChunkData>,
    /// List state for overview selection.
    pub overview_list_state: ListState,
    /// List state for chunk selection.
    pub chunk_list_state: ListState,
    /// Total number of overviews.
    pub overview_count: usize,
    /// Total number of chunks in current overview.
    pub chunk_count: usize,
}

impl ChunksTabState {
    /// Create a new chunks tab state with the given overview count.
    pub fn new(overview_count: usize) -> Self {
        let mut state = Self {
            overview_count,
            ..Default::default()
        };
        // Select first overview if available
        if overview_count > 0 {
            state.selected_overview = Some(0);
            state.overview_list_state.select(Some(0));
        }
        state
    }

    /// Select the next overview.
    pub fn select_next_overview(&mut self, overview_count: usize) {
        if overview_count == 0 {
            return;
        }
        let next = match self.selected_overview {
            Some(idx) => (idx + 1).min(overview_count - 1),
            None => 0,
        };
        self.selected_overview = Some(next);
        self.overview_list_state.select(Some(next));
    }

    /// Select the previous overview.
    pub fn select_previous_overview(&mut self, overview_count: usize) {
        if overview_count == 0 {
            return;
        }
        let prev = match self.selected_overview {
            Some(idx) => idx.saturating_sub(1),
            None => 0,
        };
        self.selected_overview = Some(prev);
        self.overview_list_state.select(Some(prev));
    }

    /// Enter the chunks view for the currently selected overview.
    pub fn enter_chunks_view(&mut self, chunk_count: usize) {
        self.view_state = ChunkViewState::BrowsingChunks;
        self.chunk_count = chunk_count;
        // Select first chunk if available
        if chunk_count > 0 {
            self.selected_chunk = Some(0);
            self.chunk_list_state.select(Some(0));
        } else {
            self.selected_chunk = None;
            self.chunk_list_state.select(None);
        }
    }

    /// Exit to the overviews view.
    pub fn exit_to_overviews(&mut self) {
        self.view_state = ChunkViewState::BrowsingOverviews;
        self.selected_chunk = None;
        self.chunk_list_state.select(None);
        self.chunk_data = None;
    }

    /// Select the next chunk.
    pub fn select_next_chunk(&mut self) {
        if self.chunk_count == 0 {
            return;
        }
        let next = match self.selected_chunk {
            Some(idx) => (idx + 1).min(self.chunk_count - 1),
            None => 0,
        };
        self.selected_chunk = Some(next);
        self.chunk_list_state.select(Some(next));
    }

    /// Select the previous chunk.
    pub fn select_previous_chunk(&mut self) {
        if self.chunk_count == 0 {
            return;
        }
        let prev = match self.selected_chunk {
            Some(idx) => idx.saturating_sub(1),
            None => 0,
        };
        self.selected_chunk = Some(prev);
        self.chunk_list_state.select(Some(prev));
    }

    /// Scroll down by half a page (Ctrl+D).
    pub fn scroll_down_half_page(&mut self, viewport_height: usize) {
        if self.chunk_count == 0 {
            return;
        }
        let half_page = viewport_height / 2;
        let next = match self.selected_chunk {
            Some(idx) => (idx + half_page).min(self.chunk_count - 1),
            None => 0,
        };
        self.selected_chunk = Some(next);
        self.chunk_list_state.select(Some(next));
    }

    /// Scroll up by half a page (Ctrl+U).
    pub fn scroll_up_half_page(&mut self, viewport_height: usize) {
        if self.chunk_count == 0 {
            return;
        }
        let half_page = viewport_height / 2;
        let prev = match self.selected_chunk {
            Some(idx) => idx.saturating_sub(half_page),
            None => 0,
        };
        self.selected_chunk = Some(prev);
        self.chunk_list_state.select(Some(prev));
    }

    /// Enter the chunk viewing mode.
    pub fn enter_chunk_view(&mut self) {
        if self.selected_chunk.is_some() {
            self.view_state = ChunkViewState::ViewingChunk;
        }
    }

    /// Exit to the chunks list view.
    pub fn exit_to_chunks(&mut self) {
        self.view_state = ChunkViewState::BrowsingChunks;
        self.chunk_data = None;
    }

    /// Clear cached chunk data.
    pub fn clear_chunk_data(&mut self) {
        self.chunk_data = None;
    }

    /// Set the chunk data.
    pub fn set_chunk_data(&mut self, data: ChunkData) {
        self.chunk_data = Some(data);
    }

    /// Check if chunk data needs to be loaded.
    pub fn needs_chunk_data(&self, overview_idx: usize, chunk_idx: usize, band_idx: usize) -> bool {
        match &self.chunk_data {
            Some(data) => data.overview_index != overview_idx || data.chunk_index != chunk_idx || data.band_index != band_idx,
            None => true,
        }
    }
}

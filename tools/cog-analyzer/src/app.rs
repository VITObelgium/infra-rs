//! Application state management for the COG Analyzer.

use std::fs::File;
use std::path::PathBuf;

use geo::cog::WebTilesReader;
use geo::geotiff::{BandIndex, GeoTiffMetadata, GeoTiffReader};
use ratatui_image::picker::Picker;

use crate::Result;
use crate::tabs::chunks::ChunksTabState;
use crate::tabs::overview::OverviewTabState;
use crate::tabs::webtiles::WebTilesTabState;

/// The main tabs available in the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Tab {
    #[default]
    Overview,
    RawChunks,
    WebTiles,
}

impl Tab {
    /// Get the next tab in order.
    pub fn next(self) -> Self {
        match self {
            Tab::Overview => Tab::RawChunks,
            Tab::RawChunks => Tab::WebTiles,
            Tab::WebTiles => Tab::Overview,
        }
    }

    /// Get the previous tab in order.
    pub fn previous(self) -> Self {
        match self {
            Tab::Overview => Tab::WebTiles,
            Tab::RawChunks => Tab::Overview,
            Tab::WebTiles => Tab::RawChunks,
        }
    }

    /// Get all tabs in order.
    pub fn all() -> [Tab; 3] {
        [Tab::Overview, Tab::RawChunks, Tab::WebTiles]
    }

    /// Get the display name of the tab.
    pub fn name(&self) -> &'static str {
        match self {
            Tab::Overview => "Overview",
            Tab::RawChunks => "Raw Chunks",
            Tab::WebTiles => "Web Tiles",
        }
    }
}

/// Main application state.
pub struct App {
    /// Whether the application is running.
    pub running: bool,

    /// Currently selected tab.
    pub current_tab: Tab,

    /// Path to the COG file.
    pub file_path: PathBuf,

    /// File size in bytes.
    pub file_size: u64,

    /// COG metadata.
    pub cog_metadata: GeoTiffMetadata,

    /// `WebTiles` reader for tile access.
    pub webtiles_reader: Option<WebTilesReader>,

    /// Currently selected band (1-based index, None means all bands or single-band COG).
    pub selected_band: Option<BandIndex>,

    /// Whether this is a multiband COG.
    pub is_multiband: bool,

    /// Total number of bands.
    pub band_count: u32,

    /// Overview tab state.
    pub overview_tab: OverviewTabState,

    /// Chunks tab state.
    pub chunks_tab: ChunksTabState,

    /// Web tiles tab state.
    pub webtiles_tab: WebTilesTabState,

    /// Error message to display (if any).
    pub error_message: Option<String>,

    /// Image picker for terminal graphics protocol.
    pub image_picker: Option<Picker>,
}

impl App {
    /// Create a new application instance from a file path.
    pub fn new(file_path: PathBuf) -> Result<Self> {
        // Get file size
        let file_size = std::fs::metadata(&file_path)?.len();

        // Read COG metadata
        let cog_reader = GeoTiffReader::from_file(&file_path)?;
        let cog_metadata = cog_reader.metadata().clone();

        // Determine if multiband
        let band_count = cog_metadata.band_count;
        let is_multiband = band_count > 1;

        // Set initial band selection
        let selected_band = if is_multiband { Some(geo::geotiff::FIRST_BAND) } else { None };

        // Try to create WebTilesReader
        let webtiles_reader = match WebTilesReader::new(cog_metadata.clone()) {
            Ok(reader) => Some(reader),
            Err(e) => {
                log::warn!("Failed to create WebTilesReader: {}", e);
                None
            }
        };

        // Initialize tab states
        let overview_count = cog_metadata.overviews.len();
        let chunks_tab = ChunksTabState::new(overview_count);

        let webtiles_tab = if let Some(ref reader) = webtiles_reader {
            WebTilesTabState::new(reader.tile_info().min_zoom, reader.tile_info().max_zoom, band_count)
        } else {
            WebTilesTabState::default()
        };

        Ok(Self {
            running: true,
            current_tab: Tab::Overview,
            file_path,
            file_size,
            cog_metadata,
            webtiles_reader,
            selected_band,
            is_multiband,
            band_count,
            overview_tab: OverviewTabState::default(),
            chunks_tab,
            webtiles_tab,
            error_message: None,
            image_picker: None,
        })
    }

    /// Initialize the image picker by querying the terminal.
    /// This should be called after the terminal is in raw mode.
    ///
    /// If `force_halfblocks` is true, skip protocol detection and use Unicode halfblocks.
    pub fn init_image_picker(&mut self, force_halfblocks: bool) {
        if force_halfblocks {
            self.image_picker = Some(Picker::halfblocks());
            return;
        }

        match Picker::from_query_stdio() {
            Ok(picker) => {
                self.image_picker = Some(picker);
            }
            Err(e) => {
                log::warn!("Failed to query terminal for graphics protocol: {}, falling back to halfblocks", e);
                self.image_picker = Some(Picker::halfblocks());
            }
        }
    }

    /// Switch to the next tab.
    pub fn next_tab(&mut self) {
        self.current_tab = self.current_tab.next();
    }

    /// Switch to the previous tab.
    pub fn previous_tab(&mut self) {
        self.current_tab = self.current_tab.previous();
    }

    /// Switch to the next band.
    pub fn next_band(&mut self) {
        if !self.is_multiband {
            return;
        }

        if let Some(band) = self.selected_band {
            let next = band.get() + 1;
            if next <= self.band_count as usize {
                self.selected_band = BandIndex::new(next);
                // Clear cached chunk data when band changes
                self.chunks_tab.clear_chunk_data();
                self.webtiles_tab.clear_tile_data();
            }
        }
    }

    /// Switch to the previous band.
    pub fn previous_band(&mut self) {
        if !self.is_multiband {
            return;
        }

        if let Some(band) = self.selected_band {
            let prev = band.get().saturating_sub(1);
            if prev >= 1 {
                self.selected_band = BandIndex::new(prev);
                // Clear cached chunk data when band changes
                self.chunks_tab.clear_chunk_data();
                self.webtiles_tab.clear_tile_data();
            }
        }
    }

    /// Get the current band index (1-based) for display.
    pub fn current_band_display(&self) -> String {
        match self.selected_band {
            Some(band) => format!("Band {} of {}", band.get(), self.band_count),
            None => "Single band".to_string(),
        }
    }

    /// Get the currently selected band index for reading data.
    pub fn get_band_index(&self) -> BandIndex {
        self.selected_band.unwrap_or(geo::geotiff::FIRST_BAND)
    }

    /// Set an error message.
    pub fn set_error(&mut self, message: String) {
        self.error_message = Some(message);
    }

    /// Clear the error message.
    pub fn clear_error(&mut self) {
        self.error_message = None;
    }

    /// Quit the application.
    pub fn quit(&mut self) {
        self.running = false;
    }

    /// Open the COG file for reading.
    pub fn open_file(&self) -> Result<File> {
        Ok(File::open(&self.file_path)?)
    }

    /// Create a `GeoTiffReader` for the file.
    pub fn create_reader(&self) -> Result<GeoTiffReader> {
        Ok(GeoTiffReader::from_file(&self.file_path)?)
    }
}

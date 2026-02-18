//! Keyboard event handler for the COG Analyzer TUI.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use geo::Tile;

use crate::Result;
use crate::app::{App, Tab};
use crate::data;
use crate::tabs::chunks::ChunkViewState;
use crate::tabs::webtiles::TileViewState;

/// Handles keyboard events and updates the application state.
pub fn handle_key_events(key_event: KeyEvent, app: &mut App) -> Result<()> {
    // Clear any error message on key press
    app.clear_error();

    // Global key handlers
    match key_event.code {
        // Exit application on `q` or `Ctrl-C`
        KeyCode::Char('q') => {
            app.quit();
            return Ok(());
        }
        KeyCode::Char('c' | 'C') if key_event.modifiers == KeyModifiers::CONTROL => {
            app.quit();
            return Ok(());
        }
        // Tab switching with Tab key
        KeyCode::Tab => {
            if key_event.modifiers == KeyModifiers::SHIFT {
                app.previous_tab();
            } else {
                app.next_tab();
            }
            return Ok(());
        }
        KeyCode::BackTab => {
            app.previous_tab();
            return Ok(());
        }
        // Direct tab selection with number keys
        KeyCode::Char('1') => {
            app.current_tab = Tab::Overview;
            return Ok(());
        }
        KeyCode::Char('2') => {
            app.current_tab = Tab::RawChunks;
            return Ok(());
        }
        KeyCode::Char('3') => {
            app.current_tab = Tab::WebTiles;
            return Ok(());
        }
        // Band switching with b/B (not available in chunks tab)
        KeyCode::Char('b') if app.current_tab != Tab::RawChunks => {
            app.next_band();
            return Ok(());
        }
        KeyCode::Char('B') if app.current_tab != Tab::RawChunks => {
            app.previous_band();
            return Ok(());
        }
        _ => {}
    }

    // Tab-specific key handlers
    match app.current_tab {
        Tab::Overview => handle_overview_keys(key_event, app),
        Tab::RawChunks => handle_chunks_keys(key_event, app),
        Tab::WebTiles => handle_webtiles_keys(key_event, app),
    }
}

/// Handle keyboard events for the Overview tab.
fn handle_overview_keys(key_event: KeyEvent, app: &mut App) -> Result<()> {
    match key_event.code {
        // Scrolling
        KeyCode::Up | KeyCode::Char('k') => {
            app.overview_tab.scroll_up();
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.overview_tab.scroll_down();
        }
        KeyCode::PageUp => {
            app.overview_tab.scroll_up_page();
        }
        KeyCode::PageDown => {
            app.overview_tab.scroll_down_page();
        }
        KeyCode::Home => {
            app.overview_tab.scroll_to_top();
        }
        KeyCode::End => {
            app.overview_tab.scroll_to_bottom();
        }
        _ => {}
    }
    Ok(())
}

/// Handle keyboard events for the Raw Chunks tab.
fn handle_chunks_keys(key_event: KeyEvent, app: &mut App) -> Result<()> {
    let overview_count = app.cog_metadata.overviews.len();

    match app.chunks_tab.view_state {
        ChunkViewState::BrowsingOverviews => {
            match key_event.code {
                // Navigation
                KeyCode::Up | KeyCode::Char('k') => {
                    app.chunks_tab.select_previous_overview(overview_count);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    app.chunks_tab.select_next_overview(overview_count);
                }
                // Drill down into chunks
                KeyCode::Enter | KeyCode::Char('l') => {
                    if let Some(overview_idx) = app.chunks_tab.selected_overview
                        && let Some(overview) = app.cog_metadata.overviews.get(overview_idx)
                    {
                        let chunk_count = overview.chunk_locations.len();
                        app.chunks_tab.enter_chunks_view(chunk_count);
                        // Automatically enter chunk view and load the first chunk
                        app.chunks_tab.enter_chunk_view();
                        load_chunk_data_if_needed(app);
                    }
                }
                _ => {}
            }
        }
        ChunkViewState::BrowsingChunks => {
            match key_event.code {
                // Navigation
                KeyCode::Up | KeyCode::Char('k') => {
                    app.chunks_tab.select_previous_chunk();
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    app.chunks_tab.select_next_chunk();
                }
                // View chunk
                KeyCode::Enter | KeyCode::Char('l') => {
                    app.chunks_tab.enter_chunk_view();
                    // Load chunk data
                    load_chunk_data_if_needed(app);
                }
                // Go back
                KeyCode::Esc | KeyCode::Char('h') => {
                    app.chunks_tab.exit_to_overviews();
                }
                _ => {}
            }
        }
        ChunkViewState::ViewingChunk => {
            // Handle Ctrl+U and Ctrl+D for half-page scrolling
            if key_event.modifiers == KeyModifiers::CONTROL {
                match key_event.code {
                    KeyCode::Char('u') => {
                        app.chunks_tab.scroll_up_half_page(20);
                        app.chunks_tab.clear_chunk_data();
                        load_chunk_data_if_needed(app);
                        return Ok(());
                    }
                    KeyCode::Char('d') => {
                        app.chunks_tab.scroll_down_half_page(20);
                        app.chunks_tab.clear_chunk_data();
                        load_chunk_data_if_needed(app);
                        return Ok(());
                    }
                    _ => {}
                }
            }

            match key_event.code {
                // Go back
                KeyCode::Esc | KeyCode::Char('h') | KeyCode::Backspace => {
                    app.chunks_tab.exit_to_chunks();
                }
                // Navigate to previous/next chunk
                KeyCode::Up | KeyCode::Char('k') => {
                    app.chunks_tab.select_previous_chunk();
                    app.chunks_tab.clear_chunk_data();
                    load_chunk_data_if_needed(app);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    app.chunks_tab.select_next_chunk();
                    app.chunks_tab.clear_chunk_data();
                    load_chunk_data_if_needed(app);
                }
                // Toggle hi-res rendering with Space
                KeyCode::Char(' ') => {
                    if let Some(ref mut chunk_data) = app.chunks_tab.chunk_data
                        && !chunk_data.show_hires
                    {
                        // Enable hi-res and create image_state if needed
                        chunk_data.show_hires = true;
                        if chunk_data.image_state.is_none()
                            && let (Some(picker), Some(image)) = (&app.image_picker, &chunk_data.image)
                        {
                            let protocol = picker.new_resize_protocol(image.clone());
                            chunk_data.image_state = Some(protocol);
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Handle keyboard events for the Web Tiles tab.
fn handle_webtiles_keys(key_event: KeyEvent, app: &mut App) -> Result<()> {
    // Check if webtiles reader is available
    let Some(ref reader) = app.webtiles_reader else {
        return Ok(());
    };

    match app.webtiles_tab.view_state {
        TileViewState::BrowsingBands => {
            match key_event.code {
                // Navigation
                KeyCode::Up | KeyCode::Char('k') => {
                    app.webtiles_tab.select_previous_band();
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    app.webtiles_tab.select_next_band();
                }
                // Drill down into zoom levels
                KeyCode::Enter | KeyCode::Char('l') => {
                    app.webtiles_tab.enter_zoom_levels_view();
                }
                _ => {}
            }
        }
        TileViewState::BrowsingZoomLevels => {
            match key_event.code {
                // Navigation
                KeyCode::Up | KeyCode::Char('k') => {
                    app.webtiles_tab.select_previous_zoom();
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    app.webtiles_tab.select_next_zoom();
                }
                // Drill down into tiles
                KeyCode::Enter | KeyCode::Char('l') => {
                    if let Some(zoom) = app.webtiles_tab.selected_zoom
                        && let Some(tile_sources) = reader.zoom_level_tile_sources(zoom)
                    {
                        // Collect tiles and sort them for consistent ordering
                        let mut tiles: Vec<Tile> = tile_sources.keys().copied().collect();
                        tiles.sort_by(|a, b| a.y().cmp(&b.y()).then_with(|| a.x().cmp(&b.x())));
                        let tile_count = tiles.len();
                        app.webtiles_tab.set_tiles_at_zoom(tiles);
                        app.webtiles_tab.enter_tiles_view(tile_count);
                        // Automatically enter tile view and load the first tile
                        app.webtiles_tab.enter_tile_view();
                        load_tile_data_if_needed(app);
                    }
                }
                // Go back (only for multiband)
                KeyCode::Esc | KeyCode::Char('h') => {
                    if app.webtiles_tab.is_multiband() {
                        app.webtiles_tab.exit_to_bands();
                    }
                }
                _ => {}
            }
        }
        TileViewState::BrowsingTiles => {
            match key_event.code {
                // Navigation
                KeyCode::Up | KeyCode::Char('k') => {
                    app.webtiles_tab.select_previous_tile();
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    app.webtiles_tab.select_next_tile();
                }
                // View tile
                KeyCode::Enter | KeyCode::Char('l') => {
                    app.webtiles_tab.enter_tile_view();
                    // Load tile data
                    load_tile_data_if_needed(app);
                }
                // Go back
                KeyCode::Esc | KeyCode::Char('h') => {
                    app.webtiles_tab.exit_to_zoom_levels();
                }
                _ => {}
            }
        }
        TileViewState::ViewingTile => {
            // Handle Ctrl+U and Ctrl+D for half-page scrolling
            if key_event.modifiers == KeyModifiers::CONTROL {
                match key_event.code {
                    KeyCode::Char('u') => {
                        app.webtiles_tab.scroll_up_half_page(20);
                        app.webtiles_tab.clear_tile_data();
                        load_tile_data_if_needed(app);
                        return Ok(());
                    }
                    KeyCode::Char('d') => {
                        app.webtiles_tab.scroll_down_half_page(20);
                        app.webtiles_tab.clear_tile_data();
                        load_tile_data_if_needed(app);
                        return Ok(());
                    }
                    _ => {}
                }
            }

            match key_event.code {
                // Go back
                KeyCode::Esc | KeyCode::Char('h') | KeyCode::Backspace => {
                    app.webtiles_tab.exit_to_tiles();
                }
                // Navigate to previous/next tile
                KeyCode::Up | KeyCode::Char('k') => {
                    app.webtiles_tab.select_previous_tile();
                    app.webtiles_tab.clear_tile_data();
                    load_tile_data_if_needed(app);
                }
                KeyCode::Down | KeyCode::Char('j') => {
                    app.webtiles_tab.select_next_tile();
                    app.webtiles_tab.clear_tile_data();
                    load_tile_data_if_needed(app);
                }
                // Toggle hi-res rendering with Space
                KeyCode::Char(' ') => {
                    if let Some(ref mut tile_data) = app.webtiles_tab.tile_data
                        && !tile_data.show_hires
                    {
                        // Enable hi-res and create image_state if needed
                        tile_data.show_hires = true;
                        if tile_data.image_state.is_none()
                            && let (Some(picker), Some(image)) = (&app.image_picker, &tile_data.image)
                        {
                            let protocol = picker.new_resize_protocol(image.clone());
                            tile_data.image_state = Some(protocol);
                        }
                    }
                }
                _ => {}
            }
        }
    }
    Ok(())
}

/// Load chunk data if we're in viewing mode and data is not yet loaded.
fn load_chunk_data_if_needed(app: &mut App) {
    if app.chunks_tab.view_state != ChunkViewState::ViewingChunk {
        return;
    }

    let Some(overview_idx) = app.chunks_tab.selected_overview else {
        return;
    };

    let Some(chunk_idx) = app.chunks_tab.selected_chunk else {
        return;
    };

    let band_idx = app.get_band_index();

    // Check if we need to load data
    if !app.chunks_tab.needs_chunk_data(overview_idx, chunk_idx, band_idx.get()) {
        return;
    }

    // Load the chunk data
    match data::load_chunk_data(&app.file_path, overview_idx, chunk_idx, band_idx, &app.cog_metadata) {
        Ok(chunk_data) => {
            // Don't create image_state yet - user must press Space for hi-res rendering
            app.chunks_tab.set_chunk_data(chunk_data);
        }
        Err(e) => {
            app.set_error(format!("Failed to load chunk data: {}", e));
        }
    }
}

/// Load tile data if we're in viewing mode and data is not yet loaded.
fn load_tile_data_if_needed(app: &mut App) {
    if app.webtiles_tab.view_state != TileViewState::ViewingTile {
        return;
    }

    let Some(tile) = app.webtiles_tab.selected_tile else {
        return;
    };

    let Some(ref webtiles_reader) = app.webtiles_reader else {
        return;
    };

    let band_idx = app.webtiles_tab.get_selected_band();

    // Check if we need to load data
    if !app.webtiles_tab.needs_tile_data(&tile, band_idx) {
        return;
    }

    // Load the tile data
    let Some(band_index) = geo::geotiff::BandIndex::new(band_idx) else {
        app.set_error(format!("Invalid band index: {}", band_idx));
        return;
    };
    match data::load_tile_data(&app.file_path, tile, band_index, webtiles_reader) {
        Ok(tile_data) => {
            // Don't create image_state yet - user must press Space for hi-res rendering
            app.webtiles_tab.set_tile_data(tile_data);
        }
        Err(e) => {
            app.set_error(format!("Failed to load tile data: {}", e));
        }
    }
}

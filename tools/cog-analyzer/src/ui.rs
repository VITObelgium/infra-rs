//! Main UI rendering logic for the COG Analyzer TUI.

use inf::colormap::{ColorMapDirection, ColorMapPreset, ProcessedColorMap};
use ratatui::{
    Frame,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, BorderType, Borders, List, ListItem, Paragraph, Tabs, Wrap},
};
use ratatui_image::{Resize, StatefulImage};

use crate::app::{App, Tab};
use crate::tabs::chunks::ChunkViewState;
use crate::tabs::webtiles::TileViewState;

/// Lazily initialized Turbo colormap for fallback visualization.
fn turbo_colormap() -> &'static ProcessedColorMap {
    use std::sync::OnceLock;
    static TURBO: OnceLock<ProcessedColorMap> = OnceLock::new();
    TURBO.get_or_init(|| ProcessedColorMap::create_for_preset(ColorMapPreset::Turbo, ColorMapDirection::Regular))
}

/// Main render function that draws the entire UI.
pub fn render(app: &mut App, frame: &mut Frame) {
    let area = frame.area();

    // Create the main layout
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Title bar
            Constraint::Length(3), // Tab bar
            Constraint::Min(0),    // Main content
            Constraint::Length(3), // Status/help bar
        ])
        .split(area);

    // Render title bar
    render_title_bar(app, frame, chunks[0]);

    // Render tab bar
    render_tab_bar(app, frame, chunks[1]);

    // Render main content based on current tab
    match app.current_tab {
        Tab::Overview => render_overview_tab(app, frame, chunks[2]),
        Tab::RawChunks => render_chunks_tab(app, frame, chunks[2]),
        Tab::WebTiles => render_webtiles_tab(app, frame, chunks[2]),
    }

    // Render status/help bar
    render_help_bar(app, frame, chunks[3]);
}

/// Render the title bar with file path and band info.
fn render_title_bar(app: &App, frame: &mut Frame, area: Rect) {
    let file_name = app
        .file_path
        .file_name()
        .map_or_else(|| app.file_path.display().to_string(), |n| n.to_string_lossy().to_string());

    let band_info = if app.is_multiband {
        format!(" [{}]", app.current_band_display())
    } else {
        String::new()
    };

    let title = format!("COG Analyzer - {}{}", file_name, band_info);

    let title_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .style(Style::default().fg(Color::Cyan));

    let title_paragraph = Paragraph::new(title)
        .block(title_block)
        .style(Style::default().fg(Color::White).bold());

    frame.render_widget(title_paragraph, area);
}

/// Render the tab bar.
fn render_tab_bar(app: &App, frame: &mut Frame, area: Rect) {
    let tab_titles: Vec<Line> = Tab::all()
        .iter()
        .map(|tab| {
            let style = if *tab == app.current_tab {
                Style::default().fg(Color::Yellow).bold()
            } else {
                Style::default().fg(Color::Gray)
            };
            Line::from(Span::styled(tab.name(), style))
        })
        .collect();

    let tabs = Tabs::new(tab_titles)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .title("Tabs"),
        )
        .select(app.current_tab as usize)
        .style(Style::default().fg(Color::White))
        .highlight_style(Style::default().fg(Color::Yellow).bold());

    frame.render_widget(tabs, area);
}

/// Render the help bar at the bottom.
fn render_help_bar(app: &App, frame: &mut Frame, area: Rect) {
    let mut help_text = vec![
        Span::styled("Tab", Style::default().fg(Color::Yellow)),
        Span::raw(": Switch tabs | "),
        Span::styled("↑↓/jk", Style::default().fg(Color::Yellow)),
        Span::raw(": Navigate | "),
    ];

    // Add tab-specific help
    match app.current_tab {
        Tab::Overview => {
            help_text.extend([
                Span::styled("PgUp/PgDn", Style::default().fg(Color::Yellow)),
                Span::raw(": Scroll | "),
            ]);
        }
        Tab::RawChunks => {
            help_text.extend([
                Span::styled("Enter/l", Style::default().fg(Color::Yellow)),
                Span::raw(": Select | "),
                Span::styled("Esc/h", Style::default().fg(Color::Yellow)),
                Span::raw(": Back | "),
            ]);
            // Add Space hint when viewing a chunk
            if app.chunks_tab.view_state == crate::tabs::chunks::ChunkViewState::ViewingChunk {
                help_text.extend([Span::styled("Space", Style::default().fg(Color::Yellow)), Span::raw(": Hi-res | ")]);
            }
        }
        Tab::WebTiles => {
            help_text.extend([
                Span::styled("Enter/l", Style::default().fg(Color::Yellow)),
                Span::raw(": Select | "),
                Span::styled("Esc/h", Style::default().fg(Color::Yellow)),
                Span::raw(": Back | "),
            ]);
            // Add Space hint when viewing a tile
            if app.webtiles_tab.view_state == crate::tabs::webtiles::TileViewState::ViewingTile {
                help_text.extend([Span::styled("Space", Style::default().fg(Color::Yellow)), Span::raw(": Hi-res | ")]);
            }
        }
    }

    help_text.extend([Span::styled("q", Style::default().fg(Color::Yellow)), Span::raw(": Quit")]);

    // Show error message if any
    let content = if let Some(ref error) = app.error_message {
        Line::from(vec![Span::styled(
            format!("Error: {}", error),
            Style::default().fg(Color::Red).bold(),
        )])
    } else {
        Line::from(help_text)
    };

    let help_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title("Help");

    let help_paragraph = Paragraph::new(content).block(help_block);

    frame.render_widget(help_paragraph, area);
}

/// Render the Overview tab content.
fn render_overview_tab(app: &mut App, frame: &mut Frame, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title("COG Metadata");

    let inner_area = block.inner(area);
    frame.render_widget(block, area);

    // Build the metadata content
    let mut lines: Vec<Line> = Vec::new();

    // Label width for alignment
    const LABEL_WIDTH: usize = 18;

    // Helper to create a labeled row with consistent alignment
    let labeled_row = |label: &str, value: String| -> Line {
        Line::from(vec![
            Span::styled(
                format!("{:>width$}  ", label, width = LABEL_WIDTH),
                Style::default().fg(Color::Cyan),
            ),
            Span::raw(value),
        ])
    };

    // File Information section
    lines.push(Line::from(""));
    lines.push(Line::from(Span::styled(
        " ▸ File Information",
        Style::default().fg(Color::Green).bold(),
    )));
    lines.push(Line::from(""));
    lines.push(labeled_row("Path", app.file_path.display().to_string()));
    lines.push(labeled_row("Size", format_file_size(app.file_size)));
    lines.push(Line::from(""));

    // Raster Information section
    lines.push(Line::from(Span::styled(
        " ▸ Raster Information",
        Style::default().fg(Color::Green).bold(),
    )));
    lines.push(Line::from(""));
    lines.push(labeled_row(
        "Bands",
        format!(
            "{} ({})",
            app.band_count,
            if app.is_multiband { "Multiband" } else { "Single band" }
        ),
    ));
    if app.is_multiband {
        lines.push(labeled_row("Current Band", app.current_band_display()));
    }

    let raster_size = app.cog_metadata.geo_reference.raster_size();
    lines.push(labeled_row(
        "Resolution",
        format!("{} × {}", raster_size.cols.count(), raster_size.rows.count()),
    ));
    lines.push(labeled_row("Data Type", format!("{:?}", app.cog_metadata.data_type)));
    lines.push(labeled_row(
        "Compression",
        app.cog_metadata
            .compression
            .map_or_else(|| "None".to_string(), |c| format!("{:?}", c)),
    ));
    lines.push(labeled_row(
        "Predictor",
        app.cog_metadata
            .predictor
            .map_or_else(|| "None".to_string(), |p| format!("{:?}", p)),
    ));
    lines.push(labeled_row("Interleave", format!("{:?}", app.cog_metadata.interleave)));
    lines.push(labeled_row("Data Layout", format!("{:?}", app.cog_metadata.data_layout)));
    lines.push(Line::from(""));

    // Geospatial Information section
    lines.push(Line::from(Span::styled(
        " ▸ Geospatial Information",
        Style::default().fg(Color::Green).bold(),
    )));
    lines.push(Line::from(""));

    let geo_ref = &app.cog_metadata.geo_reference;
    if let Some(epsg) = geo_ref.projected_epsg() {
        lines.push(labeled_row("CRS", format!("EPSG:{}", epsg)));
    }

    let bounds = geo_ref.bounding_box();
    lines.push(labeled_row(
        "Bounds",
        format!(
            "[{:.2}, {:.2}] → [{:.2}, {:.2}]",
            bounds.top_left().x(),
            bounds.top_left().y(),
            bounds.bottom_right().x(),
            bounds.bottom_right().y()
        ),
    ));

    let cell_size = geo_ref.cell_size();
    lines.push(labeled_row("Cell Size", format!("{:.6} × {:.6}", cell_size.x(), cell_size.y())));

    if let Some(nodata) = geo_ref.nodata() {
        lines.push(labeled_row("NoData", format!("{}", nodata)));
    }
    lines.push(Line::from(""));

    // Statistics section (if available)
    if let Some(ref stats) = app.cog_metadata.statistics {
        lines.push(Line::from(Span::styled(" ▸ Statistics", Style::default().fg(Color::Green).bold())));
        lines.push(Line::from(""));
        lines.push(labeled_row("Minimum", format!("{:.4}", stats.minimum_value)));
        lines.push(labeled_row("Maximum", format!("{:.4}", stats.maximum_value)));
        lines.push(labeled_row("Mean", format!("{:.4}", stats.mean)));
        lines.push(labeled_row("Std Dev", format!("{:.4}", stats.standard_deviation)));
        lines.push(labeled_row("Valid Pixels", format!("{:.2}%", stats.valid_pixel_percentage)));
        lines.push(Line::from(""));
    }

    // Overview Information section
    lines.push(Line::from(Span::styled(" ▸ Overviews", Style::default().fg(Color::Green).bold())));
    lines.push(Line::from(""));
    lines.push(labeled_row("Count", format!("{}", app.cog_metadata.overviews.len())));
    lines.push(Line::from(""));

    for (i, overview) in app.cog_metadata.overviews.iter().enumerate() {
        let chunk_count = overview.chunk_locations.len();
        let chunks_per_band = if app.is_multiband {
            chunk_count / app.band_count as usize
        } else {
            chunk_count
        };

        let overview_info = format!(
            "{:>5} × {:<5}  {:>4} chunks{}",
            overview.raster_size.cols.count(),
            overview.raster_size.rows.count(),
            chunks_per_band,
            if app.is_multiband {
                format!("  ({} total)", chunk_count)
            } else {
                String::new()
            }
        );

        lines.push(Line::from(vec![
            Span::styled(
                format!("{:>width$}  ", format!("Level {}", i), width = LABEL_WIDTH),
                Style::default().fg(Color::Yellow),
            ),
            Span::raw(overview_info),
        ]));
    }

    // WebTiles Information (if available)
    if let Some(ref reader) = app.webtiles_reader {
        lines.push(Line::from(""));
        lines.push(Line::from(Span::styled(" ▸ Web Tiles", Style::default().fg(Color::Green).bold())));
        lines.push(Line::from(""));
        let info = reader.tile_info();
        lines.push(labeled_row("Zoom Range", format!("{} – {}", info.min_zoom, info.max_zoom)));
        lines.push(labeled_row("Tile Size", format!("{} × {}", info.tile_size, info.tile_size)));
        lines.push(labeled_row(
            "Bounds",
            format!(
                "[{:.4}, {:.4}] → [{:.4}, {:.4}]",
                info.bounds.west(),
                info.bounds.south(),
                info.bounds.east(),
                info.bounds.north()
            ),
        ));
    }

    // Update scroll bounds
    let content_height = lines.len() as u16;
    let viewport_height = inner_area.height;
    app.overview_tab.update_scroll_bounds(content_height, viewport_height);

    // Create scrollable paragraph
    let paragraph = Paragraph::new(Text::from(lines))
        .wrap(Wrap { trim: false })
        .scroll((app.overview_tab.scroll_offset, 0));

    frame.render_widget(paragraph, inner_area);
}

/// Render the Raw Chunks tab content.
fn render_chunks_tab(app: &mut App, frame: &mut Frame, area: Rect) {
    // Split into left (navigation) and right (preview) panels
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(area);

    // Render navigation panel
    render_chunks_navigation(app, frame, chunks[0]);

    // Render preview panel
    render_chunks_preview(app, frame, chunks[1]);
}

/// Render the chunks navigation panel.
fn render_chunks_navigation(app: &mut App, frame: &mut Frame, area: Rect) {
    let title = match app.chunks_tab.view_state {
        ChunkViewState::BrowsingOverviews => format!(
            "Overviews{}",
            if app.is_multiband {
                format!(" ({})", app.current_band_display())
            } else {
                String::new()
            }
        ),
        ChunkViewState::BrowsingChunks | ChunkViewState::ViewingChunk => {
            if let Some(idx) = app.chunks_tab.selected_overview {
                format!("Chunks - Overview {}", idx)
            } else {
                "Chunks".to_string()
            }
        }
    };

    let block = Block::default().borders(Borders::ALL).border_type(BorderType::Rounded).title(title);

    let inner_area = block.inner(area);
    frame.render_widget(block, area);

    match app.chunks_tab.view_state {
        ChunkViewState::BrowsingOverviews => {
            // Show list of overviews
            let items: Vec<ListItem> = app
                .cog_metadata
                .overviews
                .iter()
                .enumerate()
                .map(|(i, overview)| {
                    let chunk_count = overview.chunk_locations.len();
                    let chunks_per_band = if app.is_multiband {
                        chunk_count / app.band_count as usize
                    } else {
                        chunk_count
                    };

                    let content = format!(
                        "Overview {} ({}x{}) - {} chunks",
                        i,
                        overview.raster_size.cols.count(),
                        overview.raster_size.rows.count(),
                        chunks_per_band
                    );

                    ListItem::new(content)
                })
                .collect();

            let list = List::new(items)
                .highlight_style(Style::default().bg(Color::DarkGray).fg(Color::Yellow).add_modifier(Modifier::BOLD))
                .highlight_symbol("▶ ");

            frame.render_stateful_widget(list, inner_area, &mut app.chunks_tab.overview_list_state);
        }
        ChunkViewState::BrowsingChunks | ChunkViewState::ViewingChunk => {
            // Show list of chunks for selected overview
            if let Some(overview_idx) = app.chunks_tab.selected_overview
                && let Some(overview) = app.cog_metadata.overviews.get(overview_idx)
            {
                // Calculate width needed for chunk index alignment
                let max_idx = overview.chunk_locations.len().saturating_sub(1);
                let idx_width = max_idx.to_string().len();

                let items: Vec<ListItem> = overview
                    .chunk_locations
                    .iter()
                    .enumerate()
                    .map(|(i, chunk_loc)| {
                        let sparse_marker = if chunk_loc.is_sparse() { " [Sparse]" } else { "" };
                        let size_kb = chunk_loc.size as f64 / 1024.0;
                        let content = format!("Chunk {:>width$}  {:>8.1} KB{}", i, size_kb, sparse_marker, width = idx_width);
                        ListItem::new(content)
                    })
                    .collect();

                let list = List::new(items)
                    .highlight_style(Style::default().bg(Color::DarkGray).fg(Color::Yellow).add_modifier(Modifier::BOLD))
                    .highlight_symbol("▶ ");

                frame.render_stateful_widget(list, inner_area, &mut app.chunks_tab.chunk_list_state);
            }
        }
    }
}

/// Render the chunks preview panel.
fn render_chunks_preview(app: &mut App, frame: &mut Frame, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title("Chunk Preview");

    let inner_area = block.inner(area);
    frame.render_widget(block, area);

    match app.chunks_tab.view_state {
        ChunkViewState::BrowsingOverviews => {
            // Show overview info
            if let Some(overview_idx) = app.chunks_tab.selected_overview
                && let Some(overview) = app.cog_metadata.overviews.get(overview_idx)
            {
                let info = vec![
                    Line::from(vec![
                        Span::styled("Overview ", Style::default().fg(Color::Yellow).bold()),
                        Span::raw(format!("{}", overview_idx)),
                    ]),
                    Line::from(""),
                    Line::from(vec![
                        Span::styled("Size: ", Style::default().fg(Color::Cyan)),
                        Span::raw(format!(
                            "{}x{}",
                            overview.raster_size.cols.count(),
                            overview.raster_size.rows.count()
                        )),
                    ]),
                    Line::from(vec![
                        Span::styled("Total Chunks: ", Style::default().fg(Color::Cyan)),
                        Span::raw(format!("{}", overview.chunk_locations.len())),
                    ]),
                    Line::from(""),
                    Line::from(Span::styled(
                        "Press Enter to browse chunks",
                        Style::default().fg(Color::Gray).italic(),
                    )),
                ];

                let paragraph = Paragraph::new(info);
                frame.render_widget(paragraph, inner_area);
            }
        }
        ChunkViewState::BrowsingChunks => {
            // Show chunk info
            if let (Some(overview_idx), Some(chunk_idx)) = (app.chunks_tab.selected_overview, app.chunks_tab.selected_chunk)
                && let Some(overview) = app.cog_metadata.overviews.get(overview_idx)
                && let Some(chunk_loc) = overview.chunk_locations.get(chunk_idx)
            {
                let info = vec![
                    Line::from(vec![
                        Span::styled("Chunk ", Style::default().fg(Color::Yellow).bold()),
                        Span::raw(format!("{}", chunk_idx)),
                    ]),
                    Line::from(""),
                    Line::from(vec![
                        Span::styled("Offset: ", Style::default().fg(Color::Cyan)),
                        Span::raw(format!("{}", chunk_loc.offset)),
                    ]),
                    Line::from(vec![
                        Span::styled("Size: ", Style::default().fg(Color::Cyan)),
                        Span::raw(format!("{} bytes", chunk_loc.size)),
                    ]),
                    Line::from(vec![
                        Span::styled("Sparse: ", Style::default().fg(Color::Cyan)),
                        Span::raw(if chunk_loc.is_sparse() { "Yes" } else { "No" }),
                    ]),
                    Line::from(""),
                    Line::from(Span::styled(
                        "Press Enter to view chunk data",
                        Style::default().fg(Color::Gray).italic(),
                    )),
                ];

                let paragraph = Paragraph::new(info);
                frame.render_widget(paragraph, inner_area);
            }
        }
        ChunkViewState::ViewingChunk => {
            // Show chunk visualization
            if let Some(ref mut chunk_data) = app.chunks_tab.chunk_data {
                if chunk_data.is_sparse || chunk_data.pixels.is_empty() {
                    let message = Paragraph::new("Sparse chunk - no data").style(Style::default().fg(Color::Yellow).italic());
                    frame.render_widget(message, inner_area);
                } else if chunk_data.show_hires {
                    if let Some(ref mut image_state) = chunk_data.image_state {
                        // Use high-resolution image rendering with Scale to fill available area
                        let image_widget = StatefulImage::default().resize(Resize::Scale(None));
                        frame.render_stateful_widget(image_widget, inner_area, image_state);
                    } else {
                        // Hi-res requested but image_state not ready, show loading message
                        let loading = Paragraph::new("Loading hi-res image...").style(Style::default().fg(Color::Gray).italic());
                        frame.render_widget(loading, inner_area);
                    }
                } else {
                    // Default: low-res colormap rendering (press Space for hi-res)
                    render_colormap_image(
                        &chunk_data.pixels,
                        chunk_data.width,
                        chunk_data.height,
                        chunk_data.is_sparse,
                        frame,
                        inner_area,
                    );
                }
            } else {
                let loading = Paragraph::new("Loading chunk data...").style(Style::default().fg(Color::Gray).italic());
                frame.render_widget(loading, inner_area);
            }
        }
    }
}

/// Render the Web Tiles tab content.
fn render_webtiles_tab(app: &mut App, frame: &mut Frame, area: Rect) {
    // Check if webtiles reader is available
    if app.webtiles_reader.is_none() {
        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .title("Web Tiles");

        let message = Paragraph::new("Web tiles not available for this COG file.")
            .block(block)
            .style(Style::default().fg(Color::Yellow));

        frame.render_widget(message, area);
        return;
    }

    // Split into left (navigation) and right (preview) panels
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(40), Constraint::Percentage(60)])
        .split(area);

    // Render navigation panel
    render_webtiles_navigation(app, frame, chunks[0]);

    // Render preview panel
    render_webtiles_preview(app, frame, chunks[1]);
}

/// Render the web tiles navigation panel.
fn render_webtiles_navigation(app: &mut App, frame: &mut Frame, area: Rect) {
    let title = match app.webtiles_tab.view_state {
        TileViewState::BrowsingBands => "Bands".to_string(),
        TileViewState::BrowsingZoomLevels => {
            if let Some(band) = app.webtiles_tab.selected_band {
                format!("Zoom Levels - Band {}", band)
            } else {
                "Zoom Levels".to_string()
            }
        }
        TileViewState::BrowsingTiles | TileViewState::ViewingTile => {
            if let Some(zoom) = app.webtiles_tab.selected_zoom {
                format!("Tiles - Zoom {}", zoom)
            } else {
                "Tiles".to_string()
            }
        }
    };

    let block = Block::default().borders(Borders::ALL).border_type(BorderType::Rounded).title(title);

    let inner_area = block.inner(area);
    frame.render_widget(block, area);

    let Some(ref reader) = app.webtiles_reader else {
        return;
    };

    match app.webtiles_tab.view_state {
        TileViewState::BrowsingBands => {
            // Show list of bands
            let items: Vec<ListItem> = (1..=app.webtiles_tab.band_count)
                .map(|band| {
                    let content = format!("Band {}", band);
                    ListItem::new(content)
                })
                .collect();

            let list = List::new(items)
                .highlight_style(Style::default().bg(Color::DarkGray).fg(Color::Yellow).add_modifier(Modifier::BOLD))
                .highlight_symbol("▶ ");

            frame.render_stateful_widget(list, inner_area, &mut app.webtiles_tab.band_list_state);
        }
        TileViewState::BrowsingZoomLevels => {
            // Show list of zoom levels
            let min_zoom = reader.tile_info().min_zoom;
            let max_zoom = reader.tile_info().max_zoom;

            let items: Vec<ListItem> = (min_zoom..=max_zoom)
                .map(|zoom| {
                    let tile_count = reader.zoom_level_tile_sources(zoom).map_or(0, |t| t.len());
                    let content = format!("Zoom {:>2}  {:>4} tiles", zoom, tile_count);
                    ListItem::new(content)
                })
                .collect();

            let list = List::new(items)
                .highlight_style(Style::default().bg(Color::DarkGray).fg(Color::Yellow).add_modifier(Modifier::BOLD))
                .highlight_symbol("▶ ");

            frame.render_stateful_widget(list, inner_area, &mut app.webtiles_tab.zoom_list_state);
        }
        TileViewState::BrowsingTiles | TileViewState::ViewingTile => {
            // Show list of tiles for selected zoom level
            let items: Vec<ListItem> = app
                .webtiles_tab
                .tiles_at_zoom
                .iter()
                .map(|tile| {
                    let source = reader.tile_source(tile);
                    let source_type = source.map_or_else(|| "Unknown".to_string(), |s| format!("{:?}", s));
                    // Truncate source type for display
                    let source_short = if source_type.len() > 20 {
                        format!("{}...", &source_type[..17])
                    } else {
                        source_type
                    };
                    let content = format!("({}, {}) [{}]", tile.x(), tile.y(), source_short);
                    ListItem::new(content)
                })
                .collect();

            let list = List::new(items)
                .highlight_style(Style::default().bg(Color::DarkGray).fg(Color::Yellow).add_modifier(Modifier::BOLD))
                .highlight_symbol("▶ ");

            frame.render_stateful_widget(list, inner_area, &mut app.webtiles_tab.tile_list_state);
        }
    }
}

/// Render the web tiles preview panel.
fn render_webtiles_preview(app: &mut App, frame: &mut Frame, area: Rect) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title("Tile Preview");

    let inner_area = block.inner(area);
    frame.render_widget(block, area);

    let Some(ref reader) = app.webtiles_reader else {
        return;
    };

    match app.webtiles_tab.view_state {
        TileViewState::BrowsingBands => {
            // Show band info
            if let Some(band) = app.webtiles_tab.selected_band {
                let info = vec![
                    Line::from(vec![
                        Span::styled("Band ", Style::default().fg(Color::Yellow).bold()),
                        Span::raw(format!("{}", band)),
                    ]),
                    Line::from(""),
                    Line::from(vec![
                        Span::styled("Total Bands: ", Style::default().fg(Color::Cyan)),
                        Span::raw(format!("{}", app.webtiles_tab.band_count)),
                    ]),
                    Line::from(vec![
                        Span::styled("Zoom Range: ", Style::default().fg(Color::Cyan)),
                        Span::raw(format!("{} – {}", app.webtiles_tab.min_zoom, app.webtiles_tab.max_zoom)),
                    ]),
                    Line::from(""),
                    Line::from(Span::styled(
                        "Press Enter to browse zoom levels",
                        Style::default().fg(Color::Gray).italic(),
                    )),
                ];

                let paragraph = Paragraph::new(info);
                frame.render_widget(paragraph, inner_area);
            }
        }
        TileViewState::BrowsingZoomLevels => {
            // Show zoom level info
            if let Some(zoom) = app.webtiles_tab.selected_zoom {
                let tile_count = reader.zoom_level_tile_sources(zoom).map_or(0, |t| t.len());
                let tile_size = reader.tile_info().tile_size;

                let info = vec![
                    Line::from(vec![
                        Span::styled("Zoom Level ", Style::default().fg(Color::Yellow).bold()),
                        Span::raw(format!("{}", zoom)),
                    ]),
                    Line::from(""),
                    Line::from(vec![
                        Span::styled("Tile Count: ", Style::default().fg(Color::Cyan)),
                        Span::raw(format!("{}", tile_count)),
                    ]),
                    Line::from(vec![
                        Span::styled("Tile Size: ", Style::default().fg(Color::Cyan)),
                        Span::raw(format!("{} × {}", tile_size, tile_size)),
                    ]),
                    Line::from(vec![
                        Span::styled("Band: ", Style::default().fg(Color::Cyan)),
                        Span::raw(format!("{}", app.webtiles_tab.get_selected_band())),
                    ]),
                    Line::from(""),
                    Line::from(Span::styled(
                        "Press Enter to browse tiles",
                        Style::default().fg(Color::Gray).italic(),
                    )),
                ];

                let paragraph = Paragraph::new(info);
                frame.render_widget(paragraph, inner_area);
            }
        }
        TileViewState::BrowsingTiles => {
            // Show tile info
            if let Some(tile) = app.webtiles_tab.selected_tile {
                let source = reader.tile_source(&tile);
                let source_type = source.map_or_else(|| "Unknown".to_string(), |s| format!("{:?}", s));

                let info = vec![
                    Line::from(vec![
                        Span::styled("Tile ", Style::default().fg(Color::Yellow).bold()),
                        Span::raw(format!("({}, {})", tile.x(), tile.y())),
                    ]),
                    Line::from(""),
                    Line::from(vec![
                        Span::styled("Zoom: ", Style::default().fg(Color::Cyan)),
                        Span::raw(format!("{}", tile.z())),
                    ]),
                    Line::from(vec![
                        Span::styled("Source Type: ", Style::default().fg(Color::Cyan)),
                        Span::raw(source_type),
                    ]),
                    Line::from(""),
                    Line::from(Span::styled(
                        "Press Enter to view tile data",
                        Style::default().fg(Color::Gray).italic(),
                    )),
                ];

                let paragraph = Paragraph::new(info);
                frame.render_widget(paragraph, inner_area);
            }
        }
        TileViewState::ViewingTile => {
            // Show tile visualization
            if let Some(ref mut tile_data) = app.webtiles_tab.tile_data {
                if tile_data.pixels.is_empty() {
                    let message = Paragraph::new("No tile data").style(Style::default().fg(Color::Yellow).italic());
                    frame.render_widget(message, inner_area);
                } else if tile_data.show_hires {
                    if let Some(ref mut image_state) = tile_data.image_state {
                        // Use high-resolution image rendering with Scale to fill available area
                        let image_widget = StatefulImage::default().resize(Resize::Scale(None));
                        frame.render_stateful_widget(image_widget, inner_area, image_state);
                    } else {
                        // Hi-res requested but image_state not ready, show loading message
                        let loading = Paragraph::new("Loading hi-res image...").style(Style::default().fg(Color::Gray).italic());
                        frame.render_widget(loading, inner_area);
                    }
                } else {
                    // Default: low-res colormap rendering (press Space for hi-res)
                    render_colormap_image(&tile_data.pixels, tile_data.width, tile_data.height, false, frame, inner_area);
                }
            } else {
                let loading = Paragraph::new("Loading tile data...").style(Style::default().fg(Color::Gray).italic());
                frame.render_widget(loading, inner_area);
            }
        }
    }
}

/// Render a grayscale image using Unicode block characters.
fn render_colormap_image(pixels: &[u8], width: u32, height: u32, is_sparse: bool, frame: &mut Frame, area: Rect) {
    if is_sparse || pixels.is_empty() {
        let message = Paragraph::new("Sparse chunk - no data").style(Style::default().fg(Color::Yellow).italic());
        frame.render_widget(message, area);
        return;
    }

    let term_width = area.width as usize;
    let term_height = area.height as usize;

    if term_width == 0 || term_height == 0 {
        return;
    }

    // Get the Turbo colormap
    let colormap = turbo_colormap();

    // Calculate aspect ratios to maintain image proportions
    let image_aspect = width as f64 / height as f64;
    let term_aspect = term_width as f64 / (term_height * 2) as f64; // *2 because we use half-block characters

    // Determine render dimensions that maintain aspect ratio
    let (render_width, render_height) = if image_aspect > term_aspect {
        // Image is wider - fit to width
        let render_width = term_width;
        let render_height = ((term_width as f64 / image_aspect) / 2.0).round() as usize;
        (render_width, render_height)
    } else {
        // Image is taller - fit to height
        let render_height = term_height;
        let render_width = (term_height as f64 * 2.0 * image_aspect).round() as usize;
        (render_width, render_height)
    };

    // Left-align the image (top-left)
    let offset_x = 0;
    let offset_y = 0;

    // Calculate uniform scaling factors
    let scale_x = width as f64 / render_width as f64;
    let scale_y = height as f64 / (render_height * 2) as f64; // *2 because we use half-block characters

    // Unicode block characters for colormap rendering
    // Using half-block characters for better vertical resolution
    let mut lines: Vec<Line> = Vec::new();

    for row in 0..term_height {
        let mut spans: Vec<Span> = Vec::new();

        for col in 0..term_width {
            // Check if we're within the image bounds
            if row < offset_y || row >= offset_y + render_height || col < offset_x || col >= offset_x + render_width {
                // Outside image area - render blank space
                spans.push(Span::raw(" "));
                continue;
            }

            // Adjust for offset
            let img_row = row - offset_y;
            let img_col = col - offset_x;

            // Sample top and bottom pixels for this character cell
            let top_y = ((img_row * 2) as f64 * scale_y) as usize;
            let bottom_y = ((img_row * 2 + 1) as f64 * scale_y) as usize;
            let x = (img_col as f64 * scale_x) as usize;

            let top_y = top_y.min(height as usize - 1);
            let bottom_y = bottom_y.min(height as usize - 1);
            let x = x.min(width as usize - 1);

            let top_idx = top_y * width as usize + x;
            let bottom_idx = bottom_y * width as usize + x;

            let top_value = pixels.get(top_idx).copied().unwrap_or(0);
            let bottom_value = pixels.get(bottom_idx).copied().unwrap_or(0);

            // Map through Turbo colormap
            let top_mapped = colormap.get_color_by_value(top_value);
            let bottom_mapped = colormap.get_color_by_value(bottom_value);

            // Use half-block character with foreground (top) and background (bottom) colors
            let top_color = Color::Rgb(top_mapped.r, top_mapped.g, top_mapped.b);
            let bottom_color = Color::Rgb(bottom_mapped.r, bottom_mapped.g, bottom_mapped.b);

            // Upper half block: ▀ (foreground is top, background is bottom)
            spans.push(Span::styled("▀", Style::default().fg(top_color).bg(bottom_color)));
        }

        lines.push(Line::from(spans));
    }

    let paragraph = Paragraph::new(Text::from(lines));
    frame.render_widget(paragraph, area);
}

/// Format file size in human-readable format.
fn format_file_size(size: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if size >= GB {
        format!("{:.2} GB", size as f64 / GB as f64)
    } else if size >= MB {
        format!("{:.2} MB", size as f64 / MB as f64)
    } else if size >= KB {
        format!("{:.2} KB", size as f64 / KB as f64)
    } else {
        format!("{} bytes", size)
    }
}

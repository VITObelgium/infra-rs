use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout},
    style::{Color, Style, Stylize},
    widgets::{Block, BorderType, Row, Table},
};
use tiler::LayerMetadata;
use tui_logger::{TuiLoggerLevelOutput, TuiLoggerWidget};

use super::app::App;

fn layer_metadata_rows(layer: Option<&LayerMetadata>) -> Vec<Row> {
    match layer {
        Some(layer) => vec![
            Row::new(vec!["Id".to_string(), layer.id.to_string()]),
            Row::new(vec!["Name".to_string(), layer.name.clone()]),
            Row::new(vec![
                "EPSG".to_string(),
                layer.epsg.map_or("Unknown".to_string(), |v| v.to_string()),
            ]),
            Row::new(vec!["Format".to_string(), format!("{:?}", layer.source_format)]),
            Row::new(vec!["Tile format".to_string(), format!("{}", layer.tile_format)]),
            Row::new(vec!["Nodata".to_string(), format!("{:?}", layer.nodata::<f64>())]),
            Row::new(vec!["Data type".to_string(), format!("{:?}", layer.data_type)]),
            Row::new(vec!["Min value".to_string(), layer.min_value.to_string()]),
            Row::new(vec!["Max value".to_string(), layer.max_value.to_string()]),
            Row::new(vec!["Min zoom".to_string(), layer.min_zoom.to_string()]),
            Row::new(vec!["Max zoom".to_string(), layer.max_zoom.to_string()]),
            Row::new(vec!["Bounds".to_string(), format!("{:?}", layer.bounds)]),
            Row::new(vec!["Tile scheme".to_string(), layer.scheme.clone()]),
            Row::new(vec!["Tile URL".to_string(), layer.url("http://localhost:4444")]),
        ],
        None => Vec::new(),
    }
}

/// Renders the user interface widgets.
pub fn render(app: &mut App, frame: &mut Frame) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints(vec![Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(frame.area());

    let layer_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(vec![Constraint::Percentage(30), Constraint::Percentage(70)])
        .split(layout[0]);

    let rows = app
        .layers
        .iter()
        .map(|layer| {
            Row::new(vec![
                layer.id.to_string(),
                layer.name.clone(),
                app.tiles_served.get(&layer.id).unwrap_or(&0).to_string(),
            ])
        })
        .collect::<Vec<_>>();

    let widths = [Constraint::Length(5), Constraint::Fill(80), Constraint::Length(10)];

    let meta_rows = match app.layer_table_state.selected() {
        Some(index) => layer_metadata_rows(app.layers.get(index)),
        None => Vec::new(),
    };

    let meta_widths = [Constraint::Length(10), Constraint::Fill(80)];

    frame.render_stateful_widget(
        Table::new(rows, widths)
            .column_spacing(1)
            .style(Style::new().blue())
            .header(Row::new(vec!["Id", "Name", "#Tiles"]).style(Style::new().bold()))
            .block(
                Block::bordered()
                    .title("Layers")
                    .title_alignment(Alignment::Left)
                    .border_type(BorderType::Rounded),
            )
            .row_highlight_style(Style::new().reversed()),
        layer_layout[0],
        &mut app.layer_table_state,
    );

    frame.render_widget(
        Table::new(meta_rows, meta_widths)
            .column_spacing(1)
            .style(Style::new().blue())
            .block(
                Block::bordered()
                    .title("Layer properties")
                    .title_alignment(Alignment::Left)
                    .border_type(BorderType::Rounded),
            )
            .row_highlight_style(Style::new().reversed()),
        layer_layout[1],
    );

    frame.render_widget(
        TuiLoggerWidget::default()
            .style_error(Style::default().fg(Color::Red))
            .style_debug(Style::default().fg(Color::Green))
            .style_warn(Style::default().fg(Color::Yellow))
            .style_trace(Style::default().fg(Color::Magenta))
            .style_info(Style::default().fg(Color::Cyan))
            .output_separator(':')
            .output_timestamp(Some("%H:%M:%S".to_string()))
            .output_level(Some(TuiLoggerLevelOutput::Abbreviated))
            .output_target(false)
            .output_file(false)
            .output_line(false)
            .block(
                Block::bordered()
                    .title("Log")
                    .title_alignment(Alignment::Left)
                    .border_type(BorderType::Rounded),
            ),
        layout[1],
    );
}

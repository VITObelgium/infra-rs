use std::collections::HashMap;

use ratatui::widgets::TableState;
use tiler::{LayerId, LayerMetadata};

#[derive(Debug)]
pub struct App {
    pub running: bool,
    pub layers: Vec<LayerMetadata>,
    pub tiles_served: HashMap<LayerId, u64>,
    pub layer_table_state: TableState,
}

impl Default for App {
    fn default() -> Self {
        Self {
            running: true,
            layers: Vec::new(),
            tiles_served: HashMap::new(),
            layer_table_state: TableState::default(),
        }
    }
}

impl App {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set running to false to quit the application.
    pub fn quit(&mut self) {
        self.running = false;
    }
}

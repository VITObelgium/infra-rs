use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::app::App;
use crate::Result;

/// Handles the key events and updates the state of [`App`].
pub fn handle_key_events(key_event: KeyEvent, app: &mut App) -> Result<()> {
    match key_event.code {
        // Exit application on `ESC` or `q`
        KeyCode::Esc | KeyCode::Char('q') => {
            app.quit();
        }
        // Exit application on `Ctrl-C`
        KeyCode::Char('c' | 'C') => {
            if key_event.modifiers == KeyModifiers::CONTROL {
                app.quit();
            }
        }
        // Counter handlers
        KeyCode::Up => {
            app.layer_table_state.select_previous();
        }
        KeyCode::Down => {
            app.layer_table_state.select_next();
        }
        // Other handlers you could add here.
        _ => {}
    }
    Ok(())
}

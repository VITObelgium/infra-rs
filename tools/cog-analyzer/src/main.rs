//! COG Analyzer - Terminal User Interface for analyzing Cloud Optimized GeoTIFF files.

use std::io;
use std::panic;
use std::path::PathBuf;

use clap::Parser;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use cog_analyzer::Result;
use cog_analyzer::app::App;
use cog_analyzer::event::{Event, EventHandler};
use cog_analyzer::handler::handle_key_events;
use cog_analyzer::ui;

#[derive(Parser)]
#[command(name = "cog-analyzer")]
#[command(about = "Analyze and visualize Cloud Optimized GeoTIFF files")]
#[command(version)]
struct Cli {
    /// Path to the GeoTIFF file to analyze
    file_path: PathBuf,
}

fn main() -> Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    // Initialize the application
    let mut app = App::new(cli.file_path)?;

    // Setup terminal
    let mut terminal = setup_terminal()?;

    // Initialize image picker after terminal is in raw mode
    app.init_image_picker(false);

    // Run the application
    let result = run_app(&mut terminal, &mut app);

    // Restore terminal
    restore_terminal(&mut terminal)?;

    result
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<io::Stdout>>> {
    terminal::enable_raw_mode()?;
    crossterm::execute!(io::stdout(), EnterAlternateScreen, EnableMouseCapture)?;

    // Define a custom panic hook to reset the terminal properties.
    let panic_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic| {
        let _ = terminal::disable_raw_mode();
        let _ = crossterm::execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture);
        panic_hook(panic);
    }));

    let backend = CrosstermBackend::new(io::stdout());
    let mut terminal = Terminal::new(backend)?;
    terminal.hide_cursor()?;
    terminal.clear()?;

    Ok(terminal)
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> Result<()> {
    terminal::disable_raw_mode()?;
    crossterm::execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture)?;
    terminal.show_cursor()?;
    Ok(())
}

fn run_app(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>, app: &mut App) -> Result<()> {
    let events = EventHandler::new(250);

    while app.running {
        // Draw the UI
        terminal.draw(|frame| ui::render(app, frame))?;

        // Handle events
        match events.next()? {
            Event::Key(key_event) => {
                handle_key_events(key_event, app)?;
            }
            Event::Tick | Event::Resize(_, _) => {}
        }
    }

    Ok(())
}

use app::App;
use axum::Router;
use crossterm::event::{DisableMouseCapture, EnableMouseCapture};
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use event::{Event, EventHandler};
use ratatui::backend::Backend;
use ratatui::prelude::CrosstermBackend;
use ratatui::{crossterm, Terminal};
use std::io;
use std::panic;
use tokio::sync::broadcast::Receiver;

use crate::tileapihandler::StatusEvent;
use crate::Result;

mod app;
mod event;
mod handler;
mod ui;

pub async fn launch(
    router: Router,
    listener: tokio::net::TcpListener,
    mut status_rx: Receiver<StatusEvent>,
) -> Result<()> {
    let mut app = App::new();

    let backend = CrosstermBackend::new(std::io::stdout());
    let terminal = Terminal::new(backend)?;
    let events = EventHandler::new(250);
    let mut tui = Tui::new(terminal, events);
    tui.init()?;

    let (close_tx, close_rx) = tokio::sync::oneshot::channel();
    let server_handle = tokio::spawn(async {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                _ = close_rx.await;
            })
            .await
            .unwrap();
    });

    // Start the main loop.
    while app.running {
        tui.draw(&mut app)?;
        tokio::select! {
          ev = tui.events.next() => {
            match ev {
              Ok(Event::Tick) => app.tick(),
              Ok(Event::Key(key_event)) => handler::handle_key_events(key_event, &mut app)?,
              Ok(Event::Mouse(_)) => {}
              Ok(Event::Resize(_, _)) => {}
              Err(err) => panic!("Error: {:?}", err),
            }
          }
          Ok(status) = status_rx.recv() => {
            match status {
                StatusEvent::Layers(mut layers) => {
                    layers.sort_by(|a, b| a.id.cmp(&b.id));
                    app.layers = layers;
                }
                StatusEvent::TileServed(layer_id) => {
                    if let Some(count) = app.tiles_served.get(&layer_id) {
                        app.tiles_served.insert(layer_id, count + 1);
                    } else {
                        app.tiles_served.insert(layer_id, 1);
                    }
                }
            }
          }
        }
    }

    // Exit the user interface.
    tui.exit()?;

    // Exit the server.
    _ = close_tx.send(());
    _ = server_handle.await;

    Ok(())
}

/// Representation of a terminal user interface.
///
/// It is responsible for setting up the terminal,
/// initializing the interface and handling the draw events.
#[derive(Debug)]
pub struct Tui<B: Backend> {
    /// Interface to the Terminal.
    terminal: Terminal<B>,
    /// Terminal event handler.
    pub events: EventHandler,
}

impl<B: Backend> Tui<B> {
    pub fn new(terminal: Terminal<B>, events: EventHandler) -> Self {
        Self { terminal, events }
    }

    pub fn init(&mut self) -> Result<()> {
        terminal::enable_raw_mode()?;
        crossterm::execute!(io::stdout(), EnterAlternateScreen, EnableMouseCapture)?;

        // Define a custom panic hook to reset the terminal properties.
        // This way, you won't have your terminal messed up if an unexpected error happens.
        let panic_hook = panic::take_hook();
        panic::set_hook(Box::new(move |panic| {
            Self::reset().expect("failed to reset the terminal");
            panic_hook(panic);
        }));

        self.terminal.hide_cursor()?;
        self.terminal.clear()?;
        Ok(())
    }

    pub fn draw(&mut self, app: &mut App) -> Result<()> {
        self.terminal.draw(|frame| ui::render(app, frame))?;
        Ok(())
    }

    fn reset() -> Result<()> {
        terminal::disable_raw_mode()?;
        crossterm::execute!(io::stdout(), LeaveAlternateScreen, DisableMouseCapture)?;
        Ok(())
    }

    pub fn exit(&mut self) -> Result<()> {
        Self::reset()?;
        self.terminal.show_cursor()?;
        Ok(())
    }
}

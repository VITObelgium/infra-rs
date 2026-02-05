//! Event handling for the COG Analyzer TUI.
//!
//! This module provides a synchronous event handler for terminal events
//! including keyboard input, terminal resize, and tick events.

use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use crossterm::event::{self, Event as CrosstermEvent, KeyEvent};

use crate::Result;

/// Terminal events.
#[derive(Clone, Copy, Debug)]
pub enum Event {
    /// Terminal tick (for periodic updates).
    Tick,
    /// Key press event.
    Key(KeyEvent),
    /// Terminal resize event.
    Resize(u16, u16),
}

/// Terminal event handler.
///
/// This handler spawns a background thread that polls for terminal events
/// and sends them through a channel.
#[derive(Debug)]
pub struct EventHandler {
    /// Event receiver channel.
    receiver: mpsc::Receiver<Event>,
    /// Event sender channel (kept to prevent channel closure).
    #[allow(dead_code)]
    sender: mpsc::Sender<Event>,
}

impl EventHandler {
    /// Constructs a new instance of [`EventHandler`].
    ///
    /// # Arguments
    ///
    /// * `tick_rate` - The tick rate in milliseconds for periodic updates.
    pub fn new(tick_rate: u64) -> Self {
        let tick_rate = Duration::from_millis(tick_rate);
        let (sender, receiver) = mpsc::channel();

        let event_sender = sender.clone();
        thread::spawn(move || {
            loop {
                // Poll for events with timeout
                if event::poll(tick_rate).unwrap_or(false) {
                    match event::read() {
                        Ok(CrosstermEvent::Key(key)) => {
                            // Only send key press events (not release or repeat)
                            if key.kind == event::KeyEventKind::Press {
                                if event_sender.send(Event::Key(key)).is_err() {
                                    break;
                                }
                            }
                        }
                        Ok(CrosstermEvent::Resize(width, height)) => {
                            if event_sender.send(Event::Resize(width, height)).is_err() {
                                break;
                            }
                        }
                        Ok(_) => {
                            // Ignore other events (mouse, focus, paste)
                        }
                        Err(_) => {
                            break;
                        }
                    }
                } else {
                    // Timeout - send tick event
                    if event_sender.send(Event::Tick).is_err() {
                        break;
                    }
                }
            }
        });

        Self { receiver, sender }
    }

    /// Receive the next event from the handler.
    ///
    /// This function will block until an event is available.
    pub fn next(&self) -> Result<Event> {
        Ok(self.receiver.recv()?)
    }
}

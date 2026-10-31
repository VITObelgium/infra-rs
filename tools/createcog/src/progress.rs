use std::{
    io::{IsTerminal, Write},
    sync::Arc,
    time::{Duration, Instant},
};

use kdam::{BarExt, Column, RichProgress, tqdm};
use parking_lot::Mutex;

const PROGRESS_UNITS: usize = 1000;
const BAND_UNITS: usize = 900;
const UPDATE_INTERVAL: Duration = Duration::from_secs(1);

/// A single progress bar shared by completed bands (90%) and output assembly (10%).
pub(crate) struct ProgressReporter {
    display: Mutex<ProgressDisplay>,
    enabled: bool,
}

struct ProgressDisplay {
    bar: Arc<Mutex<RichProgress>>,
    terminal: bool,
    last_report: Instant,
    finished: bool,
}

impl ProgressReporter {
    pub(crate) fn start(enabled: bool) -> crate::Result<Self> {
        let terminal = enabled && std::io::stderr().is_terminal();
        kdam::term::init(terminal);
        Ok(Self {
            display: Mutex::new(ProgressDisplay {
                bar: Arc::new(Mutex::new(RichProgress::new(
                    tqdm!(total = PROGRESS_UNITS, disable = !terminal),
                    columns(),
                ))),
                terminal,
                last_report: Instant::now(),
                finished: false,
            }),
            enabled,
        })
    }

    pub(crate) fn phase(&self, message: impl Into<String>) {
        if self.enabled && !self.display.lock().terminal {
            log_status(&message.into());
        }
    }

    pub(crate) fn bands(&self, fraction: f64) {
        if fraction.is_finite() {
            self.update(fraction.clamp(0.0, 1.0) * BAND_UNITS as f64);
        }
    }

    pub(crate) fn assembly(&self, fraction: f64) {
        if fraction.is_finite() {
            self.update(BAND_UNITS as f64 + fraction.clamp(0.0, 1.0) * (PROGRESS_UNITS - BAND_UNITS) as f64);
        }
    }

    fn update(&self, position: f64) {
        if !self.enabled {
            return;
        }
        let mut display = self.display.lock();
        let mut bar = display.bar.lock();
        // Only successful finalization may display 100%.
        let position = (position as usize).min(PROGRESS_UNITS - 1).max(bar.pb.counter);
        let changed = position > bar.pb.counter;
        let _ = bar.update_to(position);
        drop(bar);
        if !display.terminal && changed && (position == BAND_UNITS || display.last_report.elapsed() >= UPDATE_INTERVAL) {
            log_status(&format!("Creating COG: {:.1}%", position as f64 / 10.0));
            display.last_report = Instant::now();
        }
    }

    pub(crate) fn finish(&self) {
        if self.enabled {
            let mut display = self.display.lock();
            let mut bar = display.bar.lock();
            let _ = bar.update_to(PROGRESS_UNITS);
            if display.terminal {
                let _ = bar.refresh();
            } else {
                log_status("Finished; output COG is ready (100.0%)");
            }
            drop(bar);
            display.finished = true;
        }
    }

    pub(crate) fn log_writer(&self) -> ProgressLogWriter {
        ProgressLogWriter {
            bar: Arc::clone(&self.display.lock().bar),
        }
    }
}

impl Drop for ProgressReporter {
    fn drop(&mut self) {
        let display = self.display.get_mut();
        if !display.finished && display.terminal {
            let _ = display.bar.lock().clear();
        }
    }
}

fn log_status(message: &str) {
    let _ = writeln!(std::io::stderr().lock(), "{message}");
}

fn columns() -> Vec<Column> {
    vec![
        Column::Text("[bold blue]Creating COG".into()),
        Column::Animation,
        Column::Percentage(1),
        Column::Text("•".into()),
        Column::ElapsedTime,
        Column::Text("•".into()),
        Column::RemainingTime,
    ]
}

pub(crate) struct ProgressLogWriter {
    bar: Arc<Mutex<RichProgress>>,
}

impl Write for ProgressLogWriter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let message = String::from_utf8_lossy(buffer);
        self.bar.lock().write(message.trim_end_matches(['\r', '\n']))?;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "progress_tests.rs"]
mod tests;

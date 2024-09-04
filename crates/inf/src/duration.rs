use std::time::Instant;

pub struct Recorder {
    start: Instant,
}

impl Recorder {
    pub fn new() -> Self {
        Self { start: Instant::now() }
    }

    pub fn reset(&mut self) {
        self.start = Instant::now();
    }

    pub fn elapsed_time(&self) -> std::time::Duration {
        Instant::now() - self.start
    }

    pub fn elapsed_time_string(&self) -> String {
        let elapsed = chrono::TimeDelta::from_std(self.elapsed_time()).unwrap_or_default();

        if elapsed.num_seconds() > 60 {
            let minutes = elapsed.num_minutes();
            let seconds = elapsed.num_seconds() - (minutes * 60);
            format!("{} minutes {} seconds", minutes, seconds)
        } else {
            let seconds = elapsed.num_seconds();
            let milliseconds = elapsed.num_milliseconds() - (seconds * 1000);
            format!("{}.{:03} seconds", seconds, milliseconds)
        }
    }
}

impl Default for Recorder {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for Recorder {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let elapsed = self.start.elapsed();
        write!(f, "{}.{:03}s", elapsed.as_secs(), elapsed.subsec_millis())
    }
}

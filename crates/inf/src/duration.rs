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
        let elapsed = self.elapsed_time();

        if elapsed.as_secs() > 60 {
            let minutes = elapsed.as_secs() / 60;
            let seconds = elapsed.as_secs() - (minutes * 60);
            format!("{minutes} minutes {seconds} seconds")
        } else {
            let seconds = elapsed.as_secs();
            let milliseconds = elapsed.as_millis() - (seconds * 1000) as u128;
            format!("{seconds}.{milliseconds:03} seconds")
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

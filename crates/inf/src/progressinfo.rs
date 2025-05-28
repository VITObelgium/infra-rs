use crate::Result;
use std::sync::{Arc, Mutex};

/// Return value for progress notification callback
/// When the computation should be cancelled, return [`ComputationStatus::CancelRequested`]
pub enum ComputationStatus {
    Continue,
    CancelRequested,
}

/// Trait for notifying progress status
pub trait ProgressNotification {
    type Payload;

    /// Notifies the progress status that a tick has been completed
    /// Returns [`crate::Error::Cancelled`] if the operation has been cancelled and further processing should be stopped
    fn tick(&self) -> Result<()>;

    /// Provide additional context about the current progress
    fn set_payload(&self, payload: Self::Payload);

    /// Reset the progress status and update the total number of ticks
    fn reset(&self, total: u64);
}

/// Trait for notifying progress status asynchronously
/// This trait should be used when the progress notification is shared between multiple threads
pub trait AsyncProgressNotification: ProgressNotification + Send + Clone + Sync + 'static {}

/// Dummy progress notification that does nothing
/// Use this when no progress notification is needed
#[derive(Clone, Default)]
pub struct DummyProgress;

impl ProgressNotification for DummyProgress {
    type Payload = ();

    fn tick(&self) -> Result<()> {
        Ok(())
    }

    fn set_payload(&self, _payload: Self::Payload) {}
    fn reset(&self, _total: u64) {}
}

/// Progress notifier that calls a callback function.
/// Pass in a closure that receives the progress status so you can customize the progress notification
#[derive(Clone)]
pub struct CallbackProgress<T: Default + Clone, CB: FnMut(f64, T) -> ComputationStatus> {
    progress: Arc<Mutex<ProgressState<T, CB>>>,
}

impl<T: Default + Clone, CB: FnMut(f64, T) -> ComputationStatus> CallbackProgress<T, CB> {
    pub fn with_cb(callback: CB) -> Self {
        Self {
            progress: Arc::new(Mutex::new(ProgressState::new(0, callback))),
        }
    }
}

impl<T: Default + Clone, CB: FnMut(f64, T) -> ComputationStatus> ProgressNotification for CallbackProgress<T, CB> {
    type Payload = T;

    fn tick(&self) -> Result<()> {
        if let Ok(mut state) = self.progress.lock() {
            state.increment();
            state.notify_progress()
        } else {
            // The lock has been poisoned, which means that another thread panicked while holding the lock
            Err(crate::Error::Cancelled)
        }
    }

    fn set_payload(&self, payload: Self::Payload) {
        if let Ok(mut state) = self.progress.lock() {
            state.payload = payload;
        }
    }

    fn reset(&self, total: u64) {
        if let Ok(mut state) = self.progress.lock() {
            state.reset(total);
        }
    }
}

impl AsyncProgressNotification for DummyProgress {}

impl<T: Default + Clone + Send + Sync + 'static, CB: FnMut(f64, T) -> ComputationStatus> AsyncProgressNotification
    for CallbackProgress<T, CB>
where
    CB: Clone + Send + Sync + 'static,
{
}

struct ProgressState<T: Default, Cb: FnMut(f64, T) -> ComputationStatus> {
    current: u64,
    total: u64,
    payload: T,
    callback: Cb,
}

impl<T, Cb> ProgressState<T, Cb>
where
    T: Default + Clone,
    Cb: FnMut(f64, T) -> ComputationStatus,
{
    pub fn new(total: u64, cb: Cb) -> Self {
        Self {
            current: 0,
            total,
            payload: Default::default(),
            callback: cb,
        }
    }

    fn increment(&mut self) {
        self.current += 1;
    }

    fn percentage(&self) -> f64 {
        (self.current as f64 / self.total as f64).clamp(0.0, 1.0)
    }

    fn notify_progress(&mut self) -> Result<()> {
        let percentage = self.percentage();
        let payload = self.payload.clone();

        match (self.callback)(percentage, payload) {
            ComputationStatus::Continue => Ok(()),
            ComputationStatus::CancelRequested => Err(crate::Error::Cancelled),
        }
    }

    fn reset(&mut self, total: u64) {
        self.current = 0;
        self.total = total;
        self.payload = Default::default();
    }
}

#[cfg(test)]
mod tests {
    use crate::Error;

    use super::*;

    fn func_with_progress(progress: impl ProgressNotification) -> Result<()> {
        progress.reset(10);
        for _ in 0..10 {
            progress.tick()?;
        }

        Ok(())
    }

    fn func_with_payload_progres(progress: impl ProgressNotification<Payload = String>) -> Result<()> {
        progress.reset(10);
        for i in 0..10 {
            progress.set_payload(format!("Progress: {i}"));
            progress.tick()?;
        }

        Ok(())
    }

    fn func_with_progress_par<ProgressCb: AsyncProgressNotification>(progress: ProgressCb) -> Result<()> {
        progress.reset(10);

        let thread_progress = progress.clone();

        let handle = std::thread::spawn(move || {
            for _ in 0..5 {
                thread_progress.tick().unwrap();
            }
        });

        for _ in 0..5 {
            progress.tick()?;
        }

        handle.join().unwrap();

        Ok(())
    }

    #[test]
    fn test_progress() {
        func_with_progress(CallbackProgress::<(), _>::with_cb(|progress, _| {
            println!("Progress: {:.0}%", progress * 100.0);
            ComputationStatus::Continue
        }))
        .unwrap();
    }

    #[test]
    fn test_progress_with_payload() {
        func_with_payload_progres(CallbackProgress::<String, _>::with_cb(|progress, payload| {
            println!("Progress: {:.0}% (Payload = {})", progress * 100.0, payload);
            ComputationStatus::Continue
        }))
        .unwrap();
    }

    #[test]
    fn test_dummy_progress() {
        func_with_progress(DummyProgress).unwrap();
    }

    #[test]
    fn test_progress_cancellation() {
        let mut count = 0;
        let res = func_with_progress(CallbackProgress::<(), _>::with_cb(|progress, _| {
            println!("Progress: {:.0}%", progress * 100.0);
            count += 1;

            if count == 5 {
                ComputationStatus::CancelRequested
            } else {
                ComputationStatus::Continue
            }
        }));

        match res {
            Err(Error::Cancelled) => {}
            _ => panic!("Expected cancellation error"),
        }
    }

    #[test]
    fn test_progress_par() {
        func_with_progress_par(CallbackProgress::<(), _>::with_cb(|progress, _| {
            println!("Progress: {:.0}%", progress * 100.0);
            ComputationStatus::Continue
        }))
        .unwrap();
    }
}

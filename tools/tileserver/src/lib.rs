mod apperror;
pub mod difftileapihandler;
pub mod tileapihandler;
#[cfg(feature = "tui")]
pub mod tui;

pub use apperror::AppError;

type Error = tiler::Error;
type Result<T> = tiler::Result<T>;

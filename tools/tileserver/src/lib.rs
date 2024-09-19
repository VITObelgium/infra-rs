pub mod tileapihandler;
#[cfg(feature = "tui")]
pub mod tui;

type Error = tiler::Error;
type Result<T> = tiler::Result<T>;

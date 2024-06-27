#![warn(clippy::unwrap_used)]

pub type Error = inf::Error;
pub type Result<T = ()> = inf::Result<T>;

pub mod io;

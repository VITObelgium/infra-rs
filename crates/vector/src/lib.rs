#![warn(clippy::unwrap_used)]

pub type Error = inf::Error;
pub type Result<T = ()> = inf::Result<T>;

mod datarow;
pub mod fieldtype;
pub mod io;

pub use datarow::DataRow;

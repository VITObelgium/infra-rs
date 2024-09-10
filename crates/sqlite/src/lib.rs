//! This module contains a lightweight sqlite wrapper on top of the libsqlite3-sys crate.
//! This is only intended for very simple queries or sql file execution or obtaining blobs with minimal overhead.
//! For more serious database work, use rusqlite or sqlx or an orm

mod connection;
mod row;
mod statement;

#[derive(Debug, Copy, Clone)]
pub enum AccessMode {
    ReadOnly,
    ReadWrite,
    Create,
}

pub use connection::Connection;
pub use row::Row;
pub use statement::Statement;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Runtime error: {0}")]
    Runtime(String),
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("Invalid string: {0}")]
    InvalidString(#[from] std::ffi::NulError),
}

pub type Result<T> = std::result::Result<T, Error>;

//! This module contains a lightweight sqlite wrapper on top of the sqlite3-sys crate
//! This is only intended for very simple queries or sql file execution
//! For serious database work, use rusqlite or sqlx

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

pub type Error = inf::Error;
pub type Result<T> = inf::Result<T>;

//! COG Analyzer - A TUI tool for analyzing Cloud Optimized GeoTIFF files.

pub mod app;
pub mod data;
pub mod event;
pub mod handler;
pub mod tabs;
pub mod ui;

pub type Result<T = ()> = anyhow::Result<T>;

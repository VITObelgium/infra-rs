//! Tab modules for the COG Analyzer TUI.
//!
//! Each tab represents a different view of the COG file:
//! - `overview`: General metadata and file information
//! - `chunks`: Raw chunk data browser with visualization
//! - `webtiles`: Web tile browser with visualization

pub mod chunks;
pub mod overview;
pub mod webtiles;

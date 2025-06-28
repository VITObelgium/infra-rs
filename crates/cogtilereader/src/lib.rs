#![cfg_attr(feature = "simd", feature(portable_simd))]

use thiserror::Error;

pub mod cog;
mod io;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Runtime error: {0}")]
    Runtime(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("{0}")]
    GeoError(#[from] geo::Error),
    #[error("Tiff error: {0}")]
    TiffError(#[from] tiff::TiffError),
}

pub type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Default)]
pub struct CogStats {
    pub minimum_value: f64,
    pub maximum_value: f64,
    pub mean: f64,
    pub standard_deviation: f64,
    pub valid_pixel_percentage: f64,
    pub max_zoom: i32,
}

#[cfg(feature = "raster_stats")]
mod stats;

#[cfg(test)]
pub mod testutils;

#![cfg_attr(feature = "simd", feature(portable_simd))]

use thiserror::Error;

mod cog;
pub mod io;
mod utils;

pub use cog::CogAccessor;
pub use cog::CogMetadata;
pub use cog::CogTileLocation;
pub use cog::Compression;
pub use cog::Predictor;
pub use cog::TileOffsets;
pub use utils::HorizontalUnpredictable;

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
#[cfg_attr(
    target_arch = "wasm32",
    derive(tsify::Tsify, serde::Serialize, serde::Deserialize),
    tsify(from_wasm_abi, into_wasm_abi)
)]
pub struct CogStats {
    pub minimum_value: f64,
    pub maximum_value: f64,
    pub mean: f64,
    pub standard_deviation: f64,
    pub valid_pixel_percentage: f64,
    #[cfg_attr(target_arch = "wasm32", serde(skip))]
    pub max_zoom: Option<i32>,
}

#[cfg(feature = "raster_stats")]
mod stats;

#[cfg(test)]
pub mod testutils;

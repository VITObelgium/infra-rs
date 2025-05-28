#![warn(clippy::unwrap_used)]
#![cfg_attr(feature = "simd", feature(portable_simd))]
extern crate approx;

pub use error::Error;
pub type Result<T = ()> = std::result::Result<T, Error>;

#[cfg(feature = "serde")]
mod bigarray;
pub mod cast;
pub mod color;
pub mod colormap;
mod colormapper;
pub mod duration;
mod error;
pub mod fs;
pub mod interpolate;
pub mod legend;
pub mod legendscaletype;
pub mod progressinfo;
#[cfg(feature = "simd")]
pub mod simd;

#[doc(inline)]
pub use color::Color;

#[doc(inline)]
pub use legend::Legend;
#[doc(inline)]
pub use legend::MappedLegend;

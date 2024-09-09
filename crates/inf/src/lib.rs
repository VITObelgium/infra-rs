#![warn(clippy::unwrap_used)]
extern crate approx;

pub use error::Error;
pub type Result<T = ()> = std::result::Result<T, Error>;

pub mod cast;
pub mod color;
pub mod colormap;
pub mod duration;
mod error;
pub mod fs;
pub mod interpolate;
pub mod legend;
pub mod legendscaletype;
pub mod progressinfo;
pub mod rect;

#[doc(inline)]
pub use color::Color;

#[doc(inline)]
pub use legend::Legend;
#[doc(inline)]
pub use legend::MappedLegend;
#[doc(inline)]
pub use rect::Rect;

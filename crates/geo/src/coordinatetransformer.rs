#[cfg(feature = "proj")]
mod proj;
#[cfg(feature = "proj")]
pub use proj::CoordinateTransformer;

#[cfg(feature = "proj4rs")]
mod proj4rs;
#[cfg(all(feature = "proj4rs", not(feature = "proj")))]
// proj takes precedence over proj4rs if both are enabled
pub use proj4rs::CoordinateTransformer;

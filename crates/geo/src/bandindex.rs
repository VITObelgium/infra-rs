//! Band index utilities.

/// 1-based raster band index.
///
/// The band api's use a 1-based index for bands. We use `NonZeroUsize` to
/// make it impossible to represent band index 0.
pub type BandIndex = std::num::NonZeroUsize;

/// Convenience constant for the first band (band 1).
pub const FIRST_BAND: BandIndex = std::num::NonZeroUsize::new(1).unwrap();

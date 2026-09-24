//! Band index utilities.

/// 1-based raster band index.
///
/// The band APIs use a 1-based index for bands. `NonZeroUsize` makes zero unrepresentable.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct BandIndex(std::num::NonZeroUsize);

/// Convenience constant for the first band (band 1).
pub const FIRST_BAND: BandIndex = BandIndex(std::num::NonZeroUsize::new(1).unwrap());

impl BandIndex {
    pub fn new(index: usize) -> Option<Self> {
        std::num::NonZeroUsize::new(index).map(BandIndex)
    }

    pub fn get(self) -> usize {
        self.0.get()
    }
}

impl TryFrom<u32> for BandIndex {
    type Error = &'static str;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if value == 0 {
            Err("Band index must be greater than 0")
        } else {
            Ok(BandIndex(std::num::NonZeroUsize::new(value as usize).unwrap()))
        }
    }
}

impl TryFrom<i32> for BandIndex {
    type Error = &'static str;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value <= 0 {
            Err("Band index must be greater than 0")
        } else {
            Ok(BandIndex(std::num::NonZeroUsize::new(value as usize).unwrap()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BandIndex, FIRST_BAND};

    #[test]
    fn band_indices_are_one_based() {
        assert_eq!(BandIndex::new(0), None);
        assert_eq!(BandIndex::try_from(0_i32), Err("Band index must be greater than 0"));
        assert_eq!(BandIndex::try_from(-1), Err("Band index must be greater than 0"));
        assert_eq!(BandIndex::new(1), Some(FIRST_BAND));
        assert_eq!(BandIndex::try_from(0_u32), Err("Band index must be greater than 0"));
        assert_eq!(BandIndex::try_from(2_u32).unwrap().get(), 2);
        assert_eq!(BandIndex::try_from(2_i32).unwrap().get(), 2);
        assert_eq!(BandIndex::new(usize::MAX).unwrap().get(), usize::MAX);
    }
}

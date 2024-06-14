use num::{Bounded, ToPrimitive};

use crate::{Error, GeoMetadata, Nodata};

pub trait RasterNum<T: ToPrimitive>: Copy + num::NumCast + num::Zero + PartialEq + Bounded + Nodata<T> {}

/// A trait representing a raster.
/// A raster implementation provides access to the pixel data and the geographic metadata associated with the raster.
pub trait Raster<T: RasterNum<T>> {
    /// Create a new raster with the given metadata and data buffer.
    fn new(metadata: GeoMetadata, data: Vec<T>) -> Self;

    /// Create a new raster with the given metadata and filled with zeros.
    fn zeros(metadata: GeoMetadata) -> Self;

    /// Create a new raster with the given metadata and filled with zeros.
    fn filled_with(val: T, metadata: GeoMetadata) -> Self;

    /// Returns a reference to the geographic metadata associated with the raster.
    fn geo_metadata(&self) -> &GeoMetadata;

    /// Returns the width of the raster.
    fn width(&self) -> usize;

    /// Returns the height of the raster.
    fn height(&self) -> usize;

    /// Returns a mutable reference to the raster data.
    fn as_mut_slice(&mut self) -> &mut [T];

    /// Returns a reference to the raster data.
    fn as_slice(&self) -> &[T];

    /// Returns the optional nodata value that is used in the raster to identify missing data.
    fn nodata_value(&self) -> Option<T>;

    fn is_nodata(&self, value: T) -> bool {
        self.nodata_value().map_or(false, |nodata| value == nodata)
    }
}

impl RasterNum<i8> for i8 {}
impl RasterNum<u8> for u8 {}
impl RasterNum<i16> for i16 {}
impl RasterNum<u16> for u16 {}
impl RasterNum<i32> for i32 {}
impl RasterNum<u32> for u32 {}
impl RasterNum<i64> for i64 {}
impl RasterNum<u64> for u64 {}
impl RasterNum<f32> for f32 {}
impl RasterNum<f64> for f64 {}

pub fn check_dimensions<R1, R2, T1, T2>(r1: &R1, r2: &R2) -> Result<(), Error>
where
    R1: Raster<T1>,
    R2: Raster<T2>,
    T1: RasterNum<T1>,
    T2: RasterNum<T2>,
{
    if r1.width() != r2.width() || r1.height() != r2.height() {
        return Err(Error::SizeMismatch {
            size1: (r1.width(), r1.height()),
            size2: (r2.width(), r2.height()),
        });
    }

    Ok(())
}

pub fn assert_dimensions<R1, R2, T1, T2>(r1: &R1, r2: &R2)
where
    R1: Raster<T1>,
    R2: Raster<T2>,
    T1: RasterNum<T1>,
    T2: RasterNum<T2>,
{
    assert_eq!(r1.width(), r2.width(), "Raster widths do not match");
    assert_eq!(r1.height(), r2.height(), "Raster heights do not match");
}

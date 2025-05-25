use num::ToPrimitive;

/// Trait for types that can represent a no-data value in raster data.
/// Typically used for pixel values in rasters, where a specific value indicates that the pixel does not contain valid data.
/// Floating point types may use NaN as the no-data value, while integer types typically use their maximum value for unsigned types
/// and minimum value for signed types.
pub trait Nodata: ToPrimitive + PartialEq + Sized + Copy {
    const NODATA: Self;

    #[inline]
    fn is_nodata(self) -> bool {
        self == Self::NODATA
    }

    fn has_nan() -> bool;
    fn is_nan(self) -> bool;
}

impl Nodata for u8 {
    const NODATA: u8 = u8::MAX;

    fn has_nan() -> bool {
        false
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for u16 {
    const NODATA: u16 = u16::MAX;

    fn has_nan() -> bool {
        false
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for u32 {
    const NODATA: u32 = u32::MAX;

    fn has_nan() -> bool {
        false
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for u64 {
    const NODATA: u64 = u64::MAX;

    fn has_nan() -> bool {
        false
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for i8 {
    const NODATA: i8 = i8::MIN;

    fn has_nan() -> bool {
        false
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for i16 {
    const NODATA: i16 = i16::MIN;

    fn has_nan() -> bool {
        false
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for i32 {
    const NODATA: i32 = i32::MIN;

    fn has_nan() -> bool {
        false
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for i64 {
    const NODATA: i64 = i64::MIN;

    fn has_nan() -> bool {
        false
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for f32 {
    const NODATA: f32 = f32::NAN;

    fn is_nodata(self) -> bool {
        self.is_nan()
    }

    fn has_nan() -> bool {
        true
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for f64 {
    const NODATA: f64 = f64::NAN;

    fn is_nodata(self) -> bool {
        self.is_nan()
    }

    fn has_nan() -> bool {
        true
    }

    fn is_nan(self) -> bool {
        self.is_nan()
    }
}

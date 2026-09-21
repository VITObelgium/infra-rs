use num::ToPrimitive;

/// Trait for types that can represent a no-data value in raster data.
/// Typically used for pixel values in rasters, where a specific value indicates that the pixel does not contain valid data.
/// Floating point types may use NaN as the no-data value, while integer types typically use their maximum value for unsigned types
/// and minimum value for signed types.
pub trait Nodata: ToPrimitive + PartialEq + PartialOrd + Sized + Copy {
    const NODATA: Self;

    #[inline]
    fn is_nodata(self) -> bool {
        self == Self::NODATA
    }

    #[inline]
    fn is_nodata_cb(val: Self) -> bool {
        val == Self::NODATA
    }

    /// For importing foreign data that may contain nodata values not adhering to the predefined `Self::NODATA` value.
    #[inline]
    fn init_nodata(&mut self, nodata: Self) {
        if *self == nodata {
            *self = Self::NODATA;
        }
    }

    /// For exporting the data to a format where the nodata value does not match the predefined `Self::NODATA` value.
    #[inline]
    fn restore_nodata(&mut self, nodata: Self) {
        if self.is_nodata() {
            *self = nodata;
        }
    }

    #[inline]
    fn nodata_min(self, other: Self) -> Self {
        if other.is_nodata() || self < other { self } else { other }
    }

    #[inline]
    fn nodata_max(self, other: Self) -> Self {
        if other.is_nodata() || self > other { self } else { other }
    }

    fn has_nan() -> bool;
    fn is_nan(self) -> bool;
}

macro_rules! impl_nodata_fixed_point {
    ( $t:ident, $nodata:ident ) => {
        impl Nodata for $t {
            const NODATA: $t = $t::$nodata;

            fn has_nan() -> bool {
                false
            }

            fn is_nan(self) -> bool {
                false
            }
        }
    };
}

macro_rules! impl_nodata_floating_point {
    ( $t:ident ) => {
        impl Nodata for $t {
            const NODATA: $t = $t::NAN;

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
    };
}

impl_nodata_fixed_point!(u8, MAX);
impl_nodata_fixed_point!(u16, MAX);
impl_nodata_fixed_point!(u32, MAX);
impl_nodata_fixed_point!(u64, MAX);
impl_nodata_fixed_point!(i8, MIN);
impl_nodata_fixed_point!(i16, MIN);
impl_nodata_fixed_point!(i32, MIN);
impl_nodata_fixed_point!(i64, MIN);

impl_nodata_floating_point!(f32);
impl_nodata_floating_point!(f64);

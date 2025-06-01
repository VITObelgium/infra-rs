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

    #[inline]
    fn is_nodata_cb(val: Self) -> bool {
        val == Self::NODATA
    }

    /// For importing foreign data that may contain nodata values not adhereing to the predefined `Self::NODATA` value.
    fn init_nodata(&mut self, nodata: Self) {
        if *self == nodata {
            *self = Self::NODATA;
        }
    }

    /// For exporting the data to a format where the nodata value does not match the predefined `Self::NODATA` value.
    fn restore_nodata(&mut self, nodata: Self) {
        if self.is_nodata() {
            *self = nodata;
        }
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

#[cfg(feature = "simd")]
pub mod simd {
    use super::*;
    use std::simd::{LaneCount, SupportedLaneCount, prelude::*};

    const LANES: usize = inf::simd::LANES;

    pub trait NodataSimd: std::simd::cmp::SimdPartialEq {
        type Scalar;
        type Simd;
        const NODATA_SIMD: Self;

        /// For importing foreign data that may contain nodata values not adhereing to the predefined `Self::NODATA` value.
        fn init_nodata(&mut self, nodata: Self);
        /// For exporting the data to a format where the nodata value does not match the predefined `Self::NODATA` value.
        fn restore_nodata(&mut self, nodata: Self);
    }

    macro_rules! impl_nodata_simd {
        ( $t:ident ) => {
            impl NodataSimd for Simd<$t, LANES>
            where
                LaneCount<LANES>: SupportedLaneCount,
            {
                type Scalar = $t;
                type Simd = std::simd::Simd<$t, LANES>;
                const NODATA_SIMD: Self = Simd::splat($t::NODATA);

                fn init_nodata(&mut self, nodata: Self) {
                    use SimdPartialEq as _;

                    let nodata_mask = self.simd_eq(nodata);
                    *self = nodata_mask.select(Self::NODATA_SIMD, *self);
                }

                fn restore_nodata(&mut self, nodata: Self) {
                    use std::simd::cmp::SimdPartialEq as _;

                    let nodata_mask = self.simd_eq(Self::NODATA_SIMD);
                    *self = nodata_mask.select(nodata, *self);
                }
            }
        };
    }

    macro_rules! impl_nodata_simd_fp {
        ( $t:ident ) => {
            impl NodataSimd for Simd<$t, LANES>
            where
                LaneCount<LANES>: SupportedLaneCount,
            {
                type Scalar = $t;
                type Simd = std::simd::Simd<$t, LANES>;
                const NODATA_SIMD: Self = Simd::splat($t::NODATA);

                fn init_nodata(&mut self, nodata: Self) {
                    use SimdPartialEq as _;

                    let nodata_mask = self.simd_eq(nodata);
                    *self = nodata_mask.select(Self::NODATA_SIMD, *self);
                }

                fn restore_nodata(&mut self, nodata: Self) {
                    let nodata_mask = self.is_nan();
                    *self = nodata_mask.select(nodata, *self);
                }
            }
        };
    }

    impl_nodata_simd!(u8);
    impl_nodata_simd!(i8);
    impl_nodata_simd!(u16);
    impl_nodata_simd!(i16);
    impl_nodata_simd!(u32);
    impl_nodata_simd!(i32);
    impl_nodata_simd!(u64);
    impl_nodata_simd!(i64);
    impl_nodata_simd_fp!(f32);
    impl_nodata_simd_fp!(f64);
}

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

    /// For importing foreign data that may contain nodata values not adhereing to the predefined `Self::NODATA` value.
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
    fn nodata_min(&self, other: Self) -> Self {
        if other.is_nodata() || *self < other { *self } else { other }
    }

    #[inline]
    fn nodata_max(&self, other: Self) -> Self {
        if other.is_nodata() || *self > other { *self } else { other }
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

    pub trait NodataSimd: std::simd::cmp::SimdPartialEq + Sized {
        type Scalar: std::simd::SimdElement;
        type NodataMask;
        const NODATA_SIMD: Self;

        /// For importing foreign data that may contain nodata values not adhereing to the predefined `Self::NODATA` value.
        fn init_nodata(&mut self, nodata: Self);
        /// For exporting the data to a format where the nodata value does not match the predefined `Self::NODATA` value.
        fn restore_nodata(&mut self, nodata: Self);

        fn nodata_mask(&self) -> Self::NodataMask;
        fn nodata_min(&self, other: Self) -> Self;
        fn nodata_max(&self, other: Self) -> Self;
        fn reduce_min_without_nodata_check(&self) -> Self::Scalar;
        fn reduce_max_without_nodata_check(&self) -> Self::Scalar;
    }

    macro_rules! impl_nodata_simd {
        ( $t:ident ) => {
            impl NodataSimd for Simd<$t, LANES>
            where
                LaneCount<LANES>: SupportedLaneCount,
            {
                type Scalar = $t;
                type NodataMask = std::simd::Mask<<$t as std::simd::SimdElement>::Mask, LANES>;
                const NODATA_SIMD: Self = Simd::splat($t::NODATA);

                #[inline]
                fn init_nodata(&mut self, nodata: Self) {
                    use SimdPartialEq as _;

                    *self = self.simd_eq(nodata).select(Self::NODATA_SIMD, *self);
                }

                #[inline]
                fn restore_nodata(&mut self, nodata: Self) {
                    use SimdPartialEq as _;

                    let nodata_mask = self.simd_eq(Self::NODATA_SIMD);
                    *self = nodata_mask.select(nodata, *self);
                }

                #[inline]
                fn nodata_mask(&self) -> Self::Mask {
                    self.simd_eq(Self::NODATA_SIMD)
                }

                #[inline]
                fn nodata_min(&self, other: Self) -> Self
                where
                    Self: SimdOrd,
                {
                    let mut res = self.simd_min(other);
                    res = self.nodata_mask().select(other, res);
                    other.nodata_mask().select(*self, res)
                }

                #[inline]
                fn nodata_max(&self, other: Self) -> Self
                where
                    Self: SimdOrd,
                {
                    let mut res = self.simd_max(other);
                    // In fields that are nodata, take the other value
                    res = self.nodata_mask().select(other, res);
                    other.nodata_mask().select(*self, res)
                }

                #[inline]
                fn reduce_min_without_nodata_check(&self) -> Self::Scalar {
                    self.reduce_min()
                }

                #[inline]
                fn reduce_max_without_nodata_check(&self) -> Self::Scalar {
                    self.reduce_max()
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
                type NodataMask = std::simd::Mask<<$t as std::simd::SimdElement>::Mask, LANES>;
                const NODATA_SIMD: Self = Simd::splat($t::NODATA);

                #[inline]
                fn init_nodata(&mut self, nodata: Self) {
                    use SimdPartialEq as _;

                    *self = self.simd_eq(nodata).select(Self::NODATA_SIMD, *self);
                }

                #[inline]
                fn restore_nodata(&mut self, nodata: Self) {
                    *self = self.is_nan().select(nodata, *self);
                }

                #[inline]
                fn nodata_mask(&self) -> Self::Mask {
                    self.is_nan()
                }

                #[inline]
                fn nodata_min(&self, other: Self) -> Self
                where
                    Self: SimdFloat,
                {
                    // SimdFloat::simd_min already handles NaN correctly,
                    // so we can use it directly without using a nodata mask.
                    self.simd_min(other)
                }

                #[inline]
                fn nodata_max(&self, other: Self) -> Self
                where
                    Self: SimdFloat,
                {
                    // SimdFloat::simd_max already handles NaN correctly,
                    // so we can use it directly without using a nodata mask.
                    self.simd_max(other)
                }

                #[inline]
                fn reduce_min_without_nodata_check(&self) -> Self::Scalar {
                    self.reduce_min()
                }

                #[inline]
                fn reduce_max_without_nodata_check(&self) -> Self::Scalar {
                    self.reduce_max()
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

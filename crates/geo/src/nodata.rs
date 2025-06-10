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

#[cfg(feature = "simd")]
pub mod simd {

    use super::*;
    use std::simd::num::SimdInt;
    use std::simd::num::SimdUint;
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

        fn nodata_mask(self) -> Self::NodataMask;

        /// Returns the minimum value of each matching simd lane. Should only be used when you are sure that there are no nodata values in the data.
        fn min_unchecked(&self, other: Self) -> Self;
        /// Returns the maximum value of each matching simd lane. Should only be used when you are sure that there are no nodata values in the data.
        fn max_unchecked(&self, other: Self) -> Self;

        /// Returns the minimum value of each matching simd lane.
        /// If a lane is nodata, the other value is used instead.
        /// If both lanes are nodata, the result is nodata
        fn nodata_min(&self, other: Self) -> Self;
        /// Returns the maximum value of each matching simd lane.
        /// If a lane is nodata, the other value is used instead.
        /// If both lanes are nodata, the result is nodata
        fn nodata_max(&self, other: Self) -> Self;

        /// reduces to the minimum value, ignoring nodata values, returns `None` if all values are nodata
        fn reduce_min(self) -> Option<Self::Scalar>;
        /// reduces to the maximum value, ignoring nodata values, returns `None` if all values are nodata
        fn reduce_max(self) -> Option<Self::Scalar>;
        /// reduces to the minimum value, without checking for nodata values. Should only be used when you are sure that there are no nodata values in the data.
        fn reduce_min_unchecked(self) -> Self::Scalar;
        /// reduces to the maximum value, without checking for nodata values. Should only be used when you are sure that there are no nodata values in the data.
        fn reduce_max_unchecked(self) -> Self::Scalar;
    }

    macro_rules! impl_nodata_simd {
        ( $t:ident, $numtrait:ident ) => {
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
                fn nodata_mask(self) -> Self::Mask {
                    self.simd_eq(Self::NODATA_SIMD)
                }

                #[inline]
                fn min_unchecked(&self, other: Self) -> Self
                where
                    Self: SimdOrd,
                {
                    self.simd_min(other)
                }

                #[inline]
                fn max_unchecked(&self, other: Self) -> Self
                where
                    Self: SimdOrd,
                {
                    self.simd_max(other)
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
                fn reduce_min(self) -> Option<Self::Scalar> {
                    let nodata = self.nodata_mask();
                    if nodata.all() {
                        return None;
                    }

                    Some($numtrait::reduce_min(nodata.select(Simd::splat(Self::Scalar::MAX), self)))
                }

                #[inline]
                fn reduce_max(self) -> Option<Self::Scalar> {
                    let nodata = self.nodata_mask();
                    if nodata.all() {
                        return None;
                    }

                    Some($numtrait::reduce_max(nodata.select(Simd::splat(Self::Scalar::MIN), self)))
                }

                #[inline]
                fn reduce_min_unchecked(self) -> Self::Scalar {
                    $numtrait::reduce_min(self)
                }

                #[inline]
                fn reduce_max_unchecked(self) -> Self::Scalar {
                    $numtrait::reduce_max(self)
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
                fn nodata_mask(self) -> Self::Mask {
                    self.is_nan()
                }

                #[inline]
                fn min_unchecked(&self, other: Self) -> Self
                where
                    Self: SimdFloat,
                {
                    self.simd_min(other)
                }

                #[inline]
                fn max_unchecked(&self, other: Self) -> Self
                where
                    Self: SimdFloat,
                {
                    self.simd_max(other)
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
                fn nodata_max(&self, other: Self) -> Self {
                    // SimdFloat::simd_max already handles NaN correctly,
                    // so we can use it directly without using a nodata mask.
                    SimdFloat::simd_max(*self, other)
                }

                #[inline]
                fn reduce_min(self) -> Option<Self::Scalar> {
                    let reduced = SimdFloat::reduce_min(self);
                    if reduced.is_nan() {
                        return None;
                    }

                    Some(reduced)
                }

                #[inline]
                fn reduce_max(self) -> Option<Self::Scalar> {
                    let reduced = SimdFloat::reduce_max(self);
                    if reduced.is_nan() {
                        return None;
                    }

                    Some(reduced)
                }

                #[inline]
                fn reduce_min_unchecked(self) -> Self::Scalar {
                    SimdFloat::reduce_min(self)
                }

                #[inline]
                fn reduce_max_unchecked(self) -> Self::Scalar {
                    SimdFloat::reduce_max(self)
                }
            }
        };
    }

    impl_nodata_simd!(u8, SimdUint);
    impl_nodata_simd!(i8, SimdInt);
    impl_nodata_simd!(u16, SimdUint);
    impl_nodata_simd!(i16, SimdInt);
    impl_nodata_simd!(u32, SimdUint);
    impl_nodata_simd!(i32, SimdInt);
    impl_nodata_simd!(u64, SimdUint);
    impl_nodata_simd!(i64, SimdInt);
    impl_nodata_simd_fp!(f32);
    impl_nodata_simd_fp!(f64);

    #[cfg(test)]
    mod tests {

        use super::*;

        #[test]
        fn test_reduce_min_max_unsigned_integers_all_nodata() {
            let val = Simd::from_array([u8::NODATA, u8::NODATA, u8::NODATA, u8::NODATA]);

            assert_eq!(simd::NodataSimd::reduce_min(val), None);
            assert_eq!(simd::NodataSimd::reduce_max(val), None);
        }

        #[test]
        fn test_reduce_min_max_unsigned_integers() {
            let val = Simd::from_array([u8::NODATA, 5, 2, 8]);

            assert_eq!(simd::NodataSimd::reduce_min(val), Some(2));
            assert_eq!(simd::NodataSimd::reduce_max(val), Some(8));
        }

        #[test]
        fn test_reduce_min_max_signed_integers_all_nodata() {
            let val = Simd::from_array([i32::NODATA, i32::NODATA, i32::NODATA, i32::NODATA]);

            assert_eq!(simd::NodataSimd::reduce_min(val), None);
            assert_eq!(simd::NodataSimd::reduce_max(val), None);
        }

        #[test]
        fn test_reduce_min_max_signed_integers() {
            let val = Simd::from_array([0, -5, i8::NODATA, 8]);

            assert_eq!(simd::NodataSimd::reduce_min(val), Some(-5));
            assert_eq!(simd::NodataSimd::reduce_max(val), Some(8));
        }

        #[test]
        fn test_reduce_min_max_signed_floating_point_all_nodata() {
            let val = Simd::from_array([f64::NODATA, f64::NODATA, f64::NODATA, f64::NODATA]);

            assert_eq!(simd::NodataSimd::reduce_min(val), None);
            assert_eq!(simd::NodataSimd::reduce_max(val), None);
        }

        #[test]
        fn test_reduce_min_max_signed_floating_point() {
            let val = Simd::from_array([0.0, -5.0, f32::NODATA, 8.0]);

            assert_eq!(simd::NodataSimd::reduce_min(val), Some(-5.0));
            assert_eq!(simd::NodataSimd::reduce_max(val), Some(8.0));
        }

        #[test]
        fn test_nodata_min_max_unsigned_integers() {
            let val1 = Simd::from_array([u8::NODATA, u8::NODATA, 3, 2]);
            let val2 = Simd::from_array([u8::NODATA, 0, u8::NODATA, 9]);

            assert_eq!(val1.nodata_min(val2), Simd::from_array([u8::NODATA, 0, 3, 2]));
            assert_eq!(val1.nodata_max(val2), Simd::from_array([u8::NODATA, 0, 3, 9]));
        }

        #[test]
        fn test_nodata_min_max_floating_point() {
            let val1 = Simd::from_array([f32::NODATA, f32::NODATA, 3.0, 2.0]);
            let val2 = Simd::from_array([f32::NODATA, 0.0, f32::NODATA, 9.0]);

            assert!(val1.nodata_min(val2)[0].is_nan());
            assert!(val1.nodata_max(val2)[0].is_nan());

            assert_eq!(val1.nodata_min(val2).to_array()[1..], [0.0, 3.0, 2.0]);
            assert_eq!(val1.nodata_max(val2).to_array()[1..], [0.0, 3.0, 9.0]);
        }
    }
}

use crate::{ArrayDataType, Nodata};

pub trait ArrayNumScalar:
    Copy
    + Nodata
    + num::Num
    + num::NumCast
    + num::Bounded
    + num::traits::NumAssignOps
    + std::cmp::PartialOrd
    + std::fmt::Debug
    + std::string::ToString
    + approx::AbsDiffEq<Epsilon = Self>
{
    const TYPE: ArrayDataType;
    const IS_SIGNED: bool;

    fn add_nodata_aware(self, other: Self) -> Self;
    fn sub_nodata_aware(self, other: Self) -> Self;
    fn mul_nodata_aware(self, other: Self) -> Self;
    fn div_nodata_aware(self, other: Self) -> Self;

    fn add_inclusive_nodata_aware(self, other: Self) -> Self;
    fn sub_inclusive_nodata_aware(self, other: Self) -> Self;

    fn div_nodata_aware_opt(self, other: Self) -> Option<Self>;

    #[inline]
    fn add_assign_nodata_aware(&mut self, other: Self) {
        *self = self.add_nodata_aware(other);
    }

    #[inline]
    fn add_assign_inclusive_nodata_aware(&mut self, other: Self) {
        *self = self.add_inclusive_nodata_aware(other);
    }

    #[inline]
    fn sub_assign_nodata_aware(&mut self, other: Self) {
        *self = self.sub_nodata_aware(other);
    }

    #[inline]
    fn sub_assign_inclusive_nodata_aware(&mut self, other: Self) {
        *self = self.sub_inclusive_nodata_aware(other);
    }

    #[inline]
    fn mul_assign_nodata_aware(&mut self, other: Self) {
        *self = self.mul_nodata_aware(other);
    }

    #[inline]
    fn div_assign_nodata_aware(&mut self, other: Self) {
        *self = self.div_nodata_aware(other);
    }
}

#[cfg(feature = "simd")]
pub trait ArrayNumSimd: std::simd::SimdElement + std::simd::SimdCast {}

#[cfg(not(feature = "simd"))]
pub trait ArrayNum: ArrayNumScalar {}

#[cfg(feature = "simd")]
pub trait ArrayNum: ArrayNumScalar + ArrayNumSimd {}

macro_rules! add_nodata_impl {
    () => {
        #[inline]
        fn add_nodata_aware(self, other: Self) -> Self {
            if self.is_nodata() || other.is_nodata() {
                Self::NODATA
            } else {
                self.wrapping_add(other)
            }
        }

        #[inline]
        fn add_inclusive_nodata_aware(self, other: Self) -> Self {
            match (self.is_nodata(), other.is_nodata()) {
                (true, true) => Self::NODATA,
                (false, true) => self,
                (true, false) => other,
                (false, false) => self.saturating_add(other),
            }
        }
    };
}

macro_rules! add_fp_nodata_impl {
    () => {
        #[inline]
        fn add_nodata_aware(self, other: Self) -> Self {
            self + other
        }

        #[inline]
        fn add_inclusive_nodata_aware(self, other: Self) -> Self {
            match (self.is_nodata(), other.is_nodata()) {
                (true, true) => Self::NODATA,
                (false, true) => self,
                (true, false) => other,
                (false, false) => self + other,
            }
        }
    };
}

macro_rules! sub_nodata_impl {
    () => {
        #[inline]
        fn sub_nodata_aware(self, other: Self) -> Self {
            if self.is_nodata() || other.is_nodata() {
                Self::NODATA
            } else {
                self.wrapping_sub(other)
            }
        }

        #[inline]
        fn sub_inclusive_nodata_aware(self, other: Self) -> Self {
            match (self.is_nodata(), other.is_nodata()) {
                (true, true) => Self::NODATA,
                (false, true) => self,
                (true, false) => -other,
                (false, false) => self.wrapping_sub(other),
            }
        }
    };
}

macro_rules! sub_nodata_unsigned_impl {
    () => {
        #[inline]
        fn sub_nodata_aware(self, other: Self) -> Self {
            if self.is_nodata() || other.is_nodata() {
                Self::NODATA
            } else {
                self.wrapping_sub(other)
            }
        }

        #[inline]
        fn sub_inclusive_nodata_aware(self, other: Self) -> Self {
            match (self.is_nodata(), other.is_nodata()) {
                (true, true) => Self::NODATA,
                (false, true) => self,
                (true, false) => Self::NODATA,
                (false, false) => self.wrapping_sub(other),
            }
        }
    };
}

macro_rules! sub_fp_nodata_impl {
    () => {
        #[inline]
        fn sub_nodata_aware(self, other: Self) -> Self {
            if self.is_nodata() || other.is_nodata() {
                Self::NODATA
            } else {
                self - other
            }
        }

        #[inline]
        fn sub_inclusive_nodata_aware(self, other: Self) -> Self {
            match (self.is_nodata(), other.is_nodata()) {
                (true, true) => Self::NODATA,
                (false, true) => self,
                (true, false) => -other,
                (false, false) => self - other,
            }
        }
    };
}

macro_rules! mul_nodata_impl {
    () => {
        #[inline]
        fn mul_nodata_aware(self, other: Self) -> Self {
            if self.is_nodata() || other.is_nodata() {
                Self::NODATA
            } else {
                self.wrapping_mul(other)
            }
        }
    };
}

macro_rules! mul_fp_nodata_impl {
    () => {
        #[inline]
        fn mul_nodata_aware(self, other: Self) -> Self {
            self * other
        }
    };
}

macro_rules! div_nodata_impl {
    () => {
        #[inline]
        fn div_nodata_aware(self, other: Self) -> Self {
            if self.is_nodata() || other.is_nodata() || other == 0 {
                Self::NODATA
            } else {
                self / other
            }
        }

        #[inline]
        fn div_nodata_aware_opt(self, other: Self) -> Option<Self> {
            if self.is_nodata() || other.is_nodata() {
                None
            } else {
                self.checked_div(other)
            }
        }
    };
}

macro_rules! div_fp_nodata_impl {
    () => {
        #[inline]
        fn div_nodata_aware(self, other: Self) -> Self {
            if self.is_nodata() || other.is_nodata() || other == 0.0 {
                Self::NODATA
            } else {
                self / other
            }
        }

        #[inline]
        fn div_nodata_aware_opt(self, other: Self) -> Option<Self> {
            if self.is_nodata() || other.is_nodata() || other == 0.0 {
                None
            } else {
                Some(self / other)
            }
        }
    };
}

macro_rules! impl_arraynum_scalar_signed {
    ($t:ty, $raster_type:ident) => {
        impl ArrayNumScalar for $t {
            const TYPE: ArrayDataType = ArrayDataType::$raster_type;
            const IS_SIGNED: bool = true;

            add_nodata_impl!();
            sub_nodata_impl!();
            mul_nodata_impl!();
            div_nodata_impl!();
        }

        impl ArrayNum for $t {}
        #[cfg(feature = "simd")]
        impl ArrayNumSimd for $t {}
    };
}

macro_rules! impl_arraynum_scalar_unsigned {
    ($t:ty, $raster_type:ident) => {
        impl ArrayNumScalar for $t {
            const TYPE: ArrayDataType = ArrayDataType::$raster_type;
            const IS_SIGNED: bool = false;

            add_nodata_impl!();
            sub_nodata_unsigned_impl!();
            mul_nodata_impl!();
            div_nodata_impl!();
        }

        impl ArrayNum for $t {}
        #[cfg(feature = "simd")]
        impl ArrayNumSimd for $t {}
    };
}

macro_rules! impl_arraynum_scalar_fp {
    ($t:ty, $raster_type:ident) => {
        impl ArrayNumScalar for $t {
            const TYPE: ArrayDataType = ArrayDataType::$raster_type;
            const IS_SIGNED: bool = true;

            add_fp_nodata_impl!();
            sub_fp_nodata_impl!();
            mul_fp_nodata_impl!();
            div_fp_nodata_impl!();
        }

        impl ArrayNum for $t {}
        #[cfg(feature = "simd")]
        impl ArrayNumSimd for $t {}
    };
}

impl_arraynum_scalar_signed!(i8, Int8);
impl_arraynum_scalar_signed!(i16, Int16);
impl_arraynum_scalar_signed!(i32, Int32);
impl_arraynum_scalar_signed!(i64, Int64);
impl_arraynum_scalar_unsigned!(u8, Uint8);
impl_arraynum_scalar_unsigned!(u16, Uint16);
impl_arraynum_scalar_unsigned!(u32, Uint32);
impl_arraynum_scalar_unsigned!(u64, Uint64);
impl_arraynum_scalar_fp!(f32, Float32);
impl_arraynum_scalar_fp!(f64, Float64);

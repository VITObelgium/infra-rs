use crate::{ArrayDataType, Nodata};

// Type requirements for data in rasters
pub trait ArrayNum:
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

macro_rules! add_nodata_impl {
    () => {
        #[inline]
        fn add_nodata_aware(self, other: Self) -> Self {
            if self.is_nodata() || other.is_nodata() {
                Self::nodata_value()
            } else {
                self.wrapping_add(other)
            }
        }

        #[inline]
        fn add_inclusive_nodata_aware(self, other: Self) -> Self {
            match (self.is_nodata(), other.is_nodata()) {
                (true, true) => Self::nodata_value(),
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
                (true, true) => Self::nodata_value(),
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
                Self::nodata_value()
            } else {
                self.wrapping_sub(other)
            }
        }

        #[inline]
        fn sub_inclusive_nodata_aware(self, other: Self) -> Self {
            match (self.is_nodata(), other.is_nodata()) {
                (true, true) => Self::nodata_value(),
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
                Self::nodata_value()
            } else {
                self.wrapping_sub(other)
            }
        }

        #[inline]
        fn sub_inclusive_nodata_aware(self, other: Self) -> Self {
            match (self.is_nodata(), other.is_nodata()) {
                (true, true) => Self::nodata_value(),
                (false, true) => self,
                (true, false) => Self::nodata_value(),
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
                Self::nodata_value()
            } else {
                self - other
            }
        }

        #[inline]
        fn sub_inclusive_nodata_aware(self, other: Self) -> Self {
            match (self.is_nodata(), other.is_nodata()) {
                (true, true) => Self::nodata_value(),
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
                Self::nodata_value()
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
                Self::nodata_value()
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
                Self::nodata_value()
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

macro_rules! arraynum_signed_impl {
    ($t:ty, $raster_type:ident) => {
        impl ArrayNum for $t {
            const TYPE: ArrayDataType = ArrayDataType::$raster_type;
            const IS_SIGNED: bool = true;

            add_nodata_impl!();
            sub_nodata_impl!();
            mul_nodata_impl!();
            div_nodata_impl!();
        }
    };
}

macro_rules! arraynum_unsigned_impl {
    ($t:ty, $raster_type:ident) => {
        impl ArrayNum for $t {
            const TYPE: ArrayDataType = ArrayDataType::$raster_type;
            const IS_SIGNED: bool = false;

            add_nodata_impl!();
            sub_nodata_unsigned_impl!();
            mul_nodata_impl!();
            div_nodata_impl!();
        }
    };
}

macro_rules! arraynum_fp_impl {
    ($t:ty, $raster_type:ident) => {
        impl ArrayNum for $t {
            const TYPE: ArrayDataType = ArrayDataType::$raster_type;
            const IS_SIGNED: bool = true;

            add_fp_nodata_impl!();
            sub_fp_nodata_impl!();
            mul_fp_nodata_impl!();
            div_fp_nodata_impl!();
        }
    };
}

arraynum_signed_impl!(i8, Int8);
arraynum_signed_impl!(i16, Int16);
arraynum_signed_impl!(i32, Int32);
arraynum_signed_impl!(i64, Int64);
arraynum_unsigned_impl!(u8, Uint8);
arraynum_unsigned_impl!(u16, Uint16);
arraynum_unsigned_impl!(u32, Uint32);
arraynum_unsigned_impl!(u64, Uint64);

arraynum_fp_impl!(f32, Float32);
arraynum_fp_impl!(f64, Float64);

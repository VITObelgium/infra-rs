use crate::{ArrayDataType, Nodata};

// Type requirements for data in rasters
pub trait ArrayNum<T>:
    Copy
    + Nodata<T>
    + num::Num
    + num::NumCast
    + num::Bounded
    + num::traits::NumAssignOps
    + std::cmp::PartialOrd
    + std::fmt::Debug
    + std::string::ToString
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

macro_rules! rasternum_signed_impl {
    ($trait_name:path, $t:ty, $raster_type:ident) => {
        impl $trait_name for $t {
            const TYPE: ArrayDataType = ArrayDataType::$raster_type;
            const IS_SIGNED: bool = true;

            add_nodata_impl!();
            sub_nodata_impl!();
            mul_nodata_impl!();
            div_nodata_impl!();
        }
    };
}

macro_rules! rasternum_unsigned_impl {
    ($trait_name:path, $t:ty, $raster_type:ident) => {
        impl $trait_name for $t {
            const TYPE: ArrayDataType = ArrayDataType::$raster_type;
            const IS_SIGNED: bool = false;

            add_nodata_impl!();
            sub_nodata_unsigned_impl!();
            mul_nodata_impl!();
            div_nodata_impl!();
        }
    };
}

macro_rules! rasternum_fp_impl {
    ($trait_name:path, $t:ty, $raster_type:ident) => {
        impl $trait_name for $t {
            const TYPE: ArrayDataType = ArrayDataType::$raster_type;
            const IS_SIGNED: bool = true;

            add_fp_nodata_impl!();
            sub_fp_nodata_impl!();
            mul_fp_nodata_impl!();
            div_fp_nodata_impl!();
        }
    };
}

rasternum_signed_impl!(ArrayNum<i8>, i8, Int8);
rasternum_signed_impl!(ArrayNum<i16>, i16, Int16);
rasternum_signed_impl!(ArrayNum<i32>, i32, Int32);
rasternum_signed_impl!(ArrayNum<i64>, i64, Int64);
rasternum_unsigned_impl!(ArrayNum<u8>, u8, Uint8);
rasternum_unsigned_impl!(ArrayNum<u16>, u16, Uint16);
rasternum_unsigned_impl!(ArrayNum<u32>, u32, Uint32);
rasternum_unsigned_impl!(ArrayNum<u64>, u64, Uint64);

rasternum_fp_impl!(ArrayNum<f32>, f32, Float32);
rasternum_fp_impl!(ArrayNum<f64>, f64, Float64);

use crate::raster::Nodata;

pub trait RasterNum<T>:
    Copy + Nodata<T> + num::NumCast + num::Num + num::Bounded + std::fmt::Debug + num::traits::NumAssignOps
{
    fn add_nodata_aware(self, other: Self) -> Self;
    fn sub_nodata_aware(self, other: Self) -> Self;
    fn mul_nodata_aware(self, other: Self) -> Self;
    fn div_nodata_aware(self, other: Self) -> Self;

    fn div_nodata_aware_opt(self, other: Self) -> Option<Self>;

    #[inline]
    fn add_assign_nodata_aware(&mut self, other: Self) {
        *self = self.add_nodata_aware(other);
    }

    #[inline]
    fn sub_assign_nodata_aware(&mut self, other: Self) {
        *self = self.sub_nodata_aware(other);
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

macro_rules! rasternum_impl {
    ($trait_name:path, $t:ty) => {
        impl $trait_name for $t {
            #[inline]
            fn add_nodata_aware(self, other: Self) -> Self {
                if self.is_nodata() || other.is_nodata() {
                    Self::nodata_value()
                } else {
                    self.wrapping_add(other)
                }
            }

            #[inline]
            fn sub_nodata_aware(self, other: Self) -> Self {
                if self.is_nodata() || other.is_nodata() {
                    Self::nodata_value()
                } else {
                    self.wrapping_sub(other)
                }
            }

            #[inline]
            fn mul_nodata_aware(self, other: Self) -> Self {
                if self.is_nodata() || other.is_nodata() {
                    Self::nodata_value()
                } else {
                    self.wrapping_mul(other)
                }
            }

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
        }
    };
}

macro_rules! rasternum_fp_impl {
    ($trait_name:path, $t:ty) => {
        impl $trait_name for $t {
            #[inline]
            fn add_nodata_aware(self, other: Self) -> Self {
                if self.is_nodata() || other.is_nodata() {
                    Self::nodata_value()
                } else {
                    self + other
                }
            }

            #[inline]
            fn sub_nodata_aware(self, other: Self) -> Self {
                if self.is_nodata() || other.is_nodata() {
                    Self::nodata_value()
                } else {
                    self - other
                }
            }

            #[inline]
            fn mul_nodata_aware(self, other: Self) -> Self {
                if self.is_nodata() || other.is_nodata() {
                    Self::nodata_value()
                } else {
                    self * other
                }
            }

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
        }
    };
}

rasternum_impl!(RasterNum<i8>, i8);
rasternum_impl!(RasterNum<u8>, u8);
rasternum_impl!(RasterNum<i16>, i16);
rasternum_impl!(RasterNum<u16>, u16);
rasternum_impl!(RasterNum<i32>, i32);
rasternum_impl!(RasterNum<u32>, u32);
rasternum_impl!(RasterNum<i64>, i64);
rasternum_impl!(RasterNum<u64>, u64);

rasternum_fp_impl!(RasterNum<f32>, f32);
rasternum_fp_impl!(RasterNum<f64>, f64);

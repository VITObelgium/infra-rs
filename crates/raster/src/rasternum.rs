use crate::{Nodata, RasterDataType};

// Type requirements for data in rasters
pub trait RasterNum<T>:
    Copy
    + Nodata<T>
    + num::Num
    + num::NumCast
    + num::Bounded
    + num::traits::NumAssignOps
    + std::fmt::Debug
    + std::string::ToString
{
    const TYPE: RasterDataType;

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
    ($trait_name:path, $t:ty, $raster_type:ident) => {
        impl $trait_name for $t {
            const TYPE: RasterDataType = RasterDataType::$raster_type;

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
    ($trait_name:path, $t:ty, $raster_type:ident) => {
        impl $trait_name for $t {
            const TYPE: RasterDataType = RasterDataType::$raster_type;

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

rasternum_impl!(RasterNum<i8>, i8, Int8);
rasternum_impl!(RasterNum<u8>, u8, Uint8);
rasternum_impl!(RasterNum<i16>, i16, Int16);
rasternum_impl!(RasterNum<u16>, u16, Uint16);
rasternum_impl!(RasterNum<i32>, i32, Int32);
rasternum_impl!(RasterNum<u32>, u32, Uint32);
rasternum_impl!(RasterNum<i64>, i64, Int64);
rasternum_impl!(RasterNum<u64>, u64, Uint64);

rasternum_fp_impl!(RasterNum<f32>, f32, Float32);
rasternum_fp_impl!(RasterNum<f64>, f64, Float64);

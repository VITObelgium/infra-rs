use crate::raster::Nodata;

pub trait RasterNum<T: num::ToPrimitive>:
    Copy
    + PartialEq
    + num::NumCast
    + num::Zero
    + num::One
    + num::Bounded
    + Nodata<T>
    + std::fmt::Debug
    + std::ops::Add<Output = Self>
    + std::ops::Sub<Output = Self>
    + std::ops::Mul<Output = Self>
    + std::ops::Div<Output = Self>
    + std::ops::AddAssign
    + std::ops::SubAssign
    + std::ops::MulAssign
    + std::ops::DivAssign
{
    fn add_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self + other
        }
    }

    fn sub_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self - other
        }
    }

    fn mul_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self * other
        }
    }

    fn div_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() || other == Self::zero() {
            Self::nodata_value()
        } else {
            self / other
        }
    }

    fn add_assign_nodata_aware(&mut self, other: Self) {
        *self = self.add_nodata_aware(other);
    }

    fn sub_assign_nodata_aware(&mut self, other: Self) {
        *self = self.sub_nodata_aware(other);
    }

    fn mul_assign_nodata_aware(&mut self, other: Self) {
        *self = self.mul_nodata_aware(other);
    }

    fn div_assign_nodata_aware(&mut self, other: Self) {
        *self = self.div_nodata_aware(other);
    }
}

impl RasterNum<i8> for i8 {
    fn add_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_add(other)
        }
    }

    fn sub_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_sub(other)
        }
    }

    fn mul_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_mul(other)
        }
    }
}
impl RasterNum<u8> for u8 {
    fn add_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_add(other)
        }
    }

    fn sub_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_sub(other)
        }
    }

    fn mul_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_mul(other)
        }
    }
}
impl RasterNum<i16> for i16 {
    fn add_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_add(other)
        }
    }

    fn sub_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_sub(other)
        }
    }

    fn mul_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_mul(other)
        }
    }
}
impl RasterNum<u16> for u16 {
    fn add_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_add(other)
        }
    }

    fn sub_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_sub(other)
        }
    }

    fn mul_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_mul(other)
        }
    }
}
impl RasterNum<i32> for i32 {
    fn add_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_add(other)
        }
    }

    fn sub_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_sub(other)
        }
    }

    fn mul_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_mul(other)
        }
    }
}
impl RasterNum<u32> for u32 {
    fn add_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_add(other)
        }
    }

    fn sub_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_sub(other)
        }
    }

    fn mul_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_mul(other)
        }
    }
}
impl RasterNum<i64> for i64 {
    fn add_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_add(other)
        }
    }

    fn sub_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_sub(other)
        }
    }

    fn mul_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_mul(other)
        }
    }
}
impl RasterNum<u64> for u64 {
    fn add_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_add(other)
        }
    }

    fn sub_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_sub(other)
        }
    }

    fn mul_nodata_aware(self, other: Self) -> Self {
        if self.is_nodata() || other.is_nodata() {
            Self::nodata_value()
        } else {
            self.wrapping_mul(other)
        }
    }
}
impl RasterNum<f32> for f32 {}
impl RasterNum<f64> for f64 {}

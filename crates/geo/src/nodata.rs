use num::ToPrimitive;

pub trait Nodata: ToPrimitive + PartialEq + Sized + Copy {
    fn nodata_value() -> Self;

    #[inline]
    fn is_nodata(self) -> bool {
        self == Self::nodata_value()
    }

    fn has_nan() -> bool;
    fn is_nan(self) -> bool;
}

impl Nodata for u8 {
    fn nodata_value() -> u8 {
        u8::MAX
    }

    fn has_nan() -> bool {
        false
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for u16 {
    fn nodata_value() -> u16 {
        u16::MAX
    }

    fn has_nan() -> bool {
        false
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for u32 {
    fn nodata_value() -> u32 {
        u32::MAX
    }

    fn has_nan() -> bool {
        false
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for u64 {
    fn nodata_value() -> u64 {
        u64::MAX
    }

    fn has_nan() -> bool {
        false
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for i8 {
    fn nodata_value() -> i8 {
        i8::MIN
    }

    fn has_nan() -> bool {
        false
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for i16 {
    fn nodata_value() -> i16 {
        i16::MIN
    }

    fn has_nan() -> bool {
        false
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for i32 {
    fn nodata_value() -> i32 {
        i32::MIN
    }

    fn has_nan() -> bool {
        false
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for i64 {
    fn nodata_value() -> i64 {
        i64::MIN
    }

    fn has_nan() -> bool {
        false
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for f32 {
    fn nodata_value() -> f32 {
        f32::NAN
    }

    fn is_nodata(self) -> bool {
        self.is_nan()
    }

    fn has_nan() -> bool {
        true
    }

    fn is_nan(self) -> bool {
        false
    }
}

impl Nodata for f64 {
    fn nodata_value() -> f64 {
        f64::NAN
    }

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

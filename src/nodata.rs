use num::ToPrimitive;

pub trait Nodata<T: ToPrimitive>: ToPrimitive {
    fn nodata_value() -> T;
    fn is_nodata(value: T) -> bool;
    fn has_nan() -> bool;
}

impl Nodata<u8> for u8 {
    fn nodata_value() -> u8 {
        u8::MAX
    }

    fn is_nodata(value: u8) -> bool {
        value == Self::nodata_value()
    }

    fn has_nan() -> bool {
        false
    }
}

impl Nodata<u16> for u16 {
    fn nodata_value() -> u16 {
        u16::MAX
    }

    fn is_nodata(value: u16) -> bool {
        value == Self::nodata_value()
    }

    fn has_nan() -> bool {
        false
    }
}

impl Nodata<u32> for u32 {
    fn nodata_value() -> u32 {
        u32::MAX
    }

    fn is_nodata(value: u32) -> bool {
        value == Self::nodata_value()
    }

    fn has_nan() -> bool {
        false
    }
}

impl Nodata<u64> for u64 {
    fn nodata_value() -> u64 {
        u64::MAX
    }

    fn is_nodata(value: u64) -> bool {
        value == Self::nodata_value()
    }

    fn has_nan() -> bool {
        false
    }
}

impl Nodata<i8> for i8 {
    fn nodata_value() -> i8 {
        i8::MIN
    }

    fn is_nodata(value: i8) -> bool {
        value == Self::nodata_value()
    }

    fn has_nan() -> bool {
        false
    }
}

impl Nodata<i16> for i16 {
    fn nodata_value() -> i16 {
        i16::MIN
    }

    fn is_nodata(value: i16) -> bool {
        value == Self::nodata_value()
    }

    fn has_nan() -> bool {
        false
    }
}

impl Nodata<i32> for i32 {
    fn nodata_value() -> i32 {
        i32::MIN
    }

    fn is_nodata(value: i32) -> bool {
        value == Self::nodata_value()
    }

    fn has_nan() -> bool {
        false
    }
}

impl Nodata<i64> for i64 {
    fn nodata_value() -> i64 {
        i64::MIN
    }

    fn is_nodata(value: i64) -> bool {
        value == Self::nodata_value()
    }

    fn has_nan() -> bool {
        false
    }
}

impl Nodata<f32> for f32 {
    fn nodata_value() -> f32 {
        f32::NAN
    }

    fn is_nodata(value: f32) -> bool {
        value.is_nan()
    }

    fn has_nan() -> bool {
        true
    }
}

impl Nodata<f64> for f64 {
    fn nodata_value() -> f64 {
        f64::NAN
    }

    fn is_nodata(value: f64) -> bool {
        value.is_nan()
    }

    fn has_nan() -> bool {
        true
    }
}

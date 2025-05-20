use crate::{Error, Result};
use std::ops::Range;

use num::NumCast;

/// Check if a f64 value fits in a given numerical type.
pub fn fits_in_type<T: NumCast>(v: f64) -> bool {
    let x: Option<T> = NumCast::from(v);
    x.is_some()
}

pub fn option<To: NumCast>(from: Option<impl NumCast>) -> Option<To> {
    from.and_then(|x| NumCast::from(x))
}

pub fn option_or<To: NumCast>(from: Option<impl NumCast>, default: To) -> To {
    from.and_then(|x| NumCast::from(x)).unwrap_or(default)
}

pub fn range<To: NumCast>(from: Range<impl NumCast>) -> Result<Range<To>> {
    Ok(Range {
        start: NumCast::from(from.start).ok_or_else(|| Error::Runtime("Impossible range cast".into()))?,
        end: NumCast::from(from.end).ok_or_else(|| Error::Runtime("Impossible range cast".into()))?,
    })
}

pub fn slice<To: NumCast>(from: &[impl NumCast + Copy]) -> Result<Vec<To>> {
    from.iter()
        .map(|x| NumCast::from(*x).ok_or_else(|| Error::Runtime("Impossible slice cast".into())))
        .collect()
}

/// # Safety
/// Return a u8 slice to a vec of any type, only use this for structs that are #[repr(C)]
/// Otherwise the slice will contain (uninitialized) padding bytes
pub unsafe fn vec_as_u8_slice<T: Sized>(data: &[T]) -> &[u8] {
    unsafe { ::core::slice::from_raw_parts(data.as_ptr().cast::<u8>(), std::mem::size_of_val(data)) }
}

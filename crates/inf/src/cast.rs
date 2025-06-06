use crate::{Error, Result};
use std::ops::{Range, RangeInclusive};

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

pub fn inclusive_range<To: NumCast>(from: RangeInclusive<impl NumCast + Copy>) -> Result<RangeInclusive<To>> {
    Ok(RangeInclusive::new(
        NumCast::from(*from.start()).ok_or_else(|| Error::Runtime("Impossible range cast".into()))?,
        NumCast::from(*from.end()).ok_or_else(|| Error::Runtime("Impossible range cast".into()))?,
    ))
}

pub fn slice<To: NumCast>(from: &[impl NumCast + Copy]) -> Result<Vec<To>> {
    from.iter()
        .map(|x| NumCast::from(*x).ok_or_else(|| Error::Runtime("Impossible slice cast".into())))
        .collect()
}

pub fn reinterpret_vec<T: Sized, U: Sized>(mut data: Vec<T>) -> Vec<U> {
    assert!(
        std::mem::size_of::<T>() == std::mem::size_of::<U>(),
        "Cannot reinterpret Vec<T> to Vec<U> because their sizes do not match"
    );

    // Safety: This is safe because we are reinterpreting the data as a different type
    // and the size of the types must match.
    unsafe { Vec::from_raw_parts(data.as_mut_ptr().cast::<U>(), data.len(), data.capacity()) }
}

/// # Safety
/// Return a u8 slice to a vec of any type, only use this for structs that are #[repr(C)]
/// Otherwise the slice will contain (uninitialized) padding bytes
pub unsafe fn vec_as_u8_slice<T: Sized>(data: &[T]) -> &[u8] {
    unsafe { ::core::slice::from_raw_parts(data.as_ptr().cast::<u8>(), std::mem::size_of_val(data)) }
}

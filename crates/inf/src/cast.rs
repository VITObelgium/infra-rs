use num::NumCast;

/// Check if a f64 value fits in a given numerical type.
pub fn fits_in_type<T: NumCast>(v: f64) -> bool {
    let x: Option<T> = NumCast::from(v);
    x.is_some()
}

pub fn option<To, From>(from: Option<From>) -> Option<To>
where
    To: NumCast,
    From: NumCast,
{
    from.and_then(|x| NumCast::from(x))
}

pub fn option_or<To, From>(from: Option<From>, default: To) -> To
where
    To: NumCast,
    From: NumCast,
{
    from.and_then(|x| NumCast::from(x)).unwrap_or(default)
}

/// # Safety
/// Return a u8 slice to a vec of any type, only use this for structs that are #[repr(C)]
/// Otherwise the slice will contain (uninitialized) padding bytes
pub unsafe fn vec_as_u8_slice<T: Sized>(data: &[T]) -> &[u8] {
    ::core::slice::from_raw_parts(data.as_ptr().cast::<u8>(), std::mem::size_of_val(data))
}

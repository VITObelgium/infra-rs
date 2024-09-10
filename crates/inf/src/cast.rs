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

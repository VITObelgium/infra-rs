use num::NumCast;

pub fn fits_in_type<T: NumCast>(v: f64) -> bool {
    let x: Option<T> = NumCast::from(v);
    x.is_some()
}

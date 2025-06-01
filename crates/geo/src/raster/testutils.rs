use crate::GeoReference;
use RasterSize;
use approx::relative_eq;

pub const NOD: f64 = 255.0;

pub fn create_vec<T: num::NumCast>(data: &[f64]) -> Vec<T> {
    data.iter().map(|&v| num::NumCast::from(v).unwrap()).collect()
}

pub fn compare_fp_vectors(a: &[f64], b: &[f64]) -> bool {
    a.iter().zip(b.iter()).all(|(a, b)| {
        if a.is_nan() != b.is_nan() {
            return false;
        }

        if a.is_nan() == b.is_nan() {
            return true;
        }

        relative_eq!(a, b)
    })
}

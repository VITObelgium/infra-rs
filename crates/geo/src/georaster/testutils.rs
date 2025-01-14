use crate::GeoReference;
use approx::relative_eq;
use raster::RasterSize;

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

pub fn test_metadata_2x2() -> GeoReference {
    GeoReference::new(
        "EPSG:4326".to_string(),
        RasterSize { rows: 2, cols: 2 },
        [0.0, 0.0, 1.0, 1.0, 0.0, 0.0],
        Some(NOD),
    )
}

pub fn test_metadata_3x3() -> GeoReference {
    GeoReference::new(
        "EPSG:4326".to_string(),
        RasterSize { rows: 3, cols: 3 },
        [0.0, 0.0, 1.0, 1.0, 0.0, 0.0],
        Some(NOD),
    )
}

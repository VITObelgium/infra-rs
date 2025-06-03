use std::path::PathBuf;

use approx::relative_eq;
use inf::allocate::{self, AlignedVec};
use path_macro::path;

use crate::{
    ArrayNum, GeoReference, RasterSize,
    array::{Columns, Rows},
};

pub const NOD: f64 = 255.0;

pub fn workspace_test_data_dir() -> PathBuf {
    path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data")
}

pub fn number_cast<T: ArrayNum>(val: f64) -> T {
    num::NumCast::from(val).expect("F64 could not be convertd to the specified type")
}

pub fn create_vec<T: num::NumCast + ArrayNum>(data: &[f64]) -> AlignedVec<T> {
    let mut vec = allocate::aligned_vec_with_capacity(data.len());
    for &v in data.iter() {
        if relative_eq!(v, NOD) {
            vec.push(T::NODATA);
        } else {
            vec.push(num::NumCast::from(v).expect("f64 could not be converted to the specified type"));
        }
    }

    vec
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

#[allow(dead_code)]
pub fn test_metadata_2x2() -> GeoReference {
    GeoReference::new(
        "EPSG:4326".to_string(),
        RasterSize::with_rows_cols(Rows(2), Columns(2)),
        [0.0, 0.0, 1.0, 1.0, 0.0, 0.0],
        Some(NOD),
    )
}

#[allow(dead_code)]
pub fn test_metadata_3x3() -> GeoReference {
    GeoReference::new(
        "EPSG:4326".to_string(),
        RasterSize::with_rows_cols(Rows(3), Columns(3)),
        [0.0, 0.0, 1.0, 1.0, 0.0, 0.0],
        Some(NOD),
    )
}

#[cfg(feature = "gdal")]
pub fn configure_gdal_data() {
    use crate::gdalinterop;
    let mut data_dir = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "target" / "data");
    if !data_dir.exists() {
        data_dir = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / ".." / "target" / "data");
    }

    if !data_dir.exists() {
        panic!("Proj.db data directory not found: {}", data_dir.display());
    }

    assert!(data_dir.join("proj.db").exists());

    let gdal_config = gdalinterop::Config {
        debug_logging: false,
        proj_db_search_location: data_dir,
        config_options: Vec::default(),
    };

    gdal_config.apply().expect("Failed to configure GDAL");
}

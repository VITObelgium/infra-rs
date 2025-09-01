use std::{ops::RangeInclusive, path::PathBuf};

use approx::relative_eq;
use inf::allocate::{self, AlignedVec};
use num::NumCast;
use path_macro::path;
use rand::distr::{Uniform, uniform::SampleUniform};

use crate::{
    ArrayNum, GeoReference, RasterSize,
    array::{Columns, Rows},
};

pub const NOD: f64 = 255.0;

pub fn workspace_test_data_dir() -> PathBuf {
    path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data")
}

pub fn geo_test_data_dir() -> PathBuf {
    path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data")
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

pub fn create_random_vec<T: num::NumCast + ArrayNum + SampleUniform>(size: RasterSize, value_range: RangeInclusive<f64>) -> AlignedVec<T> {
    use rand::distr::Distribution;

    let mut rng = rand::rng();
    let mut vec = allocate::aligned_vec_with_capacity(size.cell_count());
    let uniform = Uniform::new_inclusive::<T, T>(
        NumCast::from(*value_range.start()).unwrap_or_else(|| {
            panic!(
                "Failed to convert start of range to type {} ({})",
                std::any::type_name::<T>(),
                *value_range.start()
            )
        }),
        NumCast::from(*value_range.end()).unwrap_or_else(|| {
            panic!(
                "Failed to convert end of range to type {} ({})",
                std::any::type_name::<T>(),
                *value_range.end()
            )
        }),
    )
    .expect("Failed to create uniform distribution");
    (0..size.cell_count()).for_each(|_| vec.push(uniform.sample(&mut rng)));
    vec
}

pub fn create_random_vec_with_nodata<T>(size: RasterSize, value_range: RangeInclusive<f64>, nodata_count: usize) -> AlignedVec<T>
where
    T: num::NumCast + ArrayNum + SampleUniform,
{
    use rand::distr::Distribution;

    let mut rng = rand::rng();
    let mut vec = create_random_vec(size, value_range);

    let uniform = Uniform::new(0, size.cell_count()).expect("Failed to create uniform distribution");
    (0..nodata_count).for_each(|_| {
        vec[uniform.sample(&mut rng)] = T::NODATA;
    });
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
        [0.0, 0.0, 1.0, 1.0, 0.0, 0.0].into(),
        Some(NOD),
    )
}

#[allow(dead_code)]
pub fn test_metadata_3x3() -> GeoReference {
    GeoReference::new(
        "EPSG:4326".to_string(),
        RasterSize::with_rows_cols(Rows(3), Columns(3)),
        [0.0, 0.0, 1.0, 1.0, 0.0, 0.0].into(),
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
        proj_db_search_location: Some(data_dir),
        config_options: Vec::default(),
    };

    gdal_config.apply().expect("Failed to configure GDAL");
}

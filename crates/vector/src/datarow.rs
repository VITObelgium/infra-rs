use gdal::vector::FieldValue;

use crate::{
    fieldtype::VectorFieldType,
    io::{self},
    Result,
};

pub trait DataRow {
    fn field_names() -> Vec<&'static str>;
    fn from_feature(feature: gdal::vector::Feature) -> Result<Self>
    where
        Self: Sized;
}

#[allow(dead_code)] // Used in the derive macro
pub fn read_feature_val<T: VectorFieldType<T>>(feature: &gdal::vector::Feature, field_name: &str) -> Result<Option<T>> {
    let index = io::field_index_from_name(feature, field_name)?;

    let field_is_valid = unsafe { gdal_sys::OGR_F_IsFieldSetAndNotNull(feature.c_feature(), index) == 1 };

    if !field_is_valid {
        return Ok(None);
    }

    match feature.field(field_name)? {
        Some(field) => {
            if !T::empty_value_is_valid() {
                if let FieldValue::StringValue(val) = &field {
                    // Don't try to parse empty strings (empty strings are not considered as null values by GDAL for csv files)
                    if val.is_empty() {
                        return Ok(None);
                    }
                }
            }

            T::read_from_field(&field)
        }
        None => Ok(None),
    }
}

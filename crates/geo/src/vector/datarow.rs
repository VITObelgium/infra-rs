use gdal::vector::FieldValue;

use crate::Result;

pub trait DataRow {
    fn field_names() -> Vec<&'static str>;
    fn from_feature(feature: gdal::vector::Feature) -> Result<Self>
    where
        Self: Sized;
}

#[doc(hidden)]
pub mod __private {
    use crate::vector::{fieldtype::VectorFieldType, io};
    use io::FeatureExtension;

    use super::*;

    // Helper function for the DataRow derive macro
    pub fn read_feature_val<T: VectorFieldType<T>>(
        feature: &gdal::vector::Feature,
        field_name: &str,
    ) -> Result<Option<T>> {
        let index = feature.field_index_from_name(field_name)?;
        if !feature.field_is_valid(index) {
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
}

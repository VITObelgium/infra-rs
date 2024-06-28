use crate::Result;
use gdal::vector::FieldValue;

pub trait VectorFieldType<T> {
    fn read_from_field(field: &FieldValue) -> Result<Option<T>>;
}

impl VectorFieldType<f64> for f64 {
    fn read_from_field(field: &FieldValue) -> Result<Option<f64>> {
        match field {
            FieldValue::RealValue(val) => Ok(Some(*val)),
            FieldValue::IntegerValue(val) => Ok(Some(*val as f64)),
            FieldValue::StringValue(val) => Ok(Some(val.parse()?)),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType<i32> for i32 {
    fn read_from_field(field: &FieldValue) -> Result<Option<i32>> {
        match field {
            FieldValue::IntegerValue(val) => Ok(Some(*val)),
            FieldValue::RealValue(val) => Ok(Some(*val as i32)),
            FieldValue::StringValue(val) => Ok(Some(val.parse()?)),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType<i64> for i64 {
    fn read_from_field(field: &FieldValue) -> Result<Option<i64>> {
        match field {
            FieldValue::IntegerValue(val) => Ok(Some(*val as i64)),
            FieldValue::RealValue(val) => Ok(Some(*val as i64)),
            FieldValue::StringValue(val) => Ok(Some(val.parse()?)),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType<String> for String {
    fn read_from_field(field: &FieldValue) -> Result<Option<String>> {
        match field {
            FieldValue::StringValue(val) => Ok(Some(val.to_string())),
            FieldValue::RealValue(val) => Ok(Some(val.to_string())),
            FieldValue::IntegerValue(val) => Ok(Some(val.to_string())),
            _ => Ok(None),
        }
    }
}

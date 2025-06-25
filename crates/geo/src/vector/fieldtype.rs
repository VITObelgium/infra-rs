use std::ops::RangeInclusive;

use crate::{Error, Result};
use gdal::vector::FieldValue;

fn parse_value_range(year_range: &str) -> Result<RangeInclusive<i32>> {
    let years: Vec<&str> = year_range.split('-').map(str::trim).collect();
    if years.len() == 1 {
        let year = years[0].parse::<i32>()?;
        Ok(RangeInclusive::new(year, year))
    } else if years.len() == 2 {
        let start_year = years[0].parse::<i32>()?;
        let end_year = years[1].parse::<i32>()?;
        Ok(RangeInclusive::new(start_year, end_year))
    } else {
        Err(Error::Runtime(format!("Invalid value range: {}", year_range)))
    }
}

pub trait VectorFieldType: Sized {
    fn empty_value_is_valid() -> bool {
        false
    }

    fn read_from_field(field: &FieldValue) -> Result<Option<Self>>;
}

impl VectorFieldType for f64 {
    fn read_from_field(field: &FieldValue) -> Result<Option<f64>> {
        match field {
            FieldValue::RealValue(val) => Ok(Some(*val)),
            FieldValue::IntegerValue(val) => Ok(Some(*val as f64)),
            FieldValue::StringValue(val) => Ok(Some(val.parse()?)),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType for i32 {
    fn read_from_field(field: &FieldValue) -> Result<Option<i32>> {
        match field {
            FieldValue::IntegerValue(val) => Ok(Some(*val)),
            FieldValue::RealValue(val) => Ok(Some(*val as i32)),
            FieldValue::StringValue(val) => Ok(Some(val.parse()?)),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType for i64 {
    fn read_from_field(field: &FieldValue) -> Result<Option<i64>> {
        match field {
            FieldValue::IntegerValue(val) => Ok(Some(*val as i64)),
            FieldValue::RealValue(val) => Ok(Some(*val as i64)),
            FieldValue::StringValue(val) => Ok(Some(val.parse()?)),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType for String {
    fn empty_value_is_valid() -> bool {
        true
    }

    fn read_from_field(field: &FieldValue) -> Result<Option<String>> {
        match field {
            FieldValue::StringValue(val) => Ok(Some(val.clone())),
            FieldValue::RealValue(val) => Ok(Some(val.to_string())),
            FieldValue::IntegerValue(val) => Ok(Some(val.to_string())),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType for RangeInclusive<i32> {
    fn empty_value_is_valid() -> bool {
        false
    }

    fn read_from_field(field: &FieldValue) -> Result<Option<RangeInclusive<i32>>> {
        match field {
            FieldValue::StringValue(val) => Ok(Some(parse_value_range(val)?)),
            FieldValue::RealValue(val) => Ok(Some(RangeInclusive::new(*val as i32, *val as i32))),
            FieldValue::IntegerValue(val) => Ok(Some(RangeInclusive::new(*val, *val))),
            _ => Ok(None),
        }
    }
}

use std::ops::RangeInclusive;

use crate::{Error, Result};
use gdal::vector::FieldValue;
use num::NumCast;

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
        Err(Error::Runtime(format!("Invalid value range: {year_range}")))
    }
}

fn parse_bool_str(val: &str) -> Option<bool> {
    match val.to_lowercase().trim() {
        "true" | "yes" | "ja" | "oui" | "1" => Some(true),
        "false" | "no" | "nee" | "non" | "0" => Some(false),
        _ => None,
    }
}

pub trait VectorFieldType: Sized {
    const EMPTY_FIELD_IS_VALID: bool;

    fn read_from_field(field: &FieldValue) -> Result<Option<Self>>;
}

impl VectorFieldType for f64 {
    const EMPTY_FIELD_IS_VALID: bool = false;

    fn read_from_field(field: &FieldValue) -> Result<Option<f64>> {
        match field {
            FieldValue::RealValue(val) => Ok(Some(*val)),
            FieldValue::IntegerValue(val) => Ok(NumCast::from(*val)),
            FieldValue::Integer64Value(val) => Ok(NumCast::from(*val)),
            FieldValue::StringValue(val) => Ok(val.parse().ok()),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType for i32 {
    const EMPTY_FIELD_IS_VALID: bool = false;

    fn read_from_field(field: &FieldValue) -> Result<Option<i32>> {
        match field {
            FieldValue::IntegerValue(val) => Ok(Some(*val)),
            FieldValue::Integer64Value(val) => Ok(NumCast::from(*val)),
            FieldValue::RealValue(val) => Ok(NumCast::from(*val)),
            FieldValue::StringValue(val) => Ok(val.parse().ok()),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType for i64 {
    const EMPTY_FIELD_IS_VALID: bool = false;

    fn read_from_field(field: &FieldValue) -> Result<Option<i64>> {
        match field {
            FieldValue::IntegerValue(val) => Ok(Some(*val as i64)),
            FieldValue::Integer64Value(val) => Ok(Some(*val)),
            FieldValue::RealValue(val) => Ok(Some(*val as i64)),
            FieldValue::StringValue(val) => Ok(Some(val.parse()?)),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType for bool {
    const EMPTY_FIELD_IS_VALID: bool = false;

    fn read_from_field(field: &FieldValue) -> Result<Option<bool>> {
        match field {
            FieldValue::IntegerValue(val) => Ok(Some(*val != 0)),
            FieldValue::Integer64Value(val) => Ok(Some(*val != 0)),
            FieldValue::StringValue(val) => Ok(parse_bool_str(val)),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType for String {
    const EMPTY_FIELD_IS_VALID: bool = true;

    fn read_from_field(field: &FieldValue) -> Result<Option<String>> {
        match field {
            FieldValue::StringValue(val) => Ok(Some(val.clone())),
            FieldValue::RealValue(val) => Ok(Some(val.to_string())),
            FieldValue::IntegerValue(val) => Ok(Some(val.to_string())),
            FieldValue::Integer64Value(val) => Ok(Some(val.to_string())),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType for RangeInclusive<i32> {
    const EMPTY_FIELD_IS_VALID: bool = false;

    fn read_from_field(field: &FieldValue) -> Result<Option<RangeInclusive<i32>>> {
        match field {
            FieldValue::StringValue(val) => Ok(Some(parse_value_range(val)?)),
            FieldValue::RealValue(val) => Ok(Some(RangeInclusive::new(*val as i32, *val as i32))),
            FieldValue::IntegerValue(val) => Ok(Some(RangeInclusive::new(*val, *val))),
            FieldValue::Integer64Value(val) => Ok(Some(RangeInclusive::new(*val as i32, *val as i32))),
            _ => Ok(None),
        }
    }
}

use std::ops::RangeInclusive;

use crate::{Error, Result, vector::dataframe::Field};
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

    fn read_from_field(field: Field) -> Result<Option<Self>>;
}

impl VectorFieldType for f64 {
    const EMPTY_FIELD_IS_VALID: bool = false;

    fn read_from_field(field: Field) -> Result<Option<Self>> {
        match field {
            Field::Float(val) => Ok(Some(val)),
            Field::Integer(val) => Ok(NumCast::from(val)),
            Field::String(val) => Ok(val.parse().ok()),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType for i32 {
    const EMPTY_FIELD_IS_VALID: bool = false;

    fn read_from_field(field: Field) -> Result<Option<Self>> {
        match field {
            Field::Float(val) => Ok(NumCast::from(val)),
            Field::Integer(val) => Ok(NumCast::from(val)),
            Field::String(val) => Ok(val.parse().ok()),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType for i64 {
    const EMPTY_FIELD_IS_VALID: bool = false;

    fn read_from_field(field: Field) -> Result<Option<Self>> {
        match field {
            Field::Float(val) => Ok(NumCast::from(val)),
            Field::Integer(val) => Ok(NumCast::from(val)),
            Field::String(val) => Ok(val.parse().ok()),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType for bool {
    const EMPTY_FIELD_IS_VALID: bool = false;

    fn read_from_field(field: Field) -> Result<Option<Self>> {
        match field {
            Field::Integer(val) => Ok(Some(val != 0)),
            Field::String(val) => Ok(parse_bool_str(&val)),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType for String {
    const EMPTY_FIELD_IS_VALID: bool = true;

    fn read_from_field(field: Field) -> Result<Option<Self>> {
        match field {
            Field::Float(val) => Ok(Some(val.to_string())),
            Field::Integer(val) => Ok(Some(val.to_string())),
            Field::String(val) => Ok(Some(val)),
            Field::Boolean(val) => Ok(Some(val.to_string())),
            Field::DateTime(val) => Ok(Some(val.to_string())),
        }
    }
}

impl VectorFieldType for RangeInclusive<i32> {
    const EMPTY_FIELD_IS_VALID: bool = false;

    fn read_from_field(field: Field) -> Result<Option<Self>> {
        match field {
            Field::Float(val) => Ok(Some(RangeInclusive::new(val as i32, val as i32))),
            Field::Integer(val) => Ok(Some(RangeInclusive::new(val as i32, val as i32))),
            Field::String(val) => Ok(Some(parse_value_range(&val)?)),
            _ => Ok(None),
        }
    }
}

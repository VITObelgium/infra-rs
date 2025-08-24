use std::ops::RangeInclusive;

use crate::{Error, Result, vector::dataframe::Field};
use chrono::DateTime;
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

pub(crate) fn parse_bool_str(val: &str) -> Option<bool> {
    match val.to_lowercase().trim() {
        "true" | "yes" | "ja" | "oui" | "1" => Some(true),
        "false" | "no" | "nee" | "non" | "0" => Some(false),
        _ => None,
    }
}

pub(crate) fn parse_date_str(val: &str) -> Option<chrono::NaiveDateTime> {
    // Strip quotes if present
    let value = val.trim_matches('"').trim();

    // Try various datetime formats
    const DATETIME_FORMATS: [&str; 8] = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
        "%m/%d/%Y",
        "%d/%m/%Y",
        "%B %d, %Y",
        "%A, %B %d, %Y",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
    ];
    for format in &DATETIME_FORMATS {
        if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, format) {
            return Some(dt);
        }
    }

    // Try parsing as date only and convert to datetime
    const DATE_FORMATS: [&str; 5] = ["%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%B %d, %Y", "%A, %B %d, %Y"];
    for format in &DATE_FORMATS {
        if let Ok(date) = chrono::NaiveDate::parse_from_str(value, format) {
            return Some(date.and_hms_opt(0, 0, 0).unwrap());
        }
    }

    None
}

pub(crate) fn date_from_integer(val: i64) -> Option<chrono::NaiveDateTime> {
    let dt = DateTime::from_timestamp_millis(val);
    dt.map(|d| d.naive_local())
}

pub trait VectorFieldType: Sized {
    const EMPTY_FIELD_IS_VALID: bool;

    fn empty_field_value() -> Option<Self>;
    fn read_from_field(field: Field) -> Result<Option<Self>>;
}

impl VectorFieldType for f64 {
    const EMPTY_FIELD_IS_VALID: bool = false;

    fn empty_field_value() -> Option<Self> {
        None
    }

    fn read_from_field(field: Field) -> Result<Option<Self>> {
        match field {
            Field::Float(val) => Ok(Some(val)),
            Field::Integer(val) => Ok(NumCast::from(val)),
            Field::String(val) => Ok(Some(val.parse()?)),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType for i32 {
    const EMPTY_FIELD_IS_VALID: bool = false;

    fn empty_field_value() -> Option<Self> {
        None
    }

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

    fn empty_field_value() -> Option<Self> {
        None
    }

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

    fn empty_field_value() -> Option<Self> {
        None
    }

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

    fn empty_field_value() -> Option<Self> {
        Some(String::default())
    }

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

    fn empty_field_value() -> Option<Self> {
        None
    }

    fn read_from_field(field: Field) -> Result<Option<Self>> {
        match field {
            Field::Float(val) => Ok(Some(RangeInclusive::new(val as i32, val as i32))),
            Field::Integer(val) => Ok(Some(RangeInclusive::new(val as i32, val as i32))),
            Field::String(val) => Ok(Some(parse_value_range(&val)?)),
            _ => Ok(None),
        }
    }
}

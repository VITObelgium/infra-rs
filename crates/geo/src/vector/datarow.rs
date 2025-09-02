/// Tools for reading rows from table based data sources into Rust structs
/// that implement the [`DataRow`] trait
use std::path::Path;

use crate::{
    Error, Result,
    vector::dataframe::{DataFrameOptions, DataFrameRow, FieldInfo, FieldType, Schema, create_dataframe_reader},
};

pub trait DataRow {
    fn field_names() -> Vec<&'static str>;
    fn from_dataframe_row(row: DataFrameRow) -> Result<Self>
    where
        Self: Sized;
}

#[doc(hidden)]
#[cfg(feature = "derive")]
pub mod __private {
    use super::*;
    use crate::vector::{dataframe::Field, fieldtype::VectorFieldType};

    // Helper function for the DataRow derive macro
    #[allow(dead_code)]
    pub fn read_feature_val<T: VectorFieldType>(field: Option<Field>) -> Result<Option<T>> {
        match field {
            Some(field) => {
                if !T::EMPTY_FIELD_IS_VALID
                    && let Field::String(val) = &field
                {
                    // Don't try to parse empty strings (empty strings are not considered as null values by GDAL for csv files)
                    if val.is_empty() {
                        return Ok(None);
                    }
                }

                T::read_from_field(field)
            }
            None => Ok(T::empty_field_value()),
        }
    }
}

/// Reads all rows from a table based data source located at `path` and returns them as a vector of `TRow` objects
/// `TRow` must implement the [`DataRow`] trait
pub fn read_dataframe_rows<TRow: DataRow, P: AsRef<Path>>(path: &P, opts: DataFrameOptions) -> Result<Vec<TRow>> {
    let rows: Result<Vec<_>> = DataRowsIterator::<TRow>::new_with_options(path, opts)?.collect();
    rows.map_err(|e| Error::Runtime(format!("Failed to read data frame rows: {e}")))
}

/// Iterator over the rows of a vector dataset that returns a an object
/// that implements the [`DataRow`] trait
pub struct DataRowsIterator<TRow: DataRow> {
    iterator: Box<dyn Iterator<Item = DataFrameRow>>,
    phantom: std::marker::PhantomData<TRow>,
}

impl<TRow: DataRow> DataRowsIterator<TRow> {
    pub fn new<P: AsRef<Path>>(path: &P, layer: Option<String>) -> Result<Self> {
        let options = DataFrameOptions {
            layer,
            ..Default::default()
        };
        Self::create_from_options(path, options)
    }

    pub fn new_with_options<P: AsRef<Path>>(path: &P, options: DataFrameOptions) -> Result<Self> {
        if options.schema_override.is_some() {
            return Err(Error::InvalidArgument(
                "schema_override can not be set for iterating DataRows, the types from the struct will be used".into(),
            ));
        }

        Self::create_from_options(path, options)
    }

    fn create_from_options<P: AsRef<Path>>(path: &P, mut options: DataFrameOptions) -> Result<Self> {
        let field_names = TRow::field_names();
        options.schema_override = Some(Schema {
            fields: field_names.iter().map(|&name| FieldInfo::new(name, FieldType::Native)).collect(),
        });

        let mut reader = create_dataframe_reader(path.as_ref())?;
        let iterator = reader.iter_rows(&options)?;

        Ok(Self {
            iterator,
            phantom: std::marker::PhantomData,
        })
    }
}

impl<TRow: DataRow> Iterator for DataRowsIterator<TRow> {
    type Item = Result<TRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next().map(TRow::from_dataframe_row)
    }
}

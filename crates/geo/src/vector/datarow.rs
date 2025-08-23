use std::path::Path;

use crate::{
    Error, Result,
    vector::dataframe::{DataFrameOptions, DataFrameRow, create_dataframe_reader},
};

pub trait DataRow {
    fn field_names() -> Vec<&'static str>;
    fn from_dataframe_row(row: DataFrameRow) -> Result<Self>
    where
        Self: Sized;
}

#[doc(hidden)]
pub mod __private {
    use super::*;
    use crate::vector::{dataframe::Field, fieldtype::VectorFieldType};

    // Helper function for the DataRow derive macro
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
            None => Ok(None),
        }
    }
}

/// Reads all rows from a table based data source located at `path` and returns them as a vector of `TRow` objects
/// `TRow` must implement the [`DataRow`] trait
pub fn read_dataframe_rows<TRow: DataRow, P: AsRef<Path>>(path: &P, opts: DataFrameOptions) -> Result<Vec<TRow>> {
    let rows: Result<Vec<_>> = DataframeIterator::<TRow>::new_with_options(path, opts)?.collect();
    rows.map_err(|e| Error::Runtime(format!("Failed to read data frame rows: {e}")))
}

/// Iterator over the rows of a vector dataset that returns a an object
/// that implements the [`DataRow`] trait
pub struct DataframeIterator<TRow: DataRow> {
    iterator: Box<dyn Iterator<Item = DataFrameRow>>,
    phantom: std::marker::PhantomData<TRow>,
}

impl<TRow: DataRow> DataframeIterator<TRow> {
    pub fn new<P: AsRef<Path>>(path: &P, layer: Option<String>) -> Result<Self> {
        let options = DataFrameOptions {
            layer,
            ..Default::default()
        };
        Self::new_with_options(path, options)
    }

    pub fn new_with_options<P: AsRef<Path>>(path: &P, options: DataFrameOptions) -> Result<Self> {
        let mut reader = create_dataframe_reader(path.as_ref())?;
        let iterator = reader.iter_rows(&options)?;

        Ok(Self {
            iterator,
            phantom: std::marker::PhantomData,
        })
    }
}

impl<TRow: DataRow> Iterator for DataframeIterator<TRow> {
    type Item = Result<TRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iterator.next().map(TRow::from_dataframe_row)
    }
}

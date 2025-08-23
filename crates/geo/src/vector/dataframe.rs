use std::path::Path;

use crate::{
    Error, Result,
    vector::{self},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
/// Specifies how to handle the header row in a tabular data source
pub enum HeaderRow {
    /// Automatically detect the presence of a header row
    #[default]
    Auto,
    /// No header row is present, all rows are treated as data rows
    None,
    /// The row to use as a header row, 0-indexed, all preceding rows are ignored
    Row(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    String,
    Integer,
    Float,
    Boolean,
    DateTime,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Field {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    DateTime(chrono::NaiveDateTime),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldInfo {
    name: String,
    field_type: FieldType,
}

impl FieldInfo {
    pub fn new(name: String, field_type: FieldType) -> Self {
        Self { name, field_type }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn field_type(&self) -> FieldType {
        self.field_type
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Schema {
    pub fields: Vec<FieldInfo>,
}

impl Schema {
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn subselection(&self, names: &[&str]) -> Schema {
        Schema {
            fields: self.fields.iter().filter(|f| names.contains(&f.name.as_str())).cloned().collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
/// Options for reading a `DataFrame` from a data source
pub struct DataFrameOptions {
    /// The name of the layer to read from, if none is specified, the first available layer is used.
    pub layer: Option<String>,
    /// The row to use as a header row, if None is specified no header row is used and all rows are treated as data rows.
    pub header_row: HeaderRow,
    /// Optional schema to override the detected schema from the data source, only the specified columns will be read using the provided datatype.
    pub schema_override: Option<Schema>,
}

pub struct DataFrameRow {
    pub fields: Vec<Option<Field>>,
}

impl DataFrameRow {
    pub fn field(&self, index: usize) -> Result<Option<Field>> {
        match self.fields.get(index) {
            Some(field) => Ok(field.clone()),
            None => Err(Error::InvalidArgument("Index out of bounds".to_string())),
        }
    }
}

/// Trait for reading tabular data from various data sources into a `DataFrame`
pub trait DataFrameReader {
    fn from_file<P: AsRef<Path>>(file_path: P) -> Result<Self>
    where
        Self: Sized;

    fn layer_names(&self) -> Result<Vec<String>>;
    fn schema(&mut self, options: &DataFrameOptions) -> Result<Schema>;
    //fn rows(&mut self, options: &DataFrameOptions, schema: &Schema) -> Result<impl Iterator<Item = impl DataFrameRow>>;
    fn iter_rows(&mut self, options: &DataFrameOptions) -> Result<Box<dyn Iterator<Item = DataFrameRow>>>;
}

// impl IntoIterator for Box<dyn DataFrameReader> {
//     type Item = DataFrameRow;
//     type IntoIter = Box<dyn Iterator<Item = Self::Item>>;

//     fn into_iter(mut self) -> Self::IntoIter {
//         self.iter_rows(&DataFrameOptions::default())
//             .unwrap_or_else(|_| Box::new(std::iter::empty()))
//     }
// }

/// Creates a `DataFrameReader` for the specified path based on the file extension.
pub fn create_dataframe_reader(path: &Path) -> Result<Box<dyn DataFrameReader>> {
    match vector::VectorFormat::guess_from_path(path) {
        #[cfg(feature = "vector-io-xlsx")]
        vector::VectorFormat::Xlsx => Ok(Box::new(vector::readers::XlsxReader::from_file(path)?)),

        #[cfg(feature = "gdal")]
        vector::VectorFormat::ShapeFile
        | vector::VectorFormat::GeoJson
        | vector::VectorFormat::GeoPackage
        | vector::VectorFormat::Csv
        | vector::VectorFormat::Tab
        | vector::VectorFormat::Parquet
        | vector::VectorFormat::Arrow => Ok(Box::new(vector::readers::GdalReader::from_file(path)?)),
        _ => Err(Error::Runtime(format!("Unsupported vector file type: {}", path.display()))),
    }
}

#[cfg(feature = "polars")]
pub mod polars {
    use polars::prelude::*;

    use crate::vector::dataframe::{DataFrameReader, Field};
    use crate::vector::{self, dataframe::DataFrameOptions};
    use crate::{Error, Result};
    use std::path::Path;

    /// Reads a `polars::frame::DataFrame` from the specified path using the provided options.
    pub fn read_dataframe(path: &Path, options: &DataFrameOptions) -> Result<polars::frame::DataFrame> {
        match vector::VectorFormat::guess_from_path(path) {
            #[cfg(feature = "vector-io-xlsx")]
            vector::VectorFormat::Xlsx => read_dataframe_with::<vector::readers::XlsxReader>(path, options),

            #[cfg(feature = "gdal")]
            vector::VectorFormat::ShapeFile
            | vector::VectorFormat::GeoJson
            | vector::VectorFormat::GeoPackage
            | vector::VectorFormat::Csv
            | vector::VectorFormat::Tab
            | vector::VectorFormat::Parquet
            | vector::VectorFormat::Arrow => read_dataframe_with::<vector::readers::GdalReader>(path, options),
            _ => Err(Error::Runtime(format!("Unsupported vector file type: {}", path.display()))),
        }
    }

    fn read_dataframe_with<R: DataFrameReader>(path: &Path, options: &DataFrameOptions) -> Result<polars::frame::DataFrame> {
        let mut reader = R::from_file(path)?;
        let schema = match &options.schema_override {
            Some(schema) => schema,
            None => &reader.schema(options)?,
        };

        let mut columns = vec![Vec::new(); schema.len()];
        for row in reader.iter_rows(options)? {
            for (index, column) in &mut columns.iter_mut().enumerate() {
                if let Some(field) = row.field(index)? {
                    column.push(match field {
                        Field::String(v) => AnyValue::StringOwned(v.into()),
                        Field::Integer(v) => AnyValue::Int64(v),
                        Field::Float(v) => AnyValue::Float64(v),
                        Field::Boolean(v) => AnyValue::Boolean(v),
                        Field::DateTime(v) => {
                            AnyValue::Datetime(v.and_utc().timestamp_nanos_opt().unwrap_or(0), TimeUnit::Nanoseconds, None)
                        }
                    });
                } else {
                    column.push(AnyValue::Null);
                }
            }
        }

        let mut df = polars::frame::DataFrame::default();
        for (column, field) in columns.into_iter().zip(&schema.fields) {
            let series = Series::from_any_values(field.name().into(), &column, true)?;
            df.with_column(Column::Series(series.into()))?;
        }

        Ok(df)
    }
}

#[cfg(test)]
#[cfg(all(feature = "vector-io-xlsx", feature = "polars"))]
mod tests {
    use super::*;
    use crate::Result;
    use path_macro::path;

    #[test]
    fn read_xlsx_dataframe() -> Result<()> {
        let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "data_types.xlsx");

        let options = DataFrameOptions::default();
        let df = polars::read_dataframe(&input_file, &options)?;
        assert_eq!(df.shape(), (5, 4));
        //dbg!(df);

        Ok(())
    }

    #[test]
    fn read_xlsx_dataframe_offset() -> Result<()> {
        let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "data_types_header_offset.xlsx");
        let mut options = DataFrameOptions {
            header_row: HeaderRow::Row(3),
            ..Default::default()
        };

        {
            options.schema_override = Some(Schema {
                fields: vec![
                    FieldInfo::new("Double Column".to_string(), FieldType::Float),
                    FieldInfo::new("Integer Column".to_string(), FieldType::Float),
                ],
            });

            let df = polars::read_dataframe(&input_file, &options)?;
            assert_eq!(df.shape(), (5, 2));
            assert_eq!(
                df.schema().get_at_index(0),
                Some((
                    &::polars::prelude::PlSmallStr::from_static("Double Column"),
                    &::polars::prelude::DataType::Float64
                ))
            );
            assert_eq!(
                df.schema().get_at_index(1),
                Some((
                    &::polars::prelude::PlSmallStr::from_static("Integer Column"),
                    &::polars::prelude::DataType::Float64
                ))
            );
        }

        {
            options.schema_override = Some(Schema {
                fields: vec![
                    FieldInfo::new("Integer Column".to_string(), FieldType::Float),
                    FieldInfo::new("Double Column".to_string(), FieldType::Float),
                ],
            });

            let df = polars::read_dataframe(&input_file, &options)?;
            assert_eq!(df.shape(), (5, 2));
            assert_eq!(
                df.schema().get_at_index(0),
                Some((
                    &::polars::prelude::PlSmallStr::from_static("Integer Column"),
                    &::polars::prelude::DataType::Float64
                ))
            );
            assert_eq!(
                df.schema().get_at_index(1),
                Some((
                    &::polars::prelude::PlSmallStr::from_static("Double Column"),
                    &::polars::prelude::DataType::Float64
                ))
            );
        }

        {
            options.schema_override = Some(Schema {
                fields: vec![FieldInfo::new("Double Column".to_string(), FieldType::Integer)],
            });

            let df = polars::read_dataframe(&input_file, &options)?;
            assert_eq!(df.shape(), (5, 1));
            assert_eq!(
                df.schema().get_at_index(0),
                Some((
                    &::polars::prelude::PlSmallStr::from_static("Double Column"),
                    &::polars::prelude::DataType::Int64 // The datatype was overridden to be integer
                ))
            );
            assert_eq!(
                df.column("Double Column")?.i64()?.into_iter().collect::<Vec<_>>(),
                vec![Some(12), None, Some(45), Some(89), Some(23)]
            );
        }

        Ok(())
    }

    #[test]
    #[cfg(feature = "gdal")]
    fn read_gdal_dataframe() -> Result<()> {
        use path_macro::path;

        // This test requires a CSV file or other GDAL-supported format
        // For now, we'll test the compilation and basic functionality
        let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "road.csv");

        let options = DataFrameOptions::default();
        let df = polars::read_dataframe(&input_file, &options)?;
        assert_eq!(df.shape(), (3, 3)); // Should have some rows or zero rows
        assert_eq!(
            df.schema().get_at_index(0),
            Some((
                &::polars::prelude::PlSmallStr::from_static("Pollutant"),
                &::polars::prelude::DataType::String
            ))
        );
        assert_eq!(
            df.schema().get_at_index(2),
            Some((
                &::polars::prelude::PlSmallStr::from_static("value"),
                &::polars::prelude::DataType::Float64
            ))
        );

        Ok(())
    }
}

use crate::vector::dataframe::{DataFrameOptions, DataFrameReader, DataFrameRow, Field, FieldInfo, FieldType, HeaderRow, Schema};
use crate::vector::fieldtype::{self, parse_bool_str};
use crate::{Error, Result};
use csv::{Reader, ReaderBuilder, StringRecord};
use std::io::BufReader;
use std::path::Path;

const DEFAULT_DATA_TYPE_DETECTION_ROWS: usize = 10;
const COLUMN_NAME_PREFIX: &str = "Field";

pub struct CsvReader {
    file_path: std::path::PathBuf,
}

impl CsvReader {
    fn create_reader(&self, has_headers: bool) -> Result<Reader<BufReader<std::fs::File>>> {
        let file = std::fs::File::open(&self.file_path)?;
        Ok(ReaderBuilder::new().has_headers(has_headers).from_reader(BufReader::new(file)))
    }

    fn infer_column_type(records: &[StringRecord], col_idx: usize) -> FieldType {
        let mut has_int = false;
        let mut has_float = false;
        let mut has_string = false;
        let mut has_bool = false;
        let mut has_date = false;

        for record in records {
            if let Some(cell) = record.get(col_idx) {
                let cell = cell.trim();

                if cell.is_empty() {
                    continue; // Skip empty cells
                }

                // Try to parse as boolean first
                if parse_bool_str(cell).is_some() {
                    has_bool = true;
                    continue;
                }

                // Try to parse as integer
                if cell.parse::<i64>().is_ok() {
                    has_int = true;
                    continue;
                }

                // Try to parse as float
                if cell.parse::<f64>().is_ok() {
                    has_float = true;
                    continue;
                }

                // Try to parse as date/datetime
                if fieldtype::parse_date_str(cell).is_some() {
                    has_date = true;
                    continue;
                }

                // Default to string
                has_string = true;
            }
        }

        // Determine the most appropriate type
        if has_string {
            FieldType::String
        } else if has_date {
            FieldType::DateTime
        } else if has_float {
            FieldType::Float
        } else if has_int {
            FieldType::Integer
        } else if has_bool {
            FieldType::Boolean
        } else {
            FieldType::String // Default fallback
        }
    }

    fn sample_records(&self, max_records: usize, has_headers: bool) -> Result<Vec<StringRecord>> {
        let mut reader = self.create_reader(has_headers)?;
        let mut records = Vec::new();

        for result in reader.records().take(max_records) {
            records.push(result?);
        }

        Ok(records)
    }
}

pub struct CsvRowIterator {
    reader: Reader<BufReader<std::fs::File>>,
    column_indices: Vec<usize>,
    field_types: Vec<FieldType>,
}

fn get_column_index_from_name(name: &str, max_columns: usize) -> Option<usize> {
    name.strip_prefix(COLUMN_NAME_PREFIX)
        .and_then(|s| s.parse::<usize>().ok())
        .and_then(|idx| if idx > 0 && idx <= max_columns { Some(idx - 1) } else { None }) // Convert to 0-based index
}

fn parse_column_indexes_from_names(fields: &[FieldInfo], max_columns: usize) -> Result<Vec<usize>> {
    fields
        .iter()
        .map(|f| f.name())
        .map(|name| get_column_index_from_name(name, max_columns).ok_or_else(|| Error::Runtime(format!("Invalid column name '{}'", name))))
        .collect()
}

impl CsvRowIterator {
    fn new(mut reader: Reader<BufReader<std::fs::File>>, schema: &Schema, has_headers: bool) -> Result<Self> {
        let column_indices = if has_headers {
            // CSV reader already handled headers, use them to map column names
            let header_record = reader.headers()?.clone();
            schema
                .fields
                .iter()
                .map(|f| {
                    header_record
                        .iter()
                        .position(|h| h == f.name())
                        .ok_or_else(|| Error::Runtime(format!("Column '{}' not found in headers", f.name())))
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            // No headers, use Field names to determine column indices
            parse_column_indexes_from_names(&schema.fields, reader.headers()?.len())?
        };

        let field_types: Vec<FieldType> = schema.fields.iter().map(|f| f.field_type()).collect();

        Ok(Self {
            reader,
            column_indices,
            field_types,
        })
    }
}

impl Iterator for CsvRowIterator {
    type Item = DataFrameRow;

    fn next(&mut self) -> Option<Self::Item> {
        let mut record = StringRecord::new();
        match self.reader.read_record(&mut record) {
            Ok(true) => {
                let fields: Vec<Result<Option<Field>>> = self
                    .column_indices
                    .iter()
                    .zip(self.field_types.iter())
                    .map(|(&col_idx, &field_type)| {
                        let value = record.get(col_idx).unwrap_or("");
                        Field::from_str(value, field_type)
                    })
                    .collect();

                Some(DataFrameRow { fields })
            }
            Ok(false) => None, // End of file
            Err(e) => {
                log::error!("Error reading CSV record: {e}");
                None
            }
        }
    }
}

impl DataFrameReader for CsvReader {
    fn from_file<P: AsRef<Path>>(file_path: P) -> Result<Self> {
        Ok(Self {
            file_path: file_path.as_ref().to_path_buf(),
        })
    }

    fn layer_names(&self) -> Result<Vec<String>> {
        // CSV files don't have multiple layers, return the filename
        Ok(vec![
            self.file_path.file_stem().and_then(|s| s.to_str()).unwrap_or("default").to_string(),
        ])
    }

    fn schema(&mut self, options: &DataFrameOptions) -> Result<Schema> {
        match options.header_row {
            HeaderRow::Row(idx) if idx > 0 => {
                return Err(Error::Runtime(
                    "CSV reader does not support header row offset greater than 0".to_string(),
                ));
            }
            _ => {}
        }

        let has_headers = options.header_row != HeaderRow::None;
        let mut reader = self.create_reader(has_headers)?;
        let mut fields = Vec::new();

        if has_headers {
            // Use the header row from csv crate
            let header_record = reader.headers()?;

            // Sample some records for type inference
            let sample_records = self.sample_records(DEFAULT_DATA_TYPE_DETECTION_ROWS, has_headers)?;

            for (col_idx, field_name) in header_record.iter().enumerate() {
                let field_type = Self::infer_column_type(&sample_records, col_idx);
                fields.push(FieldInfo::new(field_name.to_string(), field_type));
            }
        } else {
            // No headers, use generic column names
            let header_record = reader.headers()?; // This gets the first record as raw data
            let num_columns = header_record.len();

            // Sample records including the first row (since it's data, not headers)
            let sample_records = self.sample_records(DEFAULT_DATA_TYPE_DETECTION_ROWS, has_headers)?;

            for col_idx in 0..num_columns {
                let field_type = Self::infer_column_type(&sample_records, col_idx);
                fields.push(FieldInfo::new(format!("{COLUMN_NAME_PREFIX}{}", col_idx + 1), field_type));
            }
        }

        Ok(Schema { fields })
    }

    fn iter_rows(&mut self, options: &DataFrameOptions) -> Result<Box<dyn Iterator<Item = DataFrameRow>>> {
        match options.header_row {
            HeaderRow::Row(idx) if idx > 0 => {
                return Err(Error::Runtime(
                    "CSV reader does not support header row offset greater than 0".to_string(),
                ));
            }
            _ => {}
        }

        let schema = match &options.schema_override {
            Some(schema) => schema.clone(),
            None => self.schema(options)?,
        };

        let has_headers = options.header_row != HeaderRow::None;
        let reader = self.create_reader(has_headers)?;

        Ok(Box::new(CsvRowIterator::new(reader, &schema, has_headers)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::readers::readertests;

    #[test]
    fn read_csv_empty_sheet() -> Result<()> {
        readertests::read_table_empty_sheet::<CsvReader>("csv")
    }

    #[test]
    fn read_csv() -> Result<()> {
        readertests::read_table::<CsvReader>("csv")
    }

    #[test]
    fn read_csv_override_schema() -> Result<()> {
        readertests::read_table_override_schema::<CsvReader>("csv")
    }

    #[test]
    fn read_csv_sub_schema() -> Result<()> {
        readertests::read_table_sub_schema::<CsvReader>("csv")
    }

    #[test]
    fn read_csv_no_header() -> Result<()> {
        readertests::read_table_no_header::<CsvReader>("csv")
    }

    // CSV-specific tests for features not covered by shared tests
    #[test]
    fn read_csv_missing_data() -> Result<()> {
        // Test reading CSV file with missing data
        let input_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/road_missing_data.csv");

        let options = DataFrameOptions {
            header_row: HeaderRow::Row(0),
            ..Default::default()
        };

        let mut reader = CsvReader::from_file(input_file)?;

        // Test reading rows - check rows with missing data
        let mut rows_iter = reader.iter_rows(&options)?;

        // Skip first row (NO2,A_PublicTransport,10.0)
        rows_iter.next();

        // Second row: PM10,A_PublicTransport, (missing value)
        if let Some(row) = rows_iter.next() {
            assert_eq!(row.field(0)?, Some(Field::String("PM10".into())));
            assert_eq!(row.field(1)?, Some(Field::String("A_PublicTransport".into())));
            assert_eq!(row.field(2)?, None); // Missing value
        }

        // Third row: ,B_RoadTransport,11.0 (missing pollutant)
        if let Some(row) = rows_iter.next() {
            assert_eq!(row.field(0)?, None); // Missing pollutant
            assert_eq!(row.field(1)?, Some(Field::String("B_RoadTransport".into())));
            assert_eq!(row.field(2)?, Some(Field::Float(11.0)));
        }

        Ok(())
    }

    #[test]
    fn read_csv_header_offset_error() -> Result<()> {
        let input_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/road.csv");

        let options = DataFrameOptions {
            header_row: HeaderRow::Row(1), // This should error
            ..Default::default()
        };

        let mut reader = CsvReader::from_file(input_file)?;

        // Should return an error for header row offset > 0
        assert!(reader.schema(&options).is_err());
        assert!(reader.iter_rows(&options).is_err());

        Ok(())
    }

    #[test]
    fn test_csv_factory_function() -> Result<()> {
        use crate::vector::dataframe::create_dataframe_reader;

        let input_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/road.csv");

        // Test that the factory function creates the correct reader type
        let mut reader = create_dataframe_reader(&input_file)?;
        let layer_names = reader.layer_names()?;
        assert_eq!(layer_names.len(), 1);
        assert_eq!(layer_names[0], "road");

        let schema = reader.schema(&DataFrameOptions::default())?;
        assert_eq!(schema.len(), 3);
        assert_eq!(schema.fields[0].name(), "Pollutant");

        Ok(())
    }
}

use crate::vector::dataframe::{DataFrameOptions, DataFrameReader, DataFrameRow, Field, FieldInfo, FieldType, HeaderRow, Schema};
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
    fn create_reader(&self) -> Result<Reader<BufReader<std::fs::File>>> {
        let file = std::fs::File::open(&self.file_path)?;
        Ok(ReaderBuilder::new()
            .has_headers(false) // We'll handle headers manually
            .from_reader(BufReader::new(file)))
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
                if cell.eq_ignore_ascii_case("true") || cell.eq_ignore_ascii_case("false") || cell == "1" || cell == "0" {
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
                if chrono::NaiveDateTime::parse_from_str(cell, "%Y-%m-%d %H:%M:%S").is_ok()
                    || chrono::NaiveDateTime::parse_from_str(cell, "%Y-%m-%d").is_ok()
                    || chrono::NaiveDate::parse_from_str(cell, "%Y-%m-%d").is_ok()
                {
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

    fn sample_records(&self, max_records: usize) -> Result<Vec<StringRecord>> {
        let mut reader = self.create_reader()?;
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
    skip_header: bool,
    first_row_read: bool,
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
    fn new(mut reader: Reader<BufReader<std::fs::File>>, schema: &Schema, skip_header: bool) -> Result<Self> {
        let column_indices = if skip_header {
            // Read the header row to get column names
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
            // Use field names to determine column indices
            let first_record = reader.headers()?;
            parse_column_indexes_from_names(&schema.fields, first_record.len())?
        };

        let field_types: Vec<FieldType> = schema.fields.iter().map(|f| f.field_type()).collect();

        Ok(Self {
            reader,
            column_indices,
            field_types,
            skip_header,
            first_row_read: false,
        })
    }

    fn convert_string_to_field(value: &str, expected_type: FieldType) -> Result<Option<Field>> {
        let value = value.trim();

        if value.is_empty() {
            return Ok(None);
        }

        match expected_type {
            FieldType::String => Ok(Some(Field::String(value.to_string()))),
            FieldType::Integer => value
                .parse::<i64>()
                .map(|i| Some(Field::Integer(i)))
                .map_err(|e| Error::Runtime(format!("Failed to parse integer '{}': {}", value, e))),
            FieldType::Float => value
                .parse::<f64>()
                .map(|f| Some(Field::Float(f)))
                .map_err(|e| Error::Runtime(format!("Failed to parse float '{}': {}", value, e))),
            FieldType::Boolean => match value.to_lowercase().as_str() {
                "true" | "1" | "yes" | "y" => Ok(Some(Field::Boolean(true))),
                "false" | "0" | "no" | "n" => Ok(Some(Field::Boolean(false))),
                _ => Err(Error::Runtime(format!("Failed to parse boolean '{}'", value))),
            },
            FieldType::DateTime => {
                // Try multiple datetime formats
                if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
                    Ok(Some(Field::DateTime(dt)))
                } else if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(value, "%Y-%m-%d") {
                    Ok(Some(Field::DateTime(dt)))
                } else if let Ok(date) = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d") {
                    Ok(Some(Field::DateTime(date.and_hms_opt(0, 0, 0).unwrap())))
                } else {
                    Err(Error::Runtime(format!("Failed to parse datetime '{}'", value)))
                }
            }
        }
    }
}

impl Iterator for CsvRowIterator {
    type Item = DataFrameRow;

    fn next(&mut self) -> Option<Self::Item> {
        // Skip header row if needed
        if self.skip_header && !self.first_row_read {
            if self.reader.read_record(&mut StringRecord::new()).is_ok() {
                self.first_row_read = true;
            } else {
                return None;
            }
        }

        let mut record = StringRecord::new();
        match self.reader.read_record(&mut record) {
            Ok(true) => {
                let fields: Vec<Option<Field>> = self
                    .column_indices
                    .iter()
                    .zip(self.field_types.iter())
                    .map(|(&col_idx, &field_type)| {
                        let value = record.get(col_idx).unwrap_or("");
                        Self::convert_string_to_field(value, field_type).unwrap_or(None)
                    })
                    .collect();

                Some(DataFrameRow { fields })
            }
            Ok(false) | Err(_) => None,
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
        let mut reader = self.create_reader()?;
        let mut fields = Vec::new();

        if options.header_row == HeaderRow::None {
            // No header row, use generic column names
            let first_record = reader.headers()?;
            let num_columns = first_record.len();

            // Sample some records for type inference
            let sample_records = self.sample_records(DEFAULT_DATA_TYPE_DETECTION_ROWS + 1)?;

            for col_idx in 0..num_columns {
                let field_type = Self::infer_column_type(&sample_records, col_idx);
                fields.push(FieldInfo::new(format!("{COLUMN_NAME_PREFIX}{}", col_idx + 1), field_type));
            }
        } else {
            // Read the header row
            let header_record = reader.headers()?;

            // Sample some records for type inference (skip header)
            let sample_records = self.sample_records(DEFAULT_DATA_TYPE_DETECTION_ROWS + 1)?;
            let data_records = if sample_records.len() > 1 { &sample_records[1..] } else { &[] };

            for (col_idx, field_name) in header_record.iter().enumerate() {
                let field_type = Self::infer_column_type(data_records, col_idx);
                fields.push(FieldInfo::new(field_name.to_string(), field_type));
            }
        }

        Ok(Schema { fields })
    }

    fn iter_rows(&mut self, options: &DataFrameOptions) -> Result<Box<dyn Iterator<Item = DataFrameRow>>> {
        let schema = match &options.schema_override {
            Some(schema) => schema.clone(),
            None => self.schema(options)?,
        };

        let reader = self.create_reader()?;
        let skip_header = options.header_row != HeaderRow::None;

        Ok(Box::new(CsvRowIterator::new(reader, &schema, skip_header)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::readers::readertests;

    // #[test]
    // fn read_csv() -> Result<()> {
    //     // Test reading schema from CSV file
    //     let input_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/road.csv");

    //     let options = DataFrameOptions {
    //         header_row: HeaderRow::Row(0),
    //         ..Default::default()
    //     };

    //     let mut reader = CsvReader::from_file(input_file)?;
    //     let schema = reader.schema(&options)?;

    //     // Expected column names from the CSV file
    //     let expected_columns = [
    //         FieldInfo::new("Pollutant".into(), FieldType::String),
    //         FieldInfo::new("Sector".into(), FieldType::String),
    //         FieldInfo::new("value".into(), FieldType::Float),
    //     ];

    //     assert_eq!(schema.len(), expected_columns.len());
    //     for (field_info, expected) in schema.fields.iter().zip(expected_columns.iter()) {
    //         assert_eq!(field_info, expected);
    //     }

    //     // Test reading rows - just check the first row
    //     let mut rows_iter = reader.iter_rows(&options)?;
    //     if let Some(row) = rows_iter.next() {
    //         assert_eq!(row.field(0)?, Some(Field::String("NO2".into())));
    //         assert_eq!(row.field(1)?, Some(Field::String("A_PublicTransport".into())));
    //         assert_eq!(row.field(2)?, Some(Field::Float(10.0)));
    //     }

    //     Ok(())
    // }

    // #[test]
    // fn read_csv_missing_data() -> Result<()> {
    //     // Test reading CSV file with missing data
    //     let input_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/road_missing_data.csv");

    //     let options = DataFrameOptions {
    //         header_row: HeaderRow::Row(0),
    //         ..Default::default()
    //     };

    //     let mut reader = CsvReader::from_file(input_file)?;

    //     // Test reading rows - check rows with missing data
    //     let mut rows_iter = reader.iter_rows(&options)?;

    //     // Skip first row (NO2,A_PublicTransport,10.0)
    //     rows_iter.next();

    //     // Second row: PM10,A_PublicTransport, (missing value)
    //     if let Some(row) = rows_iter.next() {
    //         assert_eq!(row.field(0)?, Some(Field::String("PM10".into())));
    //         assert_eq!(row.field(1)?, Some(Field::String("A_PublicTransport".into())));
    //         assert_eq!(row.field(2)?, None); // Missing value
    //     }

    //     // Third row: ,B_RoadTransport,11.0 (missing pollutant)
    //     if let Some(row) = rows_iter.next() {
    //         assert_eq!(row.field(0)?, None); // Missing pollutant
    //         assert_eq!(row.field(1)?, Some(Field::String("B_RoadTransport".into())));
    //         assert_eq!(row.field(2)?, Some(Field::Float(11.0)));
    //     }

    //     Ok(())
    // }

    // #[test]
    // fn read_csv_no_header() -> Result<()> {
    //     // For this test, we'll treat the road.csv as if it has no header
    //     let input_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/road.csv");

    //     let options = DataFrameOptions {
    //         header_row: HeaderRow::None,
    //         ..Default::default()
    //     };

    //     let mut reader = CsvReader::from_file(input_file)?;
    //     let schema = reader.schema(&options)?;

    //     // Expected column names when no header is present
    //     let expected_columns = [
    //         FieldInfo::new("Field1".into(), FieldType::String),
    //         FieldInfo::new("Field2".into(), FieldType::String),
    //         FieldInfo::new("Field3".into(), FieldType::String), // "value" will be treated as string when header row is data
    //     ];

    //     assert_eq!(schema.len(), expected_columns.len());
    //     for (field_info, expected) in schema.fields.iter().zip(expected_columns.iter()) {
    //         assert_eq!(field_info, expected);
    //     }

    //     // Test reading rows - should include the header row as data
    //     let mut rows_iter = reader.iter_rows(&options)?;
    //     if let Some(row) = rows_iter.next() {
    //         assert_eq!(row.field(0)?, Some(Field::String("Pollutant".into()))); // Header row treated as data
    //         assert_eq!(row.field(1)?, Some(Field::String("Sector".into())));
    //         assert_eq!(row.field(2)?, Some(Field::String("value".into())));
    //     }

    //     Ok(())
    // }

    // #[test]
    // fn read_csv_sub_schema() -> Result<()> {
    //     let input_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/road.csv");

    //     let mut reader = CsvReader::from_file(input_file)?;
    //     let options = DataFrameOptions {
    //         schema_override: Some(reader.schema(&DataFrameOptions::default())?.subselection(&["Pollutant", "value"])),
    //         ..Default::default()
    //     };

    //     let mut rows_iter = reader.iter_rows(&options)?;
    //     let row = rows_iter.next().unwrap();

    //     assert_eq!(row.field(0)?, Some(Field::String("NO2".into())));
    //     assert_eq!(row.field(1)?, Some(Field::Float(10.0)));
    //     assert!(row.field(2).is_err()); // Should be out of bounds

    //     Ok(())
    // }

    // // Common test suite tests that work with CSV format
    // #[test]
    // fn read_csv_common() -> Result<()> {
    //     // Test basic CSV reading using the road.csv file
    //     let input_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/road.csv");

    //     let options = DataFrameOptions {
    //         header_row: HeaderRow::Row(0),
    //         ..Default::default()
    //     };

    //     let mut reader = CsvReader::from_file(input_file)?;
    //     let schema = reader.schema(&options)?;

    //     // Verify schema
    //     assert_eq!(schema.len(), 3);
    //     assert_eq!(schema.fields[0].name(), "Pollutant");
    //     assert_eq!(schema.fields[1].name(), "Sector");
    //     assert_eq!(schema.fields[2].name(), "value");

    //     // Test reading all rows
    //     let rows: Vec<_> = reader.iter_rows(&options)?.collect();
    //     assert_eq!(rows.len(), 3);

    //     // Verify first row
    //     assert_eq!(rows[0].field(0)?, Some(Field::String("NO2".into())));
    //     assert_eq!(rows[0].field(1)?, Some(Field::String("A_PublicTransport".into())));
    //     assert_eq!(rows[0].field(2)?, Some(Field::Float(10.0)));

    //     Ok(())
    // }

    // #[test]
    // fn read_csv_layer_names() -> Result<()> {
    //     let input_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/road.csv");
    //     let reader = CsvReader::from_file(input_file)?;

    //     let layer_names = reader.layer_names()?;
    //     assert_eq!(layer_names.len(), 1);
    //     assert_eq!(layer_names[0], "road");

    //     Ok(())
    // }

    // #[test]
    // fn test_csv_factory_function() -> Result<()> {
    //     let input_file = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/road.csv");

    //     // Test that the factory function creates the correct reader type
    //     let mut reader = create_dataframe_reader(&input_file)?;
    //     let layer_names = reader.layer_names()?;
    //     assert_eq!(layer_names.len(), 1);
    //     assert_eq!(layer_names[0], "road");

    //     let schema = reader.schema(&DataFrameOptions::default())?;
    //     assert_eq!(schema.len(), 3);
    //     assert_eq!(schema.fields[0].name(), "Pollutant");

    //     Ok(())
    // }

    #[test]
    fn read_csv_empty_sheet() -> Result<()> {
        readertests::read_table_empty_sheet::<CsvReader>("csv")
    }

    #[test]
    fn read_csv() -> Result<()> {
        readertests::read_table::<CsvReader>("csv")
    }

    #[test]
    fn read_csv_sub_schema() -> Result<()> {
        readertests::read_table_sub_schema::<CsvReader>("csv")
    }

    #[test]
    fn read_csv_no_header() -> Result<()> {
        readertests::read_table_no_header::<CsvReader>("csv")
    }
}

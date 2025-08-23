use crate::vector::dataframe::{DataFrameOptions, DataFrameReader, DataFrameRow, Field, FieldInfo, FieldType, HeaderRow, Schema};
use crate::{Error, Result};
use calamine::{Data, Reader, Xlsx, open_workbook};
use std::io::BufReader;
use std::path::Path;

const DEFAULT_DATA_TYPE_DETECTION_ROWS: usize = 10;
const COLUMN_NAME_PREFIX: &str = "Column";

pub struct XlsxReader {
    workbook: Xlsx<BufReader<std::fs::File>>,
}

impl XlsxReader {
    fn sheet_name(&self, options: &DataFrameOptions) -> Result<String> {
        Ok(if let Some(layer) = &options.layer {
            layer.clone()
        } else {
            self.workbook
                .sheet_names()
                .first()
                .ok_or_else(|| crate::Error::Runtime("No sheets found in workbook".to_string()))?
                .clone()
        })
    }

    fn infer_column_type(range: &calamine::Range<Data>, col_idx: usize, start_row: usize, num_rows: usize) -> FieldType {
        let mut has_int = false;
        let mut has_float = false;
        let mut has_string = false;
        let mut has_bool = false;
        let mut has_date = false;

        // Sample up to 10 rows to infer type
        let sample_size = std::cmp::min(num_rows, range.height() - start_row);

        for row_idx in start_row..std::cmp::min(start_row + sample_size, range.height()) {
            if let Some(cell) = range.get((row_idx, col_idx)) {
                match cell {
                    Data::Int(_) => has_int = true,
                    Data::Float(val) => {
                        if val.fract() == 0.0 {
                            has_int = true;
                        } else {
                            has_float = true;
                        }
                    }
                    Data::String(_) => has_string = true,
                    Data::Bool(_) => has_bool = true,
                    Data::Empty => {} // Skip empty cells
                    Data::DateTime(_) | Data::DateTimeIso(_) | Data::DurationIso(_) => {
                        has_date = true;
                    }
                    Data::Error(_) => {
                        has_string = true; // Treat other types as string
                    }
                }
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
}

pub struct XlsxRow {
    fields: Vec<Option<Field>>,
}

impl DataFrameRow for XlsxRow {
    fn field(&self, index: usize) -> Result<Option<Field>> {
        match self.fields.get(index) {
            Some(field) => Ok(field.clone()),
            None => Err(Error::Runtime("Index out of bounds".to_string())),
        }
    }
}

pub struct XlsxRowIterator {
    range: calamine::Range<Data>,
    current: usize,
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

impl XlsxRowIterator {
    fn new(range: calamine::Range<Data>, schema: &Schema, skip_header: bool) -> Result<Self> {
        let column_indices = if skip_header {
            let headers = range.headers().expect("Range should have headers");
            schema
                .fields
                .iter()
                .map(|f| {
                    headers
                        .iter()
                        .position(|h| h == f.name())
                        .ok_or_else(|| Error::Runtime(format!("Column '{}' not found in headers", f.name())))
                })
                .collect::<Result<Vec<_>>>()?
        } else {
            parse_column_indexes_from_names(&schema.fields, range.width())?
        };

        let field_types: Vec<FieldType> = schema.fields.iter().map(|f| f.field_type()).collect();
        Ok(Self {
            range,
            current: if skip_header { 1 } else { 0 },
            column_indices,
            field_types,
        })
    }

    fn convert_data_to_field(data: &Data, expected_type: FieldType) -> Result<Option<Field>> {
        match data {
            Data::String(s) => Ok(Some(Field::String(s.clone()))),
            Data::Int(i) => Ok(Some(Field::Integer(*i))),
            Data::Float(f) => {
                // Convert float to integer if schema expects integer and value is whole number
                if expected_type == FieldType::Integer {
                    Ok(Some(Field::Integer(*f as i64)))
                } else {
                    Ok(Some(Field::Float(*f)))
                }
            }
            Data::Bool(b) => Ok(Some(Field::Boolean(*b))),
            Data::Empty => Ok(None),
            Data::Error(e) => Err(Error::Runtime(format!("Cell contains error {e}"))),
            Data::DateTime(dt) => {
                // Convert ExcelDateTime to NaiveDateTime
                let naive_dt = chrono::NaiveDate::from_ymd_opt(1900, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap()
                    + chrono::Duration::days((dt.as_f64() - 2.0) as i64);
                Ok(Some(Field::DateTime(naive_dt)))
            }
            Data::DateTimeIso(_) | Data::DurationIso(_) => {
                // For now, convert to string representation
                Ok(Some(Field::String(format!("{:?}", data))))
            }
        }
    }
}

impl Iterator for XlsxRowIterator {
    type Item = XlsxRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.range.height() {
            None
        } else {
            let fields: Vec<Option<Field>> = self
                .column_indices
                .iter()
                .zip(self.field_types.iter())
                .map(|(&col_idx, &field_type)| {
                    let data = self.range.get((self.current, col_idx)).unwrap_or(&Data::Empty);
                    Self::convert_data_to_field(data, field_type).unwrap_or(None)
                })
                .collect();
            self.current += 1;
            Some(XlsxRow { fields })
        }
    }
}

impl DataFrameReader for XlsxReader {
    fn from_file<P: AsRef<Path>>(file_path: P) -> Result<Self> {
        Ok(Self {
            workbook: open_workbook(file_path).map_err(|e| crate::Error::CalamineError(calamine::Error::Xlsx(e)))?,
        })
    }

    fn layer_names(&self) -> Result<Vec<String>> {
        Ok(self.workbook.sheet_names())
    }

    fn schema(&mut self, options: &DataFrameOptions) -> Result<Schema> {
        let header_row = match options.header_row {
            HeaderRow::Row(idx) => calamine::HeaderRow::Row(idx as u32),
            HeaderRow::None | HeaderRow::Auto => calamine::HeaderRow::FirstNonEmptyRow,
        };

        let sheet_name = self.sheet_name(options)?;

        let range = self
            .workbook
            .with_header_row(header_row)
            .worksheet_range(&sheet_name)
            .map_err(|e| crate::Error::CalamineError(calamine::Error::Xlsx(e)))?;

        let mut fields = Vec::new();

        if options.header_row == HeaderRow::None {
            // No header row, use generic column names
            if let Some((start_row, _col)) = range.start()
                && range.height() > start_row as usize
            {
                let num_columns = range.width();
                for col_idx in 0..num_columns {
                    let field_type = Self::infer_column_type(&range, col_idx, start_row as usize, DEFAULT_DATA_TYPE_DETECTION_ROWS);
                    fields.push(FieldInfo::new(format!("{COLUMN_NAME_PREFIX}{}", col_idx + 1), field_type));
                }
            }
            return Ok(Schema { fields });
        } else if let Some(header_row) = range.headers() {
            for (col_idx, field_name) in header_row.into_iter().enumerate() {
                // Determine field type by examining the data in the column

                let field_type = if let Some((row, _col)) = range.start() {
                    Self::infer_column_type(&range, col_idx, row as usize + 1, DEFAULT_DATA_TYPE_DETECTION_ROWS)
                } else {
                    FieldType::String
                };

                fields.push(FieldInfo::new(field_name, field_type));
            }
        }

        Ok(Schema { fields })
    }

    fn rows(&mut self, options: &DataFrameOptions, schema: &Schema) -> Result<impl Iterator<Item = impl DataFrameRow>> {
        let header_row = match options.header_row {
            HeaderRow::Row(idx) => calamine::HeaderRow::Row(idx as u32),
            HeaderRow::None | HeaderRow::Auto => calamine::HeaderRow::FirstNonEmptyRow,
        };

        let sheet_name = self.sheet_name(options)?;
        let range = self
            .workbook
            .with_header_row(header_row)
            .worksheet_range(&sheet_name)
            .map_err(|e| crate::Error::CalamineError(calamine::Error::Xlsx(e)))?;

        XlsxRowIterator::new(range, schema, options.header_row != HeaderRow::None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::dataframe::{Field, FieldInfo};
    use path_macro::path;

    #[test]
    fn read_xlsx_empty_sheet() -> Result<()> {
        // Test reading schema from Excel file with specific worksheet and header row
        let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "empty_sheet.xlsx");

        let options = DataFrameOptions {
            layer: Some("VERBR_EF_ID".to_string()),
            header_row: HeaderRow::Row(0),
        };

        let mut reader = XlsxReader::from_file(input_file)?;
        let schema = reader.schema(&options)?;

        // Expected column names from the Excel file
        let expected_columns = vec![
            FieldInfo::new("VITO_installatieID".into(), FieldType::String),
            FieldInfo::new("Jaar".into(), FieldType::String),
            FieldInfo::new("Type".into(), FieldType::String),
            FieldInfo::new("Substantie".into(), FieldType::String),
            FieldInfo::new("EF".into(), FieldType::String),
            FieldInfo::new("Eenheid (NG)".into(), FieldType::String),
            FieldInfo::new("Tag".into(), FieldType::String),
            FieldInfo::new("CRF/NFR-sector".into(), FieldType::String),
            FieldInfo::new("Categorie".into(), FieldType::String),
        ];

        assert_eq!(schema.len(), expected_columns.len());
        for (field_info, expected) in schema.fields.iter().zip(expected_columns.iter()) {
            assert_eq!(field_info, expected);
        }
        Ok(())
    }

    #[test]
    fn read_xlsx() -> Result<()> {
        // Test reading schema from Excel file with specific worksheet and header row
        let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "data_types.xlsx");

        let options = DataFrameOptions {
            layer: None,
            header_row: HeaderRow::Row(0),
        };

        let mut reader = XlsxReader::from_file(input_file)?;
        let schema = reader.schema(&options)?;

        // Expected column names from the Excel file
        let expected_columns = [
            FieldInfo::new("String Column".into(), FieldType::String),
            FieldInfo::new("Double Column".into(), FieldType::Float),
            FieldInfo::new("Integer Column".into(), FieldType::Integer),
            FieldInfo::new("Date Column".into(), FieldType::DateTime),
        ];

        assert_eq!(schema.len(), expected_columns.len());
        for (field_info, expected) in schema.fields.iter().zip(expected_columns.iter()) {
            assert_eq!(field_info, expected);
        }

        // Test reading rows - just check the first row
        let mut rows_iter = reader.rows(&options, &schema)?;
        if let Some(row) = rows_iter.next() {
            assert_eq!(row.field(0)?, Some(Field::String("Alice".into())));
            assert_eq!(row.field(1)?, Some(Field::Float(12.34)));
        }

        Ok(())
    }

    #[test]
    fn read_xlsx_sub_schema() -> Result<()> {
        // Test reading schema from Excel file with specific worksheet and header row
        let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "data_types.xlsx");

        let options = DataFrameOptions::default();
        let mut reader = XlsxReader::from_file(input_file)?;
        let schema = reader
            .schema(&DataFrameOptions::default())?
            .subselection(&["String Column", "Integer Column"]);

        let mut rows_iter = reader.rows(&options, &schema)?;
        let row = rows_iter.next().unwrap();

        assert_eq!(row.field(0)?, Some(Field::String("Alice".into())));
        assert_eq!(row.field(1)?, Some(Field::Integer(42)));
        assert!(row.field(2).is_err());

        Ok(())
    }

    #[test]
    fn read_xlsx_header_offset() -> Result<()> {
        // Test reading schema from Excel file with specific worksheet and header row
        let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "data_types_header_offset.xlsx");

        let options = DataFrameOptions {
            layer: None,
            header_row: HeaderRow::Row(3),
        };

        let mut reader = XlsxReader::from_file(input_file)?;
        let schema = reader.schema(&options)?;

        // Expected column names from the Excel file
        let expected_columns = [
            FieldInfo::new("String Column".into(), FieldType::String),
            FieldInfo::new("Double Column".into(), FieldType::Float),
            FieldInfo::new("Integer Column".into(), FieldType::Integer),
            FieldInfo::new("Date Column".into(), FieldType::DateTime),
        ];

        assert_eq!(schema.len(), expected_columns.len());
        for (field_info, expected) in schema.fields.iter().zip(expected_columns.iter()) {
            assert_eq!(field_info, expected);
        }

        // Test reading rows - just check the first row
        if let Some(row) = reader.rows(&options, &schema)?.next() {
            assert_eq!(row.field(0)?, Some(Field::String("Alice".into())));
            assert_eq!(row.field(1)?, Some(Field::Float(12.34)));
        }

        {
            // Invalid column name
            let schema = Schema {
                fields: vec![FieldInfo::new("Strang Column".into(), FieldType::String)],
            };
            assert!(reader.rows(&options, &schema).is_err());
        }

        Ok(())
    }

    #[test]
    fn read_xlsx_no_header() -> Result<()> {
        let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "data_types_no_header.xlsx");

        let options = DataFrameOptions {
            layer: None,
            header_row: HeaderRow::None,
        };

        let mut reader = XlsxReader::from_file(input_file)?;
        let schema = reader.schema(&options)?;

        // Expected column names from the Excel file
        let expected_columns = [
            FieldInfo::new("Column1".into(), FieldType::String),
            FieldInfo::new("Column2".into(), FieldType::Float),
            FieldInfo::new("Column3".into(), FieldType::Integer),
            FieldInfo::new("Column4".into(), FieldType::DateTime),
        ];

        assert_eq!(schema.len(), expected_columns.len());
        for (field_info, expected) in schema.fields.iter().zip(expected_columns.iter()) {
            assert_eq!(field_info, expected);
        }

        // Test reading rows - just check the first row
        if let Some(row) = reader.rows(&options, &schema)?.next() {
            assert_eq!(row.field(0)?, Some(Field::String("Alice".into())));
            assert_eq!(row.field(1)?, Some(Field::Float(12.34)));
            assert_eq!(row.field(2)?, Some(Field::Integer(42)));
        }

        // Test reading rows - just check the first row
        let schema = schema.subselection(&["Column2", "Column3"]);
        if let Some(row) = reader.rows(&options, &schema)?.nth(2) {
            assert_eq!(row.field(0)?, Some(Field::Float(45.67)));
            assert_eq!(row.field(1)?, Some(Field::Integer(7)));
            assert!(row.field(2).is_err());
        }

        {
            // Auto generated column indexes start a 1
            let schema = Schema {
                fields: vec![FieldInfo::new("Column0".into(), FieldType::String)],
            };
            assert!(reader.rows(&options, &schema).is_err());
        }

        {
            // Column index too big
            let schema = Schema {
                fields: vec![FieldInfo::new("Column5".into(), FieldType::String)],
            };
            assert!(reader.rows(&options, &schema).is_err());
        }

        Ok(())
    }
}

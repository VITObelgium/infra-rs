use crate::vector::dataframe::{DataFrameOptions, DataFrameReader, DataFrameRow, Field, FieldInfo, FieldType, HeaderRow, Schema};
use crate::{Error, Result};
use calamine::{Data, Reader, Xlsx, open_workbook};
use std::io::BufReader;
use std::path::Path;

const DEFAULT_DATA_TYPE_DETECTION_ROWS: usize = 10;

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

impl XlsxRowIterator {
    fn new(range: calamine::Range<Data>, schema: &Schema) -> Self {
        let headers = range.headers().expect("Range should have headers");
        let column_indices: Vec<usize> = schema
            .fields
            .iter()
            .map(|f| headers.iter().position(|h| h == f.name()).unwrap_or(0))
            .collect();
        let field_types: Vec<FieldType> = schema.fields.iter().map(|f| f.field_type()).collect();
        Self {
            range,
            current: 1, // Start after header row
            column_indices,
            field_types,
        }
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
            Some(HeaderRow::Row(idx)) => calamine::HeaderRow::Row(idx as u32),
            Some(HeaderRow::None) | None => calamine::HeaderRow::FirstNonEmptyRow,
        };

        let sheet_name = self.sheet_name(options)?;

        let range = self
            .workbook
            .with_header_row(header_row)
            .worksheet_range(&sheet_name)
            .map_err(|e| crate::Error::CalamineError(calamine::Error::Xlsx(e)))?;

        let mut fields = Vec::new();

        if let Some(header_row) = range.headers() {
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
            Some(HeaderRow::Row(idx)) => calamine::HeaderRow::Row(idx as u32),
            Some(HeaderRow::None) | None => calamine::HeaderRow::FirstNonEmptyRow,
        };

        let sheet_name = self.sheet_name(options)?;
        let range = self
            .workbook
            .with_header_row(header_row)
            .worksheet_range(&sheet_name)
            .map_err(|e| crate::Error::CalamineError(calamine::Error::Xlsx(e)))?;

        Ok(XlsxRowIterator::new(range, schema))
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
            header_row: Some(HeaderRow::Row(0)),
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
            header_row: Some(HeaderRow::Row(0)),
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
            header_row: Some(HeaderRow::Row(3)),
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
}

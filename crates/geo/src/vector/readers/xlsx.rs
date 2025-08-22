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
    pub fn new<P: AsRef<Path>>(file_path: P) -> Result<Self> {
        Ok(Self {
            workbook: open_workbook(file_path).map_err(|e| crate::Error::CalamineError(calamine::Error::Xlsx(e)))?,
        })
    }

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
    cells: Vec<Data>,
}

impl DataFrameRow for XlsxRow {
    fn field(&self, index: usize) -> Result<Option<Field>> {
        match self.cells.get(index) {
            Some(Data::String(s)) => Ok(Some(Field::String(s.clone()))),
            Some(Data::Int(i)) => Ok(Some(Field::Integer(*i))),
            Some(Data::Float(f)) => Ok(Some(Field::Float(*f))),
            Some(Data::Bool(b)) => Ok(Some(Field::Boolean(*b))),
            Some(Data::Empty) => Ok(None),
            None => Err(Error::Runtime("Index out of bounds".to_string())),
            Some(Data::Error(e)) => Err(Error::Runtime(format!("Cell contains error {e}"))),
            Some(Data::DateTime(dt)) => {
                // Convert ExcelDateTime to NaiveDateTime
                let naive_dt = chrono::NaiveDate::from_ymd_opt(1900, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap()
                    + chrono::Duration::days((dt.as_f64() - 2.0) as i64);
                Ok(Some(Field::DateTime(naive_dt)))
            }
            Some(Data::DateTimeIso(_) | Data::DurationIso(_)) => {
                // For now, convert to string representation
                Ok(Some(Field::String(format!("{:?}", self.cells[index]))))
            }
        }
    }
}

pub struct XlsxRowIterator {
    range: calamine::Range<Data>,
    current: usize,
}

impl XlsxRowIterator {
    fn new(range: calamine::Range<Data>) -> Self {
        Self { range, current: 1 } // Start after header row
    }
}

impl Iterator for XlsxRowIterator {
    type Item = XlsxRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.range.height() {
            None
        } else {
            let row_data: Vec<Data> = (0..self.range.width())
                .map(|col| self.range.get((self.current, col)).cloned().unwrap_or(Data::Empty))
                .collect();
            self.current += 1;
            Some(XlsxRow { cells: row_data })
        }
    }
}

impl DataFrameReader for XlsxReader {
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

                fields.push(FieldInfo::new(field_name, field_type, col_idx));
            }
        }

        Ok(Schema { fields })
    }

    fn rows(&mut self, options: &DataFrameOptions, _schema: &Schema) -> Result<impl Iterator<Item = impl DataFrameRow>> {
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

        Ok(XlsxRowIterator::new(range))
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

        let mut reader = XlsxReader::new(input_file)?;
        let schema = reader.schema(&options)?;

        // Expected column names from the Excel file
        let expected_columns = vec![
            FieldInfo::new("VITO_installatieID".into(), FieldType::String, 0),
            FieldInfo::new("Jaar".into(), FieldType::String, 1),
            FieldInfo::new("Type".into(), FieldType::String, 2),
            FieldInfo::new("Substantie".into(), FieldType::String, 3),
            FieldInfo::new("EF".into(), FieldType::String, 4),
            FieldInfo::new("Eenheid (NG)".into(), FieldType::String, 5),
            FieldInfo::new("Tag".into(), FieldType::String, 6),
            FieldInfo::new("CRF/NFR-sector".into(), FieldType::String, 7),
            FieldInfo::new("Categorie".into(), FieldType::String, 8),
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

        let mut reader = XlsxReader::new(input_file)?;
        let schema = reader.schema(&options)?;

        // Expected column names from the Excel file
        let expected_columns = [
            FieldInfo::new("String Column".into(), FieldType::String, 0),
            FieldInfo::new("Double Column".into(), FieldType::Float, 1),
            FieldInfo::new("Integer Column".into(), FieldType::Integer, 2),
            FieldInfo::new("Date Column".into(), FieldType::DateTime, 3),
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
    fn read_xlsx_header_offset() -> Result<()> {
        // Test reading schema from Excel file with specific worksheet and header row
        let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "data_types_header_offset.xlsx");

        let options = DataFrameOptions {
            layer: None,
            header_row: Some(HeaderRow::Row(3)),
        };

        let mut reader = XlsxReader::new(input_file)?;
        let schema = reader.schema(&options)?;

        // Expected column names from the Excel file
        let expected_columns = [
            FieldInfo::new("String Column".into(), FieldType::String, 0),
            FieldInfo::new("Double Column".into(), FieldType::Float, 1),
            FieldInfo::new("Integer Column".into(), FieldType::Integer, 2),
            FieldInfo::new("Date Column".into(), FieldType::DateTime, 3),
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

use crate::Result;
use crate::vector::dataframe::{DataFrameOpenOptions, DataFrameReader, FieldInfo, FieldType, HeaderRow, Schema};
use calamine::{Data, Reader, Xlsx, open_workbook};
use std::path::Path;

const DEFAULT_DATA_TYPE_DETECTION_ROWS: usize = 10;

pub struct XlsxReader {
    file_path: String,
    options: DataFrameOpenOptions,
}

impl XlsxReader {
    pub fn new<P: AsRef<Path>>(file_path: P, options: DataFrameOpenOptions) -> Result<Self> {
        Ok(Self {
            file_path: file_path.as_ref().to_string_lossy().to_string(),
            options,
        })
    }
}

impl DataFrameReader for XlsxReader {
    fn schema(&self) -> Result<Schema> {
        let mut workbook: Xlsx<_> = open_workbook(&self.file_path).map_err(|e| crate::Error::CalamineError(calamine::Error::Xlsx(e)))?;
        let sheet_name = if let Some(ref layer) = self.options.layer {
            layer.clone()
        } else {
            workbook
                .sheet_names()
                .first()
                .ok_or_else(|| crate::Error::Runtime("No sheets found in workbook".to_string()))?
                .clone()
        };

        let range = workbook
            .worksheet_range(&sheet_name)
            .map_err(|e| crate::Error::CalamineError(calamine::Error::Xlsx(e)))?;

        let mut fields = Vec::new();

        // Determine the header row
        let header_row_idx = match self.options.header_row {
            Some(HeaderRow::Row(idx)) => idx,
            Some(HeaderRow::None) | None => 0, // Default to first row if not specified
        };

        // Get the header row
        if let Some(header_row) = range.rows().nth(header_row_idx) {
            for (col_idx, cell) in header_row.iter().enumerate() {
                let field_name = match cell {
                    Data::String(s) => s.clone(),
                    Data::Int(i) => i.to_string(),
                    Data::Float(f) => f.to_string(),
                    Data::Bool(b) => b.to_string(),
                    Data::Empty => format!("Column_{}", col_idx),
                    Data::Error(_) | Data::DateTime(_) | Data::DateTimeIso(_) | Data::DurationIso(_) => {
                        format!("Column_{}", col_idx)
                    }
                };

                // Determine field type by examining the data in the column
                let field_type = Self::infer_column_type(
                    &range,
                    col_idx,
                    header_row_idx + 1,
                    self.options.data_type_detection_rows.unwrap_or(DEFAULT_DATA_TYPE_DETECTION_ROWS),
                );

                fields.push(FieldInfo::new(field_name, field_type, col_idx));
            }
        }

        Ok(Schema { fields })
    }
}

impl XlsxReader {
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

#[cfg(test)]
mod tests {
    use path_macro::path;

    use super::*;
    use crate::{testutils::workspace_test_data_dir, vector::dataframe::FieldInfo};

    #[test]
    fn read_xlsx_empty_sheet() {
        // Test reading schema from Excel file with specific worksheet and header row
        let input_file = workspace_test_data_dir().join("empty_sheet.xlsx");

        let options = DataFrameOpenOptions {
            layer: Some("VERBR_EF_ID".to_string()),
            header_row: Some(HeaderRow::Row(0)),
            ..Default::default()
        };

        let reader = XlsxReader::new(input_file, options).unwrap();
        let schema = reader.schema().unwrap();

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
        for (field_info, expected) in schema.fields.into_iter().zip(expected_columns) {
            assert_eq!(field_info, expected);
        }
    }

    #[test]
    fn read_xlsx() {
        // Test reading schema from Excel file with specific worksheet and header row
        let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "data_types.xlsx");

        let options = DataFrameOpenOptions {
            layer: None,
            header_row: Some(HeaderRow::Row(0)),
            ..Default::default()
        };

        let reader = XlsxReader::new(input_file, options).unwrap();
        let schema = reader.schema().unwrap();

        // Expected column names from the Excel file
        let expected_columns = [
            FieldInfo::new("String Column".into(), FieldType::String, 0),
            FieldInfo::new("Double Column".into(), FieldType::Float, 1),
            FieldInfo::new("Integer Column".into(), FieldType::Integer, 2),
            FieldInfo::new("Date Column".into(), FieldType::DateTime, 3),
        ];

        assert_eq!(schema.len(), expected_columns.len());
        for (field_info, expected) in schema.fields.into_iter().zip(expected_columns) {
            assert_eq!(field_info, expected);
        }
    }
}

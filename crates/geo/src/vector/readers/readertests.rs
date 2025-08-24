use crate::Result;
use crate::vector::dataframe::{DataFrameOptions, DataFrameReader, Field, FieldInfo, FieldType, HeaderRow, Schema};
use chrono::NaiveDateTime;
use path_macro::path;

pub fn read_table_empty_sheet<R: DataFrameReader>(ext: &str) -> Result<()> {
    // Test reading schema from Excel file with specific worksheet and header row
    let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / format!("empty_sheet.{ext}"));

    let options = DataFrameOptions {
        layer: Some("VERBR_EF_ID".to_string()),
        header_row: HeaderRow::Row(0),
        ..Default::default()
    };

    let mut reader = R::from_file(input_file)?;
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

pub fn read_table<R: DataFrameReader>(ext: &str) -> Result<()> {
    // Test reading schema from input file with specific worksheet and header row
    let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / format!("data_types.{ext}"));

    let options = DataFrameOptions {
        layer: None,
        header_row: HeaderRow::Row(0),
        ..Default::default()
    };

    let mut reader = R::from_file(input_file)?;
    let schema = reader.schema(&options)?;

    // Only the csv reader can detect boolean types
    let has_bool_type = ext == "csv";

    // Expected column names from the input file
    let expected_columns = [
        FieldInfo::new("String Column".into(), FieldType::String),
        FieldInfo::new("Double Column".into(), FieldType::Float),
        FieldInfo::new("Integer Column".into(), FieldType::Integer),
        FieldInfo::new("Date Column".into(), FieldType::DateTime),
        FieldInfo::new(
            "Bool Column".into(),
            if has_bool_type { FieldType::Boolean } else { FieldType::Integer },
        ),
    ];

    assert_eq!(schema.len(), expected_columns.len());
    for (field_info, expected) in schema.fields.iter().zip(expected_columns.iter()) {
        assert_eq!(field_info, expected);
    }

    // Test reading rows - just check the first row
    let mut rows_iter = reader.iter_rows(&options)?;
    if let Some(row) = rows_iter.next() {
        assert_eq!(row.field(0)?, Some(Field::String("Alice".into())));
        assert_eq!(row.field(1)?, Some(Field::Float(12.34)));
        assert_eq!(row.field(2)?, Some(Field::Integer(42)));
        assert_eq!(
            row.field(3)?,
            Some(Field::DateTime(
                NaiveDateTime::parse_from_str("2023-05-17 0:00:00", "%Y-%m-%d %H:%M:%S").unwrap()
            ))
        );

        if has_bool_type {
            assert_eq!(row.field(4)?, Some(Field::Boolean(true)));
        } else {
            assert_eq!(row.field(4)?, Some(Field::Integer(1)));
        }
    }

    Ok(())
}

pub fn read_table_override_schema<R: DataFrameReader>(ext: &str) -> Result<()> {
    // Test reading schema from input file with specific worksheet and header row
    let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / format!("data_types.{ext}"));

    let schema = Schema {
        fields: vec![
            FieldInfo::new("String Column".into(), FieldType::Integer), // Read the strings as integer (will fail for non-integer strings)
            FieldInfo::new("Double Column".into(), FieldType::String),  // Read the doubles as string
        ],
    };

    let options = DataFrameOptions {
        layer: None,
        header_row: HeaderRow::Row(0),
        schema_override: Some(schema.clone()),
    };

    let mut reader = R::from_file(input_file)?;

    let mut rows_iter = reader.iter_rows(&options)?;
    if let Some(row) = rows_iter.next() {
        assert!(row.field(0).is_err());
        assert_eq!(row.field(1)?, Some(Field::String("12.34".into())));
    }

    Ok(())
}

pub fn read_table_sub_schema<R: DataFrameReader>(ext: &str) -> Result<()> {
    // Test reading schema from Excel file with specific worksheet and header row
    let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / format!("data_types.{ext}"));

    let mut reader = R::from_file(input_file)?;
    let options = DataFrameOptions {
        schema_override: Some(
            reader
                .schema(&DataFrameOptions::default())?
                .subselection(&["String Column", "Integer Column"]),
        ),
        ..Default::default()
    };

    let mut rows_iter = reader.iter_rows(&options)?;
    let row = rows_iter.next().unwrap();

    assert_eq!(row.field(0)?, Some(Field::String("Alice".into())));
    assert_eq!(row.field(1)?, Some(Field::Integer(42)));
    assert!(row.field(2).is_err());

    Ok(())
}

#[allow(dead_code)] // Depening on the feature flags, this function may be unused
pub fn read_table_header_offset<R: DataFrameReader>(ext: &str) -> Result<()> {
    // Test reading schema from Excel file with specific worksheet and header row
    let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / format!("data_types_header_offset.{ext}"));

    let mut options = DataFrameOptions {
        layer: None,
        header_row: HeaderRow::Row(3),
        ..Default::default()
    };

    let mut reader = R::from_file(input_file)?;

    // Expected column names from the Excel file
    let expected_columns = [
        FieldInfo::new("String Column".into(), FieldType::String),
        FieldInfo::new("Double Column".into(), FieldType::Float),
        FieldInfo::new("Integer Column".into(), FieldType::Integer),
        FieldInfo::new("Date Column".into(), FieldType::DateTime),
    ];

    let schema = reader.schema(&options)?;
    assert_eq!(schema.len(), expected_columns.len());
    for (field_info, expected) in schema.fields.iter().zip(expected_columns.iter()) {
        assert_eq!(field_info, expected);
    }

    // Test reading rows - just check the first row
    if let Some(row) = reader.iter_rows(&options)?.next() {
        assert_eq!(row.field(0)?, Some(Field::String("Alice".into())));
        assert_eq!(row.field(1)?, Some(Field::Float(12.34)));
    }

    {
        // Invalid column name
        options.schema_override = Some(Schema {
            fields: vec![FieldInfo::new("Strang Column".into(), FieldType::String)],
        });
        assert!(reader.iter_rows(&options).is_err());
    }

    Ok(())
}

pub fn read_table_no_header<R: DataFrameReader>(ext: &str) -> Result<()> {
    let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / format!("data_types_no_header.{ext}"));

    let mut options = DataFrameOptions {
        layer: None,
        header_row: HeaderRow::None,
        ..Default::default()
    };

    let mut reader = R::from_file(input_file)?;

    // Expected column names from the Excel file
    let expected_columns = [
        FieldInfo::new("Field1".into(), FieldType::String),
        FieldInfo::new("Field2".into(), FieldType::Float),
        FieldInfo::new("Field3".into(), FieldType::Integer),
        FieldInfo::new("Field4".into(), FieldType::DateTime),
    ];

    let schema = reader.schema(&options)?;
    assert_eq!(schema.len(), expected_columns.len());
    for (field_info, expected) in schema.fields.iter().zip(expected_columns.iter()) {
        assert_eq!(field_info, expected);
    }

    // Test reading rows - just check the first row
    if let Some(row) = reader.iter_rows(&options)?.next() {
        assert_eq!(row.field(0)?, Some(Field::String("Alice".into())));
        assert_eq!(row.field(1)?, Some(Field::Float(12.34)));
        assert_eq!(row.field(2)?, Some(Field::Integer(42)));
    }

    // Test reading rows - just check the first row
    options.schema_override = Some(schema.subselection(&["Field2", "Field3"]));
    if let Some(row) = reader.iter_rows(&options)?.nth(2) {
        assert_eq!(row.field(0)?, Some(Field::Float(45.67)));
        assert_eq!(row.field(1)?, Some(Field::Integer(7)));
        assert!(row.field(2).is_err());
    }

    {
        // Auto generated column indexes start a 1
        options.schema_override = Some(Schema {
            fields: vec![FieldInfo::new("Field0".into(), FieldType::String)],
        });
        assert!(reader.iter_rows(&options).is_err());
    }

    {
        // Column index too big
        options.schema_override = Some(Schema {
            fields: vec![FieldInfo::new("Field5".into(), FieldType::String)],
        });
        assert!(reader.iter_rows(&options).is_err());
    }

    Ok(())
}

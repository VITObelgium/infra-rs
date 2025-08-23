
use crate::Result;
use crate::vector::dataframe::{DataFrameOptions, DataFrameReader, DataFrameRow as _, Field, FieldInfo, FieldType, HeaderRow, Schema};
use path_macro::path;

pub fn read_xlsx_empty_sheet<R: DataFrameReader>() -> Result<()> {
    // Test reading schema from Excel file with specific worksheet and header row
    let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "empty_sheet.xlsx");

    let options = DataFrameOptions {
        layer: Some("VERBR_EF_ID".to_string()),
        header_row: HeaderRow::Row(0),
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

pub fn read_xlsx<R: DataFrameReader>() -> Result<()> {
    // Test reading schema from Excel file with specific worksheet and header row
    let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "data_types.xlsx");

    let options = DataFrameOptions {
        layer: None,
        header_row: HeaderRow::Row(0),
    };

    let mut reader = R::from_file(input_file)?;
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

pub fn read_xlsx_sub_schema<R: DataFrameReader>() -> Result<()> {
    // Test reading schema from Excel file with specific worksheet and header row
    let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "data_types.xlsx");

    let options = DataFrameOptions::default();
    let mut reader = R::from_file(input_file)?;
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

pub fn read_xlsx_header_offset<R: DataFrameReader>() -> Result<()> {
    // Test reading schema from Excel file with specific worksheet and header row
    let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "data_types_header_offset.xlsx");

    let options = DataFrameOptions {
        layer: None,
        header_row: HeaderRow::Row(3),
    };

    let mut reader = R::from_file(input_file)?;
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

pub fn read_xlsx_no_header<R: DataFrameReader>() -> Result<()> {
    let input_file = path!(env!("CARGO_MANIFEST_DIR") / "tests" / "data" / "data_types_no_header.xlsx");

    let options = DataFrameOptions {
        layer: None,
        header_row: HeaderRow::None,
    };

    let mut reader = R::from_file(input_file)?;
    let schema = reader.schema(&options)?;

    // Expected column names from the Excel file
    let expected_columns = [
        FieldInfo::new("Field1".into(), FieldType::String),
        FieldInfo::new("Field2".into(), FieldType::Float),
        FieldInfo::new("Field3".into(), FieldType::Integer),
        FieldInfo::new("Field4".into(), FieldType::DateTime),
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
    let schema = schema.subselection(&["Field2", "Field3"]);
    if let Some(row) = reader.rows(&options, &schema)?.nth(2) {
        assert_eq!(row.field(0)?, Some(Field::Float(45.67)));
        assert_eq!(row.field(1)?, Some(Field::Integer(7)));
        assert!(row.field(2).is_err());
    }

    {
        // Auto generated column indexes start a 1
        let schema = Schema {
            fields: vec![FieldInfo::new("Field0".into(), FieldType::String)],
        };
        assert!(reader.rows(&options, &schema).is_err());
    }

    {
        // Column index too big
        let schema = Schema {
            fields: vec![FieldInfo::new("Field5".into(), FieldType::String)],
        };
        assert!(reader.rows(&options, &schema).is_err());
    }

    Ok(())
}

use crate::Result;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum HeaderRow {
    /// Automatically detect the presence of a header row
    #[default]
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
    index: usize,
}

impl FieldInfo {
    pub fn new(name: String, field_type: FieldType, index: usize) -> Self {
        Self { name, field_type, index }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn field_type(&self) -> FieldType {
        self.field_type
    }

    pub fn index(&self) -> usize {
        self.index
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
}

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct DataFrameOpenOptions {
    /// The name of the layer to read from, if none is specified, the first available layer is used.
    pub layer: Option<String>,
    /// The row to use as a header row, if None is specified no header row is used and all rows are treated as data rows.
    pub header_row: Option<HeaderRow>,
    /// If specified, this schema will override the detected data types from the data source.
    pub schema_override: Option<Schema>,
    /// The reader will attempt to detect data types from the first `n` rows of the data source. If none is specified, the reader will use a default value.
    pub data_type_detection_rows: Option<usize>,
}

pub trait DataFrameReader {
    fn schema(&self) -> Result<Schema>;
    //fn read_row(&self) -> Result<DataFrameRow>;
}

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
pub struct DataFrameOptions {
    /// The name of the layer to read from, if none is specified, the first available layer is used.
    pub layer: Option<String>,
    /// The row to use as a header row, if None is specified no header row is used and all rows are treated as data rows.
    pub header_row: Option<HeaderRow>,
}

pub trait DataFrameRow {
    fn field(&self, field: usize) -> Result<Option<Field>>;
}

pub trait DataFrameReader {
    fn layer_names(&self) -> Result<Vec<String>>;
    fn schema(&mut self, options: &DataFrameOptions) -> Result<Schema>;
    fn rows(&mut self, options: &DataFrameOptions, schema: &Schema) -> Result<impl Iterator<Item = impl DataFrameRow>>;
}

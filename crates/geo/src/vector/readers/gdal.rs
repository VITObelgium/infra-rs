use std::path::Path;

use gdal::vector::{Feature, FieldValue, LayerAccess, OwnedLayer};
use gdal_sys::OGRFieldType;

use crate::vector::dataframe::{DataFrameOptions, DataFrameReader, DataFrameRow, Field, FieldInfo, FieldType, HeaderRow, Schema};
use crate::vector::io::FeatureExtension;
use crate::vector::{VectorFormat, io};
use crate::{Error, Result};

const GDAL_UNNAMED_COL_PREFIX: &str = "Field";

pub struct GdalReader {
    path: std::path::PathBuf,
}

fn create_open_options_for_file(path: &Path, options: &DataFrameOptions) -> Vec<String> {
    let mut open_options = Vec::new();
    match VectorFormat::guess_from_path(path) {
        VectorFormat::Xlsx => {
            let header_detection = match options.header_row {
                crate::vector::dataframe::HeaderRow::None => "DISABLE",
                crate::vector::dataframe::HeaderRow::Row(0) => "FORCE",
                crate::vector::dataframe::HeaderRow::Auto => "AUTO",
                crate::vector::dataframe::HeaderRow::Row(_) => {
                    // GDAL reader only supports header row at index 0 or no header
                    return vec![];
                }
            };
            open_options.push(format!("HEADERS={header_detection}"));
        }
        VectorFormat::Csv => {
            open_options.push("AUTODETECT_TYPE=YES".into());
        }
        _ => {}
    }
    open_options
}

fn create_layer_for_file(path: &Path, options: &DataFrameOptions) -> Result<OwnedLayer> {
    let open_options = create_open_options_for_file(path, options);
    let open_options_refs: Vec<&str> = open_options.iter().map(|s| s.as_str()).collect();
    let dataset = io::dataset::open_read_only_with_options(path, Some(&open_options_refs))?;

    // Get layer - use the specified layer or default to first layer
    Ok(match &options.layer {
        Some(layer_name) => dataset.into_layer_by_name(layer_name)?,
        None => dataset.into_layer(0)?,
    })
}

fn map_ogr_field_type_to_field_type(ogr_type: OGRFieldType::Type) -> FieldType {
    match ogr_type {
        OGRFieldType::OFTInteger | OGRFieldType::OFTInteger64 => FieldType::Integer,
        OGRFieldType::OFTReal => FieldType::Float,
        OGRFieldType::OFTDateTime | OGRFieldType::OFTDate | OGRFieldType::OFTTime => FieldType::DateTime,
        _ => FieldType::String, // Default to string for unsupported types
    }
}

fn convert_field_value_to_field(field_value: FieldValue, schema_type: FieldType) -> Result<Option<Field>> {
    match field_value {
        FieldValue::StringValue(val) => Field::from_string(val, schema_type),
        FieldValue::IntegerValue(val) => Field::from_integer(val as i64, schema_type),
        FieldValue::Integer64Value(val) => Field::from_integer(val, schema_type),
        FieldValue::RealValue(val) => Field::from_float(val, schema_type),
        FieldValue::DateTimeValue(val) => Ok(Some(Field::DateTime(val.naive_local()))),
        FieldValue::DateValue(val) => {
            Ok(Some(Field::DateTime(val.and_hms_opt(0, 0, 0).ok_or_else(|| {
                Error::Runtime(format!("Failed to create datetime from {:?}", val))
            })?)))
        }
        FieldValue::IntegerListValue(_)
        | FieldValue::Integer64ListValue(_)
        | FieldValue::StringListValue(_)
        | FieldValue::RealListValue(_) => Err(Error::Runtime("List values are not supported".into())),
    }
}

fn read_schema_from_layer(layer: &mut OwnedLayer, options: &DataFrameOptions) -> Result<Schema> {
    let header_detection_failed =
        options.header_row == HeaderRow::Row(0) && layer.defn().fields().all(|f| f.name().starts_with(GDAL_UNNAMED_COL_PREFIX));

    let mut fields = Vec::new();
    if header_detection_failed && layer.feature_count() == 1 {
        // Header detection failed but there is one row, use that row as header since row 0 was explicitly requested as header
        let first_row = layer.features().next().unwrap(); // we know there is one row
        for (_field_name, val) in first_row.fields() {
            fields.push(FieldInfo::new(
                val.and_then(|v| v.into_string()).unwrap_or_default(),
                FieldType::String,
            )); // Use string as it the table contains only a header row
        }
    } else {
        layer.defn().fields().for_each(|f| {
            fields.push(FieldInfo::new(f.name(), map_ogr_field_type_to_field_type(f.field_type())));
        });
    }

    Ok(Schema { fields })
}

pub struct GdalRowIterator {
    schema: Schema,
    field_type_indices: Vec<(FieldType, usize)>,
    feature_iter: Option<gdal::vector::OwnedFeatureIterator>,
}

impl GdalRowIterator {
    fn new(path: &Path, options: &DataFrameOptions) -> Result<Self> {
        let mut layer = create_layer_for_file(path, options)?;

        // Header detection logic for edge cases
        let header_detection_failed =
            options.header_row == HeaderRow::Row(0) && layer.defn().fields().all(|f| f.name().starts_with(GDAL_UNNAMED_COL_PREFIX));

        let schema = match &options.schema_override {
            Some(schema) => schema.clone(),
            None => read_schema_from_layer(&mut layer, options)?,
        };

        // Get the field indices for the requested schema columns
        let field_type_indices = schema
            .fields
            .iter()
            .map(|schema_field| {
                Ok((
                    schema_field.field_type(),
                    layer.defn().field_index(schema_field.name()).map_err(|_| {
                        Error::InvalidArgument(format!("Field '{}' not found in layer '{}'", schema_field.name(), layer.name()))
                    })?,
                ))
            })
            .collect::<Result<Vec<(FieldType, usize)>>>()?;

        // Check if we should skip all data due to header detection failure
        let feature_iter = if header_detection_failed && layer.feature_count() == 1 {
            None
        } else {
            Some(layer.owned_features())
        };

        Ok(Self {
            schema: schema.clone(),
            field_type_indices,
            feature_iter,
        })
    }

    fn read_feature_field_as(feature: &Feature, index: usize, field_type: FieldType) -> Result<Option<Field>> {
        match feature.field(index)? {
            Some(f) => convert_field_value_to_field(f, field_type),
            None => Ok(None),
        }
    }
}

impl Iterator for GdalRowIterator {
    type Item = DataFrameRow;

    fn next(&mut self) -> Option<Self::Item> {
        self.feature_iter
            .as_mut()?
            .next()
            .filter(|f| !f.fields().all(|(_name, val)| val.is_none())) // Skip empty features
            .map(|feature| {
                let mut fields: Vec<Result<Option<Field>>> = Vec::with_capacity(self.schema.fields.len());
                for (field_type, field_index) in &self.field_type_indices {
                    if feature.field_is_valid(*field_index) {
                        fields.push(Self::read_feature_field_as(&feature, *field_index, *field_type));
                    } else {
                        fields.push(Ok(None));
                    }
                }

                DataFrameRow { fields }
            })
    }
}

impl DataFrameReader for GdalReader {
    fn from_file<P: AsRef<Path>>(file_path: P) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {
            path: file_path.as_ref().to_path_buf(),
        })
    }

    fn layer_names(&self) -> Result<Vec<String>> {
        let dataset = io::dataset::open_read_only(&self.path)?;
        let mut layer_names = Vec::new();

        for i in 0..dataset.layer_count() {
            if let Ok(layer) = dataset.layer(i) {
                layer_names.push(layer.name());
            }
        }

        Ok(layer_names)
    }

    fn schema(&mut self, options: &DataFrameOptions) -> Result<Schema> {
        let mut layer = create_layer_for_file(&self.path, options)?;
        read_schema_from_layer(&mut layer, options)
    }

    fn iter_rows(&mut self, options: &DataFrameOptions) -> Result<Box<dyn Iterator<Item = DataFrameRow>>> {
        Ok(Box::new(GdalRowIterator::new(&self.path, options)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::readers::readertests;

    #[test]
    fn read_xlsx_empty_sheet() -> Result<()> {
        readertests::read_table_empty_sheet::<GdalReader>("xlsx")
    }

    #[test]
    fn read_xlsx() -> Result<()> {
        readertests::read_table::<GdalReader>("xlsx")
    }

    #[test]
    fn read_xlsx_override_schema() -> Result<()> {
        readertests::read_table_override_schema::<GdalReader>("xlsx")
    }

    #[test]
    fn read_xlsx_sub_schema() -> Result<()> {
        readertests::read_table_sub_schema::<GdalReader>("xlsx")
    }

    #[test]
    fn read_xlsx_no_header() -> Result<()> {
        readertests::read_table_no_header::<GdalReader>("xlsx")
    }
}

/// Low level functions to work with gdal vector datasets
use std::{ffi::CString, path::Path};

use crate::{Error, Result, gdalinterop};

use super::*;

use ::gdal::vector::{Defn, Feature, FieldValue, Layer, LayerAccess, OwnedLayer};
use gdal_sys::OGRFieldType;

pub mod dataset {
    use std::path::{Path, PathBuf};

    use super::*;

    use ::gdal::{Dataset, DatasetOptions, DriverManager, GdalOpenFlags, errors::GdalError};

    /// Create a new in-memory vector dataset
    /// Useful for working with vector data in memory before actually writing it to disk
    pub fn create_in_memory() -> Result<Dataset> {
        let mem_driver = ::gdal::DriverManager::get_driver_by_name(VectorFileFormat::Memory.gdal_driver_name())?;
        Ok(mem_driver.create_vector_only("in-mem")?)
    }

    fn open_with_options(path: &Path, options: DatasetOptions) -> Result<Dataset> {
        Dataset::open_ex(path, options).map_err(|err| match err {
            // Match on the error to give a cleaner error message when the file does not exist
            GdalError::NullPointer { method_name: _, msg: _ } => {
                let vec_type = VectorFileFormat::guess_from_path(path);
                if vec_type != VectorFileFormat::Unknown && DriverManager::get_driver_by_name(vec_type.gdal_driver_name()).is_err() {
                    return Error::Runtime(format!("Gdal driver not supported: {}", vec_type.gdal_driver_name()));
                }

                Error::InvalidPath(PathBuf::from(path))
            }
            _ => Error::Runtime(format!("Failed to open raster dataset: {} ({})", path.to_string_lossy(), err)),
        })
    }

    /// Open a GDAL vector dataset for reading
    pub fn open_read_only(path: &Path) -> Result<Dataset> {
        let options = DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_READONLY | GdalOpenFlags::GDAL_OF_VECTOR,
            ..Default::default()
        };

        open_with_options(path, options)
    }

    /// Open a GDAL vector dataset for reading with driver open options
    pub fn open_read_only_with_options(path: &Path, open_options: Option<&[&str]>) -> Result<Dataset> {
        let options = DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_READONLY | GdalOpenFlags::GDAL_OF_VECTOR,
            open_options,
            ..Default::default()
        };

        open_with_options(path, options)
    }
}

pub fn read_dataframe(path: &Path, layer: Option<&str>, columns: &[String]) -> Result<Vec<Vec<Option<FieldValue>>>> {
    let ds = dataset::open_read_only(path)?;
    let mut ds_layer;
    if let Some(layer_name) = layer {
        ds_layer = ds.layer_by_name(layer_name)?;
    } else {
        ds_layer = ds.layer(0)?;
    }

    let mut data = Vec::with_capacity(ds_layer.feature_count() as usize);
    let column_indexes = columns
        .iter()
        .map(|name| Ok(ds_layer.defn().field_index(name)?))
        .collect::<Result<Vec<usize>>>()?;

    for feature in ds_layer.features() {
        let mut row = Vec::with_capacity(columns.len());
        for column in &column_indexes {
            row.push(feature.field(*column)?);
        }

        data.push(row);
    }

    Ok(data)
}

pub fn read_dataframe_schema(
    path: &Path,
    layer: Option<&str>,
    data_frame_open_options: Option<&[&str]>,
) -> Result<Vec<(String, OGRFieldType::Type)>> {
    let ds = dataset::open_read_only_with_options(path, data_frame_open_options)?;
    let ds_layer = if let Some(layer_name) = layer {
        ds.layer_by_name(layer_name)?
    } else {
        ds.layer(0)?
    };

    Ok(ds_layer.defn().fields().map(|f| (f.name(), f.field_type())).collect())
}

/// Read rows from a vector dataset and invokes the provided callback function for each row
pub fn read_dataframe_rows_cb(
    path: &Path,
    layer: Option<&str>,
    filter: Option<&str>,
    columns: Option<&[String]>,
    data_frame_open_options: Option<&[&str]>,
    mut callback: impl FnMut(Vec<Option<FieldValue>>),
) -> Result<()> {
    let ds = dataset::open_read_only_with_options(path, data_frame_open_options)?;
    let mut ds_layer;
    if let Some(layer_name) = layer {
        ds_layer = match ds.layer_by_name(layer_name) {
            Ok(layer) => layer,
            Err(err) => {
                log::debug!("{err}");
                return Err(Error::InvalidArgument(format!(
                    "Layer '{}' not found in dataset '{}'",
                    layer_name,
                    path.to_string_lossy()
                )));
            }
        };
    } else {
        ds_layer = ds.layer(0)?;
    }

    if let Some(filter) = filter {
        ds_layer.set_attribute_filter(filter)?;
    }

    let column_indexes: Vec<usize> = match columns {
        Some(columns) => columns
            .iter()
            .map(|name| match ds_layer.defn().field_index(name) {
                Ok(index) => Ok(index),
                Err(_) => Err(Error::InvalidArgument(format!(
                    "Field '{}' not found in layer '{}', available fields: {}",
                    name,
                    ds_layer.name(),
                    ds_layer.defn().fields().map(|f| f.name()).collect::<Vec<String>>().join(", ")
                ))),
            })
            .collect::<Result<Vec<usize>>>()?,
        None =>
        // If no columns are specified, read all fields
        {
            (0..ds_layer.defn().field_count()?)
                .map(|i| Ok(i as usize))
                .collect::<Result<Vec<usize>>>()?
        }
    };

    for feature in ds_layer.features() {
        let mut row = Vec::with_capacity(column_indexes.len());
        let mut valid_field = false;
        for column_idx in &column_indexes {
            valid_field = valid_field || feature.field_is_valid(*column_idx);
            row.push(feature.field(*column_idx)?);
        }

        if valid_field {
            callback(row);
        }
    }

    Ok(())
}

/// [`gdal::vector::LayerAccess`] extenstion trait that implements missing functionality
/// for working with GDAL vector layers
pub trait LayerAccessExtension
where
    Self: LayerAccess,
{
    #[deprecated(
        since = "0.1.0",
        note = "This method is deprecated. Use `field_index` from the `FeatureDefinition` api instead."
    )]
    fn field_index_with_name(&self, field_name: &str) -> Result<usize> {
        let field_name_c_str = CString::new(field_name)?;
        let field_index = unsafe { gdal_sys::OGR_L_FindFieldIndex(self.c_layer(), field_name_c_str.as_ptr(), gdalinterop::TRUE) };

        if field_index == -1 {
            return Err(Error::InvalidArgument(format!(
                "Field '{}' not found in layer '{}'",
                field_name,
                self.name()
            )));
        }

        Ok(field_index as usize)
    }
}

impl LayerAccessExtension for Layer<'_> {}
impl LayerAccessExtension for OwnedLayer {}

/// [`gdal::vector::Defn`] extenstion trait that implements missing functionality
/// for working with GDAL vector layer definitions
pub trait FeatureDefinitionExtension {
    fn field_count(&self) -> Result<i32>;
}

impl FeatureDefinitionExtension for Defn {
    fn field_count(&self) -> Result<i32> {
        let field_count = unsafe { gdal_sys::OGR_FD_GetFieldCount(self.c_defn()) };
        if field_count < 0 {
            return Err(Error::Runtime("Failed to get layer field count".to_string()));
        }

        Ok(field_count)
    }
}

/// [`gdal::vector::Feature`] extenstion trait that implements missing functionality
/// for working with GDAL vector layers
pub trait FeatureExtension {
    fn field_index_from_name(&self, field_name: &str) -> Result<usize>;
    /// The field at the index is set and not null
    fn field_is_valid(&self, field_index: usize) -> bool;
}

impl FeatureExtension for Feature<'_> {
    fn field_index_from_name(&self, field_name: &str) -> Result<usize> {
        let field_name_c_str = CString::new(field_name)?;
        let field_index = unsafe { gdal_sys::OGR_F_GetFieldIndex(self.c_feature(), field_name_c_str.as_ptr()) };

        if field_index == -1 {
            return Err(Error::InvalidArgument(format!("Field '{field_name}' not found in feature")));
        }

        Ok(field_index as usize)
    }

    fn field_is_valid(&self, field_index: usize) -> bool {
        unsafe { gdal_sys::OGR_F_IsFieldSetAndNotNull(self.c_feature(), field_index as i32) == 1 }
    }
}

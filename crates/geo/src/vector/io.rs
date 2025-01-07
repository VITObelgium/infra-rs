//! Contains functions to read and write vector data using the GDAL library.

use std::{
    ffi::CString,
    path::{Path, PathBuf},
};

use gdal::{
    errors::GdalError,
    vector::{FieldValue, LayerAccess},
};

use crate::{gdalinterop, Error, Result};

use super::DataRow;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum VectorFormat {
    Memory,
    Csv,
    Tab,
    ShapeFile,
    Xlsx,
    GeoJson,
    GeoPackage,
    PostgreSQL,
    Wfs,
    Vrt,
    Parquet,
    Arrow,
    Unknown,
}

impl VectorFormat {
    pub fn gdal_driver_name(&self) -> &str {
        match self {
            VectorFormat::Memory => "Memory",
            VectorFormat::Csv | VectorFormat::Tab => "CSV",
            VectorFormat::ShapeFile => "ESRI Shapefile",
            VectorFormat::Xlsx => "XLSX",
            VectorFormat::GeoJson => "GeoJSON",
            VectorFormat::GeoPackage => "GPKG",
            VectorFormat::PostgreSQL => "PostgreSQL",
            VectorFormat::Wfs => "WFS",
            VectorFormat::Vrt => "OGR_VRT",
            VectorFormat::Parquet => "Parquet",
            VectorFormat::Arrow => "Arrow",
            VectorFormat::Unknown => "Unknown",
        }
    }

    /// Given a file path, guess the raster type based on the file extension
    pub fn guess_from_path(file_path: &Path) -> VectorFormat {
        let ext = file_path.extension().map(|ext| ext.to_string_lossy().to_lowercase());

        if let Some(ext) = ext {
            match ext.as_ref() {
                "csv" => return VectorFormat::Csv,
                "tab" => return VectorFormat::Tab,
                "shp" | "dbf" => return VectorFormat::ShapeFile,
                "xlsx" => return VectorFormat::Xlsx,
                "json" | "geojson" => return VectorFormat::GeoJson,
                "gpkg" => return VectorFormat::GeoPackage,
                "vrt" => return VectorFormat::Vrt,
                "parquet" => return VectorFormat::Parquet,
                "arrow" | "arrows" => return VectorFormat::Arrow,
                _ => {}
            }
        }

        let path = file_path.to_string_lossy();
        if path.starts_with("postgresql://") || path.starts_with("pg:") {
            VectorFormat::PostgreSQL
        } else if path.starts_with("wfs:") {
            VectorFormat::Wfs
        } else {
            VectorFormat::Unknown
        }
    }
}

/// Low level functions to work with gdal vector datasets
pub mod dataset {
    use super::*;

    /// Create a new in-memory vector dataset
    /// Useful for working with vector data in memory before actually writing it to disk
    pub fn create_in_memory() -> Result<gdal::Dataset> {
        let mem_driver = gdal::DriverManager::get_driver_by_name(VectorFormat::Memory.gdal_driver_name())?;
        Ok(mem_driver.create_vector_only("in-mem")?)
    }

    fn open_with_options(path: &Path, options: gdal::DatasetOptions) -> Result<gdal::Dataset> {
        gdal::Dataset::open_ex(path, options).map_err(|err| match err {
            // Match on the error to give a cleaner error message when the file does not exist
            GdalError::NullPointer { method_name: _, msg: _ } => {
                let vec_type = VectorFormat::guess_from_path(path);
                if vec_type != VectorFormat::Unknown
                    && gdal::DriverManager::get_driver_by_name(vec_type.gdal_driver_name()).is_err()
                {
                    return Error::Runtime(format!("Gdal driver not supported: {}", vec_type.gdal_driver_name()));
                }

                Error::InvalidPath(PathBuf::from(path))
            }
            _ => Error::Runtime(format!(
                "Failed to open raster dataset: {} ({})",
                path.to_string_lossy(),
                err
            )),
        })
    }

    /// Open a GDAL vector dataset for reading
    pub fn open_read_only(path: &Path) -> Result<gdal::Dataset> {
        let options = gdal::DatasetOptions {
            open_flags: gdal::GdalOpenFlags::GDAL_OF_READONLY | gdal::GdalOpenFlags::GDAL_OF_VECTOR,
            ..Default::default()
        };

        open_with_options(path, options)
    }

    /// Open a GDAL vector dataset for reading with driver open options
    pub fn open_read_only_with_options(path: &Path, open_options: &[&str]) -> Result<gdal::Dataset> {
        let options = gdal::DatasetOptions {
            open_flags: gdal::GdalOpenFlags::GDAL_OF_READONLY | gdal::GdalOpenFlags::GDAL_OF_VECTOR,
            open_options: Some(open_options),
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

    for feature in ds_layer.features() {
        let mut row = Vec::with_capacity(columns.len());
        for column in columns {
            row.push(feature.field(column)?);
        }

        data.push(row);
    }

    Ok(data)
}

pub fn read_dataframe_as<T: DataRow>(path: &Path, layer: Option<&str>) -> Result<Vec<T>> {
    DataframeIterator::<T>::new(&path, layer)?.collect()
}

/// Iterator over the rows of a vector dataset that returns a an object
/// that implements the [`DataRow`] trait
pub struct DataframeIterator<TRow: DataRow> {
    features: gdal::vector::OwnedFeatureIterator,
    phantom: std::marker::PhantomData<TRow>,
}

impl<TRow: DataRow> DataframeIterator<TRow> {
    pub fn new<P: AsRef<Path>>(path: &P, layer: Option<&str>) -> Result<Self> {
        let ds = dataset::open_read_only(path.as_ref())?;
        let ds_layer = if let Some(layer_name) = layer {
            ds.into_layer_by_name(layer_name)?
        } else {
            ds.into_layer(0)?
        };

        Ok(Self {
            features: ds_layer.owned_features(),
            phantom: std::marker::PhantomData,
        })
    }
}

impl<TRow: DataRow> Iterator for DataframeIterator<TRow> {
    type Item = Result<TRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.features.into_iter().next().map(TRow::from_feature)
    }
}

/// [`gdal::vector::LayerAccess`] extenstion trait that implements missing functionality
/// for working with GDAL vector layers
pub trait LayerAccessExtension
where
    Self: LayerAccess,
{
    fn field_index_with_name(&self, field_name: &str) -> Result<i32> {
        let field_name_c_str = CString::new(field_name)?;
        let field_index =
            unsafe { gdal_sys::OGR_L_FindFieldIndex(self.c_layer(), field_name_c_str.as_ptr(), gdalinterop::TRUE) };

        if field_index == -1 {
            return Err(Error::InvalidArgument(format!(
                "Field '{}' not found in layer '{}'",
                field_name,
                self.name()
            )));
        }

        Ok(field_index)
    }
}

impl LayerAccessExtension for gdal::vector::Layer<'_> {}
impl LayerAccessExtension for gdal::vector::OwnedLayer {}

/// [`gdal::vector::Defn`] extenstion trait that implements missing functionality
/// for working with GDAL vector layer definitions
pub trait FeatureDefinitionExtension {
    fn field_count(&self) -> Result<i32>;
}

impl FeatureDefinitionExtension for gdal::vector::Defn {
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
    fn field_index_from_name(&self, field_name: &str) -> Result<i32>;
    /// The field at the index is set and not null
    fn field_is_valid(&self, field_index: i32) -> bool;
}

impl FeatureExtension for gdal::vector::Feature<'_> {
    fn field_index_from_name(&self, field_name: &str) -> Result<i32> {
        let field_name_c_str = CString::new(field_name)?;
        let field_index = unsafe { gdal_sys::OGR_F_GetFieldIndex(self.c_feature(), field_name_c_str.as_ptr()) };

        if field_index == -1 {
            return Err(Error::InvalidArgument(format!(
                "Field '{}' not found in feature",
                field_name
            )));
        }

        Ok(field_index)
    }

    fn field_is_valid(&self, field_index: i32) -> bool {
        unsafe { gdal_sys::OGR_F_IsFieldSetAndNotNull(self.c_feature(), field_index) == 1 }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn vectorformat_guess_from_path() {
        assert_eq!(VectorFormat::guess_from_path(Path::new("test.csv")), VectorFormat::Csv);
        assert_eq!(VectorFormat::guess_from_path(Path::new("test.tab")), VectorFormat::Tab);
        assert_eq!(
            VectorFormat::guess_from_path(Path::new("test.shp")),
            VectorFormat::ShapeFile
        );
        assert_eq!(
            VectorFormat::guess_from_path(Path::new("test.dbf")),
            VectorFormat::ShapeFile
        );
        assert_eq!(
            VectorFormat::guess_from_path(Path::new("test.xlsx")),
            VectorFormat::Xlsx
        );
        assert_eq!(
            VectorFormat::guess_from_path(Path::new("test.json")),
            VectorFormat::GeoJson
        );
        assert_eq!(
            VectorFormat::guess_from_path(Path::new("test.geojson")),
            VectorFormat::GeoJson
        );
        assert_eq!(
            VectorFormat::guess_from_path(Path::new("test.gpkg")),
            VectorFormat::GeoPackage
        );
        assert_eq!(VectorFormat::guess_from_path(Path::new("test.vrt")), VectorFormat::Vrt);
        assert_eq!(
            VectorFormat::guess_from_path(Path::new("postgresql://")),
            VectorFormat::PostgreSQL
        );
        assert_eq!(
            VectorFormat::guess_from_path(Path::new("pg:")),
            VectorFormat::PostgreSQL
        );
        assert_eq!(VectorFormat::guess_from_path(Path::new("wfs:")), VectorFormat::Wfs);
        assert_eq!(VectorFormat::guess_from_path(Path::new("test")), VectorFormat::Unknown);
    }
}

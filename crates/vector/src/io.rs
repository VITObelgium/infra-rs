//! Contains functions to read and write vector data using the GDAL library.

use std::{
    ffi::CString,
    path::{Path, PathBuf},
};

use gdal::{
    errors::GdalError,
    vector::{FieldValue, LayerAccess},
};
use inf::gdalinterop;

use crate::{DataRow, Error, Result};

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

struct VectorTranslateOptions {
    options: *mut gdal_sys::GDALVectorTranslateOptions,
}

impl VectorTranslateOptions {
    fn new(opts: &[String]) -> Result<Self> {
        let mut c_opts = gdal::cpl::CslStringList::new();
        for opt in opts {
            c_opts.add_string(opt)?;
        }

        let options = unsafe { gdal_sys::GDALVectorTranslateOptionsNew(c_opts.as_ptr(), std::ptr::null_mut()) };
        if options.is_null() {
            return Err(Error::InvalidArgument(
                "Failed to create vector translate options".to_string(),
            ));
        }

        Ok(Self { options })
    }

    fn c_options(&mut self) -> *mut gdal_sys::GDALVectorTranslateOptions {
        self.options
    }
}

impl Drop for VectorTranslateOptions {
    fn drop(&mut self) {
        unsafe { gdal_sys::GDALVectorTranslateOptionsFree(self.c_options()) };
    }
}

/// Translate a GDAL vector dataset to disk using the provided translate options
/// The options are passed as a list of strings in the form `["-option1", "value1", "-option2", "value2"]`
/// and match the options of the gdal ogr2ogr command line tool
/// The dataset is returned in case the user wants to continue working with it but can also be ignored
pub fn translate_to_disk(ds: &gdal::Dataset, path: &Path, options: &[String]) -> Result<gdal::Dataset> {
    gdalinterop::create_output_directory_if_needed(path)?;
    let path_str = CString::new(path.to_string_lossy().as_ref())?;
    let mut opts = VectorTranslateOptions::new(options)?;
    let mut usage_error: std::ffi::c_int = 0;

    let handle = unsafe {
        gdal_sys::GDALVectorTranslate(
            path_str.as_ptr(),
            std::ptr::null_mut(),
            1,
            &mut ds.c_dataset(),
            opts.c_options(),
            &mut usage_error,
        )
    };

    if usage_error == gdalinterop::TRUE {
        return Err(Error::InvalidArgument(
            "Vector translate: invalid arguments".to_string(),
        ));
    }

    gdalinterop::check_pointer(handle, "GDALVectorTranslate")?;

    Ok(unsafe { gdal::Dataset::from_c_dataset(handle) })
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

pub fn layer_field_index<L: gdal::vector::LayerAccess, S: AsRef<str>>(layer: &L, field_name: S) -> Result<i32> {
    let field_name_c_str = CString::new(field_name.as_ref())?;
    let field_index =
        unsafe { gdal_sys::OGR_L_FindFieldIndex(layer.c_layer(), field_name_c_str.as_ptr(), gdalinterop::TRUE) };

    if field_index == -1 {
        return Err(Error::InvalidArgument(format!(
            "Field not found: {}",
            field_name.as_ref()
        )));
    }

    Ok(field_index)
}

pub fn field_index_from_name<S: AsRef<str>>(feature: &gdal::vector::Feature, field_name: S) -> Result<i32> {
    let field_name_c_str = CString::new(field_name.as_ref())?;
    let field_index = unsafe { gdal_sys::OGR_F_GetFieldIndex(feature.c_feature(), field_name_c_str.as_ptr()) };
    if field_index == -1 {
        return Err(Error::InvalidArgument(format!(
            "Field not found: {}",
            field_name.as_ref()
        )));
    }

    Ok(field_index)
}

pub fn read_dataframe(path: &Path, layer: Option<&str>, columns: &[String]) -> Result<Vec<Vec<Option<FieldValue>>>> {
    let ds = open_read_only(path)?;
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

/// Iterator over the rows of a vector dataset that returns a `DataRow` object
/// A `DataRow` object is a struct that implements the `DataRow` trait
pub struct DataframeIterator<TRow: DataRow> {
    features: gdal::vector::OwnedFeatureIterator,
    phantom: std::marker::PhantomData<TRow>,
}

impl<TRow: DataRow> DataframeIterator<TRow> {
    pub fn new<P: AsRef<Path>>(path: &P, layer: Option<&str>) -> Result<Self> {
        let ds = open_read_only(path.as_ref())?;
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

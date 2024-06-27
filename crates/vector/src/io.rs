//! Contains functions to read and write vector data using the GDAL library.

use std::path::{Path, PathBuf};

use gdal::{
    errors::GdalError,
    vector::{FieldValue, LayerAccess},
};

use crate::{Error, Result};

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
    Unknown,
}

/// Given a file path, guess the raster type based on the file extension
pub fn guess_format_from_filename(file_path: &Path) -> VectorFormat {
    let ext = file_path.extension().map(|ext| ext.to_string_lossy().to_lowercase());

    if let Some(ext) = ext {
        match ext.as_ref() {
            ".csv" => VectorFormat::Csv,
            ".tab" => VectorFormat::Tab,
            ".shp" | ".dbf" => VectorFormat::ShapeFile,
            ".xlsx" => VectorFormat::Xlsx,
            ".json" | ".geojson" => VectorFormat::GeoJson,
            ".gpkg" => VectorFormat::GeoPackage,
            ".vrt" => VectorFormat::Vrt,
            _ => {
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
    } else {
        VectorFormat::Unknown
    }
}

fn open_with_options(path: &Path, options: gdal::DatasetOptions) -> Result<gdal::Dataset> {
    gdal::Dataset::open_ex(path, options).map_err(|err| match err {
        // Match on the error to give a cleaner error message when the file does not exist
        GdalError::NullPointer { method_name: _, msg: _ } => Error::InvalidPath(PathBuf::from(path)),
        _ => Error::Runtime(format!(
            "Failed to open raster dataset: {} ({})",
            path.to_string_lossy(),
            err
        )),
    })
}

/// Open a GDAL raster dataset for reading
pub fn open_read_only(path: &Path) -> Result<gdal::Dataset> {
    let options = gdal::DatasetOptions {
        open_flags: gdal::GdalOpenFlags::GDAL_OF_READONLY | gdal::GdalOpenFlags::GDAL_OF_VECTOR,
        ..Default::default()
    };

    open_with_options(path, options)
}

/// Open a GDAL raster dataset for reading with driver open options
pub fn open_read_only_with_options(path: &Path, open_options: &[&str]) -> Result<gdal::Dataset> {
    let options = gdal::DatasetOptions {
        open_flags: gdal::GdalOpenFlags::GDAL_OF_READONLY | gdal::GdalOpenFlags::GDAL_OF_VECTOR,
        open_options: Some(open_options),
        ..Default::default()
    };

    open_with_options(path, options)
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

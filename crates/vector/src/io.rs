//! Contains functions to read and write vector data using the GDAL library.

use std::path::{Path, PathBuf};

use gdal::{
    errors::GdalError,
    vector::{Feature, FieldValue, LayerAccess},
};
use vector_derive::DataRow;

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

trait VectorFieldType<T> {
    fn read_from_field(field: &FieldValue) -> Result<Option<T>>;
}

impl VectorFieldType<f64> for f64 {
    fn read_from_field(field: &FieldValue) -> Result<Option<f64>> {
        match field {
            FieldValue::RealValue(val) => Ok(Some(*val)),
            FieldValue::IntegerValue(val) => Ok(Some(*val as f64)),
            FieldValue::StringValue(val) => Ok(Some(val.parse()?)),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType<i32> for i32 {
    fn read_from_field(field: &FieldValue) -> Result<Option<i32>> {
        match field {
            FieldValue::IntegerValue(val) => Ok(Some(*val)),
            FieldValue::RealValue(val) => Ok(Some(*val as i32)),
            FieldValue::StringValue(val) => Ok(Some(val.parse()?)),
            _ => Ok(None),
        }
    }
}

impl VectorFieldType<String> for String {
    fn read_from_field(field: &FieldValue) -> Result<Option<String>> {
        match field {
            FieldValue::StringValue(val) => Ok(Some(val.to_string())),
            FieldValue::RealValue(val) => Ok(Some(val.to_string())),
            FieldValue::IntegerValue(val) => Ok(Some(val.to_string())),
            _ => Ok(None),
        }
    }
}

/// Given a file path, guess the raster type based on the file extension
pub fn guess_format_from_filename(file_path: &Path) -> VectorFormat {
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

fn read_feature_val<T: VectorFieldType<T>>(feature: &gdal::vector::Feature, field_name: &str) -> Result<Option<T>> {
    match feature.field(field_name)? {
        Some(field) => T::read_from_field(&field),
        None => Ok(None),
    }
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

trait DataRow {
    fn field_names() -> Vec<&'static str>;
    fn from_feature(feature: gdal::vector::Feature) -> Result<Self>
    where
        Self: Sized;
}

struct VectorDataframeIterator<TRow: DataRow> {
    ds_layer: gdal::vector::OwnedLayer,
    phantom: std::marker::PhantomData<TRow>,
}

impl<TRow: DataRow> VectorDataframeIterator<TRow> {
    fn new(path: &Path) -> Result<Self> {
        let ds_layer = open_read_only(path)?.into_layer(0)?;

        // let field_names = TRow::field_names();
        // let mut field_indices = Vec::with_capacity(field_names.len());
        // for &field_name in TRow::field_names() {
        //     let col = unsafe {
        //         let cdef = ds_layer.defn().c_defn();
        //         gdal_sys::OGR_FD_GetFieldIndex(cdef, CString::new(field_name)?.as_ptr())
        //     };

        //     field_indices.push(col);
        // }

        Ok(Self {
            ds_layer,
            phantom: std::marker::PhantomData,
        })
    }
}

impl<TRow: DataRow> Iterator for VectorDataframeIterator<TRow> {
    type Item = Result<TRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.ds_layer.features().next().map(TRow::from_feature)
    }
}

#[cfg(test)]
mod tests {
    use vector_derive::DataRow;

    use super::*;

    #[derive(DataRow, Default)]
    struct PollutantData {
        #[vector(column = "Pollutant")]
        pollutant: String,
        #[vector(column = "Sector")]
        sector: String,
        value: f64,
    }

    #[test]
    fn test_iterate_features() {
        assert_eq!(PollutantData::field_names(), vec!["Pollutant", "Sector", "value"]);
    }

    #[test]
    fn test_guess_format_from_filename() {
        assert_eq!(guess_format_from_filename(Path::new("test.csv")), VectorFormat::Csv);
        assert_eq!(guess_format_from_filename(Path::new("test.tab")), VectorFormat::Tab);
        assert_eq!(
            guess_format_from_filename(Path::new("test.shp")),
            VectorFormat::ShapeFile
        );
        assert_eq!(
            guess_format_from_filename(Path::new("test.dbf")),
            VectorFormat::ShapeFile
        );
        assert_eq!(guess_format_from_filename(Path::new("test.xlsx")), VectorFormat::Xlsx);
        assert_eq!(
            guess_format_from_filename(Path::new("test.json")),
            VectorFormat::GeoJson
        );
        assert_eq!(
            guess_format_from_filename(Path::new("test.geojson")),
            VectorFormat::GeoJson
        );
        assert_eq!(
            guess_format_from_filename(Path::new("test.gpkg")),
            VectorFormat::GeoPackage
        );
        assert_eq!(guess_format_from_filename(Path::new("test.vrt")), VectorFormat::Vrt);
        assert_eq!(
            guess_format_from_filename(Path::new("postgresql://")),
            VectorFormat::PostgreSQL
        );
        assert_eq!(guess_format_from_filename(Path::new("pg:")), VectorFormat::PostgreSQL);
        assert_eq!(guess_format_from_filename(Path::new("wfs:")), VectorFormat::Wfs);
        assert_eq!(guess_format_from_filename(Path::new("test")), VectorFormat::Unknown);
    }

    #[test]
    fn test_row_data_derive() {
        let path: std::path::PathBuf = [env!("CARGO_MANIFEST_DIR"), "test", "data", "road.csv"]
            .iter()
            .collect();

        let mut iter = VectorDataframeIterator::<PollutantData>::new(path.as_path()).unwrap();

        let row = iter.next().unwrap().unwrap();
        assert_eq!(row.pollutant, "NO2");
        assert_eq!(row.sector, "A_PublicTransport");
        assert_eq!(row.value, 10.0);
    }
}

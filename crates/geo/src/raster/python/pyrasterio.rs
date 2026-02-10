use crate::{ArrowRaster, Error, RasterIO, Result, io};
use inf::GeoMetadata;
use pyo3::{Py, PyAny, PyResult, Python, pyfunction};

use super::pyraster::{PyRaster, PyRasterMetadata};

fn detect_gdal_type(data_type: Py<PyAny>) -> Result<gdal::raster::GdalDataType> {
    Python::attach(|py| {
        let data_type_str = {
            if let Ok(dtype) = data_type.extract::<String>(py) {
                dtype
            } else {
                data_type.to_string()
            }
        };

        Ok(match data_type_str.as_str() {
            "u8" | "uint8" => gdal::raster::GdalDataType::UInt8,
            "i8" | "int8" => gdal::raster::GdalDataType::Int8,
            "u16" | "uint16" => gdal::raster::GdalDataType::UInt16,
            "i16" | "int16" => gdal::raster::GdalDataType::Int16,
            "u32" | "uint32" => gdal::raster::GdalDataType::UInt32,
            "i32" | "int32" => gdal::raster::GdalDataType::Int32,
            "u64" | "uint64" => gdal::raster::GdalDataType::UInt64,
            "i64" | "int64" => gdal::raster::GdalDataType::Int64,
            "f32" | "float" | "float32" => gdal::raster::GdalDataType::Float32,
            "f64" | "double" | "float64" => gdal::raster::GdalDataType::Float64,
            _ => return Err(Error::Runtime(format!("Unknown raster data type: '{}'", data_type_str))),
        })
    })
}

pub fn read_raster_typed(dtype: gdal::raster::GdalDataType, path: &std::path::Path, bounds: Option<GeoMetadata>) -> Result<PyRaster> {
    use gdal::raster::GdalDataType;

    let band_index = 1;
    if let Some(region) = bounds.as_ref() {
        Ok(match dtype {
            GdalDataType::UInt8 => PyRaster::new(ArrowRaster::<u8>::read_bounds(path, region, band_index)?),
            GdalDataType::Int8 => PyRaster::new(ArrowRaster::<i8>::read_bounds(path, region, band_index)?),
            GdalDataType::UInt16 => PyRaster::new(ArrowRaster::<u16>::read_bounds(path, region, band_index)?),
            GdalDataType::Int16 => PyRaster::new(ArrowRaster::<i16>::read_bounds(path, region, band_index)?),
            GdalDataType::UInt32 => PyRaster::new(ArrowRaster::<u32>::read_bounds(path, region, band_index)?),
            GdalDataType::Int32 => PyRaster::new(ArrowRaster::<i32>::read_bounds(path, region, band_index)?),
            GdalDataType::UInt64 => PyRaster::new(ArrowRaster::<u64>::read_bounds(path, region, band_index)?),
            GdalDataType::Int64 => PyRaster::new(ArrowRaster::<i64>::read_bounds(path, region, band_index)?),
            GdalDataType::Float32 => PyRaster::new(ArrowRaster::<f32>::read_bounds(path, region, band_index)?),
            GdalDataType::Float64 => PyRaster::new(ArrowRaster::<f64>::read_bounds(path, region, band_index)?),
            GdalDataType::Unknown => return Err(Error::Runtime("Unknown raster data type".to_string())),
        })
    } else {
        Ok(match dtype {
            GdalDataType::UInt8 => PyRaster::new(ArrowRaster::<u8>::read(path)?),
            GdalDataType::Int8 => PyRaster::new(ArrowRaster::<i8>::read(path)?),
            GdalDataType::UInt16 => PyRaster::new(ArrowRaster::<u16>::read(path)?),
            GdalDataType::Int16 => PyRaster::new(ArrowRaster::<i16>::read(path)?),
            GdalDataType::UInt32 => PyRaster::new(ArrowRaster::<u32>::read(path)?),
            GdalDataType::Int32 => PyRaster::new(ArrowRaster::<i32>::read(path)?),
            GdalDataType::UInt64 => PyRaster::new(ArrowRaster::<u64>::read(path)?),
            GdalDataType::Int64 => PyRaster::new(ArrowRaster::<i64>::read(path)?),
            GdalDataType::Float32 => PyRaster::new(ArrowRaster::<f32>::read(path)?),
            GdalDataType::Float64 => PyRaster::new(ArrowRaster::<f64>::read(path)?),
            GdalDataType::Unknown => return Err(Error::Runtime("Unknown raster data type".to_string())),
        })
    }
}

#[pyfunction]
pub fn read_raster(path: std::path::PathBuf) -> Result<PyRaster> {
    read_raster_typed(io::detect_raster_data_type(path.as_path(), 1)?, path.as_path(), None)
}

#[pyfunction]
pub fn read_raster_as(data_type: Py<PyAny>, path: std::path::PathBuf) -> PyResult<PyRaster> {
    Ok(read_raster_typed(detect_gdal_type(data_type)?, path.as_path(), None)?)
}

#[pyfunction]
pub fn read_raster_region(path: std::path::PathBuf, meta: &PyRasterMetadata) -> Result<PyRaster> {
    read_raster_typed(io::detect_raster_data_type(path.as_path(), 1)?, path.as_path(), Some(meta.into()))
}

#[pyfunction]
pub fn read_raster_region_as(data_type: Py<PyAny>, path: std::path::PathBuf, meta: &PyRasterMetadata) -> Result<PyRaster> {
    read_raster_typed(detect_gdal_type(data_type)?, path.as_path(), Some(meta.into()))
}

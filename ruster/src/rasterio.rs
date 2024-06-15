use crate::{raster::PyRaster, Error, Result};
use inf::{arrowrasterio::read_arrow_raster, rasterio};
use pyo3::pyfunction;

#[pyfunction]
pub fn read_raster(path: std::path::PathBuf) -> Result<PyRaster> {
    use gdal::raster::GdalDataType;

    Ok(match rasterio::detect_raster_data_type(path.as_path(), 1)? {
        GdalDataType::UInt8 => PyRaster::new(read_arrow_raster::<u8>(path.as_path())?),
        GdalDataType::Int8 => PyRaster::new(read_arrow_raster::<i8>(path.as_path())?),
        GdalDataType::UInt16 => PyRaster::new(read_arrow_raster::<u16>(path.as_path())?),
        GdalDataType::Int16 => PyRaster::new(read_arrow_raster::<i16>(path.as_path())?),
        GdalDataType::UInt32 => PyRaster::new(read_arrow_raster::<u32>(path.as_path())?),
        GdalDataType::Int32 => PyRaster::new(read_arrow_raster::<i32>(path.as_path())?),
        GdalDataType::UInt64 => PyRaster::new(read_arrow_raster::<u64>(path.as_path())?),
        GdalDataType::Int64 => PyRaster::new(read_arrow_raster::<i64>(path.as_path())?),
        GdalDataType::Float32 => PyRaster::new(read_arrow_raster::<f32>(path.as_path())?),
        GdalDataType::Float64 => PyRaster::new(read_arrow_raster::<f64>(path.as_path())?),
        GdalDataType::Unknown => return Err(Error::Runtime("Unknown raster data type".to_string())),
    })
}

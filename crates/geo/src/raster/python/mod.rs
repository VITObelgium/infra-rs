// #[pymodule]
// #[pyo3(name = "ruster")]
// fn my_extension(m: &Bound<'_, PyModule>) -> PyResult<()> {
//     pyo3_log::init();

//     inf::gdalinterop::setup_logging(true);
//     m.add_function(wrap_pyfunction!(rasterio::read_raster, m)?)?;
//     m.add_function(wrap_pyfunction!(rasterio::read_raster_as, m)?)?;
//     m.add_function(wrap_pyfunction!(rasterio::read_raster_region, m)?)?;
//     m.add_function(wrap_pyfunction!(rasterio::read_raster_region_as, m)?)?;
//     m.add_class::<PyRaster>()?;
//     m.add_class::<PyRasterMetadata>()?;
//     Ok(())
// }

pub(super) mod pyraster;
//pub(super) mod pyrasterio;

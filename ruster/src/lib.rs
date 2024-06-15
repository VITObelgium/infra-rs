use pyo3::{exceptions::PyRuntimeError, prelude::*};
use pyo3::{types::PyModule, wrap_pyfunction, Bound, PyResult};

use raster::{PyMetadata, PyRaster};
use rasterio::read_raster;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    GdalError(#[from] inf::Error),
    #[error("{0}")]
    Runtime(String),
}

impl From<Error> for PyErr {
    fn from(err: Error) -> PyErr {
        PyRuntimeError::new_err(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[pymodule]
#[pyo3(name = "ruster")]
fn my_extension(m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();

    inf::rasterio::setup_logging(true);
    m.add_function(wrap_pyfunction!(read_raster, m)?)?;
    m.add_class::<PyRaster>()?;
    m.add_class::<PyMetadata>()?;
    Ok(())
}

mod raster;
mod rasterio;

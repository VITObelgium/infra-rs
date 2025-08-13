use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Raster dimensions do not match ({}x{}) <-> ({}x{})", .size1.0, .size1.1, .size2.0, .size2.1)]
    SizeMismatch { size1: (usize, usize), size2: (usize, usize) },
    #[error("The operation has been cancelled")]
    Cancelled,
    #[error("Invalid path: {0}")]
    InvalidPath(std::path::PathBuf),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Runtime error: {0}")]
    Runtime(String),
    #[error("Database error: {0}")]
    DatabaseError(String),
    #[error("Invalid string: {0}")]
    InvalidString(#[from] std::ffi::NulError),
    #[error("Invalid: {0}")]
    InvalidNumber(String),
    #[error("System time error")]
    TimeError(#[from] std::time::SystemTimeError),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[cfg(feature = "gdal")]
    #[error("GDAL error: {0}")]
    GdalError(#[from] gdal::errors::GdalError),
    #[cfg(feature = "proj4rs")]
    #[error("Proj error: {0}")]
    Proj4rsError(#[from] proj4rs::errors::Error),
    #[cfg(feature = "proj")]
    #[error("Proj error: {0}")]
    ProjError(#[from] proj::ProjError),
    #[cfg(feature = "proj")]
    #[error("Proj error: {0}")]
    ProjCreateError(#[from] proj::ProjCreateError),
    #[cfg(feature = "python")]
    #[error("Python error: {0}")]
    PythonError(#[from] pyo3::PyErr),
    #[error("Geozero error: {0}")]
    GeoZeroError(#[from] geozero::error::GeozeroError),
    #[cfg(feature = "vector")]
    #[error("Geos error: {0}")]
    GeosError(#[from] geos::Error),
    //#[error("Proj error: {0}")]
    //ProjError(#[from] proj4rs::errors::Error),
    #[error("Error: {0}")]
    Infra(#[from] inf::Error),
    #[cfg(feature = "cog")]
    #[error("Tiff error: {0}")]
    TiffError(#[from] tiff::TiffError),
}

#[cfg(feature = "python")]
impl std::convert::From<Error> for pyo3::PyErr {
    fn from(err: Error) -> pyo3::PyErr {
        match err {
            Error::IOError(_) => pyo3::PyErr::new::<pyo3::exceptions::PyIOError, _>(err.to_string()),
            Error::InvalidArgument(_) => pyo3::PyErr::new::<pyo3::exceptions::PyValueError, _>(err.to_string()),
            _ => pyo3::exceptions::PyRuntimeError::new_err(err.to_string()),
        }
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Self {
        Error::InvalidNumber(err.to_string())
    }
}

impl From<std::num::ParseFloatError> for Error {
    fn from(err: std::num::ParseFloatError) -> Self {
        Error::InvalidNumber(err.to_string())
    }
}

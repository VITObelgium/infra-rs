use thiserror::Error;

pub mod cog;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Runtime error: {0}")]
    Runtime(String),
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Tiff error: {0}")]
    TiffError(#[from] tiff::TiffError),
}

pub type Result<T = ()> = std::result::Result<T, Error>;

#[cfg(test)]
pub mod testutils;

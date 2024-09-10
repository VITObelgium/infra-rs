use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("The operation has been cancelled")]
    Cancelled,
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error("Runtime error: {0}")]
    Runtime(String),
    #[error("Invalid : {0}")]
    InvalidNumber(String),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
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

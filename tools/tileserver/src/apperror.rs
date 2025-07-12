use axum::{
    Json, http,
    response::{IntoResponse, Response},
};
use serde_json::json;

use crate::Error;

/// Our app's top level error type.
#[derive(Debug)]
pub enum AppError {
    /// Something went wrong when calling the user repo.
    Error(Error),
}

impl From<crate::Error> for AppError {
    fn from(inner: crate::Error) -> Self {
        AppError::Error(inner)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message) = match self {
            AppError::Error(err) => match err {
                Error::Runtime(err) => (http::StatusCode::INTERNAL_SERVER_ERROR, err),
                Error::GdalError(err) => (http::StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
                Error::TimeError(err) => (http::StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
                Error::IOError(err) => (http::StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
                Error::InfError(err) => (http::StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
                Error::InvalidArgument(err) => (http::StatusCode::BAD_REQUEST, err),
                #[cfg(feature = "vector-diff")]
                Error::MvtError(err) => (http::StatusCode::INTERNAL_SERVER_ERROR, err.to_string()),
                Error::GeoError(_) | Error::SqliteError(_) | Error::RasterTileError(_) => {
                    (http::StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
                }
            },
        };

        let body = Json(json!({
            "error": error_message,
        }));

        (status, body).into_response()
    }
}

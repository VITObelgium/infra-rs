//! Error types for LERC decoding

use std::fmt;

/// Result type alias for LERC operations
pub type Result<T> = std::result::Result<T, LercError>;

/// Errors that can occur during LERC decoding
#[derive(Debug, Clone)]
pub enum LercError {
    /// Invalid or corrupted LERC data
    InvalidData(String),
    /// Buffer too small for the operation
    BufferTooSmall,
    /// Checksum mismatch
    ChecksumMismatch,
    /// Unsupported LERC version
    UnsupportedVersion(i32),
    /// Unsupported data type
    UnsupportedDataType,
    /// Invalid header
    InvalidHeader(String),
    /// Invalid mask data
    InvalidMask(String),
    /// Huffman decoding error
    HuffmanError(String),
    /// RLE decoding error
    RleError(String),
    /// Bit stuffing error
    BitStufferError(String),
    /// Float point compression error
    FplError(String),
    /// I/O error
    IoError(String),
    /// Unexpected end of data
    UnexpectedEof,
}

impl fmt::Display for LercError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LercError::InvalidData(msg) => write!(f, "Invalid LERC data: {}", msg),
            LercError::BufferTooSmall => write!(f, "Buffer too small"),
            LercError::ChecksumMismatch => write!(f, "Checksum mismatch"),
            LercError::UnsupportedVersion(v) => write!(f, "Unsupported LERC version: {}", v),
            LercError::UnsupportedDataType => write!(f, "Unsupported data type"),
            LercError::InvalidHeader(msg) => write!(f, "Invalid header: {}", msg),
            LercError::InvalidMask(msg) => write!(f, "Invalid mask: {}", msg),
            LercError::HuffmanError(msg) => write!(f, "Huffman decoding error: {}", msg),
            LercError::RleError(msg) => write!(f, "RLE decoding error: {}", msg),
            LercError::BitStufferError(msg) => write!(f, "Bit stuffer error: {}", msg),
            LercError::FplError(msg) => write!(f, "Float point compression error: {}", msg),
            LercError::IoError(msg) => write!(f, "I/O error: {}", msg),
            LercError::UnexpectedEof => write!(f, "Unexpected end of data"),
        }
    }
}

impl std::error::Error for LercError {}

impl From<std::io::Error> for LercError {
    fn from(err: std::io::Error) -> Self {
        LercError::IoError(err.to_string())
    }
}

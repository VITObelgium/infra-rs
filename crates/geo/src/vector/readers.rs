#[cfg(feature = "vector-io-xlsx")]
mod xlsx;

#[cfg(feature = "vector-io-xlsx")]
pub use xlsx::XlsxReader;

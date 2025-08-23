#[cfg(feature = "gdal")]
mod gdal;
#[cfg(test)]
mod readertests;
#[cfg(feature = "vector-io-xlsx")]
mod xlsx;

#[cfg(feature = "vector-io-xlsx")]
pub use xlsx::XlsxReader;

#[cfg(feature = "gdal")]
pub use gdal::GdalReader;

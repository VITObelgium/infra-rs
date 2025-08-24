#[cfg(feature = "vector-io-csv")]
mod csv;
#[cfg(feature = "gdal")]
mod gdal;
#[cfg(test)]
mod readertests;
#[cfg(feature = "vector-io-xlsx")]
mod xlsx;

#[cfg(feature = "vector-io-csv")]
pub use csv::CsvReader;
#[cfg(feature = "vector-io-xlsx")]
pub use xlsx::XlsxReader;

#[cfg(feature = "gdal")]
pub use gdal::GdalReader;

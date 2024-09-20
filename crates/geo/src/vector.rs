#[cfg(feature = "gdal")]
pub mod algo;
mod burnvalue;
#[cfg(feature = "gdal")]
mod coveragetools;
#[cfg(feature = "gdal")]
pub mod datarow;
#[cfg(feature = "gdal")]
pub mod fieldtype;
pub mod geometrytype;
#[cfg(feature = "gdal")]
pub mod io;
#[cfg(feature = "gdal")]
pub mod polygoncoverage;

#[doc(inline)]
pub use burnvalue::BurnValue;
#[doc(inline)]
#[cfg(feature = "gdal")]
pub use datarow::DataRow;

/// The `DataRow` trait is implemented using the `DataRow` derive macro
/// This allows to read vector data in a more type-safe way directly into a struct
/// # `DataframeIterator` iterator example using the `DataRow` derive macro
/// ```
/// # use geo::vector::io::DataframeIterator;
/// # use geo::vector::DataRow;
/// # use std::path::PathBuf;
/// // Read a csv or xlsx file with the following header:
/// // Pollutant,Sector,value
/// // If the struct field names do not match the column names, use the column attribute
/// #[derive(DataRow)]
/// struct PollutantData {
///     #[vector(column = "Pollutant")]
///     pollutant: String,
///     #[vector(column = "Sector")]
///     sector: String,
///     value: f64,
///     #[vector(skip)]
///     not_in_data: String,
/// }
/// let iter = DataframeIterator::<PollutantData>::new(&PathBuf::from("pol.csv"), None);
/// ```
#[doc(inline)]
pub use vector_derive::DataRow;

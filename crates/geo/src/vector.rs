#[cfg(feature = "gdal")]
pub mod algo;
mod burnvalue;
#[cfg(feature = "gdal")]
mod coveragetools;
pub mod dataframe;
pub mod datarow;
pub mod fieldtype;
pub mod geometrytype;
#[cfg(feature = "gdal")]
pub mod io;
#[cfg(feature = "gdal")]
pub mod polygoncoverage;
pub mod readers;

#[doc(inline)]
pub use burnvalue::BurnValue;
#[doc(inline)]
#[cfg(feature = "gdal")]
pub use datarow::DataRow;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum VectorFormat {
    Memory,
    Csv,
    Tab,
    ShapeFile,
    Xlsx,
    GeoJson,
    GeoPackage,
    PostgreSQL,
    Wfs,
    Vrt,
    Parquet,
    Arrow,
    Unknown,
}

impl VectorFormat {
    /// Given a file path, guess the raster type based on the file extension
    pub fn guess_from_path(file_path: &std::path::Path) -> VectorFormat {
        let ext = file_path.extension().map(|ext| ext.to_string_lossy().to_lowercase());

        if let Some(ext) = ext {
            match ext.as_ref() {
                "csv" => return VectorFormat::Csv,
                "tab" => return VectorFormat::Tab,
                "shp" | "dbf" => return VectorFormat::ShapeFile,
                "xlsx" => return VectorFormat::Xlsx,
                "json" | "geojson" => return VectorFormat::GeoJson,
                "gpkg" => return VectorFormat::GeoPackage,
                "vrt" => return VectorFormat::Vrt,
                "parquet" => return VectorFormat::Parquet,
                "arrow" | "arrows" => return VectorFormat::Arrow,
                _ => {}
            }
        }

        let path = file_path.to_string_lossy();
        if path.starts_with("postgresql://") || path.starts_with("pg:") {
            VectorFormat::PostgreSQL
        } else if path.starts_with("wfs:") {
            VectorFormat::Wfs
        } else {
            VectorFormat::Unknown
        }
    }
}

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

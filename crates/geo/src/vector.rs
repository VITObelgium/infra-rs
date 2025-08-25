//! Vector data handling with type-safe data structures and I/O.

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

#[doc(inline)]
pub use datarow::{DataRowsIterator, read_dataframe_rows};

pub use vector_derive::DataRow;

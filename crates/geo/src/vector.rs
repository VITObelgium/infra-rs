//! Vector data handling with type-safe data structures and I/O.

#[cfg(feature = "gdal")]
#[cfg_attr(docsrs, doc(cfg(feature = "gdal")))]
pub mod algo;
mod burnvalue;
#[cfg(feature = "vector-processing")]
#[cfg_attr(docsrs, doc(cfg(feature = "vector-processing")))]
mod coveragetools;
#[cfg(feature = "vector-io")]
#[cfg_attr(docsrs, doc(cfg(feature = "vector-io")))]
pub mod dataframe;
#[cfg(feature = "vector-io")]
#[cfg_attr(docsrs, doc(cfg(feature = "vector-io")))]
pub mod datarow;
#[cfg(feature = "vector-io")]
#[cfg_attr(docsrs, doc(cfg(feature = "vector-io")))]
pub mod fieldtype;
#[cfg(feature = "gdal")]
#[cfg_attr(docsrs, doc(cfg(feature = "gdal")))]
pub mod gdalio;
pub mod geometrytype;
#[cfg(feature = "vector-io")]
#[cfg_attr(docsrs, doc(cfg(feature = "vector-io")))]
pub mod io;
#[cfg(feature = "vector-processing")]
#[cfg_attr(docsrs, doc(cfg(feature = "vector-processing")))]
pub mod polygoncoverage;
pub mod readers;

#[doc(inline)]
pub use burnvalue::BurnValue;
#[doc(inline)]
#[cfg(feature = "vector-io")]
pub use datarow::DataRow;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum VectorFileFormat {
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

impl VectorFileFormat {
    /// Given a file path, guess the raster type based on the file extension
    pub fn guess_from_path(file_path: &std::path::Path) -> VectorFileFormat {
        let ext = file_path.extension().map(|ext| ext.to_string_lossy().to_lowercase());

        if let Some(ext) = ext {
            match ext.as_ref() {
                "csv" => return VectorFileFormat::Csv,
                "tab" => return VectorFileFormat::Tab,
                "shp" | "dbf" => return VectorFileFormat::ShapeFile,
                "xlsx" => return VectorFileFormat::Xlsx,
                "json" | "geojson" => return VectorFileFormat::GeoJson,
                "gpkg" => return VectorFileFormat::GeoPackage,
                "vrt" => return VectorFileFormat::Vrt,
                "parquet" => return VectorFileFormat::Parquet,
                "arrow" | "arrows" => return VectorFileFormat::Arrow,
                _ => {}
            }
        }

        let path = file_path.to_string_lossy();
        if path.starts_with("postgresql://") || path.starts_with("pg:") {
            VectorFileFormat::PostgreSQL
        } else if path.starts_with("wfs:") {
            VectorFileFormat::Wfs
        } else {
            VectorFileFormat::Unknown
        }
    }
}

#[doc(inline)]
#[cfg(feature = "vector-io")]
pub use datarow::{DataRowsIterator, read_dataframe_rows};

#[cfg(feature = "derive")]
#[cfg_attr(docsrs, doc(cfg(feature = "derive")))]
pub use vector_derive::DataRow;

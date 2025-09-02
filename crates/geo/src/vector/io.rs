//! Contains functions to read and write vector data using the GDAL library.

use crate::vector::VectorFileFormat;

impl VectorFileFormat {
    pub fn gdal_driver_name(&self) -> &str {
        match self {
            VectorFileFormat::Memory => "Memory",
            VectorFileFormat::Csv | VectorFileFormat::Tab => "CSV",
            VectorFileFormat::ShapeFile => "ESRI Shapefile",
            VectorFileFormat::Xlsx => "XLSX",
            VectorFileFormat::GeoJson => "GeoJSON",
            VectorFileFormat::GeoPackage => "GPKG",
            VectorFileFormat::PostgreSQL => "PostgreSQL",
            VectorFileFormat::Wfs => "WFS",
            VectorFileFormat::Vrt => "OGR_VRT",
            VectorFileFormat::Parquet => "Parquet",
            VectorFileFormat::Arrow => "Arrow",
            VectorFileFormat::Unknown => "Unknown",
        }
    }
}

#[cfg(test)]
mod tests {

    use std::path::Path;

    use super::*;

    #[test]
    fn vectorformat_guess_from_path() {
        assert_eq!(VectorFileFormat::guess_from_path(Path::new("test.csv")), VectorFileFormat::Csv);
        assert_eq!(VectorFileFormat::guess_from_path(Path::new("test.tab")), VectorFileFormat::Tab);
        assert_eq!(
            VectorFileFormat::guess_from_path(Path::new("test.shp")),
            VectorFileFormat::ShapeFile
        );
        assert_eq!(
            VectorFileFormat::guess_from_path(Path::new("test.dbf")),
            VectorFileFormat::ShapeFile
        );
        assert_eq!(VectorFileFormat::guess_from_path(Path::new("test.xlsx")), VectorFileFormat::Xlsx);
        assert_eq!(VectorFileFormat::guess_from_path(Path::new("test.json")), VectorFileFormat::GeoJson);
        assert_eq!(
            VectorFileFormat::guess_from_path(Path::new("test.geojson")),
            VectorFileFormat::GeoJson
        );
        assert_eq!(
            VectorFileFormat::guess_from_path(Path::new("test.gpkg")),
            VectorFileFormat::GeoPackage
        );
        assert_eq!(VectorFileFormat::guess_from_path(Path::new("test.vrt")), VectorFileFormat::Vrt);
        assert_eq!(
            VectorFileFormat::guess_from_path(Path::new("postgresql://")),
            VectorFileFormat::PostgreSQL
        );
        assert_eq!(VectorFileFormat::guess_from_path(Path::new("pg:")), VectorFileFormat::PostgreSQL);
        assert_eq!(VectorFileFormat::guess_from_path(Path::new("wfs:")), VectorFileFormat::Wfs);
        assert_eq!(VectorFileFormat::guess_from_path(Path::new("test")), VectorFileFormat::Unknown);
    }
}

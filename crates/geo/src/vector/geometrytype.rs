/// A subset of the geometry types we are expected to actually use (maches the types in the geo_types crate)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeometryType {
    Point,
    MultiPoint,
    LineString,
    MultiLineString,
    Polygon,
    MultiPolygon,
    Triangle,
    GeometryCollection,
    Geometry,
    None,
}

impl TryFrom<gdal_sys::OGRwkbGeometryType::Type> for GeometryType {
    type Error = crate::Error;

    fn try_from(value: gdal_sys::OGRwkbGeometryType::Type) -> Result<Self, Self::Error> {
        match value {
            gdal_sys::OGRwkbGeometryType::wkbPoint => Ok(GeometryType::Point),
            gdal_sys::OGRwkbGeometryType::wkbMultiPoint => Ok(GeometryType::MultiPoint),
            gdal_sys::OGRwkbGeometryType::wkbLineString => Ok(GeometryType::LineString),
            gdal_sys::OGRwkbGeometryType::wkbMultiLineString => Ok(GeometryType::MultiLineString),
            gdal_sys::OGRwkbGeometryType::wkbPolygon => Ok(GeometryType::Polygon),
            gdal_sys::OGRwkbGeometryType::wkbMultiPolygon => Ok(GeometryType::MultiPolygon),
            gdal_sys::OGRwkbGeometryType::wkbGeometryCollection => Ok(GeometryType::GeometryCollection),
            gdal_sys::OGRwkbGeometryType::wkbUnknown => Ok(GeometryType::Geometry),
            gdal_sys::OGRwkbGeometryType::wkbNone => Ok(GeometryType::None),
            _ => Err(crate::Error::InvalidArgument(format!(
                "Unsupported geometry type: {}",
                value
            ))),
        }
    }
}

impl From<GeometryType> for gdal_sys::OGRwkbGeometryType::Type {
    fn from(value: GeometryType) -> Self {
        match value {
            GeometryType::Point => gdal_sys::OGRwkbGeometryType::wkbPoint,
            GeometryType::MultiPoint => gdal_sys::OGRwkbGeometryType::wkbMultiPoint,
            GeometryType::LineString => gdal_sys::OGRwkbGeometryType::wkbLineString,
            GeometryType::MultiLineString => gdal_sys::OGRwkbGeometryType::wkbMultiLineString,
            GeometryType::Polygon => gdal_sys::OGRwkbGeometryType::wkbPolygon,
            GeometryType::MultiPolygon => gdal_sys::OGRwkbGeometryType::wkbMultiPolygon,
            GeometryType::GeometryCollection => gdal_sys::OGRwkbGeometryType::wkbGeometryCollection,
            GeometryType::Geometry => gdal_sys::OGRwkbGeometryType::wkbUnknown,
            GeometryType::None => gdal_sys::OGRwkbGeometryType::wkbNone,
            GeometryType::Triangle => gdal_sys::OGRwkbGeometryType::wkbTriangle,
        }
    }
}

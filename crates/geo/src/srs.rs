//! Spatial reference system handling and coordinate transformations.

use crate::Result;
use crate::crs::Epsg;

#[cfg(feature = "gdal")]
mod gdal;
#[cfg(feature = "proj")]
mod proj;
#[cfg(feature = "proj4rs")]
mod proj4rs;

#[cfg(feature = "gdal")]
#[cfg_attr(docsrs, doc(cfg(feature = "gdal")))]
pub use gdal::SpatialReference as GdalSpatialReference;

#[cfg(feature = "proj")]
#[cfg_attr(docsrs, doc(cfg(feature = "proj")))]
pub use proj::CoordinateTransformer as ProjCoordinateTransformer;

#[cfg(feature = "proj4rs")]
#[cfg_attr(docsrs, doc(cfg(feature = "proj4rs")))]
pub use {proj4rs::CoordinateTransformer as Proj4rsCoordinateTransformer, proj4rs::SpatialReference as Proj4rsSpatialReference};

#[cfg(feature = "proj4rs")]
pub use {proj4rs::CoordinateTransformer, proj4rs::SpatialReference};

#[cfg(all(feature = "proj", not(feature = "proj4rs")))]
// proj4rs takes precedence over proj if both are enabled
pub use proj::CoordinateTransformer;

#[cfg(all(feature = "gdal", not(feature = "proj4rs")))]
// proj4rs takes precedence over gdal if both are enabled
pub use gdal::SpatialReference;

/// Single shot version of `SpatialReference::to_wkt`
#[allow(unreachable_code, unused)]
pub fn projection_from_epsg(epsg: Epsg) -> Result<String> {
    #[cfg(any(feature = "proj4rs", feature = "gdal"))]
    return SpatialReference::from_epsg(epsg)?.to_wkt();

    panic!("No spatial reference backend enabled. Enable either 'proj4rs' or 'gdal' feature.");
}

/// Single shot version of `SpatialReference::epsg_geog_cs`
#[allow(unreachable_code, unused)]
pub fn projection_to_geo_epsg(projection: &str) -> Option<Epsg> {
    #[cfg(any(feature = "proj4rs", feature = "gdal"))]
    return SpatialReference::from_definition(projection).ok()?.epsg_geog_cs();

    panic!("No spatial reference backend enabled. Enable either 'proj4rs' or 'gdal' feature.");
}

/// Single shot version of `SpatialReference::epsg_cs`
#[allow(unreachable_code, unused)]
pub fn projection_to_epsg(projection: &str) -> Option<Epsg> {
    #[cfg(any(feature = "proj4rs", feature = "gdal"))]
    return SpatialReference::from_definition(projection).ok()?.epsg_cs();

    panic!("No spatial reference backend enabled. Enable either 'proj4rs' or 'gdal' feature.");
}

#[cfg(all(feature = "proj4rs", feature = "proj"))]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Point, Result, crs};
    use approx::assert_relative_eq;

    #[test]
    fn compare_transformers() -> Result<()> {
        let source_crs = crs::epsg::BELGIAN_LAMBERT72;
        let target_crs = crs::epsg::WGS84_WEB_MERCATOR;

        let proj_transformer = proj::CoordinateTransformer::from_epsg(source_crs, target_crs)?;
        let proj4rs_transformer = proj4rs::CoordinateTransformer::from_epsg(source_crs, target_crs)?;

        let point = Point::new(22000.0, 245000.0);
        let proj_result = proj_transformer.transform_point(point)?;
        let proj4rs_result = proj4rs_transformer.transform_point(point)?;

        assert_relative_eq!(proj_result, proj4rs_result, epsilon = 1e-3);

        Ok(())
    }
}

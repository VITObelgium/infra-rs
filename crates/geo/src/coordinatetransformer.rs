#[cfg(feature = "proj")]
mod proj;
#[cfg(feature = "proj")]
pub use proj::CoordinateTransformer;

#[cfg(feature = "proj4rs")]
mod proj4rs;
#[cfg(all(feature = "proj4rs", not(feature = "proj")))]
// proj takes precedence over proj4rs if both are enabled
pub use proj4rs::CoordinateTransformer;

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

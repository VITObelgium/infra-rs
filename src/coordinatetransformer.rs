use gdal::spatial_ref::CoordTransform;

use crate::coordinate::to_point;
use crate::crs::Epsg;
use crate::spatialreference::SpatialReference;
use crate::to_coordinate;
use crate::Coordinate;
use crate::Error;
use crate::Point;

pub struct CoordinateTransformer {
    source_srs: SpatialReference,
    target_srs: SpatialReference,
    transformer: CoordTransform,
}

impl CoordinateTransformer {
    pub fn new(source_srs: SpatialReference, target_srs: SpatialReference) -> Result<Self, Error> {
        let transformer = CoordTransform::new(source_srs.srs(), target_srs.srs())?;
        Ok(Self {
            source_srs,
            target_srs,
            transformer,
        })
    }

    pub fn from_epsg(source_epsg: Epsg, target_epsg: Epsg) -> Result<Self, Error> {
        let source_srs = SpatialReference::from_epsg(source_epsg)?;
        let target_srs = SpatialReference::from_epsg(target_epsg)?;
        Self::new(source_srs, target_srs)
    }

    pub fn transform_point(&self, point: Point<f64>) -> Result<Point, Error> {
        let mut result_x = [point.x()];
        let mut result_y = [point.y()];
        self.transformer.transform_coords(&mut result_x, &mut result_y, &mut [])?;
        Ok(Point::new(result_x[0], result_y[0]))
    }

    pub fn transform_point_in_place(&self, point: &mut Point<f64>) -> Result<(), Error> {
        let mut result_x = [point.x()];
        let mut result_y = [point.y()];
        self.transformer.transform_coords(&mut result_x, &mut result_y, &mut [])?;

        point.set_x(result_x[0]);
        point.set_y(result_y[0]);
        Ok(())
    }

    pub fn transform_coordinate(&self, coord: Coordinate) -> Result<Coordinate, Error> {
        Ok(to_coordinate(self.transform_point(to_point(coord))?))
    }

    pub fn transform_coordinate_in_place(&self, coord: &mut Coordinate) -> Result<(), Error> {
        let res = self.transform_coordinate(*coord)?;
        *coord = res;
        Ok(())
    }

    pub fn source_projection(&self) -> Result<String, Error> {
        self.source_srs.to_wkt()
    }

    pub fn target_projection(&self) -> Result<String, Error> {
        self.target_srs.to_wkt()
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;

    use crate::{coordinate::to_point, crs, Coordinate, CoordinateTransformer, Point};

    #[test]
    fn test_projection_point() {
        let trans = CoordinateTransformer::from_epsg(crs::epsg::WGS84, crs::epsg::BELGIAN_LAMBERT72).unwrap();
        let p = trans
            .transform_point(to_point(Coordinate::latlon(51.04223683846715, 3.5713882022278653)))
            .unwrap();
        assert_relative_eq!(p, Point::new(94079.44534873398, 192751.6060780408), epsilon = 1e-1);
    }

    #[test]
    fn test_projection_coord() {
        let trans = CoordinateTransformer::from_epsg(crs::epsg::WGS84, crs::epsg::BELGIAN_LAMBERT72).unwrap();
        let p = trans.transform_coordinate(Coordinate::latlon(51.04223683846715, 3.5713882022278653)).unwrap();
        assert_relative_eq!(to_point(p), Point::new(94079.44534873398, 192751.6060780408), epsilon = 1e-1);
    }
}

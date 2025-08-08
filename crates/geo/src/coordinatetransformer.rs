use gdal::spatial_ref::CoordTransform;

use crate::Coordinate;
use crate::Point;
use crate::Result;
use crate::crs::Epsg;
use crate::spatialreference::SpatialReference;

pub struct Points {
    x: Vec<f64>,
    y: Vec<f64>,
}

impl Points {
    pub fn new() -> Self {
        Self {
            x: Vec::new(),
            y: Vec::new(),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = Point<f64>> {
        self.x.iter().zip(self.y.iter()).map(|(&x, &y)| Point::new(x, y))
    }

    pub fn with_capacity(size: usize) -> Self {
        Self {
            x: Vec::with_capacity(size),
            y: Vec::with_capacity(size),
        }
    }

    pub fn push(&mut self, point: Point<f64>) {
        self.x.push(point.x());
        self.y.push(point.y());
    }

    pub fn clear(&mut self) {
        self.x.clear();
        self.y.clear();
    }
}

impl Default for Points {
    fn default() -> Self {
        Self::new()
    }
}

pub struct CoordinateTransformer {
    source_srs: SpatialReference,
    target_srs: SpatialReference,
    transformer: CoordTransform,
}

impl CoordinateTransformer {
    pub fn new(source_srs: SpatialReference, target_srs: SpatialReference) -> Result<Self> {
        let transformer = CoordTransform::new(source_srs.srs(), target_srs.srs())?;
        Ok(Self {
            source_srs,
            target_srs,
            transformer,
        })
    }

    pub fn from_epsg(source_epsg: Epsg, target_epsg: Epsg) -> Result<Self> {
        let source_srs = SpatialReference::from_epsg(source_epsg)?;
        let target_srs = SpatialReference::from_epsg(target_epsg)?;
        Self::new(source_srs, target_srs)
    }

    pub fn transform_point(&self, point: Point) -> Result<Point> {
        let mut result_x = [point.x()];
        let mut result_y = [point.y()];
        self.transformer.transform_coords(&mut result_x, &mut result_y, &mut [])?;
        Ok(Point::new(result_x[0], result_y[0]))
    }

    pub fn transform_point_in_place(&self, point: &mut Point) -> Result<()> {
        let mut result_x = [point.x()];
        let mut result_y = [point.y()];
        self.transformer.transform_coords(&mut result_x, &mut result_y, &mut [])?;

        point.set_x(result_x[0]);
        point.set_y(result_y[0]);
        Ok(())
    }

    pub fn transform_points_in_place(&self, points: &mut Points) -> Result<()> {
        self.transformer.transform_coords(&mut points.x, &mut points.y, &mut [])?;
        Ok(())
    }

    pub fn transform_coordinate(&self, coord: Coordinate) -> Result<Coordinate> {
        Ok(Coordinate::from(self.transform_point(coord.into())?))
    }

    pub fn transform_coordinate_in_place(&self, coord: &mut Coordinate) -> Result<()> {
        let res = self.transform_coordinate(*coord)?;
        *coord = res;
        Ok(())
    }

    pub fn source_projection(&self) -> Result<String> {
        self.source_srs.to_wkt()
    }

    pub fn target_projection(&self) -> Result<String> {
        self.target_srs.to_wkt()
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;

    use crate::{Coordinate, CoordinateTransformer, Point, crs};

    #[test]
    fn test_projection_point() {
        let trans = CoordinateTransformer::from_epsg(crs::epsg::WGS84, crs::epsg::BELGIAN_LAMBERT72).unwrap();
        let p = trans
            .transform_point(Coordinate::latlon(51.04223683846715, 3.5713882022278653).into())
            .unwrap();
        assert_relative_eq!(p, Point::new(94079.44534873398, 192751.6060780408), epsilon = 1e-1);
    }

    #[test]
    fn test_projection_coord() {
        let trans = CoordinateTransformer::from_epsg(crs::epsg::WGS84, crs::epsg::BELGIAN_LAMBERT72).unwrap();
        let p = trans
            .transform_coordinate(Coordinate::latlon(51.04223683846715, 3.5713882022278653))
            .unwrap();
        assert_relative_eq!(
            Into::<Point>::into(p),
            Point::new(94079.44534873398, 192751.6060780408),
            epsilon = 1e-1
        );
    }
}

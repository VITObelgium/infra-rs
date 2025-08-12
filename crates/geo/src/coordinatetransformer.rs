use proj::Proj;

use crate::Coordinate;
use crate::Point;
use crate::Result;
use crate::crs::Epsg;

pub struct CoordinateTransformer {
    transformer: Proj,
}

impl CoordinateTransformer {
    pub fn new(source_srs: &str, target_srs: &str) -> Result<Self> {
        let transformer = Proj::new_known_crs(source_srs, target_srs, None)?;
        Ok(CoordinateTransformer { transformer })
    }

    pub fn from_epsg(source_epsg: Epsg, target_epsg: Epsg) -> Result<Self> {
        Self::new(&source_epsg.to_string(), &target_epsg.to_string())
    }

    pub fn transform_point(&self, point: Point) -> Result<Point> {
        Ok(self.transformer.convert(point)?)
    }

    pub fn transform_point_in_place(&self, point: &mut Point) -> Result<()> {
        *point = self.transformer.convert(*point)?;
        Ok(())
    }

    pub fn transform_points_in_place(&self, points: &mut [Point]) -> Result<()> {
        self.transformer.convert_array(points)?;
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

// If both `proj4rs` and `proj` features are enabled, this module will be compiled but not used.
#![allow(dead_code)]

use proj4rs::Proj;
use proj4rs::transform::transform;

use crate::Coordinate;
use crate::Point;
use crate::Result;
use crate::crs::Epsg;

pub struct CoordinateTransformer {
    source: Proj,
    target: Proj,
    source_srs: String,
    target_srs: String,
}

impl CoordinateTransformer {
    pub fn new(source_srs: &str, target_srs: &str) -> Result<Self> {
        let source = Proj::from_proj_string(source_srs)?;
        let target = Proj::from_proj_string(target_srs)?;

        Ok(CoordinateTransformer {
            source,
            target,
            source_srs: source_srs.into(),
            target_srs: target_srs.into(),
        })
    }

    pub fn from_epsg(source_epsg: Epsg, target_epsg: Epsg) -> Result<Self> {
        let source = Proj::from_epsg_code(source_epsg.into())?;
        let target = Proj::from_epsg_code(target_epsg.into())?;

        Ok(CoordinateTransformer {
            source,
            target,
            source_srs: source_epsg.to_string(),
            target_srs: target_epsg.to_string(),
        })
    }

    pub fn transform_point(&self, point: Point) -> Result<Point> {
        // proj4rs expects (longitude, latitude) order, which matches Point's (x, y)
        let mut p = point;
        transform(&self.source, &self.target, &mut p)?;
        Ok(p)
    }

    pub fn transform_point_in_place(&self, point: &mut Point) -> Result<()> {
        transform(&self.source, &self.target, point)?;
        Ok(())
    }

    pub fn transform_points_in_place(&self, points: &mut [Point]) -> Result<()> {
        for point in points.iter_mut() {
            self.transform_point_in_place(point)?;
        }
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

    pub fn source_srs(&self) -> &str {
        &self.source_srs
    }

    pub fn target_srs(&self) -> &str {
        &self.target_srs
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;

    use crate::{Coordinate, CoordinateTransformer, Point, crs};

    #[test]
    fn proj4rs_projection_point() {
        let trans = CoordinateTransformer::from_epsg(crs::epsg::WGS84, crs::epsg::BELGIAN_LAMBERT72).unwrap();
        let p = trans
            .transform_point(Coordinate::latlon(51.04223683846715, 3.5713882022278653).into())
            .unwrap();
        assert_relative_eq!(p, Point::new(94079.44534873398, 192751.6060780408), epsilon = 1e-1);
    }

    #[test]
    fn proj4rs_projection_coord() {
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

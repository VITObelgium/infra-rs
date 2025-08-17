use gdal::spatial_ref::AxisMappingStrategy;
use proj::Proj;

use crate::Coordinate;
use crate::Error;
use crate::Point;
use crate::Result;
use crate::crs::Epsg;

#[derive(Debug, Clone, PartialEq)]
pub struct SpatialReference {
    srs: gdal::spatial_ref::SpatialRef,
}

impl SpatialReference {
    pub fn new(srs: gdal::spatial_ref::SpatialRef) -> Self {
        SpatialReference { srs }
    }

    pub fn from_proj(projection: &str) -> Result<Self> {
        if projection.is_empty() {
            return Err(Error::InvalidArgument("Empty projection string".into()));
        }

        let mut srs = gdal::spatial_ref::SpatialRef::from_proj4(projection)?;
        srs.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);
        Ok(SpatialReference { srs })
    }

    pub fn from_epsg(epsg: Epsg) -> Result<Self> {
        let mut srs = gdal::spatial_ref::SpatialRef::from_epsg(epsg.code() as u32)?;
        srs.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);
        Ok(SpatialReference { srs })
    }

    pub fn from_definition(def: &str) -> Result<Self> {
        let mut srs = gdal::spatial_ref::SpatialRef::from_definition(def)?;
        srs.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);
        Ok(SpatialReference { srs })
    }

    pub fn to_wkt(&self) -> Result<String> {
        Ok(self.srs.to_wkt()?)
    }

    pub fn to_proj(&self) -> Result<String> {
        Ok(self.srs.to_proj4()?)
    }

    pub fn is_projected(&self) -> bool {
        self.srs.is_projected()
    }

    pub fn is_geographic(&self) -> bool {
        self.srs.is_geographic()
    }

    pub fn epsg_cs(&mut self) -> Option<Epsg> {
        if self.srs.auto_identify_epsg().is_ok() {
            SpatialReference::epsg_conv(self.srs.auth_code().ok())
        } else {
            None
        }
    }

    pub fn epsg_geog_cs(&self) -> Option<Epsg> {
        if let Ok(geogcs) = self.srs.geog_cs() {
            SpatialReference::epsg_conv(geogcs.auth_code().ok())
        } else {
            None
        }
    }

    pub fn srs(&self) -> &gdal::spatial_ref::SpatialRef {
        &self.srs
    }

    fn epsg_conv(epsg: Option<i32>) -> Option<Epsg> {
        epsg.map(|epsg| Epsg::new(epsg as u16))
    }
}

/// Single shot version of `SpatialReference::to_wkt`
pub fn projection_from_epsg(epsg: Epsg) -> Result<String> {
    if let Err(e) = SpatialReference::from_epsg(epsg) {
        log::error!("Error creating spatial reference: {e}");
    }

    let spatial_ref = SpatialReference::from_epsg(epsg)?;
    spatial_ref.to_wkt()
}

/// Single shot version of `SpatialReference::epsg_geog_cs`
pub fn projection_to_geo_epsg(projection: &str) -> Option<Epsg> {
    let spatial_ref = SpatialReference::from_definition(projection).ok()?;
    spatial_ref.epsg_geog_cs()
}

/// Single shot version of `SpatialReference::epsg_cs`
pub fn projection_to_epsg(projection: &str) -> Option<Epsg> {
    let mut spatial_ref = SpatialReference::from_definition(projection).ok()?;
    spatial_ref.epsg_cs()
}

#[allow(dead_code)]
pub struct CoordinateTransformer {
    transformer: Proj,
    source_srs: String,
    target_srs: String,
}

#[allow(dead_code)]
impl CoordinateTransformer {
    pub fn new(source_srs: &str, target_srs: &str) -> Result<Self> {
        let transformer = Proj::new_known_crs(source_srs, target_srs, None)?;
        Ok(CoordinateTransformer {
            transformer,
            source_srs: source_srs.into(),
            target_srs: target_srs.into(),
        })
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

    use super::{CoordinateTransformer, SpatialReference};
    use crate::{Coordinate, Point, crs};

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

    #[test]
    fn epsg_import() {
        let srs = SpatialReference::from_epsg(31370.into()).unwrap();
        assert!(srs.is_projected());
        assert!(!srs.is_geographic());
        assert_eq!(srs.epsg_geog_cs(), Some(4313.into()));

        // let mut srs = SpatialReference::from_definition(&srs.to_proj().unwrap()).unwrap();
        // assert!(srs.is_projected());
        // assert_eq!(srs.epsg_cs(), Some(31370.into()));

        let mut srs = SpatialReference::from_definition(&srs.to_wkt().unwrap()).unwrap();
        assert!(srs.is_projected());
        assert_eq!(srs.epsg_cs(), Some(31370.into()));
    }
}

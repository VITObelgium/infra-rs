// If both `proj4rs` and `proj` features are enabled, this module will be compiled but not used.
#![allow(dead_code)]

use proj4rs::Proj;
use proj4rs::proj::ProjType;
use proj4rs::transform::transform;
use proj4wkt::wkt_to_projstring;

use crate::Coordinate;
use crate::Error;
use crate::Point;
use crate::Result;
use crate::crs::Epsg;
use crate::crs::epsg;

struct WktProcessor;

#[derive(Debug, Clone)]
pub struct SpatialReference {
    srs: Proj,
    epsg: Option<Epsg>,
    epsg_geo: Option<Epsg>,
    proj_str: String,
}

impl SpatialReference {
    pub fn from_proj(projection: &str) -> Result<Self> {
        if projection.is_empty() {
            return Err(Error::InvalidArgument("Empty projection string".into()));
        }

        Ok(Self {
            srs: Proj::from_proj_string(projection)?,
            proj_str: projection.to_string(),
            epsg: None,
            epsg_geo: None,
        })
    }

    pub fn from_epsg(epsg: Epsg) -> Result<Self> {
        let proj_str = crs_definitions::from_code(epsg.code())
            .map(|def| def.proj4.to_string())
            .ok_or_else(|| Error::Runtime(format!("Failed to generate Proj4 string for EPSG code {}", epsg)))?;

        let srs = Proj::from_proj_string(&proj_str)?;

        let epsg_geo = if srs.is_latlong() { Some(epsg) } else { None };
        let epsg = if srs.is_latlong() { None } else { Some(epsg) };

        Ok(Self {
            srs,
            proj_str,
            epsg,
            epsg_geo,
        })
    }

    pub fn from_definition(def: &str) -> Result<Self> {
        let (proj, epsg, epsg_geo) = proj_epsg_from_string(def)?;
        Ok(Self {
            srs: Proj::from_proj_string(&proj)?,
            proj_str: proj,
            epsg,
            epsg_geo,
        })
    }

    pub fn to_wkt(&self) -> Result<String> {
        if self.is_geographic() && self.epsg_geo.is_some() {
            return crs_definitions::from_code(self.epsg_geo.unwrap().into())
                .map(|def| def.wkt.to_string())
                .ok_or_else(|| {
                    Error::Runtime(format!(
                        "Failed to generate WKT for geographic projection with EPSG code {}",
                        self.epsg_geo.unwrap()
                    ))
                });
        }

        match self.epsg {
            Some(epsg) => crs_definitions::from_code(epsg.into())
                .map(|def| def.wkt.to_string())
                .ok_or_else(|| Error::Runtime(format!("Failed to generate WKT for projection with EPSG code {}", epsg))),
            None => Err(Error::Runtime(
                "Failed to generate WKT for projection because of missing EPSG code".into(),
            )),
        }
    }

    pub fn to_proj(&self) -> Result<String> {
        if self.proj_str.is_empty() {
            return Err(Error::Runtime("Projection string is empty".into()));
        }

        Ok(self.proj_str.clone())
    }

    pub fn is_projected(&self) -> bool {
        self.srs.projection_type() != ProjType::Latlong
    }

    pub fn is_geographic(&self) -> bool {
        self.srs.projection_type() == ProjType::Latlong
    }

    pub fn epsg_cs(&self) -> Option<Epsg> {
        self.epsg
    }

    pub fn epsg_geog_cs(&self) -> Option<Epsg> {
        self.epsg_geo
    }

    fn proj(&self) -> &Proj {
        &self.srs
    }
}

pub struct CoordinateTransformer {
    source: SpatialReference,
    target: SpatialReference,
    source_srs: String,
    target_srs: String,
}

const WKT_ROOTS: [&str; 7] = ["GEOGCS[", "PROJCS[", "GEOCCS[", "VERT_CS[", "LOCAL_CS[", "COMPD_CS[", "FITTED_C["];
const WKT2_ROOTS: [&str; 9] = [
    "GEODCRS[",
    "GEOGCRS[",
    "PROJCRS[",
    "VERTCRS[",
    "ENGCRS[",
    "COMPOUNDCRS[",
    "BOUNDCRS[",
    "PARAMETRICCRS[",
    "TIMECRS[",
];

fn is_wkt_string(s: &str) -> bool {
    WKT_ROOTS.iter().any(|&root| s.starts_with(root)) || WKT2_ROOTS.iter().any(|&root| s.starts_with(root))
}

fn parse_wkt_epsg(s: &str) -> (Option<Epsg>, Option<Epsg>) {
    let builder = proj4wkt::Builder;

    if let Ok(node) = builder.parse(s) {
        match &node {
            proj4wkt::builder::Node::PROJCRS(crs) => {
                let epsg = crs
                    .projection
                    .authority
                    .as_ref()
                    .and_then(|auth| auth.code.parse::<u16>().ok())
                    .map(Epsg::from);

                let epsg_geo = crs
                    .geogcs
                    .authority
                    .as_ref()
                    .and_then(|auth| auth.code.parse::<u16>().ok())
                    .map(Epsg::from);

                return (epsg, epsg_geo);
            }
            proj4wkt::builder::Node::GEOGCRS(crs) => {
                let epsg = crs
                    .authority
                    .as_ref()
                    .and_then(|auth| auth.code.parse::<u16>().ok())
                    .map(Epsg::from);

                return (None, epsg);
            }
            _ => {}
        }
    }

    (None, None)
}

fn proj_epsg_from_string(srs_str: &str) -> Result<(String, Option<Epsg>, Option<Epsg>)> {
    let epsg_code = srs_str
        .strip_prefix("EPSG:")
        .and_then(|code| code.parse::<u16>().ok().map(Epsg::from));
    Ok(if let Some(epsg) = epsg_code {
        let proj_str = crs_definitions::from_code(epsg.code())
            .map(|def| def.proj4.to_string())
            .ok_or_else(|| Error::Runtime("".into()))?;
        (proj_str, Some(epsg), None)
    } else {
        if srs_str.eq_ignore_ascii_case("WGS84") {
            return Ok((srs_str.into(), Some(epsg::WGS84_WEB_MERCATOR), Some(epsg::WGS84)));
        }

        if is_wkt_string(srs_str) {
            let (epsg, epsg_geo) = parse_wkt_epsg(srs_str);

            if let Some(epsg) = epsg {
                // If we can obtain an EPSG code from the WKT string, use it as it gives more similar results to osgeo/proj
                let proj_str = crs_definitions::from_code(epsg.code())
                    .map(|def| def.proj4.to_string())
                    .ok_or_else(|| Error::Runtime("".into()))?;

                (proj_str, Some(epsg), epsg_geo)
            } else if let Some(epsg_geo) = epsg_geo {
                // If we can obtain an EPSG code from the WKT string, use it as it gives more similar results to osgeo/proj
                let proj_str = crs_definitions::from_code(epsg_geo.code())
                    .map(|def| def.proj4.to_string())
                    .ok_or_else(|| Error::Runtime("".into()))?;

                (proj_str, epsg, Some(epsg_geo))
            } else {
                let proj_str =
                    wkt_to_projstring(srs_str).map_err(|e| Error::InvalidArgument(format!("Failed to parse WKT string ({e})")))?;
                (proj_str, None, None)
            }
        } else {
            (srs_str.to_string(), None, None)
        }
    })
}

impl CoordinateTransformer {
    pub fn new(source_srs: &str, target_srs: &str) -> Result<Self> {
        let source = SpatialReference::from_definition(source_srs)?;
        let target = SpatialReference::from_definition(target_srs)?;

        Ok(CoordinateTransformer {
            source,
            target,
            source_srs: source_srs.into(),
            target_srs: target_srs.into(),
        })
    }

    pub fn from_epsg(source_epsg: Epsg, target_epsg: Epsg) -> Result<Self> {
        let source = SpatialReference::from_epsg(source_epsg)?;
        let target = SpatialReference::from_epsg(target_epsg)?;

        Ok(CoordinateTransformer {
            source,
            target,
            source_srs: source_epsg.to_string(),
            target_srs: target_epsg.to_string(),
        })
    }

    pub fn transform_point(&self, point: Point) -> Result<Point> {
        let mut p = point;
        transform(self.source.proj(), self.target.proj(), &mut p)?;
        if self.target.proj().projection_type() == ProjType::Latlong {
            // Convert back to degrees if the target is a geographic coordinate system
            p = p.to_degrees();
        }
        Ok(p)
    }

    pub fn transform_point_in_place(&self, point: &mut Point) -> Result<()> {
        transform(self.source.proj(), self.target.proj(), point)?;
        Ok(())
    }

    pub fn transform_points_in_place(&self, points: &mut [Point]) -> Result<()> {
        for point in points.iter_mut() {
            self.transform_point_in_place(point)?;
        }
        Ok(())
    }

    pub fn transform_coordinate(&self, coord: Coordinate) -> Result<Coordinate> {
        let point: Point = coord.into();
        Ok(Coordinate::from(self.transform_point(point.to_radians())?))
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
    use super::*;
    use approx::assert_relative_eq;

    use crate::{Coordinate, Point, crs};

    #[test]
    fn proj4rs_projection_point() {
        let trans = CoordinateTransformer::from_epsg(crs::epsg::WGS84, crs::epsg::BELGIAN_LAMBERT72).unwrap();
        let p = trans
            .transform_point(Point::from(Coordinate::latlon(51.04223683846715, 3.5713882022278653)).to_radians())
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

    #[test]
    fn epsg_import() {
        {
            let srs = SpatialReference::from_epsg(31370.into()).unwrap();
            assert!(srs.is_projected());
            assert!(!srs.is_geographic());
            assert_eq!(srs.epsg_cs(), Some(31370.into()));
            assert_eq!(srs.epsg_geog_cs(), None);

            let srs = SpatialReference::from_definition(&srs.to_proj().unwrap()).unwrap();
            assert!(srs.is_projected());
            // assert_eq!(srs.epsg_cs(), Some(31370.into())); // This info is currently lost after conversion to proj string
        }

        {
            let wkt = crs_definitions::from_code(crs::epsg::BELGIAN_LAMBERT72.code())
                .map(|def| def.wkt.to_string())
                .expect("Failed to get WKT for BELGIAN_LAMBERT72");

            let srs = SpatialReference::from_definition(&wkt).expect("Failed to parse wkt");
            assert!(srs.is_projected());
            assert!(!srs.is_geographic());
            assert_eq!(srs.epsg_geog_cs(), Some(crs::epsg::BELGE72_GEO));
        }

        {
            // Geographic CRS
            let srs = SpatialReference::from_epsg(crs::epsg::WGS84).unwrap();
            assert!(!srs.is_projected());
            assert!(srs.is_geographic());
            assert_eq!(srs.epsg_geog_cs(), Some(crs::epsg::WGS84));
        }

        {
            // Geographic CRS from WKT
            let wkt = crs_definitions::from_code(crs::epsg::WGS84.code())
                .map(|def| def.wkt.to_string())
                .expect("Failed to get WKT for WGS84");

            let srs = SpatialReference::from_definition(&wkt).expect("Failed to parse wkt");
            assert!(!srs.is_projected());
            assert!(srs.is_geographic());
            assert_eq!(srs.epsg_geog_cs(), Some(crs::epsg::WGS84));
        }
    }
}

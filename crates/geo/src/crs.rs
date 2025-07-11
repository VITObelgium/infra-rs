use core::fmt;
use std::f64::consts::PI;

use crate::{
    Point,
    constants::{EARTH_RADIUS_M, LATITUDE_MAX, LONGITUDE_MAX},
    coordinate::Coordinate,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "specta", derive(specta::Type))]
pub struct Epsg(u32);

impl Epsg {
    pub fn new(epsg: u32) -> Self {
        Self(epsg)
    }
}

impl From<Epsg> for u32 {
    fn from(val: Epsg) -> u32 {
        val.0
    }
}

impl From<Epsg> for u16 {
    fn from(val: Epsg) -> u16 {
        val.0 as u16
    }
}

impl From<u32> for Epsg {
    fn from(val: u32) -> Epsg {
        Epsg::new(val)
    }
}

impl fmt::Display for Epsg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EPSG:{}", self.0)
    }
}

pub mod epsg {
    use super::Epsg;

    pub const WGS84_WEB_MERCATOR: Epsg = Epsg(3857);
    pub const WGS84: Epsg = Epsg(4326); // geographic projection (lat, lon)
    pub const BELGIAN_LAMBERT72: Epsg = Epsg(31370);
    pub const BELGE72_GEO: Epsg = Epsg(4313); // geographic projection (lat, lon)
    pub const ETRS89: Epsg = Epsg(3035);
    pub const BELGIAN_LAMBERT2008: Epsg = Epsg(3812);
}

/// Dependency free conversion function between WGS84 (EPSG:4326) and Web Mercator (EPSG:3857)
pub fn lat_lon_to_web_mercator(coord: Coordinate) -> Point<f64> {
    let mut result = Point::new(0.0, 0.0);
    result.set_x(EARTH_RADIUS_M * coord.longitude.to_radians());

    if coord.latitude <= -90.0 {
        result.set_y(f64::NEG_INFINITY);
    } else if coord.latitude >= 90.0 {
        result.set_y(f64::INFINITY);
    } else {
        result.set_y(EARTH_RADIUS_M * (PI * 0.25 + 0.5 * coord.latitude.to_radians()).tan().ln());
    }

    result
}

/// Dependency free conversion function between Web Mercator (EPSG:3857) and WGS84 (EPSG:4326)
pub fn web_mercator_to_lat_lon(point: Point<f64>) -> Coordinate {
    let latitude = (2.0 * (point.y() / EARTH_RADIUS_M).exp().atan() - PI / 2.0).to_degrees();
    let longitude = point.x().to_degrees() / EARTH_RADIUS_M;

    Coordinate {
        latitude: latitude.clamp(-LATITUDE_MAX, LATITUDE_MAX),
        longitude: longitude.clamp(-LONGITUDE_MAX, LONGITUDE_MAX),
    }
}

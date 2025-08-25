//! Geographic and mathematical constants used throughout the library.

pub const EARTH_RADIUS_M: f64 = 6378137.0;
pub const EARTH_CIRCUMFERENCE_M: f64 = 2.0 * std::f64::consts::PI * EARTH_RADIUS_M;
pub const LATITUDE_MAX: f64 = 85.051_128_779_806_6;
pub const LONGITUDE_MAX: f64 = 180.0;

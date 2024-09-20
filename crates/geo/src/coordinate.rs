use approx::{AbsDiffEq, RelativeEq};
use geo_types::Point;

/// Represents a wgs84 point in the raster (lat, lon)
#[derive(Debug, PartialEq, Clone, Copy)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Coordinate {
    pub latitude: f64,
    pub longitude: f64,
}

impl Coordinate {
    pub fn latlon(lat: f64, lon: f64) -> Self {
        Coordinate {
            latitude: lat,
            longitude: lon,
        }
    }

    pub fn is_valid(&self) -> bool {
        if self.latitude.is_nan() || self.longitude.is_nan() {
            return false;
        }

        if self.latitude.abs() > 90.0 {
            // latitude must be between -90 and 90
            return false;
        }

        if !self.longitude.is_finite() {
            return false;
        }

        true
    }

    pub fn wrapped(&self) -> Self {
        let mut coord = Coordinate {
            latitude: self.latitude,
            longitude: self.longitude,
        };
        coord.wrap();
        coord
    }

    fn wrap(&mut self) {
        self.longitude = Self::wrap_value(self.longitude, -180.0, 180.0);
    }

    fn wrap_value(value: f64, min: f64, max: f64) -> f64 {
        if value >= min && value < max {
            return value;
        } else if value == max {
            return min;
        }

        let delta = max - min;
        let wrapped = min + (value - min).rem_euclid(delta);
        if value < min {
            wrapped + delta
        } else {
            wrapped
        }
    }

    pub fn distance(&self, other: &Coordinate) -> f64 {
        let lat = other.latitude - self.latitude;
        let lon = other.longitude - self.longitude;

        lat.hypot(lon)
    }
}

impl From<Point<f64>> for Coordinate {
    fn from(point: Point<f64>) -> Self {
        Coordinate::latlon(point.y(), point.x())
    }
}

impl From<Coordinate> for Point<f64> {
    fn from(val: Coordinate) -> Self {
        Point::new(val.longitude, val.latitude)
    }
}

impl std::ops::Sub for Coordinate {
    type Output = Coordinate;

    fn sub(self, rhs: Coordinate) -> Coordinate {
        Coordinate::latlon(self.latitude - rhs.latitude, self.longitude - rhs.longitude)
    }
}

impl std::ops::Add for Coordinate {
    type Output = Coordinate;

    fn add(self, rhs: Coordinate) -> Coordinate {
        Coordinate::latlon(self.latitude + rhs.latitude, self.longitude + rhs.longitude)
    }
}

impl std::fmt::Display for Coordinate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(lat:{:.6}, lng:{:.6})", self.latitude, self.longitude)
    }
}

impl AbsDiffEq for Coordinate {
    type Epsilon = <f64 as AbsDiffEq>::Epsilon;

    fn default_epsilon() -> <f64 as AbsDiffEq>::Epsilon {
        f64::default_epsilon()
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: <f64 as AbsDiffEq>::Epsilon) -> bool {
        f64::abs_diff_eq(&self.latitude, &other.latitude, epsilon)
            && f64::abs_diff_eq(&self.longitude, &other.longitude, epsilon)
    }
}

impl RelativeEq for Coordinate {
    fn default_max_relative() -> <f64 as AbsDiffEq>::Epsilon {
        f64::default_max_relative()
    }

    fn relative_eq(
        &self,
        other: &Self,
        epsilon: <f64 as AbsDiffEq>::Epsilon,
        max_relative: <f64 as AbsDiffEq>::Epsilon,
    ) -> bool {
        f64::relative_eq(&self.latitude, &other.latitude, epsilon, max_relative)
            && f64::relative_eq(&self.longitude, &other.longitude, epsilon, max_relative)
    }
}

use approx::{AbsDiffEq, RelativeEq};

use crate::coordinate::Coordinate;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LatLonBounds {
    sw: Coordinate,
    ne: Coordinate,
    bounded: bool,
}

impl LatLonBounds {
    /// Return a bounds covering the entire (unwrapped) world.
    pub const fn world() -> LatLonBounds {
        LatLonBounds {
            sw: Coordinate {
                latitude: -90.0,
                longitude: -180.0,
            },
            ne: Coordinate {
                latitude: 90.0,
                longitude: 180.0,
            },
            bounded: true,
        }
    }

    /// Return the convex hull of two points; the smallest bounds that contains both.
    pub fn hull(a: Coordinate, b: Coordinate) -> LatLonBounds {
        let mut bounds = LatLonBounds { sw: a, ne: a, bounded: true };
        bounds.extend(b);
        bounds
    }

    /// Return a bounds that may serve as the identity element for the extend operation.
    pub fn empty() -> LatLonBounds {
        let mut bounds = LatLonBounds::world();
        std::mem::swap(&mut bounds.sw, &mut bounds.ne);
        bounds
    }

    /// Construct an infinite bound, a bound for which the constrain method returns its
    /// input unmodified.
    ///
    /// Note: this is different than LatLonBounds::world() since arbitrary unwrapped
    /// coordinates are also inside the bounds.
    pub const fn infinite() -> LatLonBounds {
        LatLonBounds {
            sw: Coordinate {
                latitude: -90.0,
                longitude: -180.0,
            },
            ne: Coordinate {
                latitude: 90.0,
                longitude: 180.0,
            },
            bounded: false,
        }
    }

    pub fn valid(&self) -> bool {
        self.sw.latitude <= self.ne.latitude && self.sw.longitude <= self.ne.longitude
    }

    pub const fn south(&self) -> f64 {
        self.sw.latitude
    }

    pub const fn west(&self) -> f64 {
        self.sw.longitude
    }

    pub const fn north(&self) -> f64 {
        self.ne.latitude
    }

    pub const fn east(&self) -> f64 {
        self.ne.longitude
    }

    pub fn southwest(&self) -> Coordinate {
        self.sw
    }

    pub fn northeast(&self) -> Coordinate {
        self.ne
    }

    pub const fn southeast(&self) -> Coordinate {
        Coordinate {
            latitude: self.south(),
            longitude: self.east(),
        }
    }

    pub const fn northwest(&self) -> Coordinate {
        Coordinate {
            latitude: self.north(),
            longitude: self.west(),
        }
    }

    pub fn center(&self) -> Coordinate {
        Coordinate {
            latitude: (self.sw.latitude + self.ne.latitude) / 2.0,
            longitude: (self.sw.longitude + self.ne.longitude) / 2.0,
        }
    }

    pub fn constrain(&self, p: &Coordinate) -> Coordinate {
        if !self.bounded {
            return *p;
        }

        let mut lat = p.latitude;
        let mut lng = p.longitude;

        if !self.contains_latitude(lat) {
            lat = lat.max(self.south()).min(self.north());
        }

        if !self.contains_longitude(lng) {
            lng = lng.max(self.west()).min(self.east());
        }

        Coordinate { latitude: lat, longitude: lng }
    }

    fn contains_latitude(&self, latitude: f64) -> bool {
        latitude >= self.south() && latitude <= self.north()
    }

    fn contains_longitude(&self, longitude: f64) -> bool {
        longitude >= self.west() && longitude <= self.east()
    }

    pub fn extend(&mut self, point: Coordinate) {
        self.sw.latitude = self.sw.latitude.min(point.latitude);
        self.sw.longitude = self.sw.longitude.min(point.longitude);
        self.ne.latitude = self.ne.latitude.max(point.latitude);
        self.ne.longitude = self.ne.longitude.max(point.longitude);
    }

    pub fn extend_bounds(&mut self, bounds: &LatLonBounds) {
        self.extend(bounds.sw);
        self.extend(bounds.ne);
    }

    pub fn is_empty(&self) -> bool {
        self.sw.latitude > self.ne.latitude || self.sw.longitude > self.ne.longitude
    }

    pub fn crosses_antimeridian(&self) -> bool {
        self.sw.wrapped().longitude > self.ne.wrapped().longitude
    }

    pub fn contains(&self, area: &LatLonBounds) -> bool {
        let contains_area_latitude = area.north() <= self.north() && area.south() >= self.south();
        if !contains_area_latitude {
            return false;
        }

        let contains_unwrapped = area.east() <= self.east() && area.west() >= self.west();
        if contains_unwrapped {
            return true;
        }

        false
    }

    pub fn intersects(&self, area: &LatLonBounds) -> bool {
        let latitude_intersects = area.north() > self.south() && area.south() < self.north();
        if !latitude_intersects {
            return false;
        }

        let longitude_intersects = area.east() > self.west() && area.west() < self.east();
        if longitude_intersects {
            return true;
        }

        false
    }

    pub fn array(&self) -> [f64; 4] {
        [self.west(), self.south(), self.east(), self.north()]
    }
}

impl Default for LatLonBounds {
    fn default() -> Self {
        Self::infinite()
    }
}

pub fn intersection(b1: &LatLonBounds, b2: &LatLonBounds) -> LatLonBounds {
    let sw = Coordinate {
        latitude: b1.south().max(b2.south()),
        longitude: b1.west().max(b2.west()),
    };

    let ne = Coordinate {
        latitude: b1.north().min(b2.north()),
        longitude: b1.east().min(b2.east()),
    };

    LatLonBounds { sw, ne, bounded: true }
}

impl AbsDiffEq for LatLonBounds {
    type Epsilon = <f64 as AbsDiffEq>::Epsilon;

    fn default_epsilon() -> <f64 as AbsDiffEq>::Epsilon {
        f64::default_epsilon()
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: <f64 as AbsDiffEq>::Epsilon) -> bool {
        Coordinate::abs_diff_eq(&self.ne, &other.ne, epsilon) && Coordinate::abs_diff_eq(&self.sw, &other.sw, epsilon)
    }
}

impl RelativeEq for LatLonBounds {
    fn default_max_relative() -> <f64 as AbsDiffEq>::Epsilon {
        f64::default_max_relative()
    }

    fn relative_eq(&self, other: &Self, epsilon: <f64 as AbsDiffEq>::Epsilon, max_relative: <f64 as AbsDiffEq>::Epsilon) -> bool {
        Coordinate::relative_eq(&self.ne, &other.ne, epsilon, max_relative) && Coordinate::relative_eq(&self.sw, &other.sw, epsilon, max_relative)
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;

    use super::*;

    #[test]
    fn test_intersection() {
        // Test intersection of two non-empty bounds
        let b1 = LatLonBounds {
            sw: Coordinate {
                latitude: 10.0,
                longitude: 20.0,
            },
            ne: Coordinate {
                latitude: 30.0,
                longitude: 40.0,
            },
            bounded: true,
        };
        let b2 = LatLonBounds {
            sw: Coordinate {
                latitude: 15.0,
                longitude: 25.0,
            },
            ne: Coordinate {
                latitude: 35.0,
                longitude: 45.0,
            },
            bounded: true,
        };
        let expected = LatLonBounds {
            sw: Coordinate {
                latitude: 15.0,
                longitude: 25.0,
            },
            ne: Coordinate {
                latitude: 30.0,
                longitude: 40.0,
            },
            bounded: true,
        };
        assert_relative_eq!(intersection(&b1, &b2), expected);
    }

    #[test]
    fn test_intersection_two_empty_bounds() {
        // Test intersection of two empty bounds
        let b3 = LatLonBounds {
            sw: Coordinate { latitude: 0.0, longitude: 0.0 },
            ne: Coordinate {
                latitude: -1.0,
                longitude: -1.0,
            },
            bounded: true,
        };
        let b4 = LatLonBounds {
            sw: Coordinate { latitude: 1.0, longitude: 1.0 },
            ne: Coordinate { latitude: 2.0, longitude: 2.0 },
            bounded: true,
        };
        assert!(intersection(&b3, &b4).is_empty());
    }

    #[test]
    fn test_intersection_empty_non_empty_bounds() {
        // Test intersection of non-empty and empty bounds
        let b5 = LatLonBounds {
            sw: Coordinate {
                latitude: 10.0,
                longitude: 20.0,
            },
            ne: Coordinate {
                latitude: 30.0,
                longitude: 40.0,
            },
            bounded: true,
        };
        let b6 = LatLonBounds {
            sw: Coordinate { latitude: 0.0, longitude: 0.0 },
            ne: Coordinate {
                latitude: -1.0,
                longitude: -1.0,
            },
            bounded: true,
        };
        assert!(intersection(&b5, &b6).is_empty());
        assert!(intersection(&b6, &b5).is_empty());
    }
}

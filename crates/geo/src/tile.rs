use crate::{Point, Rect, constants::EARTH_CIRCUMFERENCE_M, coordinate::Coordinate, latlonbounds::LatLonBounds};
use std::f64::consts::PI;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ZoomLevelStrategy {
    PreferHigher,
    #[default]
    PreferLower,
    Closest,
    Manual(i32),
}

/// An XYZ web mercator tile
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Tile {
    pub x: i32,
    pub y: i32,
    pub z: i32,
}

impl Tile {
    pub const TILE_SIZE: u32 = 256;

    fn xy(coord: Coordinate) -> Point<f64> {
        let x = coord.longitude / 360.0 + 0.5;

        let sinlat = coord.latitude.to_radians().sin();
        let y = 0.5 - 0.25 * ((1.0 + sinlat) / (1.0 - sinlat)).ln() / PI;

        Point::new(x, y)
    }

    pub fn for_coordinate(coord: Coordinate, zoom: i32) -> Tile {
        let tilex;
        let tiley;

        let p = Tile::xy(coord);
        let z2 = f64::powi(2.0, zoom);

        if p.x() <= 0.0 {
            tilex = 0;
        } else if p.x() >= 1.0 {
            tilex = (z2 - 1.0) as i32;
        } else {
            tilex = ((p.x() + f64::EPSILON) * z2).floor() as i32;
        }

        if p.y() <= 0.0 {
            tiley = 0;
        } else if p.y() >= 1.0 {
            tiley = (z2 - 1.0) as i32;
        } else {
            tiley = ((p.y() + f64::EPSILON) * z2).floor() as i32;
        }

        Tile {
            x: tilex,
            y: tiley,
            z: zoom,
        }
    }

    pub fn coordinate_pixel_offset(&self, coord: Coordinate, tile_size: u32) -> Option<(u32, u32)> {
        let zoom_level_tiles = f64::powi(2.0, self.z);
        let top_left = self.upper_left();

        // Longitude is linear in Web Mercator
        let degrees_per_pixel_lon = 360.0 / zoom_level_tiles / tile_size as f64;
        let x_offset = (coord.longitude - top_left.longitude) / degrees_per_pixel_lon;

        // For latitude, we need to work in the Web Mercator Y space
        let bottom_right = self.lower_right();
        let tile_height_degrees = top_left.latitude - bottom_right.latitude;
        let degrees_per_pixel_lat = tile_height_degrees / tile_size as f64;
        let y_offset = (top_left.latitude - coord.latitude) / degrees_per_pixel_lat;

        if x_offset < -1e-6 || y_offset < -1e-6 || x_offset >= tile_size as f64 || y_offset >= tile_size as f64 {
            return None;
        }

        Some((x_offset as u32, y_offset as u32))
    }

    pub fn x(&self) -> i32 {
        self.x
    }

    pub fn y(&self) -> i32 {
        self.y
    }

    pub fn z(&self) -> i32 {
        self.z
    }

    pub fn upper_left(&self) -> Coordinate {
        let z2 = f64::powi(2.0, self.z);
        let lon_degrees = self.x as f64 / z2 * 360.0 - 180.0;
        let lat_rad = (PI * (1.0 - 2.0 * self.y as f64 / z2)).sinh().atan();

        Coordinate::latlon(lat_rad.to_degrees(), lon_degrees)
    }

    pub fn lower_right(&self) -> Coordinate {
        Tile {
            x: self.x + 1,
            y: self.y + 1,
            z: self.z,
        }
        .upper_left()
    }

    pub fn center(&self) -> Coordinate {
        let z2 = f64::powi(2.0, self.z);
        let degrees_per_tile = 360.0 / z2;

        let lon_degrees = self.x as f64 / z2 * 360.0 - 180.0;
        let lat_rad = (PI * (1.0 - 2.0 * self.y as f64 / z2)).sinh().atan();

        Coordinate::latlon(lat_rad.to_degrees() - degrees_per_tile / 2.0, lon_degrees + degrees_per_tile / 2.0)
    }

    pub fn web_mercator_bounds(&self) -> Rect<f64> {
        let tile_size = EARTH_CIRCUMFERENCE_M / f64::powi(2.0, self.z);
        let left = (self.x as f64 * tile_size) - (EARTH_CIRCUMFERENCE_M / 2.0);
        let right = left + tile_size;

        let top = (EARTH_CIRCUMFERENCE_M / 2.0) - (self.y as f64 * tile_size);
        let bottom = top - tile_size;

        Rect::from_points(Point::new(left, top), Point::new(right, bottom))
    }

    pub fn bounds(&self) -> LatLonBounds {
        let z2 = f64::powi(2.0, self.z);

        let ul_lon_deg = self.x as f64 / z2 * 360.0 - 180.0;
        let ul_lat_rad = (PI * (1.0 - 2.0 * self.y as f64 / z2)).sinh().atan();

        let lr_lon_deg = (self.x + 1) as f64 / z2 * 360.0 - 180.0;
        let lr_lat_rad = (PI * (1.0 - 2.0 * (self.y + 1) as f64 / z2)).sinh().atan();

        LatLonBounds::hull(
            Coordinate::latlon(ul_lat_rad.to_degrees(), ul_lon_deg),
            Coordinate::latlon(lr_lat_rad.to_degrees(), lr_lon_deg),
        )
    }

    pub fn direct_children(&self) -> [Tile; 4] {
        [
            Tile {
                x: self.x * 2,
                y: self.y * 2,
                z: self.z + 1,
            },
            Tile {
                x: self.x * 2 + 1,
                y: self.y * 2,
                z: self.z + 1,
            },
            Tile {
                x: self.x * 2 + 1,
                y: self.y * 2 + 1,
                z: self.z + 1,
            },
            Tile {
                x: self.x * 2,
                y: self.y * 2 + 1,
                z: self.z + 1,
            },
        ]
    }

    pub fn children(&self, target_zoom: i32) -> Vec<Tile> {
        let mut result = Vec::new();

        if self.z < target_zoom {
            for child in self.direct_children() {
                let children = child.children(target_zoom);
                result.push(child);
                result.extend(children);
            }
        }

        result
    }

    pub fn traverse<F>(&self, target_zoom: i32, mut cb: F)
    where
        F: FnMut(&Tile) -> bool,
    {
        if self.z >= target_zoom {
            return;
        }

        if !cb(self) {
            return;
        }

        for child in self.direct_children().iter() {
            if cb(child) {
                child.traverse(target_zoom, &mut cb);
            }
        }
    }

    /// Calculates the pixel size in meters for a given zoom level and tile size.
    pub fn pixel_size_at_zoom_level(zoom_level: i32, tile_size: u32) -> f64 {
        let zoom_level_offset = tile_size / Tile::TILE_SIZE - 1;
        let zoom_level = zoom_level + zoom_level_offset as i32;

        let tiles_per_row = f64::powi(2.0, zoom_level);
        let meters_per_tile = EARTH_CIRCUMFERENCE_M / tiles_per_row;

        meters_per_tile / Tile::TILE_SIZE as f64
    }

    /// Calculates the zoom level for a given pixel size
    /// strategy specifies how the zoom level should be selected when converting between pixel size and zoom level.
    ///
    /// - `PreferHigher`: Chooses the next higher integer zoom level (ceil), ensuring the pixel size is less than or equal to the requested size.
    /// - `PreferLower`: Chooses the next lower integer zoom level (floor), ensuring the pixel size is greater than or equal to the requested size.
    /// - `Closest`: Chooses the closest integer zoom level (round) to the computed value.
    /// - `Manual(i32)`: Uses a manually specified zoom level ignoring any calculations.
    pub fn zoom_level_for_pixel_size(pixel_size: f64, strategy: ZoomLevelStrategy, tile_size: u32) -> i32 {
        const INITIAL_RESOLUTION: f64 = EARTH_CIRCUMFERENCE_M / Tile::TILE_SIZE as f64; // meters/pixel at zoom 0
        let zoom_level_offset = tile_size / Tile::TILE_SIZE - 1;
        let zoom = (INITIAL_RESOLUTION / pixel_size).log2() - zoom_level_offset as f64;

        match strategy {
            ZoomLevelStrategy::PreferHigher => zoom.ceil() as i32,
            ZoomLevelStrategy::PreferLower => zoom.floor() as i32,
            ZoomLevelStrategy::Closest => zoom.round() as i32,
            ZoomLevelStrategy::Manual(z) => z,
        }
    }

    pub fn adjacent_tile_left(&self) -> Tile {
        Tile {
            x: self.x - 1,
            y: self.y,
            z: self.z,
        }
    }

    pub fn adjacent_tile_right(&self) -> Tile {
        Tile {
            x: self.x + 1,
            y: self.y,
            z: self.z,
        }
    }

    pub fn adjacent_tile_up(&self) -> Tile {
        Tile {
            x: self.x,
            y: self.y - 1,
            z: self.z,
        }
    }

    pub fn adjacent_tile_down(&self) -> Tile {
        Tile {
            x: self.x,
            y: self.y + 1,
            z: self.z,
        }
    }

    /// Returns the neighboring tiles of the current tile including the current tile
    pub fn surrounding_tiles_including_self(&self) -> Vec<Tile> {
        let mut tiles = Vec::with_capacity(9);

        for y in -1..=1 {
            for x in -1..=1 {
                let tile_x = self.x + x;
                let tile_y = self.y + y;
                if tile_x < 0 || tile_y < 0 {
                    continue;
                }

                tiles.push(Tile {
                    x: self.x + x,
                    y: self.y + y,
                    z: self.z,
                });
            }
        }

        tiles
    }
}

impl std::fmt::Display for Tile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}/{}", self.z, self.x, self.y)
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;

    use super::*;

    #[test]
    fn tile_test() {
        {
            let tile = Tile { x: 0, y: 0, z: 0 };
            let coord = tile.upper_left();
            assert_eq!(coord.longitude, -180.0);
            assert_eq!(coord.latitude, 85.0511287798066);
        }

        {
            let tile = Tile { x: 0, y: 0, z: 1 };
            let coord = tile.upper_left();
            assert_eq!(coord.longitude, -180.0);
            assert_eq!(coord.latitude, 85.0511287798066);
        }
    }

    #[test]
    fn test_pixel_size_at_zoom_level() {
        const EPS: f64 = 1e-2;

        assert_relative_eq!(Tile::pixel_size_at_zoom_level(0, Tile::TILE_SIZE), 156543.03, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(1, Tile::TILE_SIZE), 78271.52, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(2, Tile::TILE_SIZE), 39135.76, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(3, Tile::TILE_SIZE), 19567.88, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(4, Tile::TILE_SIZE), 9783.94, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(5, Tile::TILE_SIZE), 4891.97, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(6, Tile::TILE_SIZE), 2445.98, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(7, Tile::TILE_SIZE), 1222.99, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(8, Tile::TILE_SIZE), 611.50, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(9, Tile::TILE_SIZE), 305.75, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(10, Tile::TILE_SIZE), 152.87, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(11, Tile::TILE_SIZE), 76.437, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(12, Tile::TILE_SIZE), 38.219, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(13, Tile::TILE_SIZE), 19.109, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(14, Tile::TILE_SIZE), 9.5546, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(15, Tile::TILE_SIZE), 4.7773, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(16, Tile::TILE_SIZE), 2.3887, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(17, Tile::TILE_SIZE), 1.1943, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(18, Tile::TILE_SIZE), 0.5972, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(19, Tile::TILE_SIZE), 0.298, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(20, Tile::TILE_SIZE), 0.149, epsilon = EPS);
    }

    #[test]
    fn calculate_zoom_level() {
        assert_eq!(
            Tile::zoom_level_for_pixel_size(10.0, ZoomLevelStrategy::PreferHigher, Tile::TILE_SIZE),
            14
        );
        assert_eq!(
            Tile::zoom_level_for_pixel_size(100.0, ZoomLevelStrategy::PreferHigher, Tile::TILE_SIZE),
            11
        );

        assert_eq!(
            Tile::zoom_level_for_pixel_size(10.0, ZoomLevelStrategy::PreferLower, Tile::TILE_SIZE),
            13
        );
        assert_eq!(
            Tile::zoom_level_for_pixel_size(100.0, ZoomLevelStrategy::PreferLower, Tile::TILE_SIZE),
            10
        );
    }

    #[test]
    fn test_tile_for_coordinate() {
        let coord = Coordinate::latlon(51.0, 4.0);
        let tile = Tile::for_coordinate(coord, 9);
        assert_eq!(tile.x, 261);
        assert_eq!(tile.y, 171);
        assert_eq!(tile.z, 9);

        let coord = Coordinate::latlon(51.0, 4.0);
        let tile = Tile::for_coordinate(coord, 10);
        assert_eq!(tile.x, 523);
        assert_eq!(tile.y, 342);
        assert_eq!(tile.z, 10);
    }
}

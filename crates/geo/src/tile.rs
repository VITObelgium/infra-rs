use crate::{
    constants::EARTH_CIRCUMFERENCE_M, coordinate::Coordinate, crs, georeference::GeoReference,
    latlonbounds::LatLonBounds, Point, Rect,
};
use std::f64::consts::PI;

/// An XYZ web mercator tile
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Tile {
    pub x: i32,
    pub y: i32,
    pub z: i32,
}

impl Tile {
    pub const TILE_SIZE: u16 = 256;

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

    pub fn center(&self) -> Coordinate {
        let z2 = f64::powi(2.0, self.z);
        let degrees_per_tile = 360.0 / z2;

        let lon_degrees = self.x as f64 / z2 * 360.0 - 180.0;
        let lat_rad = (PI * (1.0 - 2.0 * self.y as f64 / z2)).sinh().atan();

        Coordinate::latlon(
            lat_rad.to_degrees() - degrees_per_tile / 2.0,
            lon_degrees + degrees_per_tile / 2.0,
        )
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

    pub fn tile_index(tile: &Tile, meta: &GeoReference) -> Option<i32> {
        let upper_left = crs::lat_lon_to_web_mercator(tile.center());

        if meta.is_point_on_map(upper_left) {
            let tiles_per_row = (meta.columns() as f64 / Tile::TILE_SIZE as f64) as i32;

            let mut cell = meta.point_to_cell(upper_left);
            cell.row /= Tile::TILE_SIZE as i32;
            cell.col /= Tile::TILE_SIZE as i32;
            Some(cell.row * tiles_per_row + cell.col)
        } else {
            None
        }
    }

    pub fn pixel_size_at_zoom_level(zoom_level: i32) -> f64 {
        let tiles_per_row = f64::powi(2.0, zoom_level);
        let meters_per_tile = EARTH_CIRCUMFERENCE_M / tiles_per_row;

        meters_per_tile / Tile::TILE_SIZE as f64
    }

    pub fn zoom_level_for_pixel_size(pixel_size: f64, prefer_higher: bool) -> i32 {
        let mut zoom_level = 20;
        while zoom_level > 0 {
            let zoom_level_pixel_size = Self::pixel_size_at_zoom_level(zoom_level);
            if pixel_size <= zoom_level_pixel_size {
                if pixel_size != zoom_level_pixel_size && prefer_higher {
                    // Prefer the higher zoom level
                    zoom_level += 1;
                }
                break;
            }

            zoom_level -= 1;
        }

        zoom_level
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

        assert_relative_eq!(Tile::pixel_size_at_zoom_level(0), 156543.03, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(1), 78271.52, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(2), 39135.76, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(3), 19567.88, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(4), 9783.94, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(5), 4891.97, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(6), 2445.98, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(7), 1222.99, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(8), 611.50, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(9), 305.75, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(10), 152.87, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(11), 76.437, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(12), 38.219, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(13), 19.109, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(14), 9.5546, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(15), 4.7773, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(16), 2.3887, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(17), 1.1943, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(18), 0.5972, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(19), 0.298, epsilon = EPS);
        assert_relative_eq!(Tile::pixel_size_at_zoom_level(20), 0.149, epsilon = EPS);
    }

    #[test]
    fn calculate_zoom_level() {
        assert_eq!(Tile::zoom_level_for_pixel_size(10.0, true), 14);
        assert_eq!(Tile::zoom_level_for_pixel_size(100.0, true), 11);

        assert_eq!(Tile::zoom_level_for_pixel_size(10.0, false), 13);
        assert_eq!(Tile::zoom_level_for_pixel_size(100.0, false), 10);
    }
}

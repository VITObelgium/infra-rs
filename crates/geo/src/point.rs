pub type Point<T = f64> = geo_types::Point<T>;

pub fn euclidenan_distance(p1: Point, p2: Point) -> f64 {
    let delta = p1 - p2;
    delta.x().hypot(delta.y())
}

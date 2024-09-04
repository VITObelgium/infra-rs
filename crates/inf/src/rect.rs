use std::fmt::Debug;

use crate::Point;
use geo_types::CoordNum;
use num::{abs, Signed, Zero};

#[derive(Debug)]
pub struct Rect<T>
where
    T: Copy + CoordNum,
{
    top_left: Point<T>,
    bottom_right: Point<T>,
}

impl<T> Rect<T>
where
    T: Copy + CoordNum,
{
    pub fn from_points(p1: Point<T>, p2: Point<T>) -> Self {
        let top_left = Point::new(min(p1.x(), p2.x()), max(p1.y(), p2.y()));
        let bottom_right = Point::new(max(p1.x(), p2.x()), min(p1.y(), p2.y()));

        Rect { top_left, bottom_right }
    }

    pub fn from_ne_sw(ne: Point<T>, sw: Point<T>) -> Self {
        Rect {
            top_left: ne,
            bottom_right: sw,
        }
    }

    pub fn width(&self) -> T
    where
        T: std::ops::Sub<Output = T> + Copy + PartialOrd + Zero,
    {
        if self.bottom_right.x() > self.top_left.x() {
            self.bottom_right.x() - self.top_left.x()
        } else {
            T::zero()
        }
    }

    pub fn height(&self) -> T
    where
        T: std::ops::Sub + std::ops::Neg + Copy + Signed,
    {
        abs(self.bottom_right.y() - self.top_left.y())
    }

    pub fn empty(&self) -> bool
    where
        T: PartialEq + Default + std::ops::Sub + Zero + Copy + PartialOrd + Signed,
    {
        self.width() == T::zero() || self.height() == T::zero()
    }

    pub fn top_left(&self) -> Point<T> {
        self.top_left
    }

    pub fn top_right(&self) -> Point<T> {
        Point::new(self.bottom_right.x(), self.top_left.y())
    }

    pub fn bottom_left(&self) -> Point<T> {
        Point::new(self.top_left.x(), self.bottom_right.y())
    }

    pub fn bottom_right(&self) -> Point<T> {
        self.bottom_right
    }
}

fn min<T: PartialOrd>(a: T, b: T) -> T {
    if a < b {
        a
    } else {
        b
    }
}

fn max<T: PartialOrd>(a: T, b: T) -> T {
    if b > a {
        b
    } else {
        a
    }
}

pub fn intersects<T>(lhs: &Rect<T>, rhs: &Rect<T>) -> bool
where
    T: Copy + CoordNum,
{
    lhs.top_left.x() < rhs.bottom_right.x()
        && lhs.bottom_right.x() > rhs.top_left.x()
        && lhs.top_left.y() > rhs.bottom_right.y()
        && lhs.bottom_right.y() < rhs.top_left.y()
}

pub fn intersection<T>(lhs: &Rect<T>, rhs: &Rect<T>) -> Rect<T>
where
    T: CoordNum + PartialOrd,
{
    if !intersects(lhs, rhs) {
        // Rectangles do not overlap, return an empty rectangle
        return Rect::from_points(Point::new(T::zero(), T::zero()), Point::new(T::zero(), T::zero()));
    }

    let top_left = Point::new(
        max(lhs.top_left.x(), rhs.top_left.x()),
        min(lhs.top_left.y(), rhs.top_left.y()),
    );
    let bottom_right = Point::new(
        min(lhs.bottom_right.x(), rhs.bottom_right.x()),
        max(lhs.bottom_right.y(), rhs.bottom_right.y()),
    );

    Rect::from_ne_sw(top_left, bottom_right)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rectangle_intersection() {
        let r1 = Rect::from_points(Point::new(0, 10), Point::new(10, 0));
        let r2 = Rect::from_points(Point::new(4, 4), Point::new(5, 5));

        let intersection = intersection(&r1, &r2);

        assert_eq!(intersection.top_left, Point::new(4, 5));
        assert_eq!(intersection.bottom_right, Point::new(5, 4));
    }

    #[test]
    fn test_rectangle_self_intersection() {
        let r1 = Rect::from_points(Point::new(0, 10), Point::new(10, 0));
        let intersection = intersection(&r1, &r1);
        assert_eq!(intersection.top_left, r1.top_left);
        assert_eq!(intersection.bottom_right, r1.bottom_right);
    }

    #[test]
    fn test_rectangle_self_intersection_float() {
        let r1 = Rect::from_points(
            Point::new(-30.000_000_763_788_11, 29.999999619212282),
            Point::new(60.000000763788094, 71.999_998_473_439_09),
        );
        let intersection = intersection(&r1, &r1);
        assert_eq!(intersection.top_left, r1.top_left);
        assert_eq!(intersection.bottom_right, r1.bottom_right);
    }

    #[test]
    fn test_rectangle_intersection_empty() {
        let r1 = Rect::from_points(Point::new(22000.0, 245000.0), Point::new(259000.0, 153000.0));
        let r2 = Rect::from_points(Point::new(110000.0, 95900.0), Point::new(110100.0, 95800.0));

        let intersection = intersection(&r1, &r2);

        assert_eq!(intersection.top_left, Point::new(0.0, 0.0));
        assert_eq!(intersection.bottom_right, Point::new(0.0, 0.0));
    }
}

//! Rectangle geometry utilities for spatial operations.

use std::fmt::Debug;

pub type Point<T = f64> = geo_types::Point<T>;
use approx::{AbsDiffEq, RelativeEq};
use geo_types::CoordNum;
use num::Zero;

#[derive(Debug, PartialEq)]
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

    pub fn from_nw_se(nw: Point<T>, se: Point<T>) -> Self {
        Rect {
            top_left: nw,
            bottom_right: se,
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
        T: std::ops::Sub + Copy,
    {
        if self.bottom_right.y() > self.top_left.y() {
            self.bottom_right.y() - self.top_left.y()
        } else {
            self.top_left.y() - self.bottom_right.y()
        }
    }

    pub fn is_empty(&self) -> bool
    where
        T: PartialEq + std::ops::Sub + Zero + Copy + PartialOrd,
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

    pub fn intersects(&self, other: &Rect<T>) -> bool
    where
        T: Copy + CoordNum,
    {
        !self.is_empty()
            && !other.is_empty()
            && self.top_left.x() < other.bottom_right.x()
            && self.bottom_right.x() > other.top_left.x()
            && self.top_left.y() > other.bottom_right.y()
            && self.bottom_right.y() < other.top_left.y()
    }

    pub fn intersection(&self, other: &Rect<T>) -> Rect<T>
    where
        T: CoordNum + PartialOrd,
    {
        if !self.intersects(other) {
            // Rectangles do not overlap, return an empty rectangle
            return Rect::from_points(Point::new(T::zero(), T::zero()), Point::new(T::zero(), T::zero()));
        }

        let top_left = Point::new(
            max(self.top_left.x(), other.top_left.x()),
            min(self.top_left.y(), other.top_left.y()),
        );
        let bottom_right = Point::new(
            min(self.bottom_right.x(), other.bottom_right.x()),
            max(self.bottom_right.y(), other.bottom_right.y()),
        );

        Rect::from_nw_se(top_left, bottom_right)
    }
}

fn min<T: PartialOrd>(a: T, b: T) -> T {
    if a < b { a } else { b }
}

fn max<T: PartialOrd>(a: T, b: T) -> T {
    if b > a { b } else { a }
}

impl From<Rect<f64>> for geo_types::Polygon<f64> {
    fn from(rect: Rect<f64>) -> geo_types::Polygon<f64> {
        geo_types::Polygon::new(
            geo_types::LineString::from(vec![
                rect.top_left(),
                rect.top_right(),
                rect.bottom_right,
                rect.bottom_left(),
                rect.top_left(),
            ]),
            Vec::default(),
        )
    }
}

impl<T> AbsDiffEq for Rect<T>
where
    T: PartialEq + std::fmt::Debug + Copy + CoordNum + AbsDiffEq<Epsilon = T>,
{
    type Epsilon = T;

    fn default_epsilon() -> Self::Epsilon {
        T::default_epsilon()
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        self.top_left.abs_diff_eq(&other.top_left, epsilon) && self.bottom_right.abs_diff_eq(&other.bottom_right, epsilon)
    }
}

impl<T: PartialEq + std::fmt::Debug + Copy + CoordNum + RelativeEq<Epsilon = T>> RelativeEq for Rect<T> {
    fn default_max_relative() -> Self::Epsilon {
        T::default_max_relative()
    }

    fn relative_eq(&self, other: &Self, epsilon: Self::Epsilon, max_relative: Self::Epsilon) -> bool {
        Point::<T>::relative_eq(&self.top_left, &other.top_left, epsilon, max_relative)
            && Point::<T>::relative_eq(&self.bottom_right, &other.bottom_right, epsilon, max_relative)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rectangle_intersection() {
        let r1 = Rect::from_points(Point::new(0, 10), Point::new(10, 0));
        let r2 = Rect::from_points(Point::new(4, 4), Point::new(5, 5));

        let intersection = r1.intersection(&r2);

        assert_eq!(intersection.top_left, Point::new(4, 5));
        assert_eq!(intersection.bottom_right, Point::new(5, 4));
    }

    #[test]
    fn test_rectangle_self_intersection() {
        let r1 = Rect::from_points(Point::new(0, 10), Point::new(10, 0));
        let intersection = r1.intersection(&r1);
        assert_eq!(intersection.top_left, r1.top_left);
        assert_eq!(intersection.bottom_right, r1.bottom_right);
    }

    #[test]
    fn test_rectangle_self_intersection_float() {
        let r1 = Rect::from_points(
            Point::new(-30.000_000_763_788_11, 29.999999619212282),
            Point::new(60.000000763788094, 71.999_998_473_439_09),
        );
        let intersection = r1.intersection(&r1);
        assert_eq!(intersection.top_left, r1.top_left);
        assert_eq!(intersection.bottom_right, r1.bottom_right);
    }

    #[test]
    fn test_rectangle_intersection_empty() {
        let r1 = Rect::from_points(Point::new(22000.0, 245000.0), Point::new(259000.0, 153000.0));
        let r2 = Rect::from_points(Point::new(110000.0, 95900.0), Point::new(110100.0, 95800.0));

        let intersection = r1.intersection(&r2);

        assert_eq!(intersection.top_left, Point::new(0.0, 0.0));
        assert_eq!(intersection.bottom_right, Point::new(0.0, 0.0));
    }

    #[test]
    fn adjacent_rectangles_intersection() {
        let r1 = Rect::from_points(Point::new(0, 10), Point::new(10, 0));
        let r2 = Rect::from_points(Point::new(10, 10), Point::new(20, 0));

        assert!(!r1.intersects(&r2));
        let intersection = r1.intersection(&r2);
        assert!(intersection.is_empty());
    }

    #[test]
    fn empty_rectangle_intersection() {
        let r1 = Rect::from_points(
            Point::new(313086.06785608083, 6731350.458905762),
            Point::new(469629.1017841218, 6574807.424977721),
        );

        let r2 = Rect::from_points(
            Point::new(391357.58482010243, 6731350.458905762),
            Point::new(391357.58482010243, 6574807.424977721),
        );

        assert!(!r1.intersects(&r2));
        let intersection = r1.intersection(&r2);
        assert!(intersection.is_empty());
    }
}

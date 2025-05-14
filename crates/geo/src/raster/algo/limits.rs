use std::ops::Range;

use crate::{Array, ArrayNum};
use itertools::Itertools;
use itertools::MinMaxResult::{MinMax, NoElements, OneElement};

pub fn min_max<R, T>(ras: &R) -> Range<T>
where
    R: Array<Pixel = T>,
    T: ArrayNum,
{
    match ras.iter_values().minmax() {
        NoElements => T::zero()..T::zero(),
        OneElement(x) => x..x,
        MinMax(x, y) => x..y,
    }
}

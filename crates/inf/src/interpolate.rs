use num::{Float, Num, NumCast, One, ToPrimitive, Zero};

#[inline]
pub fn linear_map_to_float<T, TFloat>(value: T, min: T, max: T) -> TFloat
where
    T: PartialOrd + Num + ToPrimitive + Into<TFloat> + Copy,
    TFloat: Float + Zero + One,
{
    //assert!(!(min.into().is_nan() || max.into().is_nan()));
    assert!(min <= max);

    if min >= max {
        return TFloat::zero();
    }

    if value < min {
        return TFloat::zero();
    } else if value > max {
        return TFloat::one();
    }

    let range_width: TFloat = NumCast::from(max - min).unwrap_or(TFloat::zero());
    (value.into() - min.into()) / range_width
}

#[cfg(feature = "simd")]
#[inline]
pub fn linear_map_to_float_simd<const N: usize>(value: std::simd::Simd<f32, N>, min: f32, max: f32) -> std::simd::Simd<f32, N>
where
    std::simd::LaneCount<N>: std::simd::SupportedLaneCount,
{
    use std::simd::cmp::SimdPartialOrd;
    use std::simd::prelude::*;

    debug_assert!(min <= max);

    let lower_edge = value.simd_lt(Simd::splat(min));
    let upper_edge = value.simd_ge(Simd::splat(max));

    let result = (value - Simd::splat(min)) / Simd::splat(max - min);
    lower_edge.select(Simd::splat(0.0), upper_edge.select(Simd::splat(1.0), result))
}

// pub fn linear_map_to_byte<T>(value: T, start: T, end: T, map_start: u8, map_end: u8) -> u8
// where
//     T: PartialOrd + Into<f32> + Copy,
// {
//     if value < start || value > end {
//         return 0;
//     }

//     if map_start == map_end {
//         return map_start;
//     }

//     let range_width = (end.into() - start.into()).into();
//     let pos: f32 = ((value.into() - start.into()) / range_width).into();

//     let map_width = (map_end - map_start) + 1;
//     let mapped = (map_start as f32 + (map_width as f32 * pos)).clamp(0.0, u8::MAX as f32);
//     mapped.try_into().unwrap_or(map_start)
// }

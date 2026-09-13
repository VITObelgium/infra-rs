use num::{Float, Num, NumCast, One, ToPrimitive, Zero};

#[inline]
pub fn linear_map_to_float<T, TFloat>(value: T, min: T, max: T) -> TFloat
where
    T: Float + PartialOrd + Num + ToPrimitive + Into<TFloat> + Copy,
    TFloat: Float + Zero + One,
{
    debug_assert!(min <= max);

    if min > max {
        return TFloat::zero();
    }

    if value <= min {
        return TFloat::zero();
    } else if value >= max {
        return TFloat::one();
    }

    let range_width: TFloat = NumCast::from(max - min).unwrap_or(TFloat::zero());
    (value.into() - min.into()) / range_width
}

#[inline]
pub fn linear_map_to_float_simd<S: fearless_simd::Simd>(simd: S, value: S::f32s, min: f32, max: f32) -> S::f32s {
    use fearless_simd::*;

    debug_assert!(min <= max);

    let lower_edge = value.simd_le(min);
    let upper_edge = value.simd_ge(max);

    let result = (value - min) / (max - min);
    let result = upper_edge.select(S::f32s::splat(simd, 1.0), result);
    lower_edge.select(S::f32s::splat(simd, 0.0), result)
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

#[cfg(test)]
mod tests {
    use super::*;
    use fearless_simd::*;

    #[test]
    fn test_negative_range() {
        assert_eq!(linear_map_to_float::<f32, f32>(-1.2f32, -1.2f32, 1.2f32), 0.0f32);
        assert_eq!(linear_map_to_float::<f32, f32>(0.0f32, -1.2f32, 1.2f32), 0.5f32);
        assert_eq!(linear_map_to_float::<f32, f32>(1.2f32, -1.2f32, 1.2f32), 1.0f32);
    }

    #[test]
    fn test_value_below_min() {
        assert_eq!(linear_map_to_float::<f32, f32>(-2.0f32, -1.0f32, 1.0f32), 0.0f32);
    }

    #[test]
    fn test_value_above_max() {
        assert_eq!(linear_map_to_float::<f32, f32>(2.0f32, -1.0f32, 1.0f32), 1.0);
    }

    #[test]
    fn test_value_at_min() {
        assert_eq!(linear_map_to_float::<f32, f32>(-1.0f32, -1.0f32, 1.0f32), 0.0);
    }

    #[test]
    fn test_value_at_max() {
        assert_eq!(linear_map_to_float::<f32, f32>(1.0f32, -1.0f32, 1.0f32), 1.0);
    }

    #[test]
    fn test_zero_range() {
        assert_eq!(linear_map_to_float::<f32, f32>(0.0, 1.0, 1.0), 0.0);
        assert_eq!(linear_map_to_float::<f32, f32>(1.0, 1.0, 1.0), 0.0);
        assert_eq!(linear_map_to_float::<f32, f32>(2.0, 1.0, 1.0), 1.0);
    }

    #[inline(always)]
    fn run<S: Simd>(simd: S, values: &[f32; 16], min: f32, max: f32) -> [f32; 3] {
        let v = S::f32s::from_fn(simd, |i| values[i]);
        let result = linear_map_to_float_simd(simd, v, min, max);
        [result[0], result[1], result[2]]
    }

    fn map(a: f32, b: f32, c: f32, min: f32, max: f32) -> [f32; 3] {
        let mut values = [0.0f32; 16];
        values[0] = a;
        values[1] = b;
        values[2] = c;
        let level = Level::new();
        dispatch!(level, simd => run(simd, &values, min, max))
    }

    #[test]
    fn test_negative_range_simd() {
        assert_eq!(map(-1.2, 0.0, 1.2, -1.2, 1.2), [0.0, 0.5, 1.0]);
    }

    #[test]
    fn test_value_below_min_simd() {
        assert_eq!(map(-2.0, -1.0, 0.0, -1.0, 1.0), [0.0, 0.0, 0.5]);
    }

    #[test]
    fn test_value_above_max_simd() {
        assert_eq!(map(2.0, 1.0, 0.0, -1.0, 1.0), [1.0, 1.0, 0.5]);
    }

    #[test]
    fn test_value_at_min_simd() {
        assert_eq!(map(-1.0, 0.0, 1.0, -1.0, 1.0), [0.0, 0.5, 1.0]);
    }

    #[test]
    fn test_value_at_max_simd() {
        assert_eq!(map(1.0, -1.0, 0.0, -1.0, 1.0), [1.0, 0.0, 0.5]);
    }

    #[test]
    fn test_zero_range_simd() {
        assert_eq!(map(0.0, 1.0, 2.0, 1.0, 1.0), [0.0, 0.0, 1.0]);
    }
}

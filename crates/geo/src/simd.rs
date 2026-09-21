use std::sync::OnceLock;

use fearless_simd::{Level, Simd, SimdBase, SimdElement, SimdMask, prelude::*};

use crate::ArrayNum;

/// Returns the best SIMD level supported by the current CPU.
/// Cached to avoid an expensive cpu feature check on every call.
#[inline]
pub fn level() -> Level {
    static LEVEL: OnceLock<Level> = OnceLock::new();
    *LEVEL.get_or_init(Level::new)
}

#[inline(always)]
pub(crate) fn nodata_mask<S, T, V>(value: V) -> V::Mask
where
    S: Simd,
    T: ArrayNum + SimdElement,
    V: SimdBase<S, Element = T>,
{
    if T::has_nan() {
        !value.simd_eq(value)
    } else {
        value.simd_eq(T::NODATA)
    }
}

#[allow(dead_code)]
#[inline(always)]
pub(crate) fn init_nodata<S, T, V>(value: V, nodata: T) -> V
where
    S: Simd,
    T: ArrayNum + SimdElement,
    V: SimdBase<S, Element = T>,
{
    value.simd_eq(nodata).select(V::splat(value.token(), T::NODATA), value)
}

#[allow(dead_code)]
#[inline(always)]
pub(crate) fn restore_nodata<S, T, V>(value: V, nodata: T) -> V
where
    S: Simd,
    T: ArrayNum + SimdElement,
    V: SimdBase<S, Element = T>,
{
    nodata_mask::<S, T, V>(value).select(V::splat(value.token(), nodata), value)
}

#[inline(always)]
pub(crate) fn nodata_min<S, T, V>(lhs: V, rhs: V) -> V
where
    S: Simd,
    T: ArrayNum + SimdElement,
    V: SimdBase<S, Element = T>,
{
    if T::has_nan() {
        lhs.min_precise(rhs)
    } else {
        let result = lhs.min(rhs);
        let result = nodata_mask::<S, T, V>(lhs).select(rhs, result);
        nodata_mask::<S, T, V>(rhs).select(lhs, result)
    }
}

#[inline(always)]
pub(crate) fn nodata_max<S, T, V>(lhs: V, rhs: V) -> V
where
    S: Simd,
    T: ArrayNum + SimdElement,
    V: SimdBase<S, Element = T>,
{
    if T::has_nan() {
        lhs.max_precise(rhs)
    } else {
        let result = lhs.max(rhs);
        let result = nodata_mask::<S, T, V>(lhs).select(rhs, result);
        nodata_mask::<S, T, V>(rhs).select(lhs, result)
    }
}

#[inline(always)]
pub(crate) fn reduce_min<S, T, V>(value: V) -> Option<T>
where
    S: Simd,
    T: ArrayNum + SimdElement,
    V: SimdBase<S, Element = T>,
{
    let nodata = nodata_mask::<S, T, V>(value);
    if nodata.all_true() {
        None
    } else {
        Some(nodata.select(V::splat(value.token(), T::max_value()), value).reduce_min())
    }
}

#[inline(always)]
pub(crate) fn reduce_max<S, T, V>(value: V) -> Option<T>
where
    S: Simd,
    T: ArrayNum + SimdElement,
    V: SimdBase<S, Element = T>,
{
    let nodata = nodata_mask::<S, T, V>(value);
    if nodata.all_true() {
        None
    } else {
        Some(nodata.select(V::splat(value.token(), T::min_value()), value).reduce_max())
    }
}

use crate::ArrayNum;

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

/// Process nodata values in the data array
/// This means replacing all the values that match the nodata value with the default nodata value for the type T
/// as defined by the [`crate::Nodata`] trait
#[simd_macro::simd_bounds]
pub fn init_nodata<T: ArrayNum>(data: &mut [T], nodata: Option<T>) {
    if let Some(nodata) = nodata {
        if nodata.is_nan() || nodata == T::NODATA {
            // the nodata value for floats is also nan, so no processing required
            // if the nodata value matches the default nodata value for the type
            return;
        }

        cfg_if::cfg_if! {
            if #[cfg(all(feature = "simd", target_arch = "aarch64"))] {
                // For aarch64, we use the SIMD implementation directly (twice as fast as the scalar implementation on apple M2)
                simd::init_nodata(data, nodata);
            } else {
                // For other architectures, we rely on the cimpiler auto-vectorization (verified to be faster than the SIMD implementation for avx2)
                for v in data.iter_mut() { v.init_nodata(nodata); }
            }
        }
    }
}

#[simd_macro::simd_bounds]
pub fn restore_nodata<T: ArrayNum>(data: &mut [T], nodata: Option<T>) {
    if let Some(nodata) = nodata {
        if nodata.is_nan() || nodata == T::NODATA {
            // the nodata value for floats is also nan, so no processing required
            // if the nodata value matches the default nodata value for the type
            return;
        }

        cfg_if::cfg_if! {
            if #[cfg(all(feature = "simd", target_arch = "aarch64"))] {
                // For aarch64, we use the SIMD implementation directly (twice as fast as the scalar implementation on apple M2)
                simd::restore_nodata(data, nodata);
            } else {
                // For other architectures, we rely on the cimpiler auto-vectorization (verified to be faster than the SIMD implementation for avx2)
                for v in data.iter_mut() {
                    v.restore_nodata(nodata);
                }
            }
        }
    }
}

#[cfg(feature = "simd")]
pub mod simd {
    use inf::simd::LANES;
    use simd_macro::simd_bounds;

    use crate::{Nodata, NodataSimd};
    use std::simd::{SimdElement, prelude::*};

    use crate::ArrayNum;

    pub fn unary_simd<T: SimdElement>(data: &[T], mut cb_scalar: impl FnMut(&T), cb_simd: impl FnMut(&Simd<T, LANES>)) {
        let (head, simd_vals, tail) = data.as_simd();

        debug_assert!(head.is_empty(), "Data alignment error");

        head.iter().for_each(&mut cb_scalar);
        simd_vals.iter().for_each(cb_simd);
        tail.iter().for_each(cb_scalar);
    }

    pub fn unary_simd_mut<T: SimdElement>(data: &mut [T], cb_scalar: impl Fn(&mut T), cb_simd: impl Fn(&mut Simd<T, LANES>)) {
        let (head, simd_vals, tail) = data.as_simd_mut();

        debug_assert!(head.is_empty(), "Data alignment error");

        head.iter_mut().for_each(&cb_scalar);
        simd_vals.iter_mut().for_each(cb_simd);
        tail.iter_mut().for_each(cb_scalar);
    }

    #[simd_bounds]
    #[allow(dead_code)]
    pub fn init_nodata<T: ArrayNum + SimdElement>(data: &mut [T], nodata: T) {
        unary_simd_mut(
            data,
            |v| Nodata::init_nodata(v, nodata),
            |v| NodataSimd::init_nodata(v, Simd::splat(nodata)),
        );
    }

    #[simd_bounds]
    #[allow(dead_code)]
    pub fn restore_nodata<T: ArrayNum>(data: &mut [T], nodata: T) {
        unary_simd_mut(
            data,
            |v| Nodata::restore_nodata(v, nodata),
            |v| NodataSimd::restore_nodata(v, Simd::splat(nodata)),
        );
    }
}

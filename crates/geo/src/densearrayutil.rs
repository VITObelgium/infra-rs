use crate::ArrayNum;

/// Process nodata values in the data array.
/// This replaces values matching the metadata nodata value with [`crate::Nodata::NODATA`].
pub fn init_nodata<T: ArrayNum>(data: &mut [T], nodata: Option<T>) {
    if let Some(nodata) = nodata {
        if nodata.is_nan() || nodata == T::NODATA {
            return;
        }

        cfg_if::cfg_if! {
            if #[cfg(target_arch = "aarch64")] {
                simd::init_nodata(data, nodata);
            } else {
                // Auto-vectorization is faster than the explicit kernel on the tested x86 targets.
                for value in data {
                    value.init_nodata(nodata);
                }
            }
        }
    }
}

/// Replace internal nodata values with the nodata value used by an external format.
pub fn restore_nodata<T: ArrayNum>(data: &mut [T], nodata: Option<T>) {
    if let Some(nodata) = nodata {
        if nodata.is_nan() || nodata == T::NODATA {
            return;
        }

        cfg_if::cfg_if! {
            if #[cfg(target_arch = "aarch64")] {
                simd::restore_nodata(data, nodata);
            } else {
                // Auto-vectorization is faster than the explicit kernel on the tested x86 targets.
                for value in data {
                    value.restore_nodata(nodata);
                }
            }
        }
    }
}

pub mod simd {
    use fearless_simd::{Simd, SimdBase, SimdElement};

    use crate::{ArrayNum, Nodata, simd::dispatch_array_num_simd};

    #[inline(always)]
    pub fn unary_simd<S, T, V>(simd: S, data: &[T], mut cb_scalar: impl FnMut(&T), mut cb_simd: impl FnMut(V))
    where
        S: Simd,
        T: SimdElement,
        V: SimdBase<S, Element = T>,
    {
        let mut chunks = data.chunks_exact(V::LEN);
        for chunk in &mut chunks {
            cb_simd(V::from_slice(simd, chunk));
        }
        chunks.remainder().iter().for_each(&mut cb_scalar);
    }

    #[inline(always)]
    pub fn unary_simd_mut<S, T, V>(simd: S, data: &mut [T], mut cb_scalar: impl FnMut(&mut T), mut cb_simd: impl FnMut(V) -> V)
    where
        S: Simd,
        T: SimdElement,
        V: SimdBase<S, Element = T>,
    {
        let mut chunks = data.chunks_exact_mut(V::LEN);
        for chunk in &mut chunks {
            cb_simd(V::from_slice(simd, chunk)).store_slice(chunk);
        }
        chunks.into_remainder().iter_mut().for_each(&mut cb_scalar);
    }

    #[allow(dead_code)]
    pub fn init_nodata<T: ArrayNum>(data: &mut [T], nodata: T) {
        fearless_simd::dispatch!(crate::simd::level(), simd => init_nodata_dispatched(simd, data, nodata));
    }

    #[allow(dead_code)]
    #[inline(always)]
    fn init_nodata_dispatched<S: Simd, T: ArrayNum>(simd: S, data: &mut [T], nodata: T) {
        macro_rules! run {
            ($simd_type:ty, $scalar:ty, $vector:ty, $simd:expr, $data:expr, $nodata:expr) => {{
                let data: &mut [$scalar] = bytemuck::cast_slice_mut($data);
                let nodata = num::cast::<T, $scalar>($nodata).expect("ArrayNum type must match its ArrayDataType");
                unary_simd_mut::<$simd_type, $scalar, $vector>(
                    $simd,
                    data,
                    |value| Nodata::init_nodata(value, nodata),
                    |value| crate::simd::init_nodata::<$simd_type, $scalar, $vector>(value, nodata),
                );
            }};
        }

        dispatch_array_num_simd!(T::TYPE, S, run, simd, data, nodata);
    }

    #[allow(dead_code)]
    pub fn restore_nodata<T: ArrayNum>(data: &mut [T], nodata: T) {
        fearless_simd::dispatch!(crate::simd::level(), simd => restore_nodata_dispatched(simd, data, nodata));
    }

    #[allow(dead_code)]
    #[inline(always)]
    fn restore_nodata_dispatched<S: Simd, T: ArrayNum>(simd: S, data: &mut [T], nodata: T) {
        macro_rules! run {
            ($simd_type:ty, $scalar:ty, $vector:ty, $simd:expr, $data:expr, $nodata:expr) => {{
                let data: &mut [$scalar] = bytemuck::cast_slice_mut($data);
                let nodata = num::cast::<T, $scalar>($nodata).expect("ArrayNum type must match its ArrayDataType");
                unary_simd_mut::<$simd_type, $scalar, $vector>(
                    $simd,
                    data,
                    |value| Nodata::restore_nodata(value, nodata),
                    |value| crate::simd::restore_nodata::<$simd_type, $scalar, $vector>(value, nodata),
                );
            }};
        }

        dispatch_array_num_simd!(T::TYPE, S, run, simd, data, nodata);
    }
}

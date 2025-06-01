use crate::ArrayNum;

/// Process nodata values in the data array
/// This means replacing all the values that match the nodata value with the default nodata value for the type T
/// as defined by the [`crate::Nodata`] trait
//#[cfg(not(feature = "simd"))]
pub fn process_nodata<T: ArrayNum>(data: &mut [T], nodata: Option<T>) {
    if let Some(nodata) = nodata {
        if nodata.is_nan() || nodata == T::NODATA {
            // the nodata value for floats is also nan, so no processing required
            // if the nodata value matches the default nodata value for the type
            return;
        }

        data.iter_mut().for_each(|v| v.init_nodata(nodata));
    }
}

// #[cfg(feature = "simd")]
// pub fn process_nodata<T: ArrayNum>(data: &mut [T], nodata: Option<T>)
// where
//     std::simd::Simd<T, LANES>: crate::NodataSimd,
// {
//     if let Some(nodata) = nodata {
//         if nodata.is_nan() || nodata == T::NODATA {
//             // the nodata value for floats is also nan, so no processing required
//             // or the nodata value matches the default nodata value for the type
//             return;
//         }

//         simd::process_nodata(data, nodata);
//     }
// }

pub fn restore_nodata<T: ArrayNum>(data: &mut [T], nodata: Option<f64>) {
    if let Some(nodata) = inf::cast::option::<T>(nodata) {
        data.iter_mut().for_each(|v| v.restore_nodata(nodata));
    }
}

// #[cfg(feature = "simd")]
// mod simd {
//     use inf::simd::LANES;

//     use crate::{Nodata, NodataSimd};
//     use std::simd::{SimdElement, prelude::*};

//     use crate::ArrayNum;

//     pub fn unary_simd<T: SimdElement>(data: &mut [T], cb_scalar: impl Fn(&mut T), cb_simd: impl Fn(&mut Simd<T, LANES>))
//     where
//         std::simd::LaneCount<LANES>: std::simd::SupportedLaneCount,
//     {
//         let (head, simd_vals, tail) = data.as_simd_mut();

//         head.iter_mut().for_each(&cb_scalar);
//         simd_vals.iter_mut().for_each(cb_simd);
//         tail.iter_mut().for_each(cb_scalar);
//     }

//     pub fn process_nodata<T: ArrayNum>(data: &mut [T], nodata: T)
//     where
//         std::simd::Simd<T, LANES>: NodataSimd,
//     {
//         let nodata_simd = Simd::splat(nodata);

//         unary_simd(
//             data,
//             |v| Nodata::init_nodata(v, nodata),
//             |v| NodataSimd::init_nodata(v, nodata_simd),
//         );
//     }
// }

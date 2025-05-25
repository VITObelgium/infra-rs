use num::NumCast;

use crate::ArrayNum;

/// Process nodata values in the data array
/// This means replacing all the values that match the nodata value with the default nodata value for the type T
/// as defined by the [`crate::Nodata`] trait
pub fn process_nodata<T: ArrayNum>(data: &mut [T], nodata: Option<f64>) {
    if let Some(nodata) = nodata {
        if nodata.is_nan() || NumCast::from(nodata) == Some(T::NODATA) {
            // the nodata value for floats is also nan, so no processing required
            // or the nodata value matches the default nodata value for the type
            return;
        }

        let nodata = NumCast::from(nodata).unwrap_or(T::NODATA);
        for v in data.iter_mut() {
            if *v == nodata {
                *v = T::NODATA;
            }
        }
    }
}

#[cfg(feature = "gdal")]
pub fn flatten_nodata<T: ArrayNum>(data: &mut [T], nodata: Option<f64>) -> crate::Result<()> {
    let nodata_value = inf::cast::option::<T>(nodata);

    if let Some(nodata) = nodata_value {
        for x in data.iter_mut() {
            if x.is_nodata() {
                *x = nodata;
            }
        }
    }

    Ok(())
}

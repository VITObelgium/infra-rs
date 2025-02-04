use crate::GeoReference;
use crate::{Array, DenseArray, ArrayNum, Result};
use num::NumCast;

pub type DenseRaster<T> = DenseArray<T, GeoReference>;

impl<T: ArrayNum<T>> DenseRaster<T> {
    #[allow(dead_code)] // this function is not used when gdal support is disabled
    pub(super) fn flatten_nodata(&mut self) -> Result<()> {
        let meta_nodata = self.metadata().nodata_as::<T>()?;

        if let Some(nodata) = meta_nodata {
            self.data.iter_mut().for_each(|x| {
                if x.is_nodata() {
                    *x = nodata;
                }
            });
        }

        Ok(())
    }
}

#[cfg(feature = "gdal")]
impl<T: ArrayNum<T> + gdal::raster::GdalType> DenseRaster<T> {
    pub fn warped_to_epsg(&self, epsg: crate::crs::Epsg) -> crate::Result<Self> {
        use super::algo;
        use super::io;

        let dest_meta = self.metadata().warped_to_epsg(epsg)?;
        let result = DenseRaster::filled_with_nodata(dest_meta);

        let src_ds = io::dataset::create_in_memory_with_data(self.metadata(), self.data.as_slice())?;
        let dst_ds = io::dataset::create_in_memory_with_data(result.metadata(), result.data.as_slice())?;

        algo::warp(&src_ds, &dst_ds, &algo::WarpOptions::default())?;

        Ok(result)
    }
}

/// Process nodata values in the data array
/// This means replacing all the values that match the nodata value with the default nodata value for the type T
/// as defined by the [`crate::Nodata`] trait
pub fn process_nodata<T: ArrayNum<T>>(data: &mut [T], nodata: Option<f64>) {
    if let Some(nodata) = nodata {
        if nodata.is_nan() || NumCast::from(nodata) == Some(T::nodata_value()) {
            // the nodata value for floats is also nan, so no processing required
            // or the nodata value matches the default nodata value for the type
            return;
        }

        let nodata = NumCast::from(nodata).unwrap_or(T::nodata_value());
        for v in data.iter_mut() {
            if *v == nodata {
                *v = T::nodata_value();
            }
        }
    }
}

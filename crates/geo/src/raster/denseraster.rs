use crate::GeoReference;
use crate::{Array, ArrayNum, DenseArray};

pub type DenseRaster<T> = DenseArray<T, GeoReference>;

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

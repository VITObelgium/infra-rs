use crate::DenseArray;
use crate::GeoReference;

pub type DenseRaster<T> = DenseArray<T, GeoReference>;

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

#[cfg(feature = "gdal")]
#[simd_macro::simd_bounds]
impl<T: crate::ArrayNum + gdal::raster::GdalType> DenseRaster<T> {
    pub fn warped_to_epsg(&self, epsg: crate::crs::Epsg) -> crate::Result<Self> {
        use super::algo;
        use super::io;
        use crate::Array;

        let dest_meta = self.metadata().warped_to_epsg(epsg)?;
        let result = DenseRaster::filled_with_nodata(dest_meta);

        let src_ds = io::dataset::create_in_memory_with_data(self.metadata(), self.data.as_slice())?;
        let dst_ds = io::dataset::create_in_memory_with_data(result.metadata(), result.data.as_slice())?;

        algo::warp(&src_ds, &dst_ds, &algo::WarpOptions::default())?;

        Ok(result)
    }
}

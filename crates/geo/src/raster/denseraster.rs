use crate::DenseArray;
use crate::GeoReference;
use crate::crs;
use crate::raster::algo;
use crate::raster::algo::TargetSrs;

pub type DenseRaster<T> = DenseArray<T, GeoReference>;

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

#[simd_macro::simd_bounds]
impl<T: crate::ArrayNum> DenseRaster<T> {
    pub fn warped_to_epsg(&self, epsg: crs::Epsg) -> crate::Result<Self> {
        let opts = algo::WarpOptions {
            target_srs: TargetSrs::Epsg(epsg),
            ..Default::default()
        };

        self.warped(&opts)
    }

    pub fn warped(&self, opts: &algo::WarpOptions) -> crate::Result<Self> {
        algo::warp(self, opts)
    }
}

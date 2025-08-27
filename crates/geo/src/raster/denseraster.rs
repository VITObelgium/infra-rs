use crate::DenseArray;
use crate::GeoReference;

#[cfg(any(feature = "proj", feature = "proj4rs"))]
use crate::{
    crs,
    raster::algo::{self, TargetSrs},
};

pub type DenseRaster<T> = DenseArray<T, GeoReference>;

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

#[simd_macro::simd_bounds]
#[cfg(any(feature = "proj", feature = "proj4rs"))]
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

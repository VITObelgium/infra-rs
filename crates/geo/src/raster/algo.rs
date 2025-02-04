//! Algorithms for raster data processing (translate, warp, ...).

mod distance;
mod nodata;
#[cfg(all(feature = "gdal", feature = "vector"))]
mod polygonize;
mod rasterdiff;
#[cfg(feature = "gdal")]
mod translate;
#[cfg(feature = "gdal")]
mod warp;

mod clusterid;
pub(crate) mod clusterutils;

use num::NumCast;
#[cfg(all(feature = "gdal", feature = "vector"))]
pub use polygonize::polygonize;

#[cfg(feature = "gdal")]
pub use {translate::translate, translate::translate_file, warp::warp, warp::warp_cli, warp::warp_to_disk_cli, warp::WarpOptions};

#[cfg(feature = "gdal")]
pub use {rasterdiff::raster_files_diff, rasterdiff::raster_files_intersection_diff};

pub use {
    clusterid::cluster_id, clusterid::cluster_id_with_obstacles, clusterid::fuzzy_cluster_id, clusterid::fuzzy_cluster_id_with_obstacles,
};

pub use {
    distance::closest_target, distance::distance, distance::distance_with_obstacles, distance::sum_targets_within_travel_distance,
    distance::sum_within_travel_distance, distance::travel_distance, distance::travel_distances_up_to,
    distance::value_at_closest_less_than_travel_target, distance::value_at_closest_target, distance::value_at_closest_travel_target,
};

pub use {nodata::is_data, nodata::is_nodata, nodata::replace_nodata, nodata::replace_nodata_in_place, nodata::turn_value_into_nodata};

pub use rasterdiff::raster_diff;
pub use rasterdiff::RasterCellMismatch;
pub use rasterdiff::RasterDiffResult;

use crate::{Array, ArrayNum};

pub fn cast<TDest, R>(src: &R) -> R::WithPixelType<TDest>
where
    R: Array,
    TDest: ArrayNum<TDest>,
    for<'a> &'a R: IntoIterator<Item = Option<R::Pixel>>,
{
    R::WithPixelType::<TDest>::from_iter(src.metadata().clone(), src.into_iter().map(|x| x.and_then(|x| NumCast::from(x))))
}

pub fn assert_dimensions(r1: &impl Array, r2: &impl Array) {
    assert_eq!(r1.width(), r2.width(), "Raster widths do not match");
    assert_eq!(r1.height(), r2.height(), "Raster heights do not match");
}

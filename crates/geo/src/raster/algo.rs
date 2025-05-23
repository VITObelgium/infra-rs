//! Algorithms for raster data processing (translate, warp, ...).

mod cast;
mod conversion;
mod distance;
mod filter;
mod limits;
mod nodata;
#[cfg(all(feature = "gdal", feature = "vector"))]
mod polygonize;
mod quantile;
mod rasterdiff;
#[cfg(feature = "gdal")]
mod translate;
#[cfg(feature = "gdal")]
mod warp;

mod clusterid;
pub(crate) mod clusterutils;

#[cfg(all(feature = "gdal", feature = "vector"))]
pub use polygonize::polygonize;

use crate::Array;

#[cfg(feature = "gdal")]
pub use {translate::translate, translate::translate_file, warp::WarpOptions, warp::warp, warp::warp_cli, warp::warp_to_disk_cli};

#[cfg(feature = "gdal")]
pub use {rasterdiff::raster_files_diff, rasterdiff::raster_files_intersection_diff};

pub use {
    clusterid::cluster_id, clusterid::cluster_id_with_obstacles, clusterid::fuzzy_cluster_id, clusterid::fuzzy_cluster_id_with_obstacles,
};

pub use conversion::replace_value;

pub use {
    cast::cast, distance::closest_target, distance::distance, distance::distance_with_obstacles,
    distance::sum_targets_within_travel_distance, distance::sum_within_travel_distance, distance::travel_distance,
    distance::travel_distances_up_to, distance::value_at_closest_less_than_travel_target, distance::value_at_closest_target,
    distance::value_at_closest_travel_target, filter::filter, limits::min_max, quantile::quantiles, quantile::quantiles_neg_pos,
};

pub use {nodata::is_data, nodata::is_nodata, nodata::replace_nodata, nodata::replace_nodata_in_place, nodata::turn_value_into_nodata};

pub use rasterdiff::RasterCellMismatch;
pub use rasterdiff::RasterDiffResult;
pub use rasterdiff::raster_diff;

pub fn assert_dimensions(r1: &impl Array, r2: &impl Array) {
    assert_eq!(r1.columns(), r2.columns(), "Raster column count does not match");
    assert_eq!(r1.rows(), r2.rows(), "Raster row count does not match");
}

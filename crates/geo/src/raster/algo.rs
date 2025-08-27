//! Algorithms for raster data processing (translate, warp, ...).

mod cast;
mod conversion;
mod distance;
mod filter;
#[cfg(feature = "gdal")]
mod gdaltranslate;
#[cfg(feature = "gdal")]
mod gdalwarp;
mod limits;
mod nodata;
#[cfg(all(feature = "gdal", feature = "vector"))]
mod polygonize;
mod quantile;
mod rasterdiff;
mod statistics;
#[cfg(any(feature = "proj", feature = "proj4rs"))]
mod warp;

mod clusterid;
pub(crate) mod clusterutils;

#[cfg(all(feature = "gdal", feature = "vector"))]
pub use polygonize::polygonize;

use crate::Array;

#[cfg(feature = "gdal")]
pub mod gdal {
    pub use super::{
        gdaltranslate::translate, gdaltranslate::translate_file, gdalwarp::GdalWarpOptions, gdalwarp::warp, gdalwarp::warp_cli,
        gdalwarp::warp_georeference, gdalwarp::warp_options_to_gdalwarp_cli_args, gdalwarp::warp_to_disk_cli,
    };
}

#[cfg(feature = "gdal")]
pub use {rasterdiff::raster_files_diff, rasterdiff::raster_files_intersection_diff};

#[cfg(any(feature = "proj", feature = "proj4rs"))]
pub use warp::{NumThreads, TargetPixelAlignment, TargetSrs, WarpOptions, WarpTargetSize, warp};

pub use {
    clusterid::cluster_id, clusterid::cluster_id_with_obstacles, clusterid::fuzzy_cluster_id, clusterid::fuzzy_cluster_id_with_obstacles,
};

#[cfg(any(feature = "proj", feature = "proj4rs"))]
pub fn warp_georeference(georef: &crate::GeoReference, opts: &WarpOptions) -> crate::Result<crate::GeoReference> {
    #[cfg(feature = "gdal")]
    return gdal::warp_georeference(georef, opts);

    #[cfg(all(not(feature = "gdal"), feature = "proj4rs"))]
    return warp::warp_georeference(georef, opts);

    #[cfg(not(any(feature = "gdal", feature = "proj4rs")))]
    panic!("No reprojection backend enabled. Enable either 'gdal' or 'proj4rs' feature.");
}

pub use conversion::replace_value;

pub use {
    cast::cast, distance::closest_target, distance::distance, distance::distance_with_obstacles,
    distance::sum_targets_within_travel_distance, distance::sum_within_travel_distance, distance::travel_distance,
    distance::travel_distances_up_to, distance::value_at_closest_less_than_travel_target, distance::value_at_closest_target,
    distance::value_at_closest_travel_target, filter::filter, filter::filter_value, limits::min_max, quantile::SplitQuantiles,
    quantile::quantiles, quantile::quantiles_neg_pos, statistics::RasterStats, statistics::statistics,
};

#[cfg(feature = "simd")]
pub mod simd {
    pub use super::{filter::simd::filter, filter::simd::filter_value, limits::simd::max, limits::simd::min, limits::simd::min_max};
}

pub use {nodata::is_data, nodata::is_nodata, nodata::replace_nodata, nodata::replace_nodata_in_place, nodata::replace_value_by_nodata};

pub use rasterdiff::{RasterCellMismatch, RasterDiffResult, array_diff, raster_diff};

pub fn assert_dimensions(r1: &impl Array, r2: &impl Array) {
    assert_eq!(r1.columns(), r2.columns(), "Raster column count does not match");
    assert_eq!(r1.rows(), r2.rows(), "Raster row count does not match");
}

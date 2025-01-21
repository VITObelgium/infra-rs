//! Algorithms for raster data processing (translate, warp, ...).

mod cast;
mod rasterdiff;

#[cfg(all(feature = "gdal", feature = "vector"))]
mod polygonize;
#[cfg(feature = "gdal")]
mod translate;
#[cfg(feature = "gdal")]
mod warp;

#[cfg(all(feature = "gdal", feature = "vector"))]
pub use polygonize::polygonize;

#[cfg(feature = "gdal")]
pub use {
    translate::translate, translate::translate_file, warp::warp, warp::warp_cli, warp::warp_to_disk_cli,
    warp::WarpOptions,
};

#[cfg(feature = "gdal")]
pub use {rasterdiff::raster_files_diff, rasterdiff::raster_files_intersection_diff};

pub use cast::cast;

pub use rasterdiff::raster_diff;
pub use rasterdiff::RasterCellMismatch;
pub use rasterdiff::RasterDiffResult;

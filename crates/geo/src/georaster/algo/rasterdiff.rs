use raster::{Cell, RasterNum};

use crate::{georaster::GeoRaster, Error, Result};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RasterCellMismatch<T: RasterNum<T>> {
    DataMismatch(Cell, T, T),
    NodataMismatch(Cell, Option<T>, Option<T>),
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RasterDiffResult<T: RasterNum<T>> {
    pub matches: usize,
    pub mismatches: Vec<RasterCellMismatch<T>>,
}

impl<T: RasterNum<T>> RasterDiffResult<T> {
    pub fn new() -> Self {
        Self {
            matches: 0,
            mismatches: Vec::default(),
        }
    }

    pub fn is_exact_match(&self) -> bool {
        self.mismatches.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &RasterCellMismatch<T>> {
        self.mismatches.iter()
    }
}

#[cfg(feature = "gdal")]
/// Compare two raster files and return a list of cell mismatches
/// The two rasters must have the same cell size and be aligned
/// Only the intersection of the two rasters will be compared
pub fn raster_files_intersection_diff<T: RasterNum<T> + gdal::raster::GdalType>(
    lhs: &std::path::Path,
    rhs: &std::path::Path,
) -> Result<RasterDiffResult<T>> {
    use crate::georaster::{DenseGeoRaster, RasterIO};

    let lhs_ras = DenseGeoRaster::<T>::read(lhs)?;
    let rhs_ras = DenseGeoRaster::<T>::read(rhs)?;

    let intersection = lhs_ras.geo_reference().intersection(rhs_ras.geo_reference())?;
    if intersection.raster_size().is_empty() {
        return Ok(RasterDiffResult::new());
    }

    let lhs_ras = DenseGeoRaster::<T>::read_bounds(lhs, &intersection, 1)?;
    let rhs_ras = DenseGeoRaster::<T>::read_bounds(rhs, &intersection, 1)?;

    raster_diff(&lhs_ras, &rhs_ras)
}

#[cfg(feature = "gdal")]
/// Compare two raster files and return a list of cell mismatches
/// The two rasters must have the same extent, size, cell size and be aligned
pub fn raster_files_diff<T: RasterNum<T> + gdal::raster::GdalType>(
    lhs: &std::path::Path,
    rhs: &std::path::Path,
) -> Result<RasterDiffResult<T>> {
    use crate::georaster::{DenseGeoRaster, RasterIO};

    let lhs_ras = DenseGeoRaster::<T>::read(lhs)?;
    let rhs_ras = DenseGeoRaster::<T>::read(rhs)?;

    raster_diff(&lhs_ras, &rhs_ras)
}

/// Compare two rasters and return a list of cell mismatches
/// The two rasters must have the same extent, size, cell size and be aligned
pub fn raster_diff<T: RasterNum<T>>(lhs: &impl GeoRaster<T>, rhs: &impl GeoRaster<T>) -> Result<RasterDiffResult<T>> {
    let left_meta = lhs.geo_reference();
    let right_meta = rhs.geo_reference();

    if left_meta.raster_size() != right_meta.raster_size() {
        return Err(Error::InvalidArgument(
            "Rasters have different sizes, diffing is not possible".to_string(),
        ));
    }

    if left_meta.cell_size() != right_meta.cell_size() {
        return Err(Error::InvalidArgument(
            "Rasters have different cell sizes, diffing is not possible".to_string(),
        ));
    }

    if !left_meta.is_aligned_with(right_meta) {
        return Err(Error::InvalidArgument(
            "Rasters are not aligned, diffing is not possible".to_string(),
        ));
    }

    let mut raster_diff = RasterDiffResult::new();
    lhs.iter_opt()
        .zip(rhs.iter_opt())
        .enumerate()
        .for_each(|(idx, (l, r))| match (l, r) {
            (Some(l), Some(r)) => {
                if l != r {
                    raster_diff
                        .mismatches
                        .push(RasterCellMismatch::DataMismatch(left_meta.cell_at_index(idx), l, r));
                } else {
                    raster_diff.matches += 1;
                }
            }
            (Some(_), None) | (None, Some(_)) => {
                raster_diff
                    .mismatches
                    .push(RasterCellMismatch::NodataMismatch(left_meta.cell_at_index(idx), l, r));
            }
            (None, None) => raster_diff.matches += 1,
        });

    Ok(raster_diff)
}

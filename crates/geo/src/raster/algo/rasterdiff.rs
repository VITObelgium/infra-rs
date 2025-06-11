use crate::{Array, ArrayNum, Cell};

use crate::{Error, GeoReference, Result};

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RasterCellMismatch<T: ArrayNum> {
    DataMismatch(Cell, T, T),
    NodataMismatch(Cell, Option<T>, Option<T>),
}

#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RasterDiffResult<T: ArrayNum> {
    pub matches: usize,
    pub mismatches: Vec<RasterCellMismatch<T>>,
}

impl<T: ArrayNum> RasterDiffResult<T> {
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

#[cfg(all(feature = "simd", feature = "gdal"))]
const LANES: usize = inf::simd::LANES;

#[cfg(feature = "gdal")]
/// Compare two raster files and return a list of cell mismatches
/// The two rasters must have the same cell size and be aligned
/// Only the intersection of the two rasters will be compared
#[simd_macro::simd_bounds]
pub fn raster_files_intersection_diff<T: ArrayNum + gdal::raster::GdalType>(
    lhs: &std::path::Path,
    rhs: &std::path::Path,
) -> Result<RasterDiffResult<T>> {
    use crate::raster::{DenseRaster, RasterIO};

    let lhs_ras = DenseRaster::<T>::read(lhs)?;
    let rhs_ras = DenseRaster::<T>::read(rhs)?;

    let intersection = lhs_ras.metadata().intersection(rhs_ras.metadata())?;
    if intersection.raster_size().is_empty() {
        return Ok(RasterDiffResult::new());
    }

    let lhs_ras = DenseRaster::<T>::read_bounds(lhs, &intersection, 1)?;
    let rhs_ras = DenseRaster::<T>::read_bounds(rhs, &intersection, 1)?;

    raster_diff(&lhs_ras, &rhs_ras)
}

#[cfg(feature = "gdal")]
/// Compare two raster files and return a list of cell mismatches
/// The two rasters must have the same extent, size, cell size and be aligned
#[simd_macro::simd_bounds]
pub fn raster_files_diff<T: ArrayNum + gdal::raster::GdalType>(
    lhs: &std::path::Path,
    rhs: &std::path::Path,
) -> Result<RasterDiffResult<T>> {
    use crate::raster::{DenseRaster, RasterIO};

    let lhs_ras = DenseRaster::<T>::read(lhs)?;
    let rhs_ras = DenseRaster::<T>::read(rhs)?;

    raster_diff(&lhs_ras, &rhs_ras)
}

/// Compare two rasters and return a list of cell mismatches
/// The two rasters must have the same extent, size, cell size and be aligned
pub fn raster_diff<T: ArrayNum>(
    lhs: &impl Array<Pixel = T, Metadata = GeoReference>,
    rhs: &impl Array<Pixel = T, Metadata = GeoReference>,
) -> Result<RasterDiffResult<T>> {
    let left_meta = lhs.metadata();
    let right_meta = rhs.metadata();

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

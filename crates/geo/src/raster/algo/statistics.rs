use std::cmp::Ordering;

use crate::{Array, ArrayMetadata, ArrayNum, DenseArray, Result};

use super::quantile;

pub struct RasterStats<T: ArrayNum> {
    pub min: T,
    pub max: T,
    pub median: f64,
    pub mean: f64,
    pub stddev: f64,
    pub quantiles: Option<Vec<f64>>,
    pub value_count: usize,
    pub sum: f64,
}

/// Calculates basic statistics for a raster array in one pass.
/// Use this if multiple statistics are needed, as it avoids multiple iterations over the data.
/// Returns `None` if the raster is empty or contains only nodata values.
pub fn statistics<T: ArrayNum, Meta: ArrayMetadata>(raster: &DenseArray<T, Meta>, quantile_vals: &[f64]) -> Result<Option<RasterStats<T>>> {
    let mut min = T::max_value();
    let mut max = T::min_value();
    let mut sum = 0.0;

    // Assume roughly 75% of the pixels will be valid data, to avoid excessive memory reallocations while iterating.
    let mut pixel_values = Vec::with_capacity((raster.len() as f64 * 0.75) as usize);

    for val in raster.into_iter().flatten() {
        if val < min {
            min = val;
        }

        if val > max {
            max = val;
        }

        sum += val.to_f64().unwrap_or(0.0);
        pixel_values.push(val);
    }

    if pixel_values.is_empty() {
        return Ok(None);
    }

    pixel_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let quantiles = quantile::array_quantiles(&pixel_values, quantile_vals)?;
    let mean = sum / pixel_values.len() as f64;
    let stddev = std_deviation(&pixel_values, mean);

    let median = if pixel_values.len() % 2 == 0 {
        let mid1 = pixel_values[pixel_values.len() / 2 - 1].to_f64().unwrap_or(f64::NAN);
        let mid2 = pixel_values[pixel_values.len() / 2].to_f64().unwrap_or(f64::NAN);
        (mid1 + mid2) / 2.0
    } else {
        pixel_values[pixel_values.len() / 2].to_f64().unwrap_or(f64::NAN)
    };

    Ok(Some(RasterStats {
        min,
        max,
        mean,
        stddev,
        median,
        quantiles,
        value_count: pixel_values.len(),
        sum,
    }))
}

fn std_deviation<T: ArrayNum>(data: &[T], data_mean: f64) -> f64 {
    let count = data.len();
    assert!(count > 0, "Cannot calculate standard deviation for an empty array");

    let variance = data
        .iter()
        .map(|value| {
            let diff = data_mean - value.to_f64().unwrap_or(0.0);
            diff * diff
        })
        .sum::<f64>()
        / count as f64;

    variance.sqrt()
}

#[cfg(test)]
mod tests {

    use approx::assert_relative_eq;

    use crate::{
        CellSize, GeoReference, Point, RasterSize,
        array::{Columns, Rows},
        raster::{DenseRaster, algo},
        testutils::NOD,
    };

    use super::*;

    #[test]
    fn test_statistics_all_nodata() -> Result<()> {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(3), Columns(2)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let raster = DenseRaster::<f64>::new_process_nodata(
            meta,
            vec![
                NOD, NOD,
                NOD, NOD,
                NOD, NOD,
            ],
        )?;

        assert!(algo::statistics(&raster, &[0.0, 0.25, 0.5, 0.75, 1.0])?.is_none());

        Ok(())
    }

    #[test]
    fn test_statistics() -> Result<()> {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(3), Columns(2)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        {
            #[rustfmt::skip]
            let raster = DenseRaster::<f64>::new_process_nodata(
                meta.clone(),
                vec![
                    3.0, 1.0,
                    4.0, NOD,
                    1.0, 2.0,
                    ],
                )?;

            let quants = algo::quantiles(&raster, &[0.0, 0.25, 0.5, 0.75, 1.0])?.expect("Quantiles should have a value");
            assert_eq!(quants, vec![1.0, 1.0, 2.0, 3.0, 4.0]);
        }

        {
            // even number of values
            #[rustfmt::skip]
            let raster = DenseRaster::<f64>::new_process_nodata(
                meta.clone(),
                vec![
                    3.0, 1.0,
                    4.0, 7.0,
                    1.0, 2.0,
                    ],
                )?;

            // Sorted vals: 1.0, 1.0, 2.0, 3.0, 4.0, 7.0
            let stats = algo::statistics(&raster, &[0.0, 0.25, 0.5, 0.75, 1.0])?.expect("Statistics should have a value");
            assert_eq!(stats.min, 1.0);
            assert_eq!(stats.max, 7.0);
            assert_eq!(stats.mean, 18.0 / 6.0);
            assert_eq!(stats.median, 2.5);
            assert_eq!(stats.sum, 18.0);
            assert_eq!(stats.value_count, 6);
            assert_relative_eq!(stats.stddev, 2.0816659994661, epsilon = 1e-8);
            assert_eq!(stats.quantiles, Some(vec![1.0, 1.25, 2.5, 3.75, 7.0]));
        }

        {
            // odd number of values
            #[rustfmt::skip]
            let raster = DenseRaster::<f64>::new_process_nodata(
                meta,
                vec![
                    3.0, 2.0,
                    4.0, 7.0,
                    1.0, NOD,
                    ],
                )?;

            // Sorted vals: 1.0, 2.0, 3.0, 4.0, 7.0
            let stats = algo::statistics(&raster, &[0.0, 0.25, 0.5, 0.75, 1.0])?.expect("Statistics should have a value");
            assert_eq!(stats.min, 1.0);
            assert_eq!(stats.max, 7.0);
            assert_eq!(stats.mean, 17.0 / 5.0);
            assert_eq!(stats.median, 3.0);
            assert_eq!(stats.sum, 17.0);
            assert_eq!(stats.value_count, 5);
            assert_relative_eq!(stats.stddev, 2.0591260281974, epsilon = 1e-8);
            assert_eq!(stats.quantiles, Some(vec![1.0, 2.0, 3.0, 4.0, 7.0]));
        }

        Ok(())
    }
}

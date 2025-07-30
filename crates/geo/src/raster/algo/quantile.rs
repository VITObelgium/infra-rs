use num::Zero;
use std::cmp::Ordering;

use crate::{Array, ArrayNum, Error, Result};

fn to_f64<T>(value: T) -> Result<f64>
where
    T: ArrayNum,
{
    value
        .to_f64()
        .ok_or_else(|| Error::InvalidArgument(format!("Failed to convert raster value to f64: '{value:?}'")))
}

pub(crate) fn array_quantiles<T>(data: &[T], quantile_vals: &[f64]) -> Result<Option<Vec<f64>>>
where
    T: ArrayNum,
{
    if data.is_empty() {
        return Ok(None);
    }

    let mut results = Vec::with_capacity(quantile_vals.len());
    let len = data.len() as f64;

    for &q in quantile_vals {
        let pos = q * (len - 1.0);
        let lower = pos.floor() as usize;
        let upper = pos.ceil() as usize;

        let value = if lower == upper {
            to_f64(data[lower])?
        } else {
            let lower_val = to_f64(data[lower])?;
            let upper_val = to_f64(data[upper])?;
            let weight = pos - lower as f64;
            lower_val * (1.0 - weight) + upper_val * weight
        };

        results.push(value);
    }

    Ok(Some(results))
}

pub fn quantiles<RasterType>(ras: &RasterType, quantile_vals: &[f64]) -> Result<Option<Vec<f64>>>
where
    RasterType: Array,
{
    if quantile_vals.iter().any(|&q| !(0.0..=1.0).contains(&q)) {
        return Err(Error::InvalidArgument("Quantile values must be between 0 and 1".to_string()));
    }

    let mut data: Vec<RasterType::Pixel> = ras.iter_values().collect();

    if data.is_empty() {
        return Ok(None);
    }

    data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    array_quantiles(&data, quantile_vals)
}

#[derive(Debug, Clone)]
pub struct SplitQuantiles<T> {
    pub negatives: Option<Vec<T>>,
    pub positives: Option<Vec<T>>,
}

/// Computes quantiles for a raster, ignoring nodata values.
/// This function is similar to `quantiles`, but it seperates the positive and negative values
/// So two quantiles are computed, one for the negative values and one for the positive values.
pub fn quantiles_neg_pos<RasterType>(ras: &RasterType, quantile_vals: &[f64]) -> Result<SplitQuantiles<f64>>
where
    RasterType: Array,
    RasterType::Pixel: ArrayNum,
{
    if quantile_vals.iter().any(|&q| !(0.0..=1.0).contains(&q)) {
        return Err(Error::InvalidArgument("Quantile values must be between 0 and 1".to_string()));
    }

    let mut data: Vec<RasterType::Pixel> = ras.iter_values().collect();
    if data.is_empty() {
        return Ok(SplitQuantiles {
            negatives: None,
            positives: None,
        });
    }

    data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let first_pos_idx = data.iter().position(|v| *v >= RasterType::Pixel::zero());

    let (negatives, positives) = data.split_at(first_pos_idx.unwrap_or(0));

    Ok(SplitQuantiles {
        negatives: array_quantiles(negatives, quantile_vals)?,
        positives: array_quantiles(positives, quantile_vals)?,
    })
}

#[cfg(test)]
mod tests {

    use inf::allocate;

    use crate::{
        ArrayInterop, CellSize, GeoReference, Point, RasterSize,
        array::{Columns, Rows},
        raster::{DenseRaster, algo},
        testutils::NOD,
    };

    use super::*;

    #[test]
    fn quantiles_all_nodata() -> Result<()> {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(3), Columns(2)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let raster = DenseRaster::<f64>::new_init_nodata(
            meta,
            allocate::aligned_vec_from_slice(&[
                NOD, NOD,
                NOD, NOD,
                NOD, NOD,
            ]),
        )?;

        assert!(algo::quantiles(&raster, &[0.0, 0.25, 0.5, 0.75, 1.0])?.is_none());

        Ok(())
    }

    #[test]
    fn quantiles() -> Result<()> {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(3), Columns(2)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        {
            #[rustfmt::skip]
            let raster = DenseRaster::<f64>::new_init_nodata(
                meta.clone(),
                allocate::aligned_vec_from_slice(&[
                    3.0, 1.0,
                    4.0, NOD,
                    1.0, 2.0,
                    ]),
                )?;

            let quants = algo::quantiles(&raster, &[0.0, 0.25, 0.5, 0.75, 1.0])?.expect("Quantiles should have a value");
            assert_eq!(quants, vec![1.0, 1.0, 2.0, 3.0, 4.0]);
        }

        {
            #[rustfmt::skip]
            let raster = DenseRaster::<f64>::new_init_nodata(
                meta,
                allocate::aligned_vec_from_slice(&[
                    3.0, 1.0,
                    4.0, 7.0,
                    1.0, 2.0,
                    ]),
                )?;

            let quants = algo::quantiles(&raster, &[0.0, 0.25, 0.5, 0.75, 1.0])?.expect("Quantiles should have a value");
            assert_eq!(quants, vec![1.0, 1.25, 2.5, 3.75, 7.0]);
        }

        Ok(())
    }

    #[cfg(feature = "gdal")]
    #[test]
    fn quantiles_on_byte_raster() -> Result<()> {
        use crate::{raster::RasterIO, testutils::workspace_test_data_dir};

        let raster = DenseRaster::<f32>::read(workspace_test_data_dir().join("landusebyte.tif"))?;
        let quants = algo::quantiles(&raster, &[0.0, 0.25, 0.5, 0.75, 1.0])?.expect("Quantiles should have a value");
        assert_eq!(quants, vec![0.0, 42.0, 138.0, 159.0, 249.0]);

        Ok(())
    }

    #[test]
    fn quantiles_neg_pos() -> Result<()> {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(3), Columns(4)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
            let raster = DenseRaster::<f64>::new_init_nodata(
                meta,
                allocate::aligned_vec_from_slice(&[
                    3.0, 1.0,
                    -1.0, -3.0,
                    -4.0, -7.0,
                    4.0, 7.0,
                    -1.0, 2.0,
                    1.0, -2.0,
                    ]),
                )?;

        let quantiles = algo::quantiles_neg_pos(&raster, &[0.0, 0.25, 0.5, 0.75, 1.0])?;
        let neg_quants = quantiles.negatives.expect("Negative quantiles should have a value");
        let pos_quants = quantiles.positives.expect("Positive quantiles should have a value");
        assert_eq!(neg_quants, vec![-7.0, -3.75, -2.5, -1.25, -1.0]);
        assert_eq!(pos_quants, vec![1.0, 1.25, 2.5, 3.75, 7.0]);

        Ok(())
    }
}

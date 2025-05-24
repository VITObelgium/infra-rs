use num::Zero;
use std::cmp::Ordering;

use crate::{Array, ArrayNum, Error, Result};

fn to_f64<T>(value: T) -> Result<f64>
where
    T: ArrayNum,
{
    value
        .to_f64()
        .ok_or_else(|| Error::InvalidArgument(format!("Failed to convert raster value to f64: '{:?}'", value)))
}

fn array_quantiles<T>(data: &[T], quantile_vals: &[f64]) -> Result<Option<Vec<f64>>>
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

/// Computes quantiles for a raster, ignoring nodata values.
/// This function is similar to `quantiles`, but it seperates the positive and negative values
/// So two quantiles are computed, one for the negative values and one for the positive values.
pub fn quantiles_neg_pos<RasterType>(ras: &RasterType, quantile_vals: &[f64]) -> Result<(Option<Vec<f64>>, Option<Vec<f64>>)>
where
    RasterType: Array,
    RasterType::Pixel: ArrayNum,
{
    if quantile_vals.iter().any(|&q| !(0.0..=1.0).contains(&q)) {
        return Err(Error::InvalidArgument("Quantile values must be between 0 and 1".to_string()));
    }

    let mut data: Vec<RasterType::Pixel> = ras.iter_values().collect();
    if data.is_empty() {
        return Ok((None, None));
    }

    data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let first_pos_idx = data.iter().position(|v| *v >= RasterType::Pixel::zero());

    let (negatives, positives) = data.split_at(first_pos_idx.unwrap_or(0));

    Ok((
        array_quantiles(negatives, quantile_vals)?,
        array_quantiles(positives, quantile_vals)?,
    ))
}

#[cfg(test)]
#[generic_tests::define]
mod unspecialized_generictests {

    use crate::{
        CellSize, GeoReference, Point, RasterSize,
        array::{Columns, Rows},
        raster::{DenseRaster, algo},
        testutils::NOD,
    };

    use super::*;

    #[test]
    fn test_quantiles_all_nodata<R: Array<Pixel = u8, Metadata = GeoReference>>() -> Result<()> {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(3), Columns(2)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
        let raster = R::WithPixelType::<f64>::new_process_nodata(
            meta,
            vec![
                NOD, NOD,
                NOD, NOD,
                NOD, NOD,
            ],
        )?;

        assert!(algo::quantiles(&raster, &[0.0, 0.25, 0.5, 0.75, 1.0])?.is_none());

        Ok(())
    }

    #[test]
    fn test_quantiles<R: Array<Pixel = u8, Metadata = GeoReference>>() -> Result<()> {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(3), Columns(2)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        {
            #[rustfmt::skip]
            let raster = R::WithPixelType::<f64>::new_process_nodata(
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
            #[rustfmt::skip]
            let raster = R::WithPixelType::<f64>::new_process_nodata(
                meta,
                vec![
                    3.0, 1.0,
                    4.0, 7.0,
                    1.0, 2.0,
                    ],
                )?;

            let quants = algo::quantiles(&raster, &[0.0, 0.25, 0.5, 0.75, 1.0])?.expect("Quantiles should have a value");
            assert_eq!(quants, vec![1.0, 1.25, 2.5, 3.75, 7.0]);
        }

        Ok(())
    }

    #[test]
    fn test_quantiles_neg_pos<R: Array<Pixel = u8, Metadata = GeoReference>>() -> Result<()> {
        let meta = GeoReference::with_origin(
            "",
            RasterSize::with_rows_cols(Rows(3), Columns(4)),
            Point::new(0.0, 0.0),
            CellSize::square(100.0),
            Some(NOD),
        );

        #[rustfmt::skip]
            let raster = R::WithPixelType::<f64>::new_process_nodata(
                meta,
                vec![
                    3.0, 1.0,
                    -1.0, -3.0,
                    -4.0, -7.0,
                    4.0, 7.0,
                    -1.0, 2.0,
                    1.0, -2.0,
                    ],
                )?;

        let (neg_quants, pos_quants) = algo::quantiles_neg_pos(&raster, &[0.0, 0.25, 0.5, 0.75, 1.0])?;
        let neg_quants = neg_quants.expect("Negative quantiles should have a value");
        let pos_quants = pos_quants.expect("Positive quantiles should have a value");
        assert_eq!(neg_quants, vec![-7.0, -3.75, -2.5, -1.25, -1.0]);
        assert_eq!(pos_quants, vec![1.0, 1.25, 2.5, 3.75, 7.0]);

        Ok(())
    }

    #[instantiate_tests(<DenseRaster<u8>>)]
    mod denseraster {}
}

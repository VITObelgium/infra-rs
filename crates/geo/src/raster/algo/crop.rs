use crate::{
    Array, ArrayMetadata, Nodata, RasterSize,
    array::{Columns, Rows},
};
use inf::allocate::{self};

pub fn crop<RasterType>(ras: RasterType) -> RasterType
where
    RasterType: Array,
{
    let rows = ras.rows().count();
    let cols = ras.columns().count();

    // Find the first row from top that contains data
    let top_row = (0..rows).find(|&row| ras.row_slice(row).iter().any(|val| !val.is_nodata()));
    let Some(top_row) = top_row else {
        // Not a single row contained data
        return RasterType::empty();
    };

    // Find the last row from bottom that contains data
    let bottom_row = (0..rows)
        .rev()
        .find(|&row| ras.row_slice(row).iter().any(|val| !val.is_nodata()))
        .unwrap_or(top_row);

    // Find the first column from left that contains data
    let left_col = (0..cols).find(|&col| ras.col_iter(col).any(|val| !val.is_nodata())).unwrap_or(0);

    // Find the last column from right that contains data
    let right_col = (0..cols)
        .rev()
        .find(|&col| ras.col_iter(col).any(|val| !val.is_nodata()))
        .unwrap_or(left_col);

    let new_rows = bottom_row - top_row + 1;
    let new_cols = right_col - left_col + 1;
    let new_size = RasterSize::with_rows_cols(Rows(new_rows), Columns(new_cols));

    if new_rows == rows && new_cols == cols {
        // No cropping needed
        return ras;
    }

    let cropped_meta = RasterType::Metadata::sized_with_nodata(new_size, ras.metadata().nodata());
    let mut cropped_data = allocate::aligned_vec_with_capacity((new_rows * new_cols) as usize);

    // TODO  optimize with a single copy per row
    for row in top_row..=bottom_row {
        for col in left_col..=right_col {
            let index = (row * cols + col) as usize;
            cropped_data.push(ras.as_slice()[index]);
        }
    }

    RasterType::new(cropped_meta, cropped_data).expect("Failed to create cropped raster")
}

#[cfg(test)]
#[generic_tests::define]
mod tests {
    use crate::testutils::{NOD, create_vec};
    use crate::{
        Array, DenseArray, RasterMetadata, RasterSize, Result,
        array::{Columns, Rows},
    };

    use super::*;

    #[test]
    fn crop_removes_nodata_edges<R: Array<Metadata = RasterMetadata>>() -> Result<()> {
        let size = RasterSize::with_rows_cols(Rows(5), Columns(6));
        #[rustfmt::skip]
        let raster = R::new(
            RasterMetadata::sized_with_nodata(size, Some(NOD)),
            create_vec(&[
                NOD, NOD, NOD, NOD, NOD, NOD,
                NOD, NOD, 1.0, 2.0, NOD, NOD,
                NOD, NOD, 3.0, 4.0, NOD, NOD,
                NOD, NOD, 5.0, 6.0, NOD, NOD,
                NOD, NOD, NOD, NOD, NOD, NOD,
            ]),
        )?;

        let expected_size = RasterSize::with_rows_cols(Rows(3), Columns(2));
        #[rustfmt::skip]
        let expected = R::new(
            RasterMetadata::sized_with_nodata(expected_size, Some(NOD)),
            create_vec(&[
                1.0, 2.0,
                3.0, 4.0,
                5.0, 6.0,
            ]),
        )?;

        let result = crop(raster);
        assert_eq!(result.size(), expected.size());
        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn crop_single_data_cell<R: Array<Metadata = RasterMetadata>>() -> Result<()> {
        let size = RasterSize::with_rows_cols(Rows(3), Columns(3));
        #[rustfmt::skip]
        let raster = R::new(
            RasterMetadata::sized_with_nodata(size, Some(NOD)),
            create_vec(&[
                NOD, NOD, NOD,
                NOD, 42.0, NOD,
                NOD, NOD, NOD,
            ]),
        )?;

        let expected_size = RasterSize::with_rows_cols(Rows(1), Columns(1));
        #[rustfmt::skip]
        let expected = R::new(
            RasterMetadata::sized_with_nodata(expected_size, Some(NOD)),
            create_vec(&[42.0]),
        )?;

        let result = crop(raster);
        assert_eq!(result.size(), expected.size());
        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn crop_all_nodata<R: Array<Metadata = RasterMetadata>>() -> Result<()> {
        let size = RasterSize::with_rows_cols(Rows(3), Columns(3));
        #[rustfmt::skip]
        let raster = R::new(
            RasterMetadata::sized_with_nodata(size, Some(NOD)),
            create_vec(&[
                NOD, NOD, NOD,
                NOD, NOD, NOD,
                NOD, NOD, NOD,
            ]),
        )?;

        let result = crop(raster);
        assert_eq!(result.size().rows.count(), 0);
        assert_eq!(result.size().cols.count(), 0);

        Ok(())
    }

    #[test]
    fn crop_no_cropping_needed<R: Array<Metadata = RasterMetadata>>() -> Result<()> {
        let size = RasterSize::with_rows_cols(Rows(2), Columns(2));
        #[rustfmt::skip]
        let raster = R::new(
            RasterMetadata::sized_with_nodata(size, Some(NOD)),
            create_vec(&[
                1.0, 2.0,
                3.0, 4.0,
            ]),
        )?;

        let result = crop(raster.clone());
        assert_eq!(result, raster);

        Ok(())
    }

    #[test]
    fn crop_partial_edges<R: Array<Metadata = RasterMetadata>>() -> Result<()> {
        let size = RasterSize::with_rows_cols(Rows(4), Columns(5));
        #[rustfmt::skip]
        let raster = R::new(
            RasterMetadata::sized_with_nodata(size, Some(NOD)),
            create_vec(&[
                NOD, NOD, NOD, NOD, NOD,
                NOD, 1.0, 2.0, 3.0, NOD,
                NOD, 4.0, NOD, 5.0, NOD,
                NOD, NOD, NOD, NOD, NOD,
            ]),
        )?;

        let expected_size = RasterSize::with_rows_cols(Rows(2), Columns(3));
        #[rustfmt::skip]
        let expected = R::new(
            RasterMetadata::sized_with_nodata(expected_size, Some(NOD)),
            create_vec(&[
                1.0, 2.0, 3.0,
                4.0, NOD, 5.0,
            ]),
        )?;

        let result = crop(raster);
        assert_eq!(result.size(), expected.size());
        assert_eq!(result, expected);

        Ok(())
    }

    #[test]
    fn crop_corners_only<R: Array<Metadata = RasterMetadata>>() -> Result<()> {
        let size = RasterSize::with_rows_cols(Rows(4), Columns(4));
        #[rustfmt::skip]
        let raster = R::new(
            RasterMetadata::sized_with_nodata(size, Some(NOD)),
            create_vec(&[
                1.0, NOD, NOD, 2.0,
                NOD, NOD, NOD, NOD,
                NOD, NOD, NOD, NOD,
                3.0, NOD, NOD, 4.0,
            ]),
        )?;

        let expected_size = RasterSize::with_rows_cols(Rows(4), Columns(4));
        #[rustfmt::skip]
        let expected = R::new(
            RasterMetadata::sized_with_nodata(expected_size, Some(NOD)),
            create_vec(&[
                1.0, NOD, NOD, 2.0,
                NOD, NOD, NOD, NOD,
                NOD, NOD, NOD, NOD,
                3.0, NOD, NOD, 4.0,
            ]),
        )?;

        let result = crop(raster);
        assert_eq!(result.size(), expected.size());
        assert_eq!(result, expected);

        Ok(())
    }

    #[instantiate_tests(<DenseArray<i8>>)]
    mod denserasteri8 {}

    #[instantiate_tests(<DenseArray<u8>>)]
    mod denserasteru8 {}

    #[instantiate_tests(<DenseArray<i32>>)]
    mod denserasteri32 {}

    #[instantiate_tests(<DenseArray<u32>>)]
    mod denserasteru32 {}

    #[instantiate_tests(<DenseArray<i64>>)]
    mod denserasteri64 {}

    #[instantiate_tests(<DenseArray<u64>>)]
    mod denserasteru64 {}

    #[instantiate_tests(<DenseArray<f32>>)]
    mod denserasterf32 {}

    #[instantiate_tests(<DenseArray<f64>>)]
    mod denserasterf64 {}
}

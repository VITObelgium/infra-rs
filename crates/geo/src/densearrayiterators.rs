use crate::{Array as _, ArrayMetadata, ArrayNum, Cell, DenseArray, RasterSize, RasterWindow};

/// Iterator over the values of a dense raster array.
/// All the values will be visited, nodata values will be returned as `None`.
pub struct DenserRasterIterator<'a, T: ArrayNum, Metadata: ArrayMetadata> {
    index: usize,
    raster: &'a DenseArray<T, Metadata>,
}

impl<'a, T: ArrayNum, Metadata: ArrayMetadata> DenserRasterIterator<'a, T, Metadata> {
    pub fn new(raster: &'a DenseArray<T, Metadata>) -> Self {
        DenserRasterIterator { index: 0, raster }
    }
}

impl<T, Metadata> Iterator for DenserRasterIterator<'_, T, Metadata>
where
    T: ArrayNum,
    Metadata: ArrayMetadata,
{
    type Item = Option<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.raster.len() {
            let result = self.raster.value(self.index);
            self.index += 1;
            Some(result)
        } else {
            None
        }
    }
}

/// Iterator over the values of a dense raster array.
/// Only the cells that contain valid data will be visited.
/// Nodata values will be skipped.
pub struct DenserRasterValueIterator<'a, T: ArrayNum, Metadata: ArrayMetadata> {
    index: usize,
    raster: &'a DenseArray<T, Metadata>,
}

impl<'a, T: ArrayNum, Metadata: ArrayMetadata> DenserRasterValueIterator<'a, T, Metadata> {
    pub fn new(raster: &'a DenseArray<T, Metadata>) -> Self {
        DenserRasterValueIterator { index: 0, raster }
    }

    fn next_value(&mut self) -> Option<T> {
        let index = self.index;
        if index < self.raster.len() {
            self.index += 1;
            self.raster.value(index)
        } else {
            None
        }
    }
}

impl<T, Metadata> Iterator for DenserRasterValueIterator<'_, T, Metadata>
where
    T: ArrayNum,
    Metadata: ArrayMetadata,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let val = self.next_value();
            if val.is_some() {
                return val;
            }

            if self.index >= self.raster.len() {
                return None;
            }
        }
    }
}

/// Visits every window cell in row-major order, yielding nodata outside the raster.
pub struct DenserRasterWindowIterator<'a, T: ArrayNum> {
    cell: Option<Cell>,
    raster_data: &'a [T],
    raster_size: RasterSize,
    window: RasterWindow,
}

impl<'a, T: ArrayNum> DenserRasterWindowIterator<'a, T> {
    pub fn new<M: ArrayMetadata>(raster: &'a DenseArray<T, M>, window: RasterWindow) -> Self {
        Self::from_buffer(raster.as_slice(), raster.size(), window)
    }

    /// Cells outside the raster dimensions or missing from the buffer yield nodata.
    /// Trailing buffer elements beyond the raster dimensions are ignored.
    pub fn from_buffer(buffer: &'a [T], raster_size: RasterSize, window: RasterWindow) -> Self {
        let cell = if window.is_empty() { None } else { Some(window.top_left()) };
        DenserRasterWindowIterator {
            cell,
            raster_data: buffer,
            raster_size,
            window,
        }
    }

    fn increment_index(&mut self) {
        let Some(mut cell) = self.cell else {
            return;
        };
        let bottom_right = self.window.bottom_right();
        if cell.col < bottom_right.col {
            cell.col += 1;
        } else if cell.row < bottom_right.row {
            cell.row += 1;
            cell.col = self.window.top_left().col;
        } else {
            self.cell = None;
            return;
        }
        self.cell = Some(cell);
    }
}

impl<T> Iterator for DenserRasterWindowIterator<'_, T>
where
    T: ArrayNum,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let cell = self.cell?;
        let result = if cell.is_valid() && cell.row < self.raster_size.rows.count() && cell.col < self.raster_size.cols.count() {
            (cell.row as usize)
                .checked_mul(self.raster_size.cols.count() as usize)
                .and_then(|offset| offset.checked_add(cell.col as usize))
                .and_then(|index| self.raster_data.get(index))
                .copied()
                .unwrap_or(T::NODATA)
        } else {
            T::NODATA
        };
        self.increment_index();
        Some(result)
    }
}

/// Visits a fully contained window through disjoint mutable row slices.
pub struct DenserRasterWindowIteratorMut<'a, T: ArrayNum> {
    rows: std::slice::ChunksExactMut<'a, T>,
    columns: std::ops::Range<usize>,
    current_row: std::slice::IterMut<'a, T>,
}

impl<'a, T: ArrayNum> DenserRasterWindowIteratorMut<'a, T> {
    pub fn new<M: ArrayMetadata>(raster: &'a mut DenseArray<T, M>, window: RasterWindow) -> Self {
        let raster_size = raster.size();
        Self::from_buffer(raster.as_mut_slice(), raster_size, window)
    }

    /// # Panics
    /// Panics if the raster dimensions are invalid, the buffer length does not match
    /// them, or a nonempty window is not fully contained in the raster.
    pub fn from_buffer(buffer: &'a mut [T], raster_size: RasterSize, window: RasterWindow) -> Self {
        assert!(raster_size.rows.count() >= 0, "Invalid raster row count");
        assert!(raster_size.cols.count() >= 0, "Invalid raster column count");
        let cols = raster_size.cols.count() as usize;
        let len = raster_size.cell_count();
        assert_eq!(buffer.len(), len, "Buffer length does not match raster dimensions");

        let (row_range, col_range) = if window.is_empty() {
            (0..0, 0..0)
        } else {
            let top_left = window.top_left();
            let bottom_right = window.bottom_right();
            assert!(
                top_left.is_valid() && bottom_right.row < raster_size.rows.count() && bottom_right.col < raster_size.cols.count(),
                "Window is not contained in raster"
            );
            (
                top_left.row as usize * cols..(bottom_right.row as usize + 1) * cols,
                top_left.col as usize..bottom_right.col as usize + 1,
            )
        };

        Self {
            rows: buffer[row_range].chunks_exact_mut(cols.max(1)),
            columns: col_range,
            current_row: [].iter_mut(),
        }
    }
}

impl<'a, T: ArrayNum> Iterator for DenserRasterWindowIteratorMut<'a, T> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.current_row.next() {
                return Some(item);
            }

            let row = self.rows.next()?;
            self.current_row = row[self.columns.clone()].iter_mut();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Columns, Nodata, Rows};

    fn size(rows: i32, cols: i32) -> RasterSize {
        RasterSize::with_rows_cols(Rows(rows), Columns(cols))
    }

    #[test]
    fn window_yields_nodata_outside_all_raster_edges() {
        let buffer = [1_i32, 2, 3, 4];
        let window = RasterWindow::new(Cell::from_row_col(-1, -1), size(4, 4));
        let values: Vec<_> = DenserRasterWindowIterator::from_buffer(&buffer, size(2, 2), window).collect();
        let nd = i32::NODATA;
        assert_eq!(values, [nd, nd, nd, nd, nd, 1, 2, nd, nd, 3, 4, nd, nd, nd, nd, nd]);
    }

    #[test]
    fn window_outside_raster_still_visits_every_cell() {
        let buffer = [1_i32, 2, 3, 4];
        for top_left in [Cell::from_row_col(-3, -3), Cell::from_row_col(3, 3)] {
            let window = RasterWindow::new(top_left, size(2, 2));
            let values: Vec<_> = DenserRasterWindowIterator::from_buffer(&buffer, size(2, 2), window).collect();
            assert_eq!(values, [i32::NODATA; 4]);
        }
    }

    #[test]
    fn window_ignores_trailing_buffer_storage() {
        let buffer = [1_i32, 2, 3, 4, 5, 6];
        let window = RasterWindow::new(Cell::from_row_col(0, 0), size(3, 2));
        let values: Vec<_> = DenserRasterWindowIterator::from_buffer(&buffer, size(2, 2), window).collect();
        assert_eq!(values, [1, 2, 3, 4, i32::NODATA, i32::NODATA]);
    }

    #[test]
    fn window_yields_nodata_for_missing_buffer_elements() {
        let buffer = [1_i32, 2, 3];
        let window = RasterWindow::new(Cell::from_row_col(0, 0), size(2, 2));
        let values: Vec<_> = DenserRasterWindowIterator::from_buffer(&buffer, size(2, 2), window).collect();
        assert_eq!(values, [1, 2, 3, i32::NODATA]);
    }

    #[test]
    fn window_over_empty_raster_yields_nodata() {
        let window = RasterWindow::new(Cell::from_row_col(-1, -1), size(2, 2));
        let values: Vec<i32> = DenserRasterWindowIterator::from_buffer(&[], size(0, 0), window).collect();
        assert_eq!(values, [i32::NODATA; 4]);
    }

    #[test]
    fn empty_window_yields_no_cells() {
        let buffer = [1_i32, 2, 3, 4];
        let window = RasterWindow::new(Cell::from_row_col(-1, -1), size(0, 2));
        let mut iter = DenserRasterWindowIterator::from_buffer(&buffer, size(2, 2), window);
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    #[test]
    fn mutable_window_references_can_be_retained() {
        let mut buffer = [0_i32, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];
        let window = RasterWindow::new(Cell::from_row_col(1, 1), size(2, 2));
        let mut cells: Vec<_> = DenserRasterWindowIteratorMut::from_buffer(&mut buffer, size(3, 4), window).collect();

        assert_eq!(cells.iter().map(|cell| **cell).collect::<Vec<_>>(), [5, 6, 9, 10]);
        for (index, cell) in cells.iter_mut().enumerate() {
            **cell = 100 + index as i32;
        }
        drop(cells);
        assert_eq!(buffer, [0, 1, 2, 3, 4, 100, 101, 7, 8, 102, 103, 11]);
    }

    #[test]
    fn mutable_window_visits_full_raster_once() {
        let mut buffer = [0_i32; 6];
        let window = RasterWindow::new(Cell::from_row_col(0, 0), size(3, 2));
        let mut iter = DenserRasterWindowIteratorMut::from_buffer(&mut buffer, size(3, 2), window);
        for cell in iter.by_ref() {
            *cell += 1;
        }
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
        assert_eq!(buffer, [1; 6]);
    }

    #[test]
    fn mutable_window_handles_empty_windows_and_rasters() {
        for window_size in [size(0, 2), size(2, 0)] {
            let mut buffer = [0_i32; 6];
            let window = RasterWindow::new(Cell::from_row_col(0, 0), window_size);
            assert!(
                DenserRasterWindowIteratorMut::from_buffer(&mut buffer, size(3, 2), window)
                    .next()
                    .is_none()
            );
        }

        let mut buffer: [i32; 0] = [];
        let window = RasterWindow::new(Cell::from_row_col(0, 0), size(0, 0));
        assert!(
            DenserRasterWindowIteratorMut::from_buffer(&mut buffer, size(0, 0), window)
                .next()
                .is_none()
        );
    }

    #[test]
    #[should_panic(expected = "Window is not contained in raster")]
    fn mutable_window_rejects_overlapping_rows() {
        let mut buffer = [0_i32; 6];
        let window = RasterWindow::new(Cell::from_row_col(0, 0), size(2, 3));
        let _ = DenserRasterWindowIteratorMut::from_buffer(&mut buffer, size(3, 2), window);
    }

    #[test]
    fn mutable_window_rejects_out_of_bounds_coordinates() {
        for (top_left, window_size) in [
            (Cell::from_row_col(-1, 0), size(2, 1)),
            (Cell::from_row_col(0, -1), size(1, 2)),
            (Cell::from_row_col(3, 0), size(1, 1)),
            (Cell::from_row_col(0, 2), size(1, 1)),
            (Cell::from_row_col(2, 0), size(2, 1)),
        ] {
            let result = std::panic::catch_unwind(|| {
                let mut buffer = [0_i32; 6];
                let window = RasterWindow::new(top_left, window_size);
                let _ = DenserRasterWindowIteratorMut::from_buffer(&mut buffer, size(3, 2), window);
            });
            assert!(result.is_err(), "Accepted invalid window at {top_left:?}");
        }
    }

    #[test]
    fn mutable_window_rejects_mismatched_buffers() {
        for len in [5, 7] {
            let result = std::panic::catch_unwind(|| {
                let mut buffer = vec![0_i32; len];
                let window = RasterWindow::new(Cell::from_row_col(0, 0), size(1, 1));
                let _ = DenserRasterWindowIteratorMut::from_buffer(&mut buffer, size(3, 2), window);
            });
            assert!(result.is_err());
        }
    }

    #[test]
    fn mutable_window_rejects_negative_raster_dimensions() {
        for raster_size in [size(-1, 2), size(3, -1)] {
            let result = std::panic::catch_unwind(|| {
                let mut buffer: [i32; 0] = [];
                let window = RasterWindow::new(Cell::from_row_col(0, 0), size(0, 0));
                let _ = DenserRasterWindowIteratorMut::from_buffer(&mut buffer, raster_size, window);
            });
            assert!(result.is_err());
        }
    }
}

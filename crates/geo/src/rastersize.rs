use crate::array::{Columns, Rows};

/// Raster size represented by rows and columns.
#[derive(Clone, Copy, PartialEq, Eq, Default)]
pub struct RasterSize {
    pub rows: Rows,
    pub cols: Columns,
}

impl RasterSize {
    pub const fn with_rows_cols(rows: Rows, cols: Columns) -> Self {
        RasterSize { rows, cols }
    }

    pub const fn square(size: i32) -> Self {
        RasterSize {
            rows: Rows(size),
            cols: Columns(size),
        }
    }

    pub fn empty() -> Self {
        Self::with_rows_cols(Rows(0), Columns(0))
    }

    pub fn is_empty(&self) -> bool {
        self.rows.count() == 0 || self.cols.count() == 0
    }

    pub fn cell_count(&self) -> usize {
        self.rows * self.cols
    }

    pub fn max_dimension(&self) -> i32 {
        self.rows.count().max(self.cols.count())
    }
}

impl std::fmt::Display for RasterSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(rows: {}, cols: {})", self.rows, self.cols)
    }
}

impl std::fmt::Debug for RasterSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

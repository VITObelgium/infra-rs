use crate::ArrayMetadata;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct RasterSize {
    pub rows: usize,
    pub cols: usize,
}

impl RasterSize {
    pub const fn with_rows_cols(rows: usize, cols: usize) -> Self {
        RasterSize { rows, cols }
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0 || self.cols == 0
    }

    pub fn cell_count(&self) -> usize {
        self.rows * self.cols
    }
}

impl ArrayMetadata for RasterSize {
    fn size(&self) -> RasterSize {
        *self
    }

    fn with_size(size: RasterSize) -> Self {
        size
    }

    fn with_rows_cols(rows: usize, cols: usize) -> Self {
        RasterSize::with_rows_cols(rows, cols)
    }
}

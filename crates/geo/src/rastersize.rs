use crate::{
    ArrayMetadata, GeoReference,
    array::{Columns, Rows},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct RasterSize {
    pub rows: Rows,
    pub cols: Columns,
}

impl RasterSize {
    pub const fn with_rows_cols(rows: Rows, cols: Columns) -> Self {
        RasterSize { rows, cols }
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
}

impl std::fmt::Display for RasterSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(rows: {}, cols: {})", self.rows, self.cols)
    }
}

impl ArrayMetadata for RasterSize {
    fn size(&self) -> RasterSize {
        *self
    }

    fn nodata(&self) -> Option<f64> {
        None
    }

    fn with_size(size: RasterSize) -> Self {
        size
    }

    fn with_rows_cols(rows: Rows, cols: Columns) -> Self {
        RasterSize::with_rows_cols(rows, cols)
    }

    fn with_geo_reference(georef: GeoReference) -> Self {
        georef.size()
    }

    fn geo_reference(&self) -> GeoReference {
        GeoReference::without_spatial_reference(*self, None)
    }
}

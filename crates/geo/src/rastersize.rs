use crate::{
    array::{Columns, Rows},
    ArrayMetadata, GeoReference,
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

    pub fn is_empty(&self) -> bool {
        self.rows.count() == 0 || self.cols.count() == 0
    }

    pub fn cell_count(&self) -> usize {
        self.rows * self.cols
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

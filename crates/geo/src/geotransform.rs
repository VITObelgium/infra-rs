use std::fmt::Debug;

use approx::{AbsDiffEq, RelativeEq};

use crate::{Cell, CellSize, Error, Point, Result};

#[derive(Clone, Copy, PartialEq, Default)]
pub struct GeoTransform([f64; 6]);

impl GeoTransform {
    /// Creates a new `GeoTransform` from the provided coefficients.
    ///
    /// The coefficients are in the order: [top left x, pixel width, rotation (0 if north is up), top left y, rotation (0 if north is up), pixel height].
    pub const fn new(coefficients: [f64; 6]) -> Self {
        GeoTransform(coefficients)
    }

    pub fn from_top_left_and_cell_size(top_left: Point, cell_size: CellSize) -> Self {
        Self::new([top_left.x(), cell_size.x(), 0.0, top_left.y(), 0.0, cell_size.y()])
    }

    pub fn apply_to_cell(&self, cell: Cell) -> Point<f64> {
        self.apply(cell.col as f64, cell.row as f64)
    }

    /// Translates a cell to a point in the raster.
    /// Cell (0, 0) is the top left corner of the raster.
    pub fn apply(&self, col: f64, row: f64) -> Point<f64> {
        let x = self.0[0] + self.0[1] * col + self.0[2] * row;
        let y = self.0[3] + self.0[4] * col + self.0[5] * row;
        Point::new(x, y)
    }

    pub fn top_left(&self) -> Point {
        Point::new(self.0[0], self.0[3])
    }

    /// The horizontal cell size
    pub fn cell_size_x(&self) -> f64 {
        self.0[1]
    }

    pub fn set_cell_size_x(&mut self, size: f64) {
        self.0[1] = size;
    }

    /// The verical cell size
    pub fn cell_size_y(&self) -> f64 {
        self.0[5]
    }

    pub fn set_cell_size_y(&mut self, size: f64) {
        self.0[5] = size;
    }

    /// Returns the coefficients of the transformation.
    pub fn coefficients(&self) -> [f64; 6] {
        self.0
    }

    pub fn invert(&self) -> Result<Self> {
        let gt_in = &self.0;
        let mut gt_out = [0.0; 6];

        if gt_in[2] == 0.0 && gt_in[4] == 0.0 && gt_in[1] != 0.0 && gt_in[5] != 0.0 {
            // Special case: no rotation, to avoid computing determinate and potential precision issues.
            // X = gt_in[0] + x * gt_in[1]
            // Y = gt_in[3] + y * gt_in[5]
            // -->
            // x = -gt_in[0] / gt_in[1] + (1 / gt_in[1]) * X
            // y = -gt_in[3] / gt_in[5] + (1 / gt_in[5]) * Y

            gt_out[0] = -gt_in[0] / gt_in[1];
            gt_out[1] = 1.0 / gt_in[1];
            gt_out[2] = 0.0;
            gt_out[3] = -gt_in[3] / gt_in[5];
            gt_out[4] = 0.0;
            gt_out[5] = 1.0 / gt_in[5];
        }

        // Assume a 3rd row that is [1 0 0].
        // Compute determinate.

        let det = gt_in[1] * gt_in[5] - gt_in[2] * gt_in[4];
        let magnitude = f64::max(f64::max(gt_in[1].abs(), gt_in[2].abs()), f64::max(gt_in[4].abs(), gt_in[5].abs()));

        if det.abs() <= 1e-10 * magnitude * magnitude {
            return Err(Error::Runtime(
                "GeoTransform::inverse: Determinate is too small, cannot compute inverse.".to_string(),
            ));
        }

        let inv_det = 1.0 / det;

        // Compute adjoint, and divide by determinate
        gt_out[1] = gt_in[5] * inv_det;
        gt_out[4] = -gt_in[4] * inv_det;

        gt_out[2] = -gt_in[2] * inv_det;
        gt_out[5] = gt_in[1] * inv_det;

        gt_out[0] = (gt_in[2] * gt_in[3] - gt_in[0] * gt_in[5]) * inv_det;
        gt_out[3] = (-gt_in[1] * gt_in[3] + gt_in[0] * gt_in[4]) * inv_det;

        Ok(gt_out.into())
    }
}

impl From<[f64; 6]> for GeoTransform {
    fn from(coefficients: [f64; 6]) -> Self {
        GeoTransform(coefficients)
    }
}

impl From<GeoTransform> for [f64; 6] {
    fn from(geo_trans: GeoTransform) -> [f64; 6] {
        geo_trans.0
    }
}

impl Debug for GeoTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GeoTransform(topleft: ({}, {}), pixel_width: {}, pixel_height: {})",
            self.0[0],
            self.0[3],
            self.cell_size_x(),
            self.cell_size_y()
        )
    }
}

impl AbsDiffEq for GeoTransform {
    type Epsilon = f64;

    fn default_epsilon() -> Self::Epsilon {
        f64::default_epsilon()
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        self.0.abs_diff_eq(&other.0, epsilon)
    }
}

impl RelativeEq for GeoTransform {
    fn default_max_relative() -> Self::Epsilon {
        f64::default_max_relative()
    }

    fn relative_eq(&self, other: &Self, epsilon: Self::Epsilon, max_relative: Self::Epsilon) -> bool {
        self.0.relative_eq(&other.0, epsilon, max_relative)
    }
}

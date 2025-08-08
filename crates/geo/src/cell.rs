use crate::{RasterSize, array::Columns, array::Rows};

/// Represents a point in the raster using row, col coordinates
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Cell {
    pub row: i32,
    pub col: i32,
}

impl Cell {
    pub const fn from_row_col(row: i32, col: i32) -> Self {
        Cell { row, col }
    }

    pub const fn invalid() -> Self {
        Cell { row: -1, col: -1 }
    }

    pub const fn is_valid(&self) -> bool {
        self.row >= 0 && self.col >= 0
    }

    pub fn left(&self) -> Cell {
        Cell::from_row_col(self.row, self.col - 1)
    }

    pub fn right(&self) -> Cell {
        Cell::from_row_col(self.row, self.col + 1)
    }

    pub fn above(&self) -> Cell {
        Cell::from_row_col(self.row - 1, self.col)
    }

    pub fn below(&self) -> Cell {
        Cell::from_row_col(self.row + 1, self.col)
    }

    pub fn above_left(&self) -> Cell {
        Cell::from_row_col(self.row - 1, self.col - 1)
    }

    pub fn above_right(&self) -> Cell {
        Cell::from_row_col(self.row - 1, self.col + 1)
    }

    pub fn below_left(&self) -> Cell {
        Cell::from_row_col(self.row + 1, self.col - 1)
    }

    pub fn below_right(&self) -> Cell {
        Cell::from_row_col(self.row + 1, self.col + 1)
    }

    pub fn increment(&mut self, cols_in_grid: i32) {
        self.col += 1;
        if self.col >= cols_in_grid {
            self.col = 0;
            self.row += 1;
        }
    }

    pub fn index_in_raster(&mut self, cols_in_grid: i32) -> usize {
        assert!(self.is_valid());
        if !self.is_valid() {
            return 0;
        }

        (self.row * cols_in_grid + self.col) as usize
    }

    pub fn distance(&self, other: &Cell) -> f64 {
        let x = other.col - self.col;
        let y = other.row - self.row;

        ((x * x + y * y) as f64).sqrt()
    }
}

impl PartialOrd for Cell {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Cell {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.row.cmp(&other.row).then(self.col.cmp(&other.col))
    }
}

/// Iterator over the cells in a raster
/// The iterator will yield each cell in the raster based on the specified number of rows and columns.
/// Iteration will occur from the top-left cell to the bottom-right cell in row-major order.
pub struct CellIterator {
    rows: Rows,
    cols: Columns,
    current: Cell,
}

impl CellIterator {
    pub fn for_rows_cols(rows: Rows, cols: Columns) -> Self {
        CellIterator {
            rows,
            cols,
            current: Cell::from_row_col(0, 0),
        }
    }

    pub fn for_raster_with_size(size: RasterSize) -> Self {
        CellIterator {
            rows: size.rows,
            cols: size.cols,
            current: Cell::from_row_col(0, 0),
        }
    }

    pub fn for_single_row_from_raster_with_size(size: RasterSize, row: i32) -> Self {
        assert!(row >= 0 && row < size.rows.count(), "Row index out of bounds");
        CellIterator {
            rows: Rows(row + 1),
            cols: size.cols,
            current: Cell::from_row_col(row, 0),
        }
    }
}

impl Iterator for CellIterator {
    type Item = Cell;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.row >= self.rows.count() {
            return None;
        }

        let current = self.current;
        self.current.increment(self.cols.count());
        Some(current)
    }
}

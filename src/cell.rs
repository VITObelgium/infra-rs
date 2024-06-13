/// Represents a point in the raster using row, col coordinates
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cell {
    pub row: i32,
    pub col: i32,
}

impl Cell {
    pub const fn new(row: i32, col: i32) -> Self {
        Cell { row, col }
    }

    pub const fn is_valid(&self) -> bool {
        self.row >= 0 && self.col >= 0
    }

    pub fn left(&self) -> Cell {
        Cell::new(self.row, self.col - 1)
    }

    pub fn right(&self) -> Cell {
        Cell::new(self.row, self.col + 1)
    }

    pub fn above(&self) -> Cell {
        Cell::new(self.row - 1, self.col)
    }

    pub fn below(&self) -> Cell {
        Cell::new(self.row + 1, self.col)
    }

    pub fn above_left(&self) -> Cell {
        Cell::new(self.row - 1, self.col - 1)
    }

    pub fn above_right(&self) -> Cell {
        Cell::new(self.row - 1, self.col + 1)
    }

    pub fn below_left(&self) -> Cell {
        Cell::new(self.row + 1, self.col - 1)
    }

    pub fn below_right(&self) -> Cell {
        Cell::new(self.row + 1, self.col + 1)
    }

    pub fn increment(&mut self, cols_in_grid: i32) {
        self.col += 1;
        if self.col >= cols_in_grid {
            self.col = 0;
            self.row += 1;
        }
    }

    pub fn distance(&self, other: &Cell) -> f64 {
        let x = other.col - self.col;
        let y = other.row - self.row;

        ((x * x + y * y) as f64).sqrt()
    }
}

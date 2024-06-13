/// Represents a point in the raster using r,c coordinates
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
}

pub fn left_cell(cell: &Cell) -> Cell {
    Cell::new(cell.row, cell.col - 1)
}

pub fn right_cell(cell: &Cell) -> Cell {
    Cell::new(cell.row, cell.col + 1)
}

pub fn top_cell(cell: &Cell) -> Cell {
    Cell::new(cell.row - 1, cell.col)
}

pub fn bottom_cell(cell: &Cell) -> Cell {
    Cell::new(cell.row + 1, cell.col)
}

pub fn top_left_cell(cell: &Cell) -> Cell {
    Cell::new(cell.row - 1, cell.col - 1)
}

pub fn top_right_cell(cell: &Cell) -> Cell {
    Cell::new(cell.row - 1, cell.col + 1)
}

pub fn bottom_left_cell(cell: &Cell) -> Cell {
    Cell::new(cell.row + 1, cell.col - 1)
}

pub fn bottom_right_cell(cell: &Cell) -> Cell {
    Cell::new(cell.row + 1, cell.col + 1)
}

pub fn increment_cell(cell: &mut Cell, cols: i32) {
    cell.col += 1;
    if cell.col >= cols {
        cell.col = 0;
        cell.row += 1;
    }
}

pub fn distance(lhs: &Cell, rhs: &Cell) -> f64 {
    let x = rhs.col - lhs.col;
    let y = rhs.row - lhs.row;

    ((x * x + y * y) as f64).sqrt()
}

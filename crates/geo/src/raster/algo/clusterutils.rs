use crate::{
    array::{Columns, Rows},
    Array, ArrayNum, Cell, DenseArray,
};

pub const MARK_TODO: u8 = 0;
pub const MARK_BORDER: u8 = 1;
pub const MARK_DONE: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterDiagonals {
    Include,
    Exclude,
}

pub fn handle_time_cell<T>(
    delta_d: f32,
    cell: Cell,
    new_cell: Cell,
    distance_to_target: &mut impl Array<Pixel = f32>,
    mark: &mut impl Array<Pixel = u8>,
    travel_time: &impl Array<Pixel = T>,
    border: &mut FiLo<Cell>,
) where
    T: ArrayNum<T>,
{
    if distance_to_target.cell_is_nodata(cell) || distance_to_target.cell_is_nodata(new_cell) {
        return;
    }

    let alternative_dist = distance_to_target[cell] + delta_d * travel_time[new_cell].to_f32().expect("Invalid travel time");
    let d = &mut distance_to_target[new_cell];
    if *d > alternative_dist {
        *d = alternative_dist;
        let m = &mut mark[new_cell];
        if *m != MARK_BORDER {
            *m = MARK_BORDER;
            border.push_back(new_cell);
        }
    }
}

pub fn visit_neighbour_cells<F>(cell: Cell, rows: Rows, cols: Columns, mut callable: F)
where
    F: FnMut(Cell),
{
    let is_left_border = cell.col == 0;
    let is_right_border = cell.col == cols.count() - 1;

    let is_top_border = cell.row == 0;
    let is_bottom_border = cell.row == rows.count() - 1;

    if !is_right_border {
        callable(cell.right());
    }

    if !is_left_border {
        callable(cell.left());
    }

    if !is_bottom_border {
        callable(cell.below());
    }

    if !is_top_border {
        callable(cell.above());
    }
}

pub fn visit_neighbour_diag_cells<F>(cell: Cell, rows: Rows, cols: Columns, mut callable: F)
where
    F: FnMut(Cell),
{
    let is_left_border = cell.col == 0;
    let is_right_border = cell.col == cols.count() - 1;

    let is_top_border = cell.row == 0;
    let is_bottom_border = cell.row == rows.count() - 1;

    if !(is_bottom_border || is_right_border) {
        callable(cell.below_right());
    }

    if !(is_top_border || is_right_border) {
        callable(cell.above_right());
    }

    if !(is_bottom_border || is_left_border) {
        callable(cell.below_left());
    }

    if !(is_top_border || is_left_border) {
        callable(cell.above_left());
    }
}

pub fn show_warning_if_clustering_on_floats<T: ArrayNum<T>>() {
    if T::has_nan() {
        log::warn!("Performing cluster operation on floating point raster");
    }
}

pub struct FiLo<T> {
    head: usize,
    tail: usize,
    filo: Vec<T>,
}

impl<T: Default + Copy> FiLo<T> {
    pub fn new(n_rows: Rows, n_cols: Columns) -> Self {
        Self {
            head: 0,
            tail: 0,
            filo: vec![T::default(); n_rows * n_cols + 1],
        }
    }

    pub fn clear(&mut self) {
        self.head = 0;
        self.tail = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.tail == self.head
    }

    pub fn push_back(&mut self, value: T) {
        self.filo[self.tail] = value;
        self.tail = (self.tail + 1) % self.filo.len();

        if self.tail == self.head {
            panic!("FiLo overflow");
        }
    }

    pub fn pop_head(&mut self) -> T {
        if self.is_empty() {
            panic!("FiLo underflow");
        }

        let ret = unsafe {
            // SAFETY: `self.head` is always in bounds
            *self.filo.get_unchecked(self.head)
        };
        self.head = (self.head + 1) % self.filo.len();
        ret
    }
}

pub fn insert_cell(cell: Cell, cluster_cells: &mut Vec<Cell>, mark: &mut DenseArray<u8>, border: &mut FiLo<Cell>) {
    mark[cell] = MARK_BORDER;
    border.push_back(cell);
    cluster_cells.push(cell);
}

pub fn insert_border_cell(cell: Cell, mark: &mut impl Array<Pixel = u8>, border: &mut FiLo<Cell>) {
    mark[cell] = MARK_BORDER;
    border.push_back(cell);
}

pub fn handle_cell<T: ArrayNum<T>>(
    cell: Cell,
    cluster_value: T,
    cluster_cells: &mut Vec<Cell>,
    mark: &mut DenseArray<u8>,
    border: &mut FiLo<Cell>,
    raster: &impl Array<Pixel = T>,
) {
    if raster.cell_is_nodata(cell) {
        return;
    }

    if raster[cell] == cluster_value && mark[cell] == MARK_TODO {
        insert_cell(cell, cluster_cells, mark, border);
    }
}

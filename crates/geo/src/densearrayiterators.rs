use crate::{Array as _, ArrayMetadata, ArrayNum, Cell, DenseArray, RasterWindow};

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

pub struct DenserRasterWindowIterator<'a, T: ArrayNum, Metadata: ArrayMetadata> {
    cell: Cell,
    raster: &'a DenseArray<T, Metadata>,
    window: RasterWindow,
}

impl<'a, T: ArrayNum, Metadata: ArrayMetadata> DenserRasterWindowIterator<'a, T, Metadata> {
    pub fn new(raster: &'a DenseArray<T, Metadata>, window: RasterWindow) -> Self {
        let cell = window.top_left();
        DenserRasterWindowIterator { cell, raster, window }
    }

    fn increment_index(&mut self) {
        let right_edge = self.window.bottom_right().col as usize;
        let mut cell = self.cell;
        cell.col += 1;
        if cell.col as usize > right_edge {
            cell.row += 1;
            cell.col = self.window.top_left().col;
        }

        if cell.row > self.window.bottom_right().row {
            self.cell = Cell::invalid(); // No more cells to iterate
        } else {
            self.cell = cell;
        }
    }
}

impl<T, Metadata> Iterator for DenserRasterWindowIterator<'_, T, Metadata>
where
    T: ArrayNum,
    Metadata: ArrayMetadata,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cell.is_valid() {
            let result = self.raster[self.cell];
            self.increment_index();
            Some(result)
        } else {
            None
        }
    }
}

pub struct DenserRasterWindowIteratorMut<'a, T: ArrayNum, Metadata: ArrayMetadata> {
    cell: Cell,
    raster: &'a mut DenseArray<T, Metadata>,
    window: RasterWindow,
}

impl<'a, T: ArrayNum, Metadata: ArrayMetadata> DenserRasterWindowIteratorMut<'a, T, Metadata> {
    pub fn new(raster: &'a mut DenseArray<T, Metadata>, window: RasterWindow) -> Self {
        let cell = window.top_left();
        DenserRasterWindowIteratorMut { cell, raster, window }
    }

    fn increment_index(&mut self) {
        let right_edge = self.window.bottom_right().col as usize;
        let mut cell = self.cell;
        cell.col += 1;
        if cell.col as usize > right_edge {
            cell.row += 1;
            cell.col = self.window.top_left().col;
        }

        if cell.row > self.window.bottom_right().row {
            self.cell = Cell::invalid(); // No more cells to iterate
        } else {
            self.cell = cell;
        }
    }
}

impl<'a, T: ArrayNum, Metadata: ArrayMetadata> Iterator for DenserRasterWindowIteratorMut<'a, T, Metadata> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cell.is_valid() {
            // SAFETY: Only one mutable reference per cell is ever handed out by this iterator.
            let index = self.cell.index_in_raster(self.raster.columns().count());
            let len = self.raster.data.len();
            if index >= len {
                self.cell = Cell::invalid();
                return None;
            }

            // Use split_at_mut to get a unique mutable reference
            let ptr = self.raster.data.as_mut_ptr();
            let item = unsafe { &mut *ptr.add(index) };
            self.increment_index();
            Some(item)
        } else {
            None
        }
    }
}

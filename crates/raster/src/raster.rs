use crate::{Cell, RasterNum};

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

/// A trait representing a raster.
/// A raster implementation provides access to the pixel data and the geographic metadata associated with the raster.
pub trait Raster<T: RasterNum<T>>:
    PartialEq
    + Clone
    + std::ops::Add<T, Output = Self>
    + std::ops::Sub<T, Output = Self>
    + std::ops::Mul<T, Output = Self>
    + std::ops::Div<T, Output = Self>
    + std::ops::Add<Self, Output = Self>
    + std::ops::Sub<Self, Output = Self>
    + std::ops::Mul<Self, Output = Self>
    + std::ops::Div<Self, Output = Self>
    + std::ops::AddAssign<T>
    + std::ops::SubAssign<T>
    + std::ops::MulAssign<T>
    + std::ops::DivAssign<T>
    + std::ops::AddAssign<Self>
    + std::ops::SubAssign<Self>
    + std::ops::MulAssign<Self>
    + std::ops::DivAssign<Self>
    + for<'a> std::ops::AddAssign<&'a Self>
    + for<'a> std::ops::SubAssign<&'a Self>
    + for<'a> std::ops::MulAssign<&'a Self>
    + for<'a> std::ops::DivAssign<&'a Self>
    + crate::ops::AddInclusive<Self, Output = Self>
    + crate::ops::SubInclusive<Self, Output = Self>
    + crate::ops::AddAssignInclusive<Self>
    + crate::ops::SubAssignInclusive<Self>
    + for<'a> crate::ops::AddAssignInclusive<&'a Self>
    + for<'a> crate::ops::SubAssignInclusive<&'a Self>
// + for<'a> std::ops::Add<&'a Self, Output = Self>
// + for<'a> std::ops::Sub<&'a Self, Output = Self>
// + for<'a> std::ops::Mul<&'a Self, Output = Self>
// + for<'a> std::ops::Div<&'a Self, Output = Self>
where
    Self: Sized + std::fmt::Debug,
{
    /// Returns the width of the raster.
    fn width(&self) -> usize;

    /// Returns the height of the raster.
    fn height(&self) -> usize;

    /// Returns the size data structure of the raster.
    fn size(&self) -> RasterSize {
        RasterSize {
            cols: self.width(),
            rows: self.height(),
        }
    }

    fn len(&self) -> usize {
        self.width() * self.height()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a mutable reference to the raster data.
    fn as_mut_slice(&mut self) -> &mut [T];

    /// Returns a reference to the raster data.
    fn as_slice(&self) -> &[T];

    /// Returns a copy of the data as a vector of optional values where None represents nodata values.
    fn masked_data(&self) -> Vec<Option<T>>;

    /// Returns the optional nodata value that is used in the raster to identify missing data.
    fn nodata_value(&self) -> Option<T>;

    /// Returns the number of nodata values in the raster
    fn nodata_count(&self) -> usize;

    /// Return true if the cell at the given index contains valid data
    fn index_has_data(&self, index: usize) -> bool;

    /// Return the value at the given index or None if the index contains nodata
    fn value(&self, index: usize) -> Option<T>;

    /// Return the sum of all the data values
    fn sum(&self) -> f64;

    /// Return an iterator over the raster data, nodata values are represented as None
    fn iter_opt(&self) -> impl Iterator<Item = Option<T>>;

    /// Return an iterator over the raster data, nodata values are represented as None
    fn iter(&self) -> std::slice::Iter<T>;

    /// Return a mutable iterator over the raster data
    fn iter_mut(&mut self) -> std::slice::IterMut<T>;

    fn set_cell_value(&mut self, cell: Cell, val: T);
}

pub trait RasterCreation<T: RasterNum<T>> {
    /// Create a new raster with the given metadata and data buffer.
    fn new(size: RasterSize, data: Vec<T>) -> Self;

    fn from_iter<Iter>(size: RasterSize, iter: Iter) -> Self
    where
        Iter: Iterator<Item = Option<T>>;

    /// Create a new raster with the given metadata and filled with zeros.
    fn zeros(size: RasterSize) -> Self;

    /// Create a new raster with the given metadata and filled with the provided value.
    fn filled_with(val: T, size: RasterSize) -> Self;

    /// Create a new raster filled with nodata.
    fn filled_with_nodata(size: RasterSize) -> Self;
}

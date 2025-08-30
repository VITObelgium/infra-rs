use inf::{allocate::AlignedVec, cast};

use crate::{ArrayDataType, Cell, Error, GeoReference, Nodata, RasterSize, Result, arraynum::ArrayNum};
use std::fmt::Debug;

pub trait ArrayMetadata: Clone + Debug {
    fn size(&self) -> RasterSize;
    fn nodata(&self) -> Option<f64>;
    fn geo_reference(&self) -> GeoReference;

    fn sized(size: RasterSize, dtype: ArrayDataType) -> Self;
    fn sized_for_type<T: ArrayNum>(size: RasterSize) -> Self {
        Self::sized(size, T::TYPE)
    }

    fn sized_with_nodata(raster_size: RasterSize, nodata: Option<f64>) -> Self;
    fn sized_with_nodata_as<T: ArrayNum>(size: RasterSize, nodata: Option<T>) -> Self {
        Self::sized_with_nodata(size, cast::option(nodata))
    }

    fn with_geo_reference(georef: GeoReference) -> Self;

    fn cell_at_index(&self, index: usize) -> Cell {
        let col_count = self.size().cols.count() as usize;
        Cell::from_row_col((index / col_count) as i32, (index % col_count) as i32)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Rows(pub i32);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Columns(pub i32);

impl std::fmt::Display for Rows {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for Columns {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Rows {
    pub fn count(&self) -> i32 {
        self.0
    }
}

impl Columns {
    pub fn count(&self) -> i32 {
        self.0
    }
}

impl From<i32> for Rows {
    fn from(val: i32) -> Self {
        Rows(val)
    }
}

impl From<i32> for Columns {
    fn from(val: i32) -> Self {
        Columns(val)
    }
}

impl std::ops::Mul<Columns> for Rows {
    type Output = usize;

    fn mul(self, rhs: Columns) -> Self::Output {
        self.0 as usize * rhs.0 as usize
    }
}

impl std::ops::Mul<Rows> for Columns {
    type Output = usize;

    fn mul(self, rhs: Rows) -> Self::Output {
        self.0 as usize * rhs.0 as usize
    }
}

/// A struct representing a rectangular window in a raster.
/// Used for operations on a subregion of the raster data.
pub struct RasterWindow {
    top_left: Cell,
    bottom_right: Cell,
}

impl RasterWindow {
    pub fn new(top_left: Cell, size: RasterSize) -> Self {
        let bottom_right = Cell {
            row: top_left.row + size.rows.count() - 1,
            col: top_left.col + size.cols.count() - 1,
        };
        RasterWindow { top_left, bottom_right }
    }

    pub fn is_empty(&self) -> bool {
        self.top_left.row > self.bottom_right.row || self.top_left.col > self.bottom_right.col
    }

    pub fn top_left(&self) -> Cell {
        self.top_left
    }

    pub fn bottom_right(&self) -> Cell {
        self.bottom_right
    }
}

/// A trait representing a raster.
/// A raster implementation provides access to the pixel data and the geographic metadata associated with the raster.
pub trait Array:
    PartialEq
    + approx::AbsDiffEq<Epsilon = <Self::Pixel as approx::AbsDiffEq>::Epsilon>
    + Clone
    + Sized
    + std::fmt::Debug
    + std::ops::Add<Self::Pixel, Output = Self>
    + std::ops::Sub<Self::Pixel, Output = Self>
    + std::ops::Mul<Self::Pixel, Output = Self>
    + std::ops::Div<Self::Pixel, Output = Self>
    + std::ops::Add<Self, Output = Self>
    + std::ops::Sub<Self, Output = Self>
    + std::ops::Mul<Self, Output = Self>
    + std::ops::Div<Self, Output = Self>
    + std::ops::AddAssign<Self::Pixel>
    + std::ops::SubAssign<Self::Pixel>
    + std::ops::MulAssign<Self::Pixel>
    + std::ops::DivAssign<Self::Pixel>
    + std::ops::AddAssign<Self>
    + std::ops::SubAssign<Self>
    + std::ops::MulAssign<Self>
    + std::ops::DivAssign<Self>
    + for<'a> std::ops::AddAssign<&'a Self>
    + for<'a> std::ops::SubAssign<&'a Self>
    + for<'a> std::ops::MulAssign<&'a Self>
    + for<'a> std::ops::DivAssign<&'a Self>
    + crate::arrayops::AddInclusive<Self, Output = Self>
    + crate::arrayops::SubInclusive<Self, Output = Self>
    + crate::arrayops::AddAssignInclusive<Self>
    + crate::arrayops::SubAssignInclusive<Self>
    + for<'a> crate::arrayops::AddAssignInclusive<&'a Self>
    + for<'a> crate::arrayops::SubAssignInclusive<&'a Self>
    + std::ops::Index<Cell, Output = Self::Pixel>
    + std::ops::IndexMut<Cell, Output = Self::Pixel>
// + for<'a> std::ops::Add<&'a Self, Output = Self>
// + for<'a> std::ops::Sub<&'a Self, Output = Self>
// + for<'a> std::ops::Mul<&'a Self, Output = Self>
// + for<'a> std::ops::Div<&'a Self, Output = Self>
{
    type Pixel: ArrayNum;

    //#[cfg(feature = "simd")]
    //type SimdPixel: NodataSimd<Scalar = Self::Pixel>;

    type Metadata: ArrayMetadata;

    type WithPixelType<TDest: ArrayNum>: Array<Pixel = TDest, Metadata = Self::Metadata>;

    //
    // Creation functions
    //

    /// Create a new raster with the given metadata and data buffer.
    /// Important! The nodata value is assumed to be the default value for the pixel type.
    /// If this is not the case `Array::new_process_nodata` should be used instead.
    fn new(meta: Self::Metadata, data: AlignedVec<Self::Pixel>) -> Result<Self>;

    /// Create a new raster from an iterator of optional pixels where None values will become nodata.
    fn from_iter_opt<Iter>(meta: Self::Metadata, iter: Iter) -> Result<Self>
    where
        Iter: Iterator<Item = Option<Self::Pixel>>;

    /// Create a new raster with the given metadata and filled with zeros.
    fn zeros(meta: Self::Metadata) -> Self;

    /// Create a new raster with the given metadata and filled with the provided value.
    fn filled_with(val: Option<Self::Pixel>, meta: Self::Metadata) -> Self;

    /// Create a new raster filled with nodata.
    fn filled_with_nodata(meta: Self::Metadata) -> Self;

    //
    // Trait methods
    //

    /// Returns the metadata reference.
    fn metadata(&self) -> &Self::Metadata;

    /// Returns the number of rows in the raster (height).
    fn rows(&self) -> Rows;

    /// Returns the number of columns in the raster (width).
    fn columns(&self) -> Columns;

    /// Returns the size data structure of the raster.
    fn size(&self) -> RasterSize {
        self.metadata().size()
    }

    fn len(&self) -> usize {
        self.rows() * self.columns()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a mutable reference to the raster data.
    fn as_mut_slice(&mut self) -> &mut [Self::Pixel];

    /// Returns a reference to the raster data.
    fn as_slice(&self) -> &[Self::Pixel];

    /// Returns a copy of the data as a vector of optional values where None represents nodata values.
    fn masked_data(&self) -> Vec<Option<Self::Pixel>>;

    /// Returns the number of nodata values in the raster
    fn nodata_count(&self) -> usize;

    /// Returns true if any of the cells in the raster contain valid data
    fn contains_data(&self) -> bool {
        self.iter().any(|&x| !x.is_nodata())
    }

    /// Return true if the cell at the given index contains valid data
    fn index_has_data(&self, index: usize) -> bool;

    /// Return true if the cell at the given index contains nodata
    fn index_is_nodata(&self, index: usize) -> bool {
        !self.index_has_data(index)
    }

    /// Return true if the provided cell contains nodata
    fn cell_is_nodata(&self, cell: Cell) -> bool {
        self.index_is_nodata((cell.row * self.columns().count() + cell.col) as usize)
    }

    /// Return true if the provided cell contains nodata
    fn cell_has_data(&self, cell: Cell) -> bool {
        !self.cell_is_nodata(cell)
    }

    /// Return the value at the given index or None if the index contains nodata
    fn value(&self, index: usize) -> Option<Self::Pixel>;

    /// Return the value at the given index or None if the index contains nodata
    fn value_mut(&mut self, index: usize) -> Option<&mut Self::Pixel>;

    /// Return the sum of all the data values
    fn sum(&self) -> f64;

    /// Return an iterator over the raster data, nodata values are represented as None
    fn iter_opt(&self) -> impl Iterator<Item = Option<Self::Pixel>>;

    /// Return an iterator over the raster data, nodata values are skipped
    fn iter_values(&self) -> impl Iterator<Item = Self::Pixel>;

    /// Return an iterator over the raster data, nodata values are `Self::Pixel::NODATA`
    fn iter(&self) -> std::slice::Iter<'_, Self::Pixel>;

    /// Return a mutable iterator over the raster data
    fn iter_mut(&mut self) -> std::slice::IterMut<'_, Self::Pixel>;

    /// Return an iterator over the raster subwindow, nodata values are `Self::Pixel::NODATA`
    fn iter_window(&self, window: RasterWindow) -> impl Iterator<Item = Self::Pixel>;

    /// Return an iterator over the raster subwindow, nodata values are `Self::Pixel::NODATA`
    fn iter_window_mut(&mut self, window: RasterWindow) -> impl Iterator<Item = &mut Self::Pixel>;

    /// Return the value at the given cell or None if the cell contains nodata
    /// Use this for cases where a single cell value is needed not in a loop to
    /// to process the entire raster
    fn cell_value(&self, cell: Cell) -> Option<Self::Pixel> {
        self.value((cell.row * self.columns().count() + cell.col) as usize)
    }

    /// Return the value at the given cell or None if the cell contains nodata
    /// Use this for cases where a single cell value is needed not in a loop to
    /// to process the entire raster
    fn cell_value_mut(&mut self, cell: Cell) -> Option<&mut Self::Pixel> {
        self.value_mut((cell.row * self.columns().count() + cell.col) as usize)
    }

    /// Set the value at the given cell, if the value is None the cell will be set to nodata
    /// Use this for cases where a single cell value is needed not in a loop to
    /// to process the entire raster
    fn set_cell_value(&mut self, cell: Cell, val: Option<Self::Pixel>);

    /// Assigns the value to all the elements of the raster, even nodata
    fn fill(&mut self, val: Self::Pixel);

    fn cast_to<TDest: ArrayNum>(&self) -> Self::WithPixelType<TDest>;
}

pub trait ArrayCopy<T: ArrayNum, Rhs = Self> {
    /// Create a new raster with the same metadata and data as the provided raster.
    fn new_with_dimensions_of(ras: &Rhs, fill: T) -> Self;
}

pub trait ArrayInterop: Array + Sized {
    /// Create a new raster with the given metadata and data buffer.
    /// The nodata value from the provided Metadata will be used to convert all the values in the
    /// data buffer that match the nodata value to the internal nodata value.
    fn new_init_nodata(meta: Self::Metadata, data: AlignedVec<Self::Pixel>) -> Result<Self>;

    fn init_nodata(&mut self);

    fn restore_nodata(&mut self);
}

pub fn check_dimensions(lhs: &impl Array, rhs: &impl Array) -> Result<()> {
    if lhs.size() != rhs.size() {
        return Err(Error::InvalidArgument(format!(
            "The rasters have different dimensions {:?} <-> {:?}",
            lhs.size(),
            rhs.size()
        )));
    }

    Ok(())
}

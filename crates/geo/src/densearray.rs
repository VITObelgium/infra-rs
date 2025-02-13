use crate::{
    array::{Columns, Rows},
    densearrayutil,
    raster::{self},
    Array, ArrayCopy, ArrayMetadata, ArrayNum, Cell, Error, RasterSize, Result,
};
use approx::{AbsDiffEq, RelativeEq};

/// Raster implementation using a dense data structure.
/// The nodata values are stored as the [`crate::Nodata::nodata_value`] for the type T in the same array data structure
/// So no additional data is allocated for tracking nodata cells.
#[derive(Debug, Clone)]
pub struct DenseArray<T: ArrayNum, Metadata: ArrayMetadata = RasterSize> {
    pub(super) meta: Metadata,
    pub(super) data: Vec<T>,
}

impl<T: ArrayNum, Metadata: ArrayMetadata> DenseArray<T, Metadata> {
    pub fn empty() -> Self {
        DenseArray {
            meta: Metadata::with_rows_cols(Rows(0), Columns(0)),
            data: Vec::new(),
        }
    }

    pub fn into_raw_parts(self) -> (Metadata, Vec<T>) {
        (self.meta, self.data)
    }

    pub fn unary<F: Fn(T) -> T>(&self, op: F) -> Self {
        DenseArray::new(self.metadata().clone(), self.data.iter().map(|&a| op(a)).collect()).unwrap()
        // only fails on size mismatch
    }

    pub fn unary_inplace<F: Fn(&mut T)>(&mut self, op: F) {
        self.data.iter_mut().for_each(op);
    }

    pub fn unary_mut<F: Fn(T) -> T>(mut self, op: F) -> Self {
        self.data.iter_mut().for_each(|x| *x = op(*x));
        self
    }

    pub fn binary<F: Fn(T, T) -> T>(&self, other: &Self, op: F) -> Self {
        raster::algo::assert_dimensions(self, other);

        let data = self.data.iter().zip(other.data.iter()).map(|(&a, &b)| op(a, b)).collect();

        DenseArray::new(self.metadata().clone(), data).unwrap() // only fails on size mismatch
    }

    pub fn binary_inplace<F: Fn(&mut T, T)>(&mut self, other: &Self, op: F) {
        raster::algo::assert_dimensions(self, other);
        self.data.iter_mut().zip(other.data.iter()).for_each(|(a, &b)| op(a, b));
    }

    pub fn binary_mut<F: Fn(T, T) -> T>(mut self, other: &Self, op: F) -> Self {
        raster::algo::assert_dimensions(&self, other);

        self.data.iter_mut().zip(other.data.iter()).for_each(|(a, &b)| *a = op(*a, b));
        self
    }

    fn cell_index(&self, cell: Cell) -> usize {
        (cell.row * self.columns().count() + cell.col) as usize
    }
}

impl<T: ArrayNum, Metadata: ArrayMetadata> AsRef<[T]> for DenseArray<T, Metadata> {
    fn as_ref(&self) -> &[T] {
        self.data.as_ref()
    }
}

impl<T: ArrayNum, Metadata: ArrayMetadata> AsMut<[T]> for DenseArray<T, Metadata> {
    fn as_mut(&mut self) -> &mut [T] {
        self.data.as_mut()
    }
}

impl<T: ArrayNum, R: Array<Metadata = Metadata>, Metadata: ArrayMetadata> ArrayCopy<T, R> for DenseArray<T, Metadata> {
    fn new_with_dimensions_of(ras: &R, fill: T) -> Self {
        DenseArray::new(ras.metadata().clone(), vec![fill; ras.size().cell_count()]).unwrap()
    }
}

impl<T: ArrayNum, Metadata: ArrayMetadata> Array for DenseArray<T, Metadata> {
    type Pixel = T;
    type WithPixelType<U: ArrayNum> = DenseArray<U, Metadata>;
    type Metadata = Metadata;

    fn new(meta: Metadata, data: Vec<T>) -> Result<Self> {
        if meta.size().cell_count() != data.len() {
            return Err(Error::InvalidArgument(format!(
                "Data length does not match the number of cells in the metadata: {} != {}",
                data.len(),
                meta.size().cell_count()
            )));
        }

        Ok(DenseArray { meta, data })
    }

    fn new_process_nodata(meta: Self::Metadata, mut data: Vec<Self::Pixel>) -> Result<Self> {
        densearrayutil::process_nodata(&mut data, meta.nodata());
        Self::new(meta, data)
    }

    fn from_iter<Iter>(meta: Metadata, iter: Iter) -> Result<Self>
    where
        Self: Sized,
        Iter: Iterator<Item = Option<T>>,
    {
        let mut data = Vec::with_capacity(meta.size().cell_count());
        for val in iter {
            data.push(val.unwrap_or(T::nodata_value()));
        }

        Self::new(meta, data)
    }

    fn zeros(meta: Metadata) -> Self {
        DenseArray::filled_with(Some(T::zero()), meta)
    }

    fn filled_with(val: Option<T>, meta: Metadata) -> Self {
        if let Some(val) = val {
            let cell_count = meta.size().cell_count();
            DenseArray::new(meta, vec![val; cell_count]).unwrap()
        } else {
            DenseArray::filled_with_nodata(meta)
        }
    }

    fn filled_with_nodata(meta: Metadata) -> Self {
        let cell_count = meta.size().cell_count();
        DenseArray::new(meta, vec![T::nodata_value(); cell_count]).unwrap()
    }

    /// Returns the metadata reference.
    fn metadata(&self) -> &Self::Metadata {
        &self.meta
    }

    fn rows(&self) -> Rows {
        self.meta.size().rows
    }

    fn columns(&self) -> Columns {
        self.meta.size().cols
    }

    fn size(&self) -> RasterSize {
        self.meta.size()
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_mut_slice(&mut self) -> &mut [T] {
        self.data.as_mut_slice()
    }

    fn as_slice(&self) -> &[T] {
        self.data.as_slice()
    }

    fn nodata_count(&self) -> usize {
        self.data.iter().filter(|x| x.is_nodata()).count()
    }

    fn value(&self, index: usize) -> Option<T> {
        assert!(index < self.len());
        if index >= self.len() {
            return None;
        }

        let val = self.data[index];
        if T::is_nodata(val) {
            None
        } else {
            Some(val)
        }
    }

    fn index_has_data(&self, index: usize) -> bool {
        self.data[index] != T::nodata_value()
    }

    fn masked_data(&self) -> Vec<Option<T>> {
        self.data.iter().map(|&v| if v.is_nodata() { None } else { Some(v) }).collect()
    }

    fn sum(&self) -> f64 {
        self.data
            .iter()
            .filter(|&&x| !x.is_nodata())
            .fold(0.0, |acc, x| acc + x.to_f64().unwrap_or(0.0))
    }

    fn iter(&self) -> std::slice::Iter<T> {
        self.data.iter()
    }

    fn iter_mut(&mut self) -> std::slice::IterMut<T> {
        self.data.iter_mut()
    }

    fn iter_opt(&self) -> impl Iterator<Item = Option<T>> {
        DenserRasterIterator::new(self)
    }

    fn cell_value(&self, cell: Cell) -> Option<T> {
        self.value(self.cell_index(cell))
    }

    fn set_cell_value(&mut self, cell: Cell, val: Option<T>) {
        let index = self.cell_index(cell);
        self.data[index] = val.unwrap_or(T::nodata_value());
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn contains_data(&self) -> bool {
        self.iter().any(|&x| !x.is_nodata())
    }

    fn index_is_nodata(&self, index: usize) -> bool {
        !self.index_has_data(index)
    }

    fn cell_is_nodata(&self, cell: Cell) -> bool {
        self.index_is_nodata(self.cell_index(cell))
    }

    fn fill(&mut self, val: Self::Pixel) {
        self.data.iter_mut().for_each(|x| *x = val);
    }
}

impl<'a, T: ArrayNum, Metadata: ArrayMetadata> IntoIterator for &'a DenseArray<T, Metadata> {
    type Item = Option<T>;
    type IntoIter = DenserRasterIterator<'a, T, Metadata>;

    fn into_iter(self) -> Self::IntoIter {
        DenserRasterIterator::new(self)
    }
}

pub struct DenserRasterIterator<'a, T: ArrayNum, Metadata: ArrayMetadata> {
    index: usize,
    raster: &'a DenseArray<T, Metadata>,
}

impl<'a, T: ArrayNum, Metadata: ArrayMetadata> DenserRasterIterator<'a, T, Metadata> {
    fn new(raster: &'a DenseArray<T, Metadata>) -> Self {
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

impl<T: ArrayNum, Metadata: ArrayMetadata> PartialEq for DenseArray<T, Metadata> {
    fn eq(&self, other: &Self) -> bool {
        if self.size() != other.size() {
            return false;
        }

        self.data
            .iter()
            .zip(other.data.iter())
            .all(|(&a, &b)| match (a.is_nodata(), b.is_nodata()) {
                (true, true) => true,
                (false, false) => a == b,
                _ => false,
            })
    }
}

impl<T: ArrayNum, Metadata: ArrayMetadata> AbsDiffEq for DenseArray<T, Metadata> {
    type Epsilon = T;

    fn default_epsilon() -> Self::Epsilon {
        T::default_epsilon()
    }

    fn abs_diff_eq(&self, other: &Self, epsilon: Self::Epsilon) -> bool {
        if self.size() != other.size() {
            return false;
        }

        self.data
            .iter()
            .zip(other.data.iter())
            .all(|(&a, &b)| match (a.is_nodata(), b.is_nodata()) {
                (true, true) => true,
                (false, false) => a.abs_diff_eq(&b, epsilon),
                _ => false,
            })
    }
}

impl<T: ArrayNum + RelativeEq, Metadata: ArrayMetadata> RelativeEq for DenseArray<T, Metadata> {
    fn default_max_relative() -> T::Epsilon {
        T::default_max_relative()
    }

    fn relative_eq(&self, other: &Self, epsilon: Self::Epsilon, max_relative: Self::Epsilon) -> bool {
        if self.size() != other.size() {
            return false;
        }

        self.data
            .iter()
            .zip(other.data.iter())
            .all(|(&a, &b)| match (a.is_nodata(), b.is_nodata()) {
                (true, true) => true,
                (false, false) => a.relative_eq(&b, epsilon, max_relative),
                _ => false,
            })
    }
}

impl<T: ArrayNum, Metadata: ArrayMetadata> std::ops::Index<Cell> for DenseArray<T, Metadata> {
    type Output = T;

    fn index(&self, cell: Cell) -> &Self::Output {
        unsafe {
            // SAFETY: The index is checked to be within bounds
            self.data.get_unchecked(self.cell_index(cell))
        }
    }
}

impl<T: ArrayNum, Metadata: ArrayMetadata> std::ops::IndexMut<Cell> for DenseArray<T, Metadata> {
    fn index_mut(&mut self, cell: Cell) -> &mut Self::Output {
        unsafe {
            // SAFETY: The index is checked to be within bounds
            let index = self.cell_index(cell);
            self.data.get_unchecked_mut(index)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{testutils::compare_fp_vectors, Nodata};

    use super::*;

    #[test]
    fn cast_dense_raster() {
        let ras = DenseArray::new(
            RasterSize::with_rows_cols(Rows(2), Columns(2)),
            vec![1, 2, <i32 as Nodata>::nodata_value(), 4],
        )
        .unwrap();

        let f64_ras = raster::algo::cast::<f64, _>(&ras);
        compare_fp_vectors(f64_ras.as_slice(), &[1.0, 2.0, <f64 as Nodata>::nodata_value(), 4.0]);
    }
}

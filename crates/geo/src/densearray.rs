use crate::{
    raster::{self},
    Array, ArrayCopy, ArrayCreation, ArrayMetadata, Cell, RasterNum, RasterSize,
};

/// Raster implementation using a dense data structure.
/// The nodata values are stored as the [`crate::Nodata::nodata_value`] for the type T in the same array data structure
/// So no additional data is allocated for tracking nodata cells.
#[derive(Debug, Clone)]
pub struct DenseArray<T: RasterNum<T>, Metadata: ArrayMetadata = RasterSize> {
    pub(super) meta: Metadata,
    pub(super) data: Vec<T>,
}

impl<T: RasterNum<T>, Metadata: ArrayMetadata> DenseArray<T, Metadata> {
    pub fn empty() -> Self {
        DenseArray {
            meta: Metadata::with_rows_cols(0, 0),
            data: Vec::new(),
        }
    }

    pub fn into_raw_parts(self) -> (Metadata, Vec<T>) {
        (self.meta, self.data)
    }

    pub fn unary<F: Fn(T) -> T>(&self, op: F) -> Self {
        DenseArray::new(self.metadata().clone(), self.data.iter().map(|&a| op(a)).collect())
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

        DenseArray::new(self.metadata().clone(), data)
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
}

impl<T: RasterNum<T>, Metadata: ArrayMetadata> AsRef<[T]> for DenseArray<T, Metadata> {
    fn as_ref(&self) -> &[T] {
        self.data.as_ref()
    }
}

impl<T: RasterNum<T>, Metadata: ArrayMetadata> AsMut<[T]> for DenseArray<T, Metadata> {
    fn as_mut(&mut self) -> &mut [T] {
        self.data.as_mut()
    }
}

impl<T: RasterNum<T>, Metadata: ArrayMetadata> ArrayCreation for DenseArray<T, Metadata> {
    type Pixel = T;
    type Metadata = Metadata;

    fn new(meta: Metadata, data: Vec<T>) -> Self {
        DenseArray { meta, data }
    }

    fn from_iter<Iter>(meta: Metadata, iter: Iter) -> Self
    where
        Self: Sized,
        Iter: Iterator<Item = Option<T>>,
    {
        let mut data = Vec::with_capacity(meta.size().cell_count());
        for val in iter {
            data.push(val.unwrap_or(T::nodata_value()));
        }

        DenseArray { meta, data }
    }

    fn zeros(meta: Metadata) -> Self {
        DenseArray::filled_with(T::zero(), meta)
    }

    fn filled_with(val: T, meta: Metadata) -> Self {
        let cell_count = meta.size().cell_count();
        DenseArray::new(meta, vec![val; cell_count])
    }

    fn filled_with_nodata(meta: Metadata) -> Self {
        let cell_count = meta.size().cell_count();
        DenseArray::new(meta, vec![T::nodata_value(); cell_count])
    }
}

impl<T: RasterNum<T>, R: Array<Metadata = Metadata>, Metadata: ArrayMetadata> ArrayCopy<T, R> for DenseArray<T, Metadata> {
    fn new_with_dimensions_of(ras: &R, fill: T) -> Self {
        DenseArray::new(ras.metadata().clone(), vec![fill; ras.size().cell_count()])
    }
}

impl<T: RasterNum<T>, Metadata: ArrayMetadata> Array for DenseArray<T, Metadata> {
    type Pixel = T;
    type WithPixelType<U: RasterNum<U>> = DenseArray<U, Metadata>;
    type Metadata = Metadata;

    /// Returns the metadata reference.
    fn metadata(&self) -> &Self::Metadata {
        &self.meta
    }

    fn width(&self) -> usize {
        self.meta.size().cols
    }

    fn height(&self) -> usize {
        self.meta.size().rows
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
        self.value(cell.row as usize * self.size().cols + cell.col as usize)
    }

    fn set_cell_value(&mut self, cell: Cell, val: Option<T>) {
        let size = self.size();
        self.data[cell.row as usize * size.cols + cell.col as usize] = val.unwrap_or(T::nodata_value());
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
        self.index_is_nodata(cell.row as usize * self.width() + cell.col as usize)
    }
}

impl<'a, T: RasterNum<T>, Metadata: ArrayMetadata> IntoIterator for &'a DenseArray<T, Metadata> {
    type Item = Option<T>;
    type IntoIter = DenserRasterIterator<'a, T, Metadata>;

    fn into_iter(self) -> Self::IntoIter {
        DenserRasterIterator::new(self)
    }
}

pub struct DenserRasterIterator<'a, T: RasterNum<T>, Metadata: ArrayMetadata> {
    index: usize,
    raster: &'a DenseArray<T, Metadata>,
}

impl<'a, T: RasterNum<T>, Metadata: ArrayMetadata> DenserRasterIterator<'a, T, Metadata> {
    fn new(raster: &'a DenseArray<T, Metadata>) -> Self {
        DenserRasterIterator { index: 0, raster }
    }
}

impl<T, Metadata> Iterator for DenserRasterIterator<'_, T, Metadata>
where
    T: RasterNum<T>,
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

impl<T: RasterNum<T>, Metadata: ArrayMetadata> PartialEq for DenseArray<T, Metadata> {
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

impl<T: RasterNum<T>, Metadata: ArrayMetadata> std::ops::Index<Cell> for DenseArray<T, Metadata> {
    type Output = T;

    fn index(&self, cell: Cell) -> &Self::Output {
        unsafe {
            // SAFETY: The index is checked to be within bounds
            self.data.get_unchecked(cell.row as usize * self.size().cols + cell.col as usize)
        }
    }
}

impl<T: RasterNum<T>, Metadata: ArrayMetadata> std::ops::IndexMut<Cell> for DenseArray<T, Metadata> {
    fn index_mut(&mut self, cell: Cell) -> &mut Self::Output {
        let cols = self.size().cols;
        unsafe {
            // SAFETY: The index is checked to be within bounds
            self.data.get_unchecked_mut(cell.row as usize * cols + cell.col as usize)
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
            RasterSize::with_rows_cols(2, 2),
            vec![1, 2, <i32 as Nodata<i32>>::nodata_value(), 4],
        );

        let f64_ras = raster::algo::cast::<f64, _>(&ras);
        compare_fp_vectors(f64_ras.as_slice(), &[1.0, 2.0, <f64 as Nodata<f64>>::nodata_value(), 4.0]);
    }
}

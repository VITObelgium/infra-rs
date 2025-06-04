use crate::{
    Array, ArrayCopy, ArrayMetadata, ArrayNum, Cell, Error, RasterSize, Result,
    array::{ArrayInterop, Columns, Rows, Window},
    densearrayiterators, densearrayutil,
    raster::{self},
};
use approx::{AbsDiffEq, RelativeEq};
use inf::allocate::{self, AlignedVec};
use num::NumCast;

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

/// Raster implementation using a dense data structure.
/// The nodata values are stored as the [`crate::Nodata::NODATA`] for the type T in the same array data structure
/// So no additional data is allocated for tracking nodata cells.
#[derive(Debug)]
pub struct DenseArray<T: ArrayNum, Metadata: ArrayMetadata = RasterSize> {
    pub(super) meta: Metadata,
    pub(super) data: AlignedVec<T>,
}

/// Clone for `DenseArray`
/// When simd is enabled we need to ensure that the cloned vec is properly aligned.
impl<T: Clone + ArrayNum, Metadata: Clone + ArrayMetadata> Clone for DenseArray<T, Metadata> {
    #[inline]
    fn clone(&self) -> DenseArray<T, Metadata> {
        #[cfg(feature = "simd")]
        {
            let mut data = allocate::aligned_vec_with_capacity(self.data.len());
            unsafe {
                // SAFETY: We allocated with len capacituy, so we can safely set the length
                data.set_len(self.data.len());
            }
            data.copy_from_slice(self.data.as_slice());

            DenseArray {
                meta: Clone::clone(&self.meta),
                data,
            }
        }

        #[cfg(not(feature = "simd"))]
        // If simd is not enabled, we can just clone the data directly
        DenseArray {
            meta: Clone::clone(&self.meta),
            data: Clone::clone(&self.data),
        }
    }
}

impl<T: ArrayNum, Metadata: ArrayMetadata> DenseArray<T, Metadata> {
    pub fn empty() -> Self {
        DenseArray {
            meta: Metadata::with_rows_cols(Rows(0), Columns(0)),
            data: allocate::new_aligned_vec(),
        }
    }

    pub fn into_raw_parts(self) -> (Metadata, AlignedVec<T>) {
        (self.meta, self.data)
    }

    pub fn into_raw_parts_global_alloc(self) -> (Metadata, Vec<T>) {
        #[cfg(feature = "simd")]
        return (self.meta, self.data.to_vec_in(std::alloc::Global));
        #[cfg(not(feature = "simd"))]
        return (self.meta, self.data);
    }

    pub fn unary<TDest: ArrayNum>(&self, op: impl Fn(T) -> TDest) -> <DenseArray<T, Metadata> as Array>::WithPixelType<TDest> {
        DenseArray::new(
            self.metadata().clone(),
            allocate::aligned_vec_from_iter(self.data.iter().map(|&a| op(a))),
        )
        .expect("Raster size bug")
    }

    pub fn unary_inplace(&mut self, op: impl Fn(&mut T)) {
        self.data.iter_mut().for_each(op);
    }

    pub fn unary_mut(mut self, op: impl Fn(T) -> T) -> Self {
        self.data.iter_mut().for_each(|x| *x = op(*x));
        self
    }

    pub fn binary<TDest: ArrayNum>(
        &self,
        other: &Self,
        op: impl Fn(T, T) -> TDest,
    ) -> <DenseArray<T, Metadata> as Array>::WithPixelType<TDest> {
        raster::algo::assert_dimensions(self, other);

        let data = allocate::aligned_vec_from_iter(self.data.iter().zip(other.data.iter()).map(|(&a, &b)| op(a, b)));

        DenseArray::new(self.metadata().clone(), data).expect("Raster size bug")
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

    pub fn vec_mut(&mut self) -> &mut AlignedVec<T> {
        &mut self.data
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
        DenseArray::new(
            ras.metadata().clone(),
            allocate::aligned_vec_filled_with(fill, ras.size().cell_count()),
        )
        .expect("Raster size bug")
    }
}

impl<T: ArrayNum, Metadata: ArrayMetadata> Array for DenseArray<T, Metadata> {
    type Pixel = T;
    type WithPixelType<U: ArrayNum> = DenseArray<U, Metadata>;
    type Metadata = Metadata;

    fn new(meta: Metadata, data: AlignedVec<T>) -> Result<Self> {
        if meta.size().cell_count() != data.len() {
            return Err(Error::InvalidArgument(format!(
                "Data length does not match the number of cells in the metadata: {} != {}",
                data.len(),
                meta.size().cell_count()
            )));
        }

        Ok(DenseArray { meta, data })
    }

    fn from_iter_opt<Iter>(meta: Metadata, iter: Iter) -> Result<Self>
    where
        Self: Sized,
        Iter: Iterator<Item = Option<T>>,
    {
        let mut data = allocate::aligned_vec_with_capacity(meta.size().cell_count());
        for val in iter {
            data.push(val.unwrap_or(T::NODATA));
        }

        Self::new(meta, data)
    }

    fn zeros(meta: Metadata) -> Self {
        DenseArray::filled_with(Some(T::zero()), meta)
    }

    fn filled_with(val: Option<T>, meta: Metadata) -> Self {
        if let Some(val) = val {
            let cell_count = meta.size().cell_count();
            DenseArray::new(meta, allocate::aligned_vec_filled_with(val, cell_count)).expect("Raster size bug")
        } else {
            DenseArray::filled_with_nodata(meta)
        }
    }

    fn filled_with_nodata(meta: Metadata) -> Self {
        let cell_count = meta.size().cell_count();
        DenseArray::new(meta, allocate::aligned_vec_filled_with(T::NODATA, cell_count)).expect("Raster size bug")
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
        let val = self.data.get(index);
        val.and_then(|&val| if val.is_nodata() { None } else { Some(val) })
    }

    fn value_mut(&mut self, index: usize) -> Option<&mut T> {
        assert!(index < self.len());
        let val = self.data.get_mut(index);
        val.and_then(|val| if val.is_nodata() { None } else { Some(val) })
    }

    fn index_has_data(&self, index: usize) -> bool {
        self.data[index] != T::NODATA
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

    fn iter_window(&self, window: Window) -> impl Iterator<Item = T> {
        densearrayiterators::DenserRasterWindowIterator::new(self, window)
    }

    fn iter_window_mut(&mut self, window: Window) -> impl Iterator<Item = &mut T> {
        densearrayiterators::DenserRasterWindowIteratorMut::new(self, window)
    }

    fn iter_opt(&self) -> impl Iterator<Item = Option<T>> {
        densearrayiterators::DenserRasterIterator::new(self)
    }

    fn iter_values(&self) -> impl Iterator<Item = T> {
        densearrayiterators::DenserRasterValueIterator::new(self)
    }

    fn cell_value(&self, cell: Cell) -> Option<T> {
        self.value(self.cell_index(cell))
    }

    fn set_cell_value(&mut self, cell: Cell, val: Option<T>) {
        let index = self.cell_index(cell);
        self.data[index] = val.unwrap_or(T::NODATA);
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

    fn cast_to<U: ArrayNum>(&self) -> <DenseArray<T, Metadata> as Array>::WithPixelType<U> {
        DenseArray::from_iter_opt(self.metadata().clone(), self.iter_opt().map(|v| v.and_then(|v| NumCast::from(v))))
            .expect("Raster size bug")
    }
}

impl<T: ArrayNum, Metadata: ArrayMetadata> ArrayInterop for DenseArray<T, Metadata> {
    type Pixel = <Self as Array>::Pixel;
    type Metadata = <Self as Array>::Metadata;

    #[simd_macro::simd_bounds]
    fn new_init_nodata(meta: Self::Metadata, data: AlignedVec<Self::Pixel>) -> Result<Self> {
        let mut raster = Self::new(meta, data)?;
        raster.init_nodata();
        Ok(raster)
    }

    #[simd_macro::simd_bounds]
    fn init_nodata(&mut self) {
        let nodata = inf::cast::option(self.metadata().nodata());
        densearrayutil::process_nodata(self.as_mut_slice(), nodata);
    }

    #[simd_macro::simd_bounds]
    fn restore_nodata(&mut self) {
        let nodata = inf::cast::option(self.metadata().nodata());
        densearrayutil::restore_nodata(&mut self.data, nodata);
    }
}

impl<'a, T: ArrayNum, Metadata: ArrayMetadata> IntoIterator for &'a DenseArray<T, Metadata> {
    type Item = Option<T>;
    type IntoIter = densearrayiterators::DenserRasterIterator<'a, T, Metadata>;

    fn into_iter(self) -> Self::IntoIter {
        densearrayiterators::DenserRasterIterator::new(self)
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
    use crate::{Nodata, testutils::compare_fp_vectors};

    use super::*;

    #[test]
    fn cast_dense_raster() {
        let ras = DenseArray::new(
            RasterSize::with_rows_cols(Rows(2), Columns(2)),
            allocate::aligned_vec_from_slice(&[1, 2, <i32 as Nodata>::NODATA, 4]),
        )
        .unwrap();

        let f64_ras = raster::algo::cast::<f64, _>(&ras);
        compare_fp_vectors(f64_ras.as_slice(), &[1.0, 2.0, <f64 as Nodata>::NODATA, 4.0]);
    }
}

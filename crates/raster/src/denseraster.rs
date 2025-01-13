use num::NumCast;

use crate::{algo, raster::RasterCreation, Raster, RasterNum, RasterSize};

/// Raster implementation using a dense data structure.
/// The nodata values are stored as the [`crate::Nodata::nodata_value`] for the type T in the same array data structure
/// So no additional data is allocated for tracking nodata cells.
#[derive(Debug, Clone)]
pub struct DenseRaster<T: RasterNum<T>> {
    pub(super) size: RasterSize,
    pub(super) data: Vec<T>,
}

impl<T: RasterNum<T>> DenseRaster<T> {
    pub fn to_raw_parts(self) -> (RasterSize, Vec<T>) {
        (self.size, self.data)
    }

    pub fn unary<F: Fn(T) -> T>(&self, op: F) -> Self {
        DenseRaster::new(self.size, self.data.iter().map(|&a| op(a)).collect())
    }

    pub fn unary_inplace<F: Fn(&mut T)>(&mut self, op: F) {
        self.data.iter_mut().for_each(op);
    }

    pub fn unary_mut<F: Fn(T) -> T>(mut self, op: F) -> Self {
        self.data.iter_mut().for_each(|x| *x = op(*x));
        self
    }

    pub fn binary<F: Fn(T, T) -> T>(&self, other: &Self, op: F) -> Self {
        algo::assert_dimensions(self, other);

        let data = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(&a, &b)| op(a, b))
            .collect();

        DenseRaster::new(self.size, data)
    }

    pub fn binary_inplace<F: Fn(&mut T, T)>(&mut self, other: &Self, op: F) {
        algo::assert_dimensions(self, other);
        self.data.iter_mut().zip(other.data.iter()).for_each(|(a, &b)| op(a, b));
    }

    pub fn binary_mut<F: Fn(T, T) -> T>(mut self, other: &Self, op: F) -> Self {
        algo::assert_dimensions(&self, other);

        self.data
            .iter_mut()
            .zip(other.data.iter())
            .for_each(|(a, &b)| *a = op(*a, b));
        self
    }
}

impl<T: RasterNum<T>> RasterCreation<T> for DenseRaster<T> {
    fn new(size: RasterSize, data: Vec<T>) -> Self {
        DenseRaster { size, data }
    }

    fn from_iter<Iter>(size: RasterSize, iter: Iter) -> Self
    where
        Self: Sized,
        Iter: Iterator<Item = Option<T>>,
    {
        let mut data = Vec::with_capacity(size.cell_count());
        for val in iter {
            data.push(val.unwrap_or(T::nodata_value()));
        }

        DenseRaster { size, data }
    }

    fn zeros(size: RasterSize) -> Self {
        DenseRaster::filled_with(T::zero(), size)
    }

    fn filled_with(val: T, size: RasterSize) -> Self {
        DenseRaster::new(size, vec![val; size.cell_count()])
    }

    fn filled_with_nodata(size: RasterSize) -> Self {
        DenseRaster::new(size, vec![T::nodata_value(); size.cell_count()])
    }
}

impl<T: RasterNum<T>> Raster<T> for DenseRaster<T> {
    fn width(&self) -> usize {
        self.size.cols
    }

    fn height(&self) -> usize {
        self.size.rows
    }

    fn size(&self) -> RasterSize {
        self.size
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

    fn nodata_value(&self) -> Option<T> {
        Some(T::nodata_value())
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
        self.data
            .iter()
            .map(|&v| if v.is_nodata() { None } else { Some(v) })
            .collect()
    }

    fn sum(&self) -> f64 {
        self.data
            .iter()
            .filter(|&&x| !x.is_nodata())
            .fold(0.0, |acc, x| acc + NumCast::from(*x).unwrap_or(0.0))
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
}

impl<'a, T: RasterNum<T>> IntoIterator for &'a DenseRaster<T> {
    type Item = Option<T>;
    type IntoIter = DenserRasterIterator<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        DenserRasterIterator::new(self)
    }
}

pub struct DenserRasterIterator<'a, T: RasterNum<T>> {
    index: usize,
    raster: &'a DenseRaster<T>,
}

impl<'a, T: RasterNum<T>> DenserRasterIterator<'a, T> {
    fn new(raster: &'a DenseRaster<T>) -> Self {
        DenserRasterIterator { index: 0, raster }
    }
}

impl<T> Iterator for DenserRasterIterator<'_, T>
where
    T: RasterNum<T>,
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

impl<T: RasterNum<T>> PartialEq for DenseRaster<T> {
    fn eq(&self, other: &Self) -> bool {
        if self.size != other.size {
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

#[cfg(test)]
mod tests {
    use crate::{testutils::compare_fp_vectors, Nodata};

    use super::*;

    #[test]
    fn cast_dense_raster() {
        let ras = DenseRaster::new(
            RasterSize::with_rows_cols(2, 2),
            vec![1, 2, <i32 as Nodata<i32>>::nodata_value(), 4],
        );

        let f64_ras = algo::cast::<f64, _, DenseRaster<f64>, _>(&ras);
        compare_fp_vectors(
            f64_ras.as_slice(),
            &[1.0, 2.0, <f64 as Nodata<f64>>::nodata_value(), 4.0],
        );
    }
}

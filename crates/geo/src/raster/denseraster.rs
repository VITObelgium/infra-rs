use crate::{
    crs,
    raster::algo::{self, WarpOptions},
    GeoReference, Result,
};
use gdal::raster::GdalType;
use num::NumCast;

use super::{Raster, RasterNum};

/// Raster implementation using a dense data structure.
/// The nodata values are stored as the [`crate::Nodata::nodata_value`] for the type T in the same array data structure
/// So no additional data is allocated for tracking nodata cells.
#[derive(Debug, Clone)]
pub struct DenseRaster<T: RasterNum<T>> {
    pub(super) metadata: GeoReference,
    pub(super) data: Vec<T>,
}

impl<T: RasterNum<T>> DenseRaster<T> {
    #[allow(dead_code)] // this function is not used when gdal support is disabled
    pub(super) fn flatten_nodata(&mut self) {
        if self.nodata_value().is_none() {
            return;
        }

        self.data.iter_mut().for_each(|x| {
            if x.is_nodata() {
                *x = T::nodata_value();
            }
        });
    }

    pub fn unary<F: Fn(T) -> T>(&self, op: F) -> Self {
        DenseRaster::new(self.metadata.clone(), self.data.iter().map(|&a| op(a)).collect())
    }

    pub fn unary_inplace<F: Fn(&mut T)>(&mut self, op: F) {
        self.data.iter_mut().for_each(op);
    }

    pub fn unary_mut<F: Fn(T) -> T>(mut self, op: F) -> Self {
        self.data.iter_mut().for_each(|x| *x = op(*x));
        self
    }

    pub fn binary<F: Fn(T, T) -> T>(&self, other: &Self, op: F) -> Self {
        crate::raster::assert_dimensions(self, other);

        let data = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(&a, &b)| op(a, b))
            .collect();

        DenseRaster::new(self.metadata.clone(), data)
    }

    pub fn binary_inplace<F: Fn(&mut T, T)>(&mut self, other: &Self, op: F) {
        crate::raster::assert_dimensions(self, other);
        self.data.iter_mut().zip(other.data.iter()).for_each(|(a, &b)| op(a, b));
    }

    pub fn binary_mut<F: Fn(T, T) -> T>(mut self, other: &Self, op: F) -> Self {
        crate::raster::assert_dimensions(&self, other);

        self.data
            .iter_mut()
            .zip(other.data.iter())
            .for_each(|(a, &b)| *a = op(*a, b));
        self
    }
}

#[cfg(feature = "gdal")]
impl<T: RasterNum<T> + GdalType> DenseRaster<T> {
    pub fn warped_to_epsg(&self, epsg: crs::Epsg) -> Result<Self> {
        use super::io;

        let dest_meta = self.metadata.warped_to_epsg(epsg)?;
        let result = DenseRaster::filled_with_nodata(dest_meta);

        let src_ds = io::dataset::create_in_memory_with_data(&self.metadata, self.data.as_slice())?;
        let dst_ds = io::dataset::create_in_memory_with_data(&result.metadata, result.data.as_slice())?;

        algo::warp(&src_ds, &dst_ds, &WarpOptions::default())?;

        Ok(result)
    }
}

impl<T: RasterNum<T>> Raster<T> for DenseRaster<T> {
    fn new(metadata: GeoReference, mut data: Vec<T>) -> Self {
        process_nodata(&mut data, metadata.nodata());
        DenseRaster { metadata, data }
    }

    fn from_iter<Iter>(metadata: GeoReference, iter: Iter) -> Self
    where
        Self: Sized,
        Iter: Iterator<Item = Option<T>>,
    {
        let mut data = Vec::with_capacity(metadata.rows() * metadata.columns());
        for val in iter {
            data.push(val.unwrap_or(T::nodata_value()));
        }

        DenseRaster { metadata, data }
    }

    fn zeros(meta: GeoReference) -> Self {
        DenseRaster::filled_with(T::zero(), meta)
    }

    fn filled_with(val: T, meta: GeoReference) -> Self {
        let data_size = meta.rows() * meta.columns();
        DenseRaster::new(meta, vec![val; data_size])
    }

    fn filled_with_nodata(meta: GeoReference) -> Self {
        let data_size = meta.rows() * meta.columns();
        DenseRaster::new(meta, vec![T::nodata_value(); data_size])
    }

    fn geo_metadata(&self) -> &GeoReference {
        &self.metadata
    }

    fn width(&self) -> usize {
        self.metadata.columns()
    }

    fn height(&self) -> usize {
        self.metadata.rows()
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
        match self.metadata.nodata() {
            Some(nodata) => NumCast::from(nodata),
            None => None,
        }
    }

    fn nodata_count(&self) -> usize {
        if self.nodata_value().is_none() {
            return 0;
        }

        self.data.iter().filter(|x| x.is_nodata()).count()
    }

    fn value(&self, index: usize) -> Option<T> {
        assert!(index < self.len());

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

    fn iter(&self) -> impl Iterator<Item = Option<T>> {
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
        if self.metadata != other.metadata {
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

fn process_nodata<T: RasterNum<T>>(data: &mut [T], nodata: Option<f64>) {
    if let Some(nodata) = nodata {
        if nodata.is_nan() || NumCast::from(nodata) == Some(T::nodata_value()) {
            // the nodata value for floats is also nan, so no processing required
            // or the nodata value matches the default nodata value for the type
            return;
        }

        let nodata = NumCast::from(nodata).unwrap_or(T::nodata_value());
        for v in data.iter_mut() {
            if *v == nodata {
                *v = T::nodata_value();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raster::{
        self,
        testutils::{compare_fp_vectors, test_metadata_2x2},
        Nodata,
    };

    #[test]
    fn cast_dense_raster() {
        let ras = DenseRaster::new(test_metadata_2x2(), vec![1, 2, <i32 as Nodata<i32>>::nodata_value(), 4]);

        let f64_ras = raster::cast::<f64, _, DenseRaster<f64>, _>(&ras);
        compare_fp_vectors(
            f64_ras.as_slice(),
            &[1.0, 2.0, <f64 as Nodata<f64>>::nodata_value(), 4.0],
        );
    }
}

use crate::GeoReference;
use num::NumCast;
use raster::{DenseRaster, Raster, RasterCreation, RasterNum};

use super::{GeoRaster, GeoRasterCreation};

/// Raster implementation using a dense data structure.
/// The nodata values are stored as the [`crate::Nodata::nodata_value`] for the type T in the same array data structure
/// So no additional data is allocated for tracking nodata cells.
#[derive(Debug, Clone)]
pub struct DenseGeoRaster<T: RasterNum<T>> {
    pub(super) metadata: GeoReference,
    pub(super) data: DenseRaster<T>,
}

impl<T: RasterNum<T>> DenseGeoRaster<T> {
    pub fn from_dense_raster(metadata: GeoReference, data: DenseRaster<T>) -> Self {
        assert!(metadata.raster_size().cell_count() == data.len());
        DenseGeoRaster { metadata, data }
    }
}

impl<T: RasterNum<T>> GeoRaster<T> for DenseGeoRaster<T> {
    fn geo_reference(&self) -> &GeoReference {
        &self.metadata
    }
}

impl<T: RasterNum<T>> GeoRasterCreation<T> for DenseGeoRaster<T> {
    fn new(metadata: GeoReference, mut data: Vec<T>) -> Self {
        process_nodata(&mut data, metadata.nodata());
        let raster_size = metadata.raster_size();
        DenseGeoRaster {
            metadata,
            data: DenseRaster::new(raster_size, data),
        }
    }

    fn from_iter<Iter>(metadata: GeoReference, iter: Iter) -> Self
    where
        Self: Sized,
        Iter: Iterator<Item = Option<T>>,
    {
        let raster_size = metadata.raster_size();
        DenseGeoRaster {
            metadata,
            data: DenseRaster::from_iter(raster_size, iter),
        }
    }

    fn zeros(meta: GeoReference) -> Self {
        let raster_size = meta.raster_size();
        DenseGeoRaster::from_dense_raster(meta, DenseRaster::zeros(raster_size))
    }

    fn filled_with(val: T, meta: GeoReference) -> Self {
        let raster_size = meta.raster_size();
        DenseGeoRaster::from_dense_raster(meta, DenseRaster::filled_with(val, raster_size))
    }

    fn filled_with_nodata(meta: GeoReference) -> Self {
        let raster_size = meta.raster_size();
        DenseGeoRaster::from_dense_raster(meta, DenseRaster::filled_with_nodata(raster_size))
    }
}

impl<T: RasterNum<T>> DenseGeoRaster<T> {
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

    pub fn to_raw_parts(self) -> (GeoReference, Vec<T>) {
        let (_, vec) = self.data.to_raw_parts();
        (self.metadata, vec)
    }

    pub fn unary<F: Fn(T) -> T>(&self, op: F) -> Self {
        DenseGeoRaster::new(self.metadata.clone(), self.data.iter().map(|&a| op(a)).collect())
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

        let data = self
            .data
            .iter()
            .zip(other.data.iter())
            .map(|(&a, &b)| op(a, b))
            .collect();

        DenseGeoRaster::new(self.metadata.clone(), data)
    }

    pub fn binary_inplace<F: Fn(&mut T, T)>(&mut self, other: &Self, op: F) {
        raster::algo::assert_dimensions(self, other);
        self.data.iter_mut().zip(other.data.iter()).for_each(|(a, &b)| op(a, b));
    }

    pub fn binary_mut<F: Fn(T, T) -> T>(mut self, other: &Self, op: F) -> Self {
        raster::algo::assert_dimensions(&self, other);

        self.data
            .iter_mut()
            .zip(other.data.iter())
            .for_each(|(a, &b)| *a = op(*a, b));
        self
    }
}

#[cfg(feature = "gdal")]
impl<T: RasterNum<T> + gdal::raster::GdalType> DenseGeoRaster<T> {
    pub fn warped_to_epsg(&self, epsg: crate::crs::Epsg) -> crate::Result<Self> {
        use super::algo;
        use super::io;

        let dest_meta = self.metadata.warped_to_epsg(epsg)?;
        let result = DenseGeoRaster::filled_with_nodata(dest_meta);

        let src_ds = io::dataset::create_in_memory_with_data(&self.metadata, self.data.as_slice())?;
        let dst_ds = io::dataset::create_in_memory_with_data(&result.metadata, result.data.as_slice())?;

        algo::warp(&src_ds, &dst_ds, &algo::WarpOptions::default())?;

        Ok(result)
    }
}

impl<T: RasterNum<T>> Raster<T> for DenseGeoRaster<T> {
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

        self.data.nodata_count()
    }

    fn value(&self, index: usize) -> Option<T> {
        self.data.value(index)
    }

    fn index_has_data(&self, index: usize) -> bool {
        self.data.index_has_data(index)
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
        self.data.into_iter()
    }

    fn set_cell_value(&mut self, cell: raster::Cell, val: T) {
        self.data.set_cell_value(cell, val);
    }
}

impl<'a, T: RasterNum<T>> IntoIterator for &'a DenseGeoRaster<T> {
    type Item = Option<T>;
    type IntoIter = DenseGeoRasterIterator<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        DenseGeoRasterIterator::new(self)
    }
}

pub struct DenseGeoRasterIterator<'a, T: RasterNum<T>> {
    index: usize,
    raster: &'a DenseGeoRaster<T>,
}

impl<'a, T: RasterNum<T>> DenseGeoRasterIterator<'a, T> {
    fn new(raster: &'a DenseGeoRaster<T>) -> Self {
        DenseGeoRasterIterator { index: 0, raster }
    }
}

impl<T> Iterator for DenseGeoRasterIterator<'_, T>
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

impl<T: RasterNum<T>> PartialEq for DenseGeoRaster<T> {
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

/// Process nodata values in the data array
/// This means replacing all the values that match the nodata value with the default nodata value for the type T
/// as defined by the [`crate::Nodata`] trait
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
    use raster::Nodata;

    use super::*;
    use crate::georaster::{
        algo,
        testutils::{compare_fp_vectors, test_metadata_2x2},
    };

    #[test]
    fn cast_dense_raster() {
        let ras = DenseGeoRaster::new(test_metadata_2x2(), vec![1, 2, <i32 as Nodata<i32>>::nodata_value(), 4]);

        let f64_ras: DenseGeoRaster<f64> = algo::cast(&ras);
        compare_fp_vectors(
            f64_ras.as_slice(),
            &[1.0, 2.0, <f64 as Nodata<f64>>::nodata_value(), 4.0],
        );
    }
}

use arrow::{
    array::{Array, ArrowNativeTypeOp, PrimitiveArray, PrimitiveIter},
    buffer::ScalarBuffer,
    datatypes::ArrowPrimitiveType,
};

use num::NumCast;

use crate::{
    raster::{Raster, RasterNum},
    GeoReference,
};

use super::arrowutil::ArrowType;

pub trait ArrowRasterNum<T: num::ToPrimitive>: RasterNum<T> + ArrowType + ArrowNativeTypeOp {}

impl ArrowRasterNum<i8> for i8 {}
impl ArrowRasterNum<u8> for u8 {}
impl ArrowRasterNum<i16> for i16 {}
impl ArrowRasterNum<u16> for u16 {}
impl ArrowRasterNum<i32> for i32 {}
impl ArrowRasterNum<u32> for u32 {}
impl ArrowRasterNum<i64> for i64 {}
impl ArrowRasterNum<u64> for u64 {}
impl ArrowRasterNum<f32> for f32 {}
impl ArrowRasterNum<f64> for f64 {}

pub struct ArrowRaster<T: ArrowRasterNum<T>> {
    pub(super) metadata: GeoReference,
    pub(super) data: PrimitiveArray<T::TArrow>,
}

impl<T: ArrowRasterNum<T>> ArrowRaster<T>
where
    T::TArrow: ArrowPrimitiveType<Native = T>,
{
    pub fn mask_vec(&self) -> Vec<Option<T>> {
        self.data.iter().collect()
    }

    /// make sure the null entries in the raster contain the nodata value
    /// Call this function before writing the raster to disk
    pub fn flatten_nodata(&mut self) {
        if self.data.null_count() == 0 {
            return;
        }

        if let Some(nodata) = self.metadata.nodata() {
            let nodata = NumCast::from(nodata).unwrap_or(T::nodata_value());
            self.metadata.set_nodata(nodata.to_f64());

            if let (_dt, data, Some(mask)) = self.data.clone().into_parts() {
                let mut vec_data = data.to_vec();
                (0..data.len()).for_each(|i| {
                    if mask.is_null(i) {
                        vec_data[i] = nodata;
                    }
                });

                self.data = PrimitiveArray::<T::TArrow>::new(ScalarBuffer::from(vec_data), Some(mask));
            }
        }
    }

    pub fn arrow_array(&self) -> &PrimitiveArray<T::TArrow> {
        &self.data
    }
}

impl<T: ArrowRasterNum<T>> Raster<T> for ArrowRaster<T>
where
    T::TArrow: ArrowPrimitiveType<Native = T>,
{
    fn new(metadata: GeoReference, data: Vec<T>) -> Self {
        let nod = metadata.nodata();
        let data: PrimitiveArray<T::TArrow> = data.iter().map(|&v| (v.to_f64() != nod).then_some(v)).collect();
        ArrowRaster { metadata, data }
    }

    fn from_iter<Iter>(metadata: GeoReference, iter: Iter) -> Self
    where
        Self: Sized,
        Iter: Iterator<Item = Option<T>>,
    {
        ArrowRaster {
            metadata,
            data: iter.collect(),
        }
    }

    fn zeros(meta: GeoReference) -> Self {
        ArrowRaster::filled_with(T::zero(), meta)
    }

    fn filled_with(val: T, meta: GeoReference) -> Self {
        let data_size = meta.rows() * meta.columns();
        ArrowRaster::new(meta, vec![val; data_size])
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

    fn as_mut_slice(&mut self) -> &mut [T] {
        //self.data.values().inner().as_mut_slice()
        unimplemented!();
    }

    fn as_slice(&self) -> &[T] {
        self.data.values().inner().typed_data()
    }

    fn nodata_value(&self) -> Option<T> {
        match self.metadata.nodata() {
            Some(nodata) => NumCast::from(nodata),
            None => None,
        }
    }

    fn nodata_count(&self) -> usize {
        self.data.null_count()
    }

    fn index_has_data(&self, index: usize) -> bool {
        self.data.is_valid(index)
    }

    fn masked_data(&self) -> Vec<Option<T>> {
        self.data.iter().collect()
    }

    fn value(&self, index: usize) -> Option<T> {
        if self.index_has_data(index) {
            Some(self.data.value(index))
        } else {
            None
        }
    }

    fn sum(&self) -> f64 {
        // using the sum from compute uses the same data type as the raster so is not accurate for e.g. f32
        self.data
            .iter()
            .filter_map(|x| x.and_then(|v| v.to_f64()))
            .fold(0.0, |acc, x| acc + x)
    }
}

impl<'a, T: ArrowRasterNum<T>> IntoIterator for &'a ArrowRaster<T>
where
    T::TArrow: ArrowPrimitiveType<Native = T>,
{
    type Item = Option<T>;
    type IntoIter = PrimitiveIter<'a, T::TArrow>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        raster::{self, testutils::*, Nodata},
        RasterSize,
    };

    #[test]
    fn cast_arrow_raster() {
        let ras = ArrowRaster::new(test_metadata_2x2(), vec![1, 2, <i32 as Nodata<i32>>::nodata_value(), 4]);

        let f64_ras = raster::cast::<f64, _, ArrowRaster<f64>, _>(&ras);
        compare_fp_vectors(
            f64_ras.as_slice(),
            &[1.0, 2.0, <f64 as Nodata<f64>>::nodata_value(), 4.0],
        );
    }

    #[test]
    fn test_flatten() {
        let metadata = GeoReference::new(
            "EPSG:4326".to_string(),
            RasterSize { rows: 2, cols: 2 },
            [0.0, 0.0, 1.0, 1.0, 0.0, 0.0],
            Some(-9999.0),
        );

        let data1 = vec![1, 2, -9999, 4];
        let data2 = vec![-9999, 6, 7, 8];
        let raster1 = ArrowRaster::new(metadata.clone(), data1);
        let raster2 = ArrowRaster::new(metadata.clone(), data2);

        let mut result = &raster1 + &raster2;
        // The first element should be nodata
        assert!(!result.index_has_data(0));
        assert!(!result.index_has_data(2));
        // The internal buffer value is undefined, due to the operation will no longer match the nodata value
        assert!(result.as_slice()[0] != -9999);
        assert!(result.as_slice()[2] != -9999);

        // Flatten the nodata values
        result.flatten_nodata();

        // The first element should still be nodata
        assert!(!result.index_has_data(0));
        assert!(!result.index_has_data(2));
        // The internal buffer value should now match the nodata value
        assert_eq!(result.as_slice()[0], -9999);
        assert_eq!(result.as_slice()[2], -9999);
    }
}

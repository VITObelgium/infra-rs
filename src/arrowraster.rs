use arrow::{
    array::{downcast_array, Array, ArrowNativeTypeOp, PrimitiveArray},
    compute,
    datatypes::ArrowPrimitiveType,
};

use num::NumCast;

use crate::{
    arrowutil::ArrowType,
    raster::{self, RasterNum},
    GeoMetadata, Raster,
};

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
    metadata: GeoMetadata,
    data: PrimitiveArray<T::TArrow>,
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
            let nodata = NumCast::from(nodata).unwrap_or(T::value());
            self.metadata.set_nodata(nodata.to_f64());

            if let (_dt, data, Some(mask)) = self.data.clone().into_parts() {
                let mut vec_data = data.to_vec();
                (0..data.len()).for_each(|i| {
                    if mask.is_null(i) {
                        vec_data[i] = nodata;
                    }
                });

                self.data = PrimitiveArray::<T::TArrow>::new(data, Some(mask));
            }
        }
    }

    pub fn sum(&self) -> f64 {
        // using the sum from compute uses the same data type as the raster so is not accurate for e.g. f32
        self.data
            .iter()
            .filter_map(|x| x.and_then(|v| v.to_f64()))
            .fold(0.0, |acc, x| acc + x)
    }

    pub fn nodata_count(&self) -> usize {
        self.data.null_count()
    }

    pub fn arrow_array(&self) -> &PrimitiveArray<T::TArrow> {
        &self.data
    }
}

impl<T: ArrowRasterNum<T> + std::ops::Add<Output = T>> std::ops::Add for ArrowRaster<T>
where
    T::TArrow: ArrowPrimitiveType<Native = T>,
{
    type Output = ArrowRaster<T>;

    fn add(self, other: ArrowRaster<T>) -> ArrowRaster<T> {
        raster::assert_dimensions(&self, &other);

        // Create a new ArrowRaster with the same metadata
        let metadata = self.metadata.clone();

        match compute::kernels::numeric::add_wrapping(&self.data, &other.data) {
            Ok(data) => {
                let data = downcast_array::<PrimitiveArray<T::TArrow>>(&*data);
                ArrowRaster { metadata, data }
            }
            Err(e) => panic!("Error adding rasters: {:?}", e),
        }
    }
}

impl<T: ArrowRasterNum<T> + std::ops::Add<Output = T>> std::ops::Add for &ArrowRaster<T>
where
    T::TArrow: ArrowPrimitiveType<Native = T>,
{
    type Output = ArrowRaster<T>;

    fn add(self, other: &ArrowRaster<T>) -> ArrowRaster<T> {
        raster::assert_dimensions(self, other);

        match compute::kernels::numeric::add_wrapping(&self.data, &other.data) {
            Ok(data) => {
                let data = downcast_array::<PrimitiveArray<T::TArrow>>(&*data);
                ArrowRaster {
                    metadata: self.metadata.clone(),
                    data,
                }
            }
            Err(e) => panic!("Error adding rasters: {:?}", e),
        }
    }
}

impl<T: ArrowRasterNum<T> + std::ops::Mul<Output = T>> std::ops::Mul<T> for ArrowRaster<T>
where
    T::TArrow: ArrowPrimitiveType<Native = T>,
{
    type Output = ArrowRaster<T>;

    fn mul(self, scalar: T) -> ArrowRaster<T> {
        match compute::kernels::numeric::mul_wrapping(&self.data, &PrimitiveArray::<T::TArrow>::new_scalar(scalar)) {
            Ok(data) => ArrowRaster {
                metadata: self.metadata.clone(),
                data: downcast_array::<PrimitiveArray<T::TArrow>>(&data),
            },
            Err(e) => panic!("Error adding rasters: {:?}", e),
        }
    }
}

impl<T: ArrowRasterNum<T> + std::ops::Mul<Output = T>> std::ops::Mul<T> for &ArrowRaster<T>
where
    T::TArrow: ArrowPrimitiveType<Native = T>,
{
    type Output = ArrowRaster<T>;

    fn mul(self, scalar: T) -> ArrowRaster<T> {
        ArrowRaster {
            metadata: self.metadata.clone(),
            data: self.data.unary(|v| v * scalar),
        }
    }
}

impl<T: ArrowRasterNum<T>> Raster<T> for ArrowRaster<T>
where
    T::TArrow: ArrowPrimitiveType<Native = T>,
{
    fn new(metadata: GeoMetadata, data: Vec<T>) -> Self {
        let nod = metadata.nodata();
        let data: PrimitiveArray<T::TArrow> = data.iter().map(|&v| (v.to_f64() != nod).then_some(v)).collect();
        ArrowRaster { metadata, data }
    }

    fn zeros(meta: GeoMetadata) -> Self {
        ArrowRaster::filled_with(T::zero(), meta)
    }

    fn filled_with(val: T, meta: GeoMetadata) -> Self {
        let data_size = meta.rows() * meta.columns();
        ArrowRaster::new(meta, vec![val; data_size])
    }

    fn geo_metadata(&self) -> &GeoMetadata {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GeoMetadata, RasterSize};

    #[test]
    fn test_add_rasters() {
        let metadata = GeoMetadata::new(
            "EPSG:4326".to_string(),
            RasterSize { rows: 2, cols: 2 },
            [0.0, 0.0, 1.0, 1.0, 0.0, 0.0],
            Some(-9999.0),
        );

        let data1 = vec![1, 2, -9999, 4];
        let data2 = vec![-9999, 6, 7, 8];
        let raster1 = ArrowRaster::new(metadata.clone(), data1);
        let raster2 = ArrowRaster::new(metadata.clone(), data2);

        {
            let result = &raster1 + &raster2;
            assert_eq!(result.mask_vec(), [None, Some(8), None, Some(12)]);
        }

        {
            let result = raster1 + raster2;
            assert_eq!(result.mask_vec(), [None, Some(8), None, Some(12)]);
        }
    }

    #[test]
    fn test_sum() {
        let metadata = GeoMetadata::new(
            "EPSG:4326".to_string(),
            RasterSize { rows: 2, cols: 2 },
            [0.0, 0.0, 1.0, 1.0, 0.0, 0.0],
            Some(-9999.0),
        );

        assert_eq!(ArrowRaster::new(metadata.clone(), vec![1, 2, -9999, 4]).sum(), 7.0);
    }

    #[test]
    fn test_multiply_scalar() {
        let metadata = GeoMetadata::new(
            "EPSG:4326".to_string(),
            RasterSize { rows: 2, cols: 2 },
            [0.0, 0.0, 1.0, 1.0, 0.0, 0.0],
            Some(-9999.0),
        );

        let raster = ArrowRaster::new(metadata.clone(), vec![1, 2, -9999, 4]);

        {
            let result = &raster * 2;
            assert_eq!(result.mask_vec(), [Some(2), Some(4), None, Some(8)]);
        }

        {
            let result = raster * 2;
            assert_eq!(result.mask_vec(), [Some(2), Some(4), None, Some(8)]);
        }
    }
}

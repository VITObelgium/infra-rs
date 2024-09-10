use arrow::{
    array::{downcast_array, PrimitiveArray},
    compute,
    datatypes::ArrowPrimitiveType,
};

use crate::raster::{self, ArrowRaster, ArrowRasterNum};

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


impl<T: ArrowRasterNum<T> + std::ops::Mul<Output = T>> std::ops::Mul for ArrowRaster<T>
where
    T::TArrow: ArrowPrimitiveType<Native = T>,
{
    type Output = ArrowRaster<T>;

    fn mul(self, other: ArrowRaster<T>) -> ArrowRaster<T> {
        raster::assert_dimensions(&self, &other);

        match compute::kernels::numeric::mul_wrapping(&self.data, &other.data) {
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

impl<T: ArrowRasterNum<T> + std::ops::Mul<Output = T>> std::ops::Mul for &ArrowRaster<T>
where
    T::TArrow: ArrowPrimitiveType<Native = T>,
{
    type Output = ArrowRaster<T>;

    fn mul(self, other: &ArrowRaster<T>) -> ArrowRaster<T> {
        raster::assert_dimensions(self, other);

        match compute::kernels::numeric::mul_wrapping(&self.data, &other.data) {
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
            Err(e) => panic!("Error multiplying rasters: {:?}", e),
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

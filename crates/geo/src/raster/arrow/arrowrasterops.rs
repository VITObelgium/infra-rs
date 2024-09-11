use arrow::{
    array::{downcast_array, PrimitiveArray},
    compute,
    datatypes::ArrowPrimitiveType,
};

use crate::raster::{self, ArrowRaster, ArrowRasterNum};

macro_rules! arrow_raster_op {
    ($op_trait:path, $scalar_op_trait:path, $op_assign_trait:path, $op_assign_ref_trait:path, $op_fn:ident, $op_assign_fn:ident, $kernel:ident) => {
        impl<T> $op_trait for ArrowRaster<T>
        where
            T: ArrowRasterNum<T>,
            T::TArrow: ArrowPrimitiveType<Native = T>,
        {
            type Output = ArrowRaster<T>;

            fn $op_fn(self, other: ArrowRaster<T>) -> ArrowRaster<T> {
                raster::assert_dimensions(&self, &other);

                // Create a new ArrowRaster with the same metadata
                let metadata = self.metadata.clone();

                match compute::kernels::numeric::$kernel(&self.data, &other.data) {
                    Ok(data) => {
                        let data = downcast_array::<PrimitiveArray<T::TArrow>>(&*data);
                        ArrowRaster { metadata, data }
                    }
                    Err(e) => panic!("Error on raster operation: {:?}", e),
                }
            }
        }

        impl<T> $op_trait for &ArrowRaster<T>
        where
            T: ArrowRasterNum<T>,
            T::TArrow: ArrowPrimitiveType<Native = T>,
        {
            type Output = ArrowRaster<T>;

            fn $op_fn(self, other: &ArrowRaster<T>) -> ArrowRaster<T> {
                raster::assert_dimensions(self, other);

                match compute::kernels::numeric::$kernel(&self.data, &other.data) {
                    Ok(data) => {
                        let data = downcast_array::<PrimitiveArray<T::TArrow>>(&*data);
                        ArrowRaster {
                            metadata: self.metadata.clone(),
                            data,
                        }
                    }
                    Err(e) => panic!("Error on raster operation: {:?}", e),
                }
            }
        }

        impl<T: ArrowRasterNum<T>> $scalar_op_trait for ArrowRaster<T>
        where
            T::TArrow: ArrowPrimitiveType<Native = T>,
        {
            type Output = ArrowRaster<T>;

            fn $op_fn(mut self, scalar: T) -> ArrowRaster<T> {
                self.data = match self.data.unary_mut(|v| v.$op_fn(scalar)) {
                    Ok(data) => data,
                    Err(e) => panic!("Error on raster operation: {:?}", e),
                };

                self
            }
        }

        impl<T: ArrowRasterNum<T>> $scalar_op_trait for &ArrowRaster<T>
        where
            T::TArrow: ArrowPrimitiveType<Native = T>,
        {
            type Output = ArrowRaster<T>;

            fn $op_fn(self, scalar: T) -> ArrowRaster<T> {
                ArrowRaster {
                    metadata: self.metadata.clone(),
                    data: self.data.unary(|v| v.$op_fn(scalar)),
                }
            }
        }

        impl<T> $op_assign_trait for ArrowRaster<T>
        where
            T: ArrowRasterNum<T>,
            T::TArrow: ArrowPrimitiveType<Native = T>,
        {
            fn $op_assign_fn(&mut self, other: ArrowRaster<T>) {
                raster::assert_dimensions(self, &other);

                match compute::kernels::numeric::$kernel(&self.data, &other.data) {
                    Ok(data) => {
                        self.data = downcast_array::<PrimitiveArray<T::TArrow>>(&*data);
                    }
                    Err(e) => panic!("Error adding rasters: {:?}", e),
                }
            }
        }

        impl<T> $op_assign_ref_trait for ArrowRaster<T>
        where
            T: ArrowRasterNum<T>,
            T::TArrow: ArrowPrimitiveType<Native = T>,
        {
            fn $op_assign_fn(&mut self, other: &ArrowRaster<T>) {
                raster::assert_dimensions(self, other);

                match compute::kernels::numeric::$kernel(&self.data, &other.data) {
                    Ok(data) => {
                        self.data = downcast_array::<PrimitiveArray<T::TArrow>>(&*data);
                    }
                    Err(e) => panic!("Error adding rasters: {:?}", e),
                }
            }
        }
    };
}

arrow_raster_op!(
    std::ops::Add,
    std::ops::Add<T>,
    std::ops::AddAssign,
    std::ops::AddAssign<&ArrowRaster<T>>,
    add,
    add_assign,
    add_wrapping
);
arrow_raster_op!(
    std::ops::Sub,
    std::ops::Sub<T>,
    std::ops::SubAssign,
    std::ops::SubAssign<&ArrowRaster<T>>,
    sub,
    sub_assign,
    sub_wrapping
);
arrow_raster_op!(
    std::ops::Mul,
    std::ops::Mul<T>,
    std::ops::MulAssign,
    std::ops::MulAssign<&ArrowRaster<T>>,
    mul,
    mul_assign,
    mul_wrapping
);
arrow_raster_op!(
    std::ops::Div,
    std::ops::Div<T>,
    std::ops::DivAssign,
    std::ops::DivAssign<&ArrowRaster<T>>,
    div,
    div_assign,
    div
);

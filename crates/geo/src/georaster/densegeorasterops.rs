use super::{DenseGeoRaster, RasterNum};
use std::ops::AddAssign;
use std::ops::DivAssign;
use std::ops::MulAssign;
use std::ops::SubAssign;

/// Macro to generate numeric raster operations.
macro_rules! dense_raster_op {
    (   $op_trait:path, // name of the trait e.g. std::ops::Add
        $scalar_op_trait:path, // name of the trait with scalar argument e.g. std::ops::Add<T>
        $op_assign_trait:path, // name of the trait with assignment e.g. std::ops::AddAssign
        $op_assign_scalar_trait:path, // name of the trait with scalar assignment e.g. std::ops::AddAssign<T>
        $op_assign_ref_trait:path, // name of the trait with reference assignment e.g. std::ops::AddAssign<&DenseGeoRaster<T>>
        $op_fn:ident, // name of the operation function inside the trait e.g. add
        $op_assign_fn:ident, // name of the assignment function inside the trait e.g. add_assign
    ) => {
        impl<T: RasterNum<T>> $op_trait for DenseGeoRaster<T> {
            type Output = DenseGeoRaster<T>;

            fn $op_fn(mut self, other: DenseGeoRaster<T>) -> DenseGeoRaster<T> {
                (&mut self.data).$op_assign_fn(&other.data);
                self
            }
        }

        impl<T: RasterNum<T>> $op_trait for &DenseGeoRaster<T> {
            type Output = DenseGeoRaster<T>;

            fn $op_fn(self, other: &DenseGeoRaster<T>) -> DenseGeoRaster<T> {
                DenseGeoRaster::from_dense_raster(self.metadata.clone(), (&self.data).$op_fn(&other.data))
            }
        }

        impl<T: RasterNum<T>> $op_assign_trait for DenseGeoRaster<T> {
            fn $op_assign_fn(&mut self, other: DenseGeoRaster<T>) {
                self.data.$op_assign_fn(other.data);
            }
        }

        impl<T: RasterNum<T>> $op_assign_scalar_trait for DenseGeoRaster<T> {
            fn $op_assign_fn(&mut self, scalar: T) {
                self.data.$op_assign_fn(scalar);
            }
        }

        impl<T: RasterNum<T>> $op_assign_ref_trait for DenseGeoRaster<T> {
            fn $op_assign_fn(&mut self, other: &DenseGeoRaster<T>) {
                self.data.$op_assign_fn(&other.data);
            }
        }

        impl<T: RasterNum<T>> $scalar_op_trait for DenseGeoRaster<T> {
            type Output = DenseGeoRaster<T>;

            fn $op_fn(mut self, scalar: T) -> DenseGeoRaster<T> {
                self.data.$op_assign_fn(scalar);
                self
            }
        }

        impl<T: RasterNum<T>> $scalar_op_trait for &DenseGeoRaster<T> {
            type Output = DenseGeoRaster<T>;

            fn $op_fn(self, scalar: T) -> DenseGeoRaster<T> {
                DenseGeoRaster::from_dense_raster(self.metadata.clone(), (&self.data).$op_fn(scalar))
            }
        }
    };
}

dense_raster_op!(
    std::ops::Add,
    std::ops::Add<T>,
    std::ops::AddAssign,
    std::ops::AddAssign<T>,
    std::ops::AddAssign<&DenseGeoRaster<T>>,
    add,
    add_assign,
);
dense_raster_op!(
    std::ops::Sub,
    std::ops::Sub<T>,
    std::ops::SubAssign,
    std::ops::SubAssign<T>,
    std::ops::SubAssign<&DenseGeoRaster<T>>,
    sub,
    sub_assign,
);
dense_raster_op!(
    std::ops::Mul,
    std::ops::Mul<T>,
    std::ops::MulAssign,
    std::ops::MulAssign<T>,
    std::ops::MulAssign<&DenseGeoRaster<T>>,
    mul,
    mul_assign,
);
dense_raster_op!(
    std::ops::Div,
    std::ops::Div<T>,
    std::ops::DivAssign,
    std::ops::DivAssign<T>,
    std::ops::DivAssign<&DenseGeoRaster<T>>,
    div,
    div_assign,
);

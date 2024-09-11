use crate::raster;

use super::{DenseRaster, Raster, RasterNum};

macro_rules! dense_raster_op {
    ($op_trait:path, $scalar_op_trait:path, $op_assign_trait:path, $op_assign_scalar_trait:path, $op_assign_ref_trait:path, $op_fn:ident, $op_assign_fn:ident) => {
        impl<T> $op_trait for DenseRaster<T>
        where
            T: RasterNum<T>,
        {
            type Output = DenseRaster<T>;

            fn $op_fn(mut self, other: DenseRaster<T>) -> DenseRaster<T> {
                raster::assert_dimensions(&self, &other);

                for (x, &y) in self.as_mut_slice().iter_mut().zip(other.as_slice().iter()) {
                    if T::is_nodata(*x) || T::is_nodata(y) {
                        *x = T::nodata_value();
                    } else {
                        x.$op_assign_fn(y);
                    }
                }

                self
            }
        }

        impl<T> $op_trait for &DenseRaster<T>
        where
            T: RasterNum<T>,
        {
            type Output = DenseRaster<T>;

            fn $op_fn(self, other: &DenseRaster<T>) -> DenseRaster<T> {
                raster::assert_dimensions(self, other);

                // Create a new DenseRaster with the same metadata
                let metadata = self.metadata.clone();
                let mut data = Vec::with_capacity(self.data.len());
                // Perform element-wise addition
                for (x, y) in self.data.iter().zip(other.data.iter()) {
                    if T::is_nodata(*x) || T::is_nodata(*y) {
                        data.push(T::nodata_value());
                    } else {
                        data.push(x.$op_fn(*y))
                    }
                }

                DenseRaster { metadata, data }
            }
        }

        impl<T> $op_assign_trait for DenseRaster<T>
        where
            T: RasterNum<T>,
        {
            fn $op_assign_fn(&mut self, other: DenseRaster<T>) {
                raster::assert_dimensions(self, &other);

                for (x, &y) in self.as_mut_slice().iter_mut().zip(other.as_slice().iter()) {
                    if T::is_nodata(*x) || T::is_nodata(y) {
                        *x = T::nodata_value();
                    } else {
                        x.$op_assign_fn(y);
                    }
                }
            }
        }

        impl<T> $op_assign_scalar_trait for DenseRaster<T>
        where
            T: RasterNum<T>,
        {
            fn $op_assign_fn(&mut self, scalar: T) {
                for x in self.as_mut_slice().iter_mut() {
                    if !T::is_nodata(*x) {
                        x.$op_assign_fn(scalar);
                    }
                }
            }
        }

        impl<T> $op_assign_ref_trait for DenseRaster<T>
        where
            T: RasterNum<T>,
        {
            fn $op_assign_fn(&mut self, other: &DenseRaster<T>) {
                raster::assert_dimensions(self, other);

                for (x, &y) in self.as_mut_slice().iter_mut().zip(other.as_slice().iter()) {
                    if T::is_nodata(*x) || T::is_nodata(y) {
                        *x = T::nodata_value();
                    } else {
                        x.$op_assign_fn(y);
                    }
                }
            }
        }

        impl<T> $scalar_op_trait for DenseRaster<T>
        where
            T: RasterNum<T>,
        {
            type Output = DenseRaster<T>;

            fn $op_fn(mut self, scalar: T) -> DenseRaster<T> {
                for x in self.as_mut_slice() {
                    if !T::is_nodata(*x) {
                        x.$op_assign_fn(scalar);
                    }
                }

                self
            }
        }

        impl<T> $scalar_op_trait for &DenseRaster<T>
        where
            T: RasterNum<T>,
        {
            type Output = DenseRaster<T>;

            fn $op_fn(self, scalar: T) -> DenseRaster<T> {
                let mut data = Vec::with_capacity(self.data.len());

                for x in self.as_slice() {
                    if T::is_nodata(*x) {
                        data.push(T::nodata_value());
                    } else {
                        data.push(x.$op_fn(scalar));
                    }
                }

                DenseRaster {
                    metadata: self.metadata.clone(),
                    data,
                }
            }
        }
    };
}

dense_raster_op!(
    std::ops::Add,
    std::ops::Add<T>,
    std::ops::AddAssign,
    std::ops::AddAssign<T>,
    std::ops::AddAssign<&DenseRaster<T>>,
    add,
    add_assign
);
dense_raster_op!(
    std::ops::Sub,
    std::ops::Sub<T>,
    std::ops::SubAssign,
    std::ops::SubAssign<T>,
    std::ops::SubAssign<&DenseRaster<T>>,
    sub,
    sub_assign
);
dense_raster_op!(
    std::ops::Mul,
    std::ops::Mul<T>,
    std::ops::MulAssign,
    std::ops::MulAssign<T>,
    std::ops::MulAssign<&DenseRaster<T>>,
    mul,
    mul_assign
);
dense_raster_op!(
    std::ops::Div,
    std::ops::Div<T>,
    std::ops::DivAssign,
    std::ops::DivAssign<T>,
    std::ops::DivAssign<&DenseRaster<T>>,
    div,
    div_assign
);

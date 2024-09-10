use crate::raster;

use super::{DenseRaster, Raster, RasterNum};

macro_rules! expr {
    ($e:expr) => {
        $e
    };
}

#[macro_export]
macro_rules! dense_raster_op {
    ( $op_trait:path, $scalar_op_trait:path, $op_assign_trait:path, $op_fn:ident, $op:tt, $op_assign:tt) => {
        impl<T> $op_trait for DenseRaster<T>
        where
            T: RasterNum<T>
        {
            type Output = DenseRaster<T>;

            fn $op_fn(mut self, other: DenseRaster<T>) -> DenseRaster<T> {
                raster::assert_dimensions(&self, &other);

                for (x, &y) in self.as_mut_slice().iter_mut().zip(other.as_slice().iter()) {
                    if T::is_nodata(*x) || T::is_nodata(y) {
                        *x = T::nodata_value();
                    } else {
                        expr!(*x $op_assign y);
                    }
                }

                self
            }
        }

        impl<T> $op_trait for &DenseRaster<T>
        where
            T: RasterNum<T>
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
                        data.push(expr!(*x $op *y));
                    }
                }

                DenseRaster { metadata, data }
            }
        }

        impl<T> $scalar_op_trait for DenseRaster<T>
        where
            T: RasterNum<T>
        {
            type Output = DenseRaster<T>;

            fn $op_fn(mut self, scalar: T) -> DenseRaster<T> {
                for x in self.as_mut_slice() {
                    if !T::is_nodata(*x) {
                        expr!(*x $op_assign scalar);
                    }
                }

                self
            }
        }

        impl<T> $scalar_op_trait for &DenseRaster<T>
        where
            T: RasterNum<T>
        {
            type Output = DenseRaster<T>;

            fn $op_fn(self, scalar: T) -> DenseRaster<T> {
                let mut data = Vec::with_capacity(self.data.len());

                for x in self.as_slice() {
                    if T::is_nodata(*x) {
                        data.push(T::nodata_value());
                    } else {
                        data.push(expr!(*x $op scalar));
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

dense_raster_op!(std::ops::Add, std::ops::Add<T>, std::ops::AddAssign, add, +, +=);
dense_raster_op!(std::ops::Sub, std::ops::Sub<T>, std::ops::MulAssign, sub, -, -=);
dense_raster_op!(std::ops::Mul, std::ops::Mul<T>, std::ops::MulAssign, mul, *, *=);
dense_raster_op!(std::ops::Div, std::ops::Div<T>, std::ops::DivAssign, div, /, /=);

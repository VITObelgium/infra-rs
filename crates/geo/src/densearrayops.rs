use crate::{ArrayMetadata, ArrayNum, DenseArray, arrayops};

/// Macro to generate numeric raster operations.
macro_rules! dense_raster_op {
    (   $op_trait:path, // name of the trait e.g. std::ops::Add
        $scalar_op_trait:path, // name of the trait with scalar argument e.g. std::ops::Add<T>
        $op_assign_trait:path, // name of the trait with assignment e.g. std::ops::AddAssign
        $op_assign_scalar_trait:path, // name of the trait with scalar assignment e.g. std::ops::AddAssign<T>
        $op_assign_ref_trait:path, // name of the trait with reference assignment e.g. std::ops::AddAssign<&DenseArray<T>>
        $op_fn:ident, // name of the operation function inside the trait e.g. add
        $op_assign_fn:ident, // name of the assignment function inside the trait e.g. add_assign
        $op_nodata_fn:ident, // name of the operation function with nodata handling e.g. add_nodata_aware
        $op_assign_nodata_fn:ident // name of the assignment function with nodata handling e.g. add_assign_nodata_aware
    ) => {
        impl<T: ArrayNum, Metadata: ArrayMetadata> $op_trait for DenseArray<T, Metadata> {
            type Output = DenseArray<T, Metadata>;

            fn $op_fn(self, other: DenseArray<T, Metadata>) -> DenseArray<T, Metadata> {
                self.binary_mut(&other, |x, y| x.$op_nodata_fn(y))
            }
        }

        impl<T: ArrayNum, Metadata: ArrayMetadata> $op_trait for &DenseArray<T, Metadata> {
            type Output = DenseArray<T, Metadata>;

            fn $op_fn(self, other: &DenseArray<T, Metadata>) -> DenseArray<T, Metadata> {
                #[cfg(feature = "simd")]
                return self.binary_simd(
                    other,
                    |x, y| x.$op_nodata_fn(y),
                    |x, y| paste::paste! { T::[<$op_nodata_fn _simd>](x, y) },
                );
                #[cfg(not(feature = "simd"))]
                return self.binary(other, |x, y| x.$op_nodata_fn(y));
            }
        }

        impl<T: ArrayNum, Metadata: ArrayMetadata> $op_assign_trait for DenseArray<T, Metadata> {
            fn $op_assign_fn(&mut self, other: DenseArray<T, Metadata>) {
                self.binary_inplace(&other, |x, y| {
                    x.$op_assign_nodata_fn(y);
                });
            }
        }

        impl<T: ArrayNum, Metadata: ArrayMetadata> $op_assign_scalar_trait for DenseArray<T, Metadata> {
            fn $op_assign_fn(&mut self, scalar: T) {
                self.unary_inplace(|x| {
                    x.$op_assign_nodata_fn(scalar);
                });
            }
        }

        impl<T: ArrayNum, Metadata: ArrayMetadata> $op_assign_ref_trait for DenseArray<T, Metadata> {
            fn $op_assign_fn(&mut self, other: &DenseArray<T, Metadata>) {
                self.binary_inplace(&other, |x, y| {
                    x.$op_assign_nodata_fn(y);
                });
            }
        }

        impl<T: ArrayNum, Metadata: ArrayMetadata> $scalar_op_trait for DenseArray<T, Metadata> {
            type Output = DenseArray<T, Metadata>;

            fn $op_fn(self, scalar: T) -> DenseArray<T, Metadata> {
                self.unary_mut(|x| x.$op_nodata_fn(scalar))
            }
        }

        impl<T: ArrayNum, Metadata: ArrayMetadata> $scalar_op_trait for &DenseArray<T, Metadata> {
            type Output = DenseArray<T, Metadata>;

            fn $op_fn(self, scalar: T) -> DenseArray<T, Metadata> {
                self.unary(|x| x.$op_nodata_fn(scalar))
            }
        }
    };
}

dense_raster_op!(
    std::ops::Add,
    std::ops::Add<T>,
    std::ops::AddAssign,
    std::ops::AddAssign<T>,
    std::ops::AddAssign<&DenseArray<T, Metadata>>,
    add,
    add_assign,
    add_nodata_aware,
    add_assign_nodata_aware
);
dense_raster_op!(
    std::ops::Sub,
    std::ops::Sub<T>,
    std::ops::SubAssign,
    std::ops::SubAssign<T>,
    std::ops::SubAssign<&DenseArray<T, Metadata>>,
    sub,
    sub_assign,
    sub_nodata_aware,
    sub_assign_nodata_aware
);
dense_raster_op!(
    std::ops::Mul,
    std::ops::Mul<T>,
    std::ops::MulAssign,
    std::ops::MulAssign<T>,
    std::ops::MulAssign<&DenseArray<T, Metadata>>,
    mul,
    mul_assign,
    mul_nodata_aware,
    mul_assign_nodata_aware
);
dense_raster_op!(
    std::ops::Div,
    std::ops::Div<T>,
    std::ops::DivAssign,
    std::ops::DivAssign<T>,
    std::ops::DivAssign<&DenseArray<T, Metadata>>,
    div,
    div_assign,
    div_nodata_aware,
    div_assign_nodata_aware
);

/// Macro to generate numeric inclusive raster operations.
macro_rules! dense_raster_op_inclusive {
    (   $op_trait:path, // name of the trait e.g. std::ops::Add
        $op_assign_trait:path, // name of the trait with assignment e.g. std::ops::AddAssign
        $op_assign_ref_trait:path, // name of the trait with reference assignment e.g. std::ops::AddAssign<&DenseArray<T>>
        $op_fn:ident, // name of the operation function inside the trait e.g. add
        $op_assign_fn:ident, // name of the assignment function inside the trait e.g. add_assign
        $op_nodata_fn:ident, // name of the operation function with nodata handling e.g. add_nodata_aware
        $op_assign_nodata_fn:ident // name of the assignment function with nodata handling e.g. add_assign_nodata_aware
    ) => {
        impl<T: ArrayNum, Metadata: ArrayMetadata> $op_trait for DenseArray<T, Metadata> {
            type Output = DenseArray<T, Metadata>;

            fn $op_fn(mut self, rhs: Self) -> DenseArray<T, Metadata> {
                self.binary_inplace(&rhs, |x, y| x.$op_assign_nodata_fn(y));
                self
            }
        }

        impl<T: ArrayNum, Metadata: ArrayMetadata> $op_trait for &DenseArray<T, Metadata> {
            type Output = DenseArray<T, Metadata>;

            fn $op_fn(self, rhs: Self) -> DenseArray<T, Metadata> {
                self.binary(rhs, |x, y| x.$op_nodata_fn(y))
            }
        }

        impl<T: ArrayNum, Metadata: ArrayMetadata> $op_assign_trait for DenseArray<T, Metadata> {
            fn $op_assign_fn(&mut self, rhs: Self) {
                self.binary_inplace(&rhs, |x, y| x.$op_assign_nodata_fn(y));
            }
        }

        impl<T: ArrayNum, Metadata: ArrayMetadata> $op_assign_ref_trait for DenseArray<T, Metadata> {
            fn $op_assign_fn(&mut self, rhs: &DenseArray<T, Metadata>) {
                self.binary_inplace(rhs, |x, y| x.$op_assign_nodata_fn(y));
            }
        }
    };
}

dense_raster_op_inclusive!(
    arrayops::AddInclusive,
    arrayops::AddAssignInclusive,
    arrayops::AddAssignInclusive<&DenseArray<T, Metadata>>,
    add_inclusive,
    add_assign_inclusive,
    add_inclusive_nodata_aware,
    add_assign_inclusive_nodata_aware
);

dense_raster_op_inclusive!(
    arrayops::SubInclusive,
    arrayops::SubAssignInclusive,
    arrayops::SubAssignInclusive<&DenseArray<T, Metadata>>,
    sub_inclusive,
    sub_assign_inclusive,
    sub_inclusive_nodata_aware,
    sub_assign_inclusive_nodata_aware
);

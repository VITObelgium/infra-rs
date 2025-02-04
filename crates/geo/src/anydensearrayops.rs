use crate::{RasterSize, AnyDenseArray, DenseArray};

fn assert_same_data_type(a: &AnyDenseArray, b: &AnyDenseArray) {
    assert_eq!(
        a.data_type(),
        b.data_type(),
        "AnyDenseArray data types must be the same for performing numeric operations"
    );
}

/// Macro to generate numeric raster operations.
macro_rules! any_dense_raster_op {
    (   $op_trait:path, // name of the trait e.g. std::ops::Add
        $op_assign_trait:path, // name of the trait with assignment e.g. std::ops::AddAssign
        $op_assign_ref_trait:path, // name of the trait with reference assignment e.g. std::ops::AddAssign<&AnyDenseArray>
        $op_fn:ident, // name of the operation function inside the trait e.g. add
        $op_assign_fn:ident, // name of the assignment function inside the trait e.g. add_assign
    ) => {
        impl $op_trait for AnyDenseArray {
            type Output = AnyDenseArray;

            fn $op_fn(self, other: AnyDenseArray) -> AnyDenseArray {
                assert_same_data_type(&self, &other);
                match self {
                    AnyDenseArray::U8(raster) => AnyDenseArray::U8((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::U16(raster) => AnyDenseArray::U16((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::U32(raster) => AnyDenseArray::U32((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::U64(raster) => AnyDenseArray::U64((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::I8(raster) => AnyDenseArray::I8((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::I16(raster) => AnyDenseArray::I16((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::I32(raster) => AnyDenseArray::I32((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::I64(raster) => AnyDenseArray::I64((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::F32(raster) => AnyDenseArray::F32((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::F64(raster) => AnyDenseArray::F64((&raster).$op_fn(&other.try_into().unwrap())),
                }
            }
        }

        impl $op_trait for &AnyDenseArray {
            type Output = AnyDenseArray;

            fn $op_fn(self, other: &AnyDenseArray) -> AnyDenseArray {
                assert_same_data_type(&self, &other);
                match self {
                    AnyDenseArray::U8(raster) => {
                        AnyDenseArray::U8(raster.$op_fn(TryInto::<&DenseArray<u8, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::U16(raster) => {
                        AnyDenseArray::U16(raster.$op_fn(TryInto::<&DenseArray<u16, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::U32(raster) => {
                        AnyDenseArray::U32(raster.$op_fn(TryInto::<&DenseArray<u32, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::U64(raster) => {
                        AnyDenseArray::U64(raster.$op_fn(TryInto::<&DenseArray<u64, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::I8(raster) => {
                        AnyDenseArray::I8(raster.$op_fn(TryInto::<&DenseArray<i8, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::I16(raster) => {
                        AnyDenseArray::I16(raster.$op_fn(TryInto::<&DenseArray<i16, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::I32(raster) => {
                        AnyDenseArray::I32(raster.$op_fn(TryInto::<&DenseArray<i32, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::I64(raster) => {
                        AnyDenseArray::I64(raster.$op_fn(TryInto::<&DenseArray<i64, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::F32(raster) => {
                        AnyDenseArray::F32(raster.$op_fn(TryInto::<&DenseArray<f32, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::F64(raster) => {
                        AnyDenseArray::F64(raster.$op_fn(TryInto::<&DenseArray<f64, RasterSize>>::try_into(other).unwrap()))
                    }
                }
            }
        }

        impl $op_assign_trait for AnyDenseArray {
            fn $op_assign_fn(&mut self, other: AnyDenseArray) {
                assert_same_data_type(self, &other);
                match self {
                    AnyDenseArray::U8(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseArray::U16(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseArray::U32(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseArray::U64(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseArray::I8(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseArray::I16(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseArray::I32(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseArray::I64(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseArray::F32(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseArray::F64(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                }
            }
        }

        impl $op_assign_ref_trait for AnyDenseArray {
            fn $op_assign_fn(&mut self, other: &AnyDenseArray) {
                assert_same_data_type(self, &other);
                match self {
                    AnyDenseArray::U8(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u8, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::U16(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u16, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::U32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u32, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::U64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u64, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::I8(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i8, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::I16(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i16, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::I32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i32, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::I64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i64, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::F32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<f32, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::F64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<f64, RasterSize>>::try_into(other).unwrap()),
                }
            }
        }
    };
}

macro_rules! any_dense_raster_inclusive_op {
    (   $op_trait:path, // name of the trait e.g. ops::AddInclusive
        $op_assign_trait:path, // name of the trait with assignment e.g. ops::AddAssignInclusive
        $op_assign_ref_trait:path, // name of the trait with reference assignment e.g. std::ops::AddAssign<&AnyDenseArray>
        $op_fn:ident, // name of the operation function inside the trait e.g. add_inclusive
        $op_assign_fn:ident, // name of the assignment function inside the trait e.g. add_assign_inclusive
    ) => {
        impl $op_trait for AnyDenseArray {
            type Output = AnyDenseArray;

            fn $op_fn(self, other: AnyDenseArray) -> AnyDenseArray {
                assert_same_data_type(&self, &other);
                match self {
                    AnyDenseArray::U8(raster) => AnyDenseArray::U8((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::U16(raster) => AnyDenseArray::U16((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::U32(raster) => AnyDenseArray::U32((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::U64(raster) => AnyDenseArray::U64((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::I8(raster) => AnyDenseArray::I8((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::I16(raster) => AnyDenseArray::I16((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::I32(raster) => AnyDenseArray::I32((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::I64(raster) => AnyDenseArray::I64((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::F32(raster) => AnyDenseArray::F32((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseArray::F64(raster) => AnyDenseArray::F64((&raster).$op_fn(&other.try_into().unwrap())),
                }
            }
        }

        impl $op_trait for &AnyDenseArray {
            type Output = AnyDenseArray;

            fn $op_fn(self, other: &AnyDenseArray) -> AnyDenseArray {
                assert_same_data_type(&self, &other);
                match self {
                    AnyDenseArray::U8(raster) => {
                        AnyDenseArray::U8((&raster).$op_fn(TryInto::<&DenseArray<u8, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::U16(raster) => {
                        AnyDenseArray::U16((&raster).$op_fn(TryInto::<&DenseArray<u16, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::U32(raster) => {
                        AnyDenseArray::U32((&raster).$op_fn(TryInto::<&DenseArray<u32, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::U64(raster) => {
                        AnyDenseArray::U64((&raster).$op_fn(TryInto::<&DenseArray<u64, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::I8(raster) => {
                        AnyDenseArray::I8((&raster).$op_fn(TryInto::<&DenseArray<i8, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::I16(raster) => {
                        AnyDenseArray::I16((&raster).$op_fn(TryInto::<&DenseArray<i16, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::I32(raster) => {
                        AnyDenseArray::I32((&raster).$op_fn(TryInto::<&DenseArray<i32, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::I64(raster) => {
                        AnyDenseArray::I64((&raster).$op_fn(TryInto::<&DenseArray<i64, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::F32(raster) => {
                        AnyDenseArray::F32((&raster).$op_fn(TryInto::<&DenseArray<f32, RasterSize>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::F64(raster) => {
                        AnyDenseArray::F64((&raster).$op_fn(TryInto::<&DenseArray<f64, RasterSize>>::try_into(other).unwrap()))
                    }
                }
            }
        }

        impl $op_assign_trait for AnyDenseArray {
            fn $op_assign_fn(&mut self, other: AnyDenseArray) {
                assert_same_data_type(self, &other);
                println!("self");

                match self {
                    AnyDenseArray::U8(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u8, RasterSize>>::try_into(&other).unwrap()),
                    AnyDenseArray::U16(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u16, RasterSize>>::try_into(&other).unwrap()),
                    AnyDenseArray::U32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u32, RasterSize>>::try_into(&other).unwrap()),
                    AnyDenseArray::U64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u64, RasterSize>>::try_into(&other).unwrap()),
                    AnyDenseArray::I8(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i8, RasterSize>>::try_into(&other).unwrap()),
                    AnyDenseArray::I16(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i16, RasterSize>>::try_into(&other).unwrap()),
                    AnyDenseArray::I32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i32, RasterSize>>::try_into(&other).unwrap()),
                    AnyDenseArray::I64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i64, RasterSize>>::try_into(&other).unwrap()),
                    AnyDenseArray::F32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<f32, RasterSize>>::try_into(&other).unwrap()),
                    AnyDenseArray::F64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<f64, RasterSize>>::try_into(&other).unwrap()),
                }
            }
        }

        impl $op_assign_ref_trait for AnyDenseArray {
            fn $op_assign_fn(&mut self, other: &AnyDenseArray) {
                assert_same_data_type(self, &other);
                match self {
                    AnyDenseArray::U8(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u8, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::U16(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u16, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::U32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u32, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::U64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u64, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::I8(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i8, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::I16(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i16, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::I32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i32, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::I64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i64, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::F32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<f32, RasterSize>>::try_into(other).unwrap()),
                    AnyDenseArray::F64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<f64, RasterSize>>::try_into(other).unwrap()),
                }
            }
        }
    };
}

any_dense_raster_op!(
    std::ops::Add,
    std::ops::AddAssign,
    std::ops::AddAssign<&AnyDenseArray>,
    add,
    add_assign,
);

any_dense_raster_inclusive_op!(
    crate::arrayops::AddInclusive,
    crate::arrayops::AddAssignInclusive,
    crate::arrayops::AddAssignInclusive<&AnyDenseArray>,
    add_inclusive,
    add_assign_inclusive,
);

any_dense_raster_op!(
    std::ops::Sub,
    std::ops::SubAssign,
    std::ops::SubAssign<&AnyDenseArray>,
    sub,
    sub_assign,
);

any_dense_raster_inclusive_op!(
    crate::arrayops::SubInclusive,
    crate::arrayops::SubAssignInclusive,
    crate::arrayops::SubAssignInclusive<&AnyDenseArray>,
    sub_inclusive,
    sub_assign_inclusive,
);

any_dense_raster_op!(
    std::ops::Mul,
    std::ops::MulAssign,
    std::ops::MulAssign<&AnyDenseArray>,
    mul,
    mul_assign,
);
any_dense_raster_op!(
    std::ops::Div,
    std::ops::DivAssign,
    std::ops::DivAssign<&AnyDenseArray>,
    div,
    div_assign,
);

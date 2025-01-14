use crate::DenseRaster;

use super::AnyDenseRaster;

fn assert_same_data_type(a: &AnyDenseRaster, b: &AnyDenseRaster) {
    assert_eq!(
        a.data_type(),
        b.data_type(),
        "AnyDenseRaster data types must be the same for performing numeric operations"
    );
}

/// Macro to generate numeric raster operations.
macro_rules! any_dense_raster_op {
    (   $op_trait:path, // name of the trait e.g. std::ops::Add
        $op_assign_trait:path, // name of the trait with assignment e.g. std::ops::AddAssign
        $op_assign_ref_trait:path, // name of the trait with reference assignment e.g. std::ops::AddAssign<&AnyDenseRaster>
        $op_fn:ident, // name of the operation function inside the trait e.g. add
        $op_assign_fn:ident, // name of the assignment function inside the trait e.g. add_assign
    ) => {
        impl $op_trait for AnyDenseRaster {
            type Output = AnyDenseRaster;

            fn $op_fn(self, other: AnyDenseRaster) -> AnyDenseRaster {
                assert_same_data_type(&self, &other);
                match self {
                    AnyDenseRaster::U8(raster) => AnyDenseRaster::U8((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::U16(raster) => AnyDenseRaster::U16((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::U32(raster) => AnyDenseRaster::U32((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::U64(raster) => AnyDenseRaster::U64((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::I8(raster) => AnyDenseRaster::I8((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::I16(raster) => AnyDenseRaster::I16((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::I32(raster) => AnyDenseRaster::I32((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::I64(raster) => AnyDenseRaster::I64((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::F32(raster) => AnyDenseRaster::F32((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::F64(raster) => AnyDenseRaster::F64((&raster).$op_fn(&other.try_into().unwrap())),
                }
            }
        }

        impl $op_trait for &AnyDenseRaster {
            type Output = AnyDenseRaster;

            fn $op_fn(self, other: &AnyDenseRaster) -> AnyDenseRaster {
                assert_same_data_type(&self, &other);
                match self {
                    AnyDenseRaster::U8(raster) => {
                        AnyDenseRaster::U8(raster.$op_fn(TryInto::<&DenseRaster<u8>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::U16(raster) => {
                        AnyDenseRaster::U16(raster.$op_fn(TryInto::<&DenseRaster<u16>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::U32(raster) => {
                        AnyDenseRaster::U32(raster.$op_fn(TryInto::<&DenseRaster<u32>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::U64(raster) => {
                        AnyDenseRaster::U64(raster.$op_fn(TryInto::<&DenseRaster<u64>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::I8(raster) => {
                        AnyDenseRaster::I8(raster.$op_fn(TryInto::<&DenseRaster<i8>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::I16(raster) => {
                        AnyDenseRaster::I16(raster.$op_fn(TryInto::<&DenseRaster<i16>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::I32(raster) => {
                        AnyDenseRaster::I32(raster.$op_fn(TryInto::<&DenseRaster<i32>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::I64(raster) => {
                        AnyDenseRaster::I64(raster.$op_fn(TryInto::<&DenseRaster<i64>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::F32(raster) => {
                        AnyDenseRaster::F32(raster.$op_fn(TryInto::<&DenseRaster<f32>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::F64(raster) => {
                        AnyDenseRaster::F64(raster.$op_fn(TryInto::<&DenseRaster<f64>>::try_into(other).unwrap()))
                    }
                }
            }
        }

        impl $op_assign_trait for AnyDenseRaster {
            fn $op_assign_fn(&mut self, other: AnyDenseRaster) {
                assert_same_data_type(self, &other);
                match self {
                    AnyDenseRaster::U8(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseRaster::U16(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseRaster::U32(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseRaster::U64(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseRaster::I8(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseRaster::I16(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseRaster::I32(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseRaster::I64(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseRaster::F32(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                    AnyDenseRaster::F64(raster) => raster.$op_assign_fn(&other.try_into().unwrap()),
                }
            }
        }

        impl $op_assign_ref_trait for AnyDenseRaster {
            fn $op_assign_fn(&mut self, other: &AnyDenseRaster) {
                assert_same_data_type(self, &other);
                match self {
                    AnyDenseRaster::U8(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<u8>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::U16(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<u16>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::U32(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<u32>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::U64(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<u64>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::I8(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<i8>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::I16(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<i16>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::I32(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<i32>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::I64(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<i64>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::F32(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<f32>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::F64(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<f64>>::try_into(other).unwrap())
                    }
                }
            }
        }
    };
}

macro_rules! any_dense_raster_inclusive_op {
    (   $op_trait:path, // name of the trait e.g. ops::AddInclusive
        $op_assign_trait:path, // name of the trait with assignment e.g. ops::AddAssignInclusive
        $op_assign_ref_trait:path, // name of the trait with reference assignment e.g. std::ops::AddAssign<&AnyDenseRaster>
        $op_fn:ident, // name of the operation function inside the trait e.g. add_inclusive
        $op_assign_fn:ident, // name of the assignment function inside the trait e.g. add_assign_inclusive
    ) => {
        impl $op_trait for AnyDenseRaster {
            type Output = AnyDenseRaster;

            fn $op_fn(self, other: AnyDenseRaster) -> AnyDenseRaster {
                assert_same_data_type(&self, &other);
                match self {
                    AnyDenseRaster::U8(raster) => AnyDenseRaster::U8((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::U16(raster) => AnyDenseRaster::U16((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::U32(raster) => AnyDenseRaster::U32((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::U64(raster) => AnyDenseRaster::U64((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::I8(raster) => AnyDenseRaster::I8((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::I16(raster) => AnyDenseRaster::I16((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::I32(raster) => AnyDenseRaster::I32((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::I64(raster) => AnyDenseRaster::I64((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::F32(raster) => AnyDenseRaster::F32((&raster).$op_fn(&other.try_into().unwrap())),
                    AnyDenseRaster::F64(raster) => AnyDenseRaster::F64((&raster).$op_fn(&other.try_into().unwrap())),
                }
            }
        }

        impl $op_trait for &AnyDenseRaster {
            type Output = AnyDenseRaster;

            fn $op_fn(self, other: &AnyDenseRaster) -> AnyDenseRaster {
                assert_same_data_type(&self, &other);
                match self {
                    AnyDenseRaster::U8(raster) => {
                        AnyDenseRaster::U8((&raster).$op_fn(TryInto::<&DenseRaster<u8>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::U16(raster) => {
                        AnyDenseRaster::U16((&raster).$op_fn(TryInto::<&DenseRaster<u16>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::U32(raster) => {
                        AnyDenseRaster::U32((&raster).$op_fn(TryInto::<&DenseRaster<u32>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::U64(raster) => {
                        AnyDenseRaster::U64((&raster).$op_fn(TryInto::<&DenseRaster<u64>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::I8(raster) => {
                        AnyDenseRaster::I8((&raster).$op_fn(TryInto::<&DenseRaster<i8>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::I16(raster) => {
                        AnyDenseRaster::I16((&raster).$op_fn(TryInto::<&DenseRaster<i16>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::I32(raster) => {
                        AnyDenseRaster::I32((&raster).$op_fn(TryInto::<&DenseRaster<i32>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::I64(raster) => {
                        AnyDenseRaster::I64((&raster).$op_fn(TryInto::<&DenseRaster<i64>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::F32(raster) => {
                        AnyDenseRaster::F32((&raster).$op_fn(TryInto::<&DenseRaster<f32>>::try_into(other).unwrap()))
                    }
                    AnyDenseRaster::F64(raster) => {
                        AnyDenseRaster::F64((&raster).$op_fn(TryInto::<&DenseRaster<f64>>::try_into(other).unwrap()))
                    }
                }
            }
        }

        impl $op_assign_trait for AnyDenseRaster {
            fn $op_assign_fn(&mut self, other: AnyDenseRaster) {
                assert_same_data_type(self, &other);
                println!("self");

                match self {
                    AnyDenseRaster::U8(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<u8>>::try_into(&other).unwrap())
                    }
                    AnyDenseRaster::U16(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<u16>>::try_into(&other).unwrap())
                    }
                    AnyDenseRaster::U32(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<u32>>::try_into(&other).unwrap())
                    }
                    AnyDenseRaster::U64(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<u64>>::try_into(&other).unwrap())
                    }
                    AnyDenseRaster::I8(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<i8>>::try_into(&other).unwrap())
                    }
                    AnyDenseRaster::I16(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<i16>>::try_into(&other).unwrap())
                    }
                    AnyDenseRaster::I32(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<i32>>::try_into(&other).unwrap())
                    }
                    AnyDenseRaster::I64(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<i64>>::try_into(&other).unwrap())
                    }
                    AnyDenseRaster::F32(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<f32>>::try_into(&other).unwrap())
                    }
                    AnyDenseRaster::F64(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<f64>>::try_into(&other).unwrap())
                    }
                }
            }
        }

        impl $op_assign_ref_trait for AnyDenseRaster {
            fn $op_assign_fn(&mut self, other: &AnyDenseRaster) {
                assert_same_data_type(self, &other);
                match self {
                    AnyDenseRaster::U8(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<u8>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::U16(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<u16>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::U32(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<u32>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::U64(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<u64>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::I8(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<i8>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::I16(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<i16>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::I32(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<i32>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::I64(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<i64>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::F32(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<f32>>::try_into(other).unwrap())
                    }
                    AnyDenseRaster::F64(raster) => {
                        raster.$op_assign_fn(TryInto::<&DenseRaster<f64>>::try_into(other).unwrap())
                    }
                }
            }
        }
    };
}

any_dense_raster_op!(
    std::ops::Add,
    std::ops::AddAssign,
    std::ops::AddAssign<&AnyDenseRaster>,
    add,
    add_assign,
);

any_dense_raster_inclusive_op!(
    crate::ops::AddInclusive,
    crate::ops::AddAssignInclusive,
    crate::ops::AddAssignInclusive<&AnyDenseRaster>,
    add_inclusive,
    add_assign_inclusive,
);

any_dense_raster_op!(
    std::ops::Sub,
    std::ops::SubAssign,
    std::ops::SubAssign<&AnyDenseRaster>,
    sub,
    sub_assign,
);

any_dense_raster_inclusive_op!(
    crate::ops::SubInclusive,
    crate::ops::SubAssignInclusive,
    crate::ops::SubAssignInclusive<&AnyDenseRaster>,
    sub_inclusive,
    sub_assign_inclusive,
);

any_dense_raster_op!(
    std::ops::Mul,
    std::ops::MulAssign,
    std::ops::MulAssign<&AnyDenseRaster>,
    mul,
    mul_assign,
);
any_dense_raster_op!(
    std::ops::Div,
    std::ops::DivAssign,
    std::ops::DivAssign<&AnyDenseRaster>,
    div,
    div_assign,
);

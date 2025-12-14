use crate::{AnyDenseArray, ArrayDataType, ArrayMetadata, DenseArray};

fn assert_same_data_type<Metadata: ArrayMetadata>(a: &AnyDenseArray<Metadata>, b: &AnyDenseArray<Metadata>) {
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
        impl<Metadata: ArrayMetadata> $op_trait for AnyDenseArray<Metadata> {
            type Output = AnyDenseArray<Metadata>;

            fn $op_fn(self, other: AnyDenseArray<Metadata>) -> AnyDenseArray<Metadata> {
                let output_type = if stringify!($op_fn) == "div" {
                    output_type_for_inputs_division(self.data_type(), other.data_type())
                } else {
                    output_type_for_inputs(self.data_type(), other.data_type())
                };

                let lhs = self.cast(output_type);
                let rhs = other.cast(output_type);

                match output_type {
                    ArrayDataType::Uint8 => AnyDenseArray::U8(lhs.as_densearray_ref::<u8>().$op_fn(rhs.as_densearray_ref::<u8>())),
                    ArrayDataType::Uint16 => AnyDenseArray::U16(lhs.as_densearray_ref::<u16>().$op_fn(rhs.as_densearray_ref::<u16>())),
                    ArrayDataType::Uint32 => AnyDenseArray::U32(lhs.as_densearray_ref::<u32>().$op_fn(rhs.as_densearray_ref::<u32>())),
                    ArrayDataType::Uint64 => AnyDenseArray::U64(lhs.as_densearray_ref::<u64>().$op_fn(rhs.as_densearray_ref::<u64>())),
                    ArrayDataType::Int8 => AnyDenseArray::I8(lhs.as_densearray_ref::<i8>().$op_fn(rhs.as_densearray_ref::<i8>())),
                    ArrayDataType::Int16 => AnyDenseArray::I16(lhs.as_densearray_ref::<i16>().$op_fn(rhs.as_densearray_ref::<i16>())),
                    ArrayDataType::Int32 => AnyDenseArray::I32(lhs.as_densearray_ref::<i32>().$op_fn(rhs.as_densearray_ref::<i32>())),
                    ArrayDataType::Int64 => AnyDenseArray::I64(lhs.as_densearray_ref::<i64>().$op_fn(rhs.as_densearray_ref::<i64>())),
                    ArrayDataType::Float32 => AnyDenseArray::F32(lhs.as_densearray_ref::<f32>().$op_fn(rhs.as_densearray_ref::<f32>())),
                    ArrayDataType::Float64 => AnyDenseArray::F64(lhs.as_densearray_ref::<f64>().$op_fn(rhs.as_densearray_ref::<f64>())),
                }
            }
        }

        impl<Metadata: ArrayMetadata> $op_trait for &AnyDenseArray<Metadata> {
            type Output = AnyDenseArray<Metadata>;

            fn $op_fn(self, other: &AnyDenseArray<Metadata>) -> AnyDenseArray<Metadata> {
                assert_same_data_type(&self, &other);
                match self {
                    AnyDenseArray::U8(raster) => {
                        AnyDenseArray::U8(raster.$op_fn(TryInto::<&DenseArray<u8, Metadata>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::U16(raster) => {
                        AnyDenseArray::U16(raster.$op_fn(TryInto::<&DenseArray<u16, Metadata>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::U32(raster) => {
                        AnyDenseArray::U32(raster.$op_fn(TryInto::<&DenseArray<u32, Metadata>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::U64(raster) => {
                        AnyDenseArray::U64(raster.$op_fn(TryInto::<&DenseArray<u64, Metadata>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::I8(raster) => {
                        AnyDenseArray::I8(raster.$op_fn(TryInto::<&DenseArray<i8, Metadata>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::I16(raster) => {
                        AnyDenseArray::I16(raster.$op_fn(TryInto::<&DenseArray<i16, Metadata>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::I32(raster) => {
                        AnyDenseArray::I32(raster.$op_fn(TryInto::<&DenseArray<i32, Metadata>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::I64(raster) => {
                        AnyDenseArray::I64(raster.$op_fn(TryInto::<&DenseArray<i64, Metadata>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::F32(raster) => {
                        AnyDenseArray::F32(raster.$op_fn(TryInto::<&DenseArray<f32, Metadata>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::F64(raster) => {
                        AnyDenseArray::F64(raster.$op_fn(TryInto::<&DenseArray<f64, Metadata>>::try_into(other).unwrap()))
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
                    AnyDenseArray::U8(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u8, _>>::try_into(other).unwrap()),
                    AnyDenseArray::U16(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u16, _>>::try_into(other).unwrap()),
                    AnyDenseArray::U32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u32, _>>::try_into(other).unwrap()),
                    AnyDenseArray::U64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u64, _>>::try_into(other).unwrap()),
                    AnyDenseArray::I8(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i8, _>>::try_into(other).unwrap()),
                    AnyDenseArray::I16(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i16, _>>::try_into(other).unwrap()),
                    AnyDenseArray::I32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i32, _>>::try_into(other).unwrap()),
                    AnyDenseArray::I64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i64, _>>::try_into(other).unwrap()),
                    AnyDenseArray::F32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<f32, _>>::try_into(other).unwrap()),
                    AnyDenseArray::F64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<f64, _>>::try_into(other).unwrap()),
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
                        AnyDenseArray::U8((&raster).$op_fn(TryInto::<&DenseArray<u8, _>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::U16(raster) => {
                        AnyDenseArray::U16((&raster).$op_fn(TryInto::<&DenseArray<u16, _>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::U32(raster) => {
                        AnyDenseArray::U32((&raster).$op_fn(TryInto::<&DenseArray<u32, _>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::U64(raster) => {
                        AnyDenseArray::U64((&raster).$op_fn(TryInto::<&DenseArray<u64, _>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::I8(raster) => {
                        AnyDenseArray::I8((&raster).$op_fn(TryInto::<&DenseArray<i8, _>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::I16(raster) => {
                        AnyDenseArray::I16((&raster).$op_fn(TryInto::<&DenseArray<i16, _>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::I32(raster) => {
                        AnyDenseArray::I32((&raster).$op_fn(TryInto::<&DenseArray<i32, _>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::I64(raster) => {
                        AnyDenseArray::I64((&raster).$op_fn(TryInto::<&DenseArray<i64, _>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::F32(raster) => {
                        AnyDenseArray::F32((&raster).$op_fn(TryInto::<&DenseArray<f32, _>>::try_into(other).unwrap()))
                    }
                    AnyDenseArray::F64(raster) => {
                        AnyDenseArray::F64((&raster).$op_fn(TryInto::<&DenseArray<f64, _>>::try_into(other).unwrap()))
                    }
                }
            }
        }

        impl $op_assign_trait for AnyDenseArray {
            fn $op_assign_fn(&mut self, other: AnyDenseArray) {
                assert_same_data_type(self, &other);
                println!("self");

                match self {
                    AnyDenseArray::U8(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u8, _>>::try_into(&other).unwrap()),
                    AnyDenseArray::U16(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u16, _>>::try_into(&other).unwrap()),
                    AnyDenseArray::U32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u32, _>>::try_into(&other).unwrap()),
                    AnyDenseArray::U64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u64, _>>::try_into(&other).unwrap()),
                    AnyDenseArray::I8(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i8, _>>::try_into(&other).unwrap()),
                    AnyDenseArray::I16(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i16, _>>::try_into(&other).unwrap()),
                    AnyDenseArray::I32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i32, _>>::try_into(&other).unwrap()),
                    AnyDenseArray::I64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i64, _>>::try_into(&other).unwrap()),
                    AnyDenseArray::F32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<f32, _>>::try_into(&other).unwrap()),
                    AnyDenseArray::F64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<f64, _>>::try_into(&other).unwrap()),
                }
            }
        }

        impl $op_assign_ref_trait for AnyDenseArray {
            fn $op_assign_fn(&mut self, other: &AnyDenseArray) {
                assert_same_data_type(self, &other);
                match self {
                    AnyDenseArray::U8(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u8, _>>::try_into(other).unwrap()),
                    AnyDenseArray::U16(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u16, _>>::try_into(other).unwrap()),
                    AnyDenseArray::U32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u32, _>>::try_into(other).unwrap()),
                    AnyDenseArray::U64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<u64, _>>::try_into(other).unwrap()),
                    AnyDenseArray::I8(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i8, _>>::try_into(other).unwrap()),
                    AnyDenseArray::I16(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i16, _>>::try_into(other).unwrap()),
                    AnyDenseArray::I32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i32, _>>::try_into(other).unwrap()),
                    AnyDenseArray::I64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<i64, _>>::try_into(other).unwrap()),
                    AnyDenseArray::F32(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<f32, _>>::try_into(other).unwrap()),
                    AnyDenseArray::F64(raster) => raster.$op_assign_fn(TryInto::<&DenseArray<f64, _>>::try_into(other).unwrap()),
                }
            }
        }
    };
}

fn output_type_for_inputs(data_type_1: ArrayDataType, data_type_2: ArrayDataType) -> ArrayDataType {
    if data_type_1 == data_type_2 {
        return data_type_1;
    }

    match (data_type_1, data_type_2) {
        (ArrayDataType::Int8 | ArrayDataType::Int16 | ArrayDataType::Int32, _) => combined_output_signed(data_type_2, false),
        (ArrayDataType::Uint8 | ArrayDataType::Uint16 | ArrayDataType::Uint32, _) => combined_output_unsigned(data_type_2, false),
        (ArrayDataType::Int64, _) => combined_output_signed(data_type_2, true),
        (ArrayDataType::Uint64, _) => combined_output_unsigned(data_type_2, true),
        (ArrayDataType::Float32, _) => combined_output_f32(data_type_2),
        (ArrayDataType::Float64, _) => ArrayDataType::Float64,
    }
}

fn output_type_for_inputs_division(data_type_1: ArrayDataType, data_type_2: ArrayDataType) -> ArrayDataType {
    if (data_type_1 != ArrayDataType::Float64 || data_type_2 != ArrayDataType::Float64)
        && (data_type_1 == ArrayDataType::Float32 || data_type_2 == ArrayDataType::Float32)
    {
        // don't upgrade to float64 if one of the inputs is float32 and no float64 inputs are present
        return ArrayDataType::Float32;
    }

    ArrayDataType::Float64
}

fn combined_output_signed(other: ArrayDataType, wide: bool) -> ArrayDataType {
    match other {
        ArrayDataType::Int8
        | ArrayDataType::Int16
        | ArrayDataType::Int32
        | ArrayDataType::Int64
        | ArrayDataType::Uint8
        | ArrayDataType::Uint16
        | ArrayDataType::Uint32
        | ArrayDataType::Uint64 => {
            if wide {
                ArrayDataType::Int64
            } else {
                ArrayDataType::Int32
            }
        }
        ArrayDataType::Float32 => ArrayDataType::Float32,
        ArrayDataType::Float64 => ArrayDataType::Float64,
    }
}

fn combined_output_unsigned(other: ArrayDataType, wide: bool) -> ArrayDataType {
    match other {
        ArrayDataType::Int8 | ArrayDataType::Int16 | ArrayDataType::Int32 | ArrayDataType::Int64 => {
            if wide {
                ArrayDataType::Int64
            } else {
                ArrayDataType::Int32
            }
        }
        ArrayDataType::Uint8 | ArrayDataType::Uint16 | ArrayDataType::Uint32 | ArrayDataType::Uint64 => {
            if wide {
                ArrayDataType::Uint64
            } else {
                ArrayDataType::Uint32
            }
        }
        ArrayDataType::Float32 => ArrayDataType::Float32,
        ArrayDataType::Float64 => ArrayDataType::Float64,
    }
}

fn combined_output_f32(other: ArrayDataType) -> ArrayDataType {
    match other {
        ArrayDataType::Int64 | ArrayDataType::Uint64 | ArrayDataType::Float64 => ArrayDataType::Float64,
        _ => ArrayDataType::Float32,
    }
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

#[cfg(test)]
mod tests {

    use inf::allocate;

    use crate::{
        Array, ArrayDataType, RasterMetadata, RasterSize,
        array::{Columns, Rows},
    };

    use super::*;

    #[test]
    fn division_output_type() {
        const TILE_WIDTH: Columns = Columns(2);
        const TILE_HEIGHT: Rows = Rows(2);

        let int_raster1 = AnyDenseArray::U32(
            DenseArray::new(
                RasterMetadata::sized_for_type::<u32>(RasterSize::with_rows_cols(TILE_HEIGHT, TILE_WIDTH)),
                allocate::aligned_vec_from_iter::<u32, _>(0..(TILE_WIDTH * TILE_HEIGHT) as u32),
            )
            .unwrap(),
        );

        let int_raster2 = AnyDenseArray::U32(
            DenseArray::new(
                RasterMetadata::sized_for_type::<u32>(RasterSize::with_rows_cols(TILE_HEIGHT, TILE_WIDTH)),
                allocate::aligned_vec_from_iter::<u32, _>(0..(TILE_WIDTH * TILE_HEIGHT) as u32),
            )
            .unwrap(),
        );

        let float32_raster = int_raster1.cast(ArrayDataType::Float32);

        {
            let result = int_raster1.clone() / int_raster2.clone();
            assert_eq!(result.data_type(), ArrayDataType::Float64);
        }

        {
            let result = int_raster1.clone() / float32_raster.clone();
            assert_eq!(result.data_type(), ArrayDataType::Float32);
        }

        {
            let result = float32_raster.clone() / int_raster1.clone();
            assert_eq!(result.data_type(), ArrayDataType::Float32);
        }
    }
}

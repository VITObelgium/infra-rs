use arrow::datatypes::{
    ArrowPrimitiveType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};

pub trait ArrowType {
    type TArrow: ArrowPrimitiveType;
}

impl ArrowType for f32 {
    type TArrow = Float32Type;
}

impl ArrowType for f64 {
    type TArrow = Float64Type;
}

impl ArrowType for u8 {
    type TArrow = UInt8Type;
}

impl ArrowType for u16 {
    type TArrow = UInt16Type;
}

impl ArrowType for u32 {
    type TArrow = UInt32Type;
}

impl ArrowType for u64 {
    type TArrow = UInt64Type;
}

impl ArrowType for i8 {
    type TArrow = Int8Type;
}

impl ArrowType for i16 {
    type TArrow = Int16Type;
}

impl ArrowType for i32 {
    type TArrow = Int32Type;
}

impl ArrowType for i64 {
    type TArrow = Int64Type;
}

use arrow::datatypes::{
    ArrowPrimitiveType, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};

pub trait ArrowType {
    type TArrow: ArrowPrimitiveType;

    fn arrow_data_type() -> arrow::datatypes::DataType;
}

impl ArrowType for f32 {
    type TArrow = Float32Type;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Float32
    }
}

impl ArrowType for f64 {
    type TArrow = Float64Type;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Float64
    }
}

impl ArrowType for u8 {
    type TArrow = UInt8Type;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::UInt8
    }
}

impl ArrowType for u16 {
    type TArrow = UInt16Type;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::UInt16
    }
}

impl ArrowType for u32 {
    type TArrow = UInt32Type;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::UInt32
    }
}

impl ArrowType for u64 {
    type TArrow = UInt64Type;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::UInt64
    }
}

impl ArrowType for i8 {
    type TArrow = Int8Type;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Int8
    }
}

impl ArrowType for i16 {
    type TArrow = Int16Type;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Int16
    }
}

impl ArrowType for i32 {
    type TArrow = Int32Type;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Int32
    }
}

impl ArrowType for i64 {
    type TArrow = Int64Type;

    fn arrow_data_type() -> arrow::datatypes::DataType {
        arrow::datatypes::DataType::Int64
    }
}

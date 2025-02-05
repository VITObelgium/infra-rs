use inf::cast;

use crate::{
    array::{Columns, Rows},
    Array, ArrayDataType, ArrayMetadata, ArrayNum, Cell, DenseArray, Error, RasterSize, Result,
};

/// Type erased `RasterTile`
#[derive(Clone)]
pub enum AnyDenseArray<Metadata: ArrayMetadata = RasterSize> {
    U8(DenseArray<u8, Metadata>),
    U16(DenseArray<u16, Metadata>),
    U32(DenseArray<u32, Metadata>),
    U64(DenseArray<u64, Metadata>),
    I8(DenseArray<i8, Metadata>),
    I16(DenseArray<i16, Metadata>),
    I32(DenseArray<i32, Metadata>),
    I64(DenseArray<i64, Metadata>),
    F32(DenseArray<f32, Metadata>),
    F64(DenseArray<f64, Metadata>),
}

#[macro_export]
macro_rules! unerase_raster_type_op {
    ( $raster_op:ident, $ret:path ) => {
        pub fn $raster_op(&self) -> $ret {
            match self {
                AnyDenseArray::U8(raster) => raster.$raster_op(),
                AnyDenseArray::U16(raster) => raster.$raster_op(),
                AnyDenseArray::U32(raster) => raster.$raster_op(),
                AnyDenseArray::U64(raster) => raster.$raster_op(),
                AnyDenseArray::I8(raster) => raster.$raster_op(),
                AnyDenseArray::I16(raster) => raster.$raster_op(),
                AnyDenseArray::I32(raster) => raster.$raster_op(),
                AnyDenseArray::I64(raster) => raster.$raster_op(),
                AnyDenseArray::F32(raster) => raster.$raster_op(),
                AnyDenseArray::F64(raster) => raster.$raster_op(),
            }
        }
    };
}

#[macro_export]
macro_rules! unerase_raster_type_op_ref {
    ( $raster_op:ident, $ret:path ) => {
        pub fn $raster_op(&self) -> &$ret {
            match self {
                AnyDenseArray::U8(raster) => raster.$raster_op(),
                AnyDenseArray::U16(raster) => raster.$raster_op(),
                AnyDenseArray::U32(raster) => raster.$raster_op(),
                AnyDenseArray::U64(raster) => raster.$raster_op(),
                AnyDenseArray::I8(raster) => raster.$raster_op(),
                AnyDenseArray::I16(raster) => raster.$raster_op(),
                AnyDenseArray::I32(raster) => raster.$raster_op(),
                AnyDenseArray::I64(raster) => raster.$raster_op(),
                AnyDenseArray::F32(raster) => raster.$raster_op(),
                AnyDenseArray::F64(raster) => raster.$raster_op(),
            }
        }
    };
}

impl<Metadata: ArrayMetadata> AnyDenseArray<Metadata> {
    unerase_raster_type_op!(rows, Rows);
    unerase_raster_type_op!(columns, Columns);
    unerase_raster_type_op_ref!(metadata, Metadata);

    pub fn filled_with(fill: Option<f64>, metadata: Metadata, datatype: ArrayDataType) -> Self {
        match datatype {
            ArrayDataType::Uint8 => AnyDenseArray::U8(DenseArray::filled_with(cast::option::<u8>(fill), metadata)),
            ArrayDataType::Uint16 => AnyDenseArray::U16(DenseArray::filled_with(cast::option::<u16>(fill), metadata)),
            ArrayDataType::Uint32 => AnyDenseArray::U32(DenseArray::filled_with(cast::option::<u32>(fill), metadata)),
            ArrayDataType::Uint64 => AnyDenseArray::U64(DenseArray::filled_with(cast::option::<u64>(fill), metadata)),
            ArrayDataType::Int8 => AnyDenseArray::I8(DenseArray::filled_with(cast::option::<i8>(fill), metadata)),
            ArrayDataType::Int16 => AnyDenseArray::I16(DenseArray::filled_with(cast::option::<i16>(fill), metadata)),
            ArrayDataType::Int32 => AnyDenseArray::I32(DenseArray::filled_with(cast::option::<i32>(fill), metadata)),
            ArrayDataType::Int64 => AnyDenseArray::I64(DenseArray::filled_with(cast::option::<i64>(fill), metadata)),
            ArrayDataType::Float32 => AnyDenseArray::F32(DenseArray::filled_with(cast::option::<f32>(fill), metadata)),
            ArrayDataType::Float64 => AnyDenseArray::F64(DenseArray::filled_with(cast::option::<f64>(fill), metadata)),
        }
    }

    pub fn empty(datatype: ArrayDataType) -> Self {
        match datatype {
            ArrayDataType::Uint8 => AnyDenseArray::U8(DenseArray::empty()),
            ArrayDataType::Uint16 => AnyDenseArray::U16(DenseArray::empty()),
            ArrayDataType::Uint32 => AnyDenseArray::U32(DenseArray::empty()),
            ArrayDataType::Uint64 => AnyDenseArray::U64(DenseArray::empty()),
            ArrayDataType::Int8 => AnyDenseArray::I8(DenseArray::empty()),
            ArrayDataType::Int16 => AnyDenseArray::I16(DenseArray::empty()),
            ArrayDataType::Int32 => AnyDenseArray::I32(DenseArray::empty()),
            ArrayDataType::Int64 => AnyDenseArray::I64(DenseArray::empty()),
            ArrayDataType::Float32 => AnyDenseArray::F32(DenseArray::empty()),
            ArrayDataType::Float64 => AnyDenseArray::F64(DenseArray::empty()),
        }
    }

    pub fn data_type(&self) -> ArrayDataType {
        match self {
            AnyDenseArray::U8(_) => ArrayDataType::Uint8,
            AnyDenseArray::U16(_) => ArrayDataType::Uint16,
            AnyDenseArray::U32(_) => ArrayDataType::Uint32,
            AnyDenseArray::U64(_) => ArrayDataType::Uint64,
            AnyDenseArray::I8(_) => ArrayDataType::Int8,
            AnyDenseArray::I16(_) => ArrayDataType::Int16,
            AnyDenseArray::I32(_) => ArrayDataType::Int32,
            AnyDenseArray::I64(_) => ArrayDataType::Int64,
            AnyDenseArray::F32(_) => ArrayDataType::Float32,
            AnyDenseArray::F64(_) => ArrayDataType::Float64,
        }
    }

    pub fn cell_value<T: ArrayNum<T>>(&self, cell: Cell) -> Option<T> {
        match self {
            AnyDenseArray::U8(raster) => raster.cell_value(cell).and_then(|v| T::from(v)),
            AnyDenseArray::U16(raster) => raster.cell_value(cell).and_then(|v| T::from(v)),
            AnyDenseArray::U32(raster) => raster.cell_value(cell).and_then(|v| T::from(v)),
            AnyDenseArray::U64(raster) => raster.cell_value(cell).and_then(|v| T::from(v)),
            AnyDenseArray::I8(raster) => raster.cell_value(cell).and_then(|v| T::from(v)),
            AnyDenseArray::I16(raster) => raster.cell_value(cell).and_then(|v| T::from(v)),
            AnyDenseArray::I32(raster) => raster.cell_value(cell).and_then(|v| T::from(v)),
            AnyDenseArray::I64(raster) => raster.cell_value(cell).and_then(|v| T::from(v)),
            AnyDenseArray::F32(raster) => raster.cell_value(cell).and_then(|v| T::from(v)),
            AnyDenseArray::F64(raster) => raster.cell_value(cell).and_then(|v| T::from(v)),
        }
    }
}

#[cfg(feature = "gdal")]
impl<Metadata: ArrayMetadata> AnyDenseArray<Metadata> {
    pub fn read(path: &std::path::Path) -> Result<Self> {
        let data_type = crate::raster::io::dataset::detect_data_type(path, 1)?;
        let data_type = match data_type {
            gdal::raster::GdalDataType::Unknown => {
                return Err(Error::Runtime(format!("Failed to detect data type from: {}", path.display())));
            }
            gdal::raster::GdalDataType::UInt8 => ArrayDataType::Uint8,
            gdal::raster::GdalDataType::Int8 => ArrayDataType::Int8,
            gdal::raster::GdalDataType::UInt16 => ArrayDataType::Uint16,
            gdal::raster::GdalDataType::Int16 => ArrayDataType::Int16,
            gdal::raster::GdalDataType::UInt32 => ArrayDataType::Uint32,
            gdal::raster::GdalDataType::Int32 => ArrayDataType::Int32,
            gdal::raster::GdalDataType::UInt64 => ArrayDataType::Uint64,
            gdal::raster::GdalDataType::Int64 => ArrayDataType::Int64,
            gdal::raster::GdalDataType::Float32 => ArrayDataType::Float32,
            gdal::raster::GdalDataType::Float64 => ArrayDataType::Float64,
        };

        Self::read_as(data_type, path)
    }

    pub fn read_as(data_type: ArrayDataType, path: &std::path::Path) -> Result<Self> {
        use crate::raster::RasterIO;

        Ok(match data_type {
            ArrayDataType::Int8 => AnyDenseArray::I8(DenseArray::<i8, _>::read(path)?),
            ArrayDataType::Uint8 => AnyDenseArray::U8(DenseArray::<u8, _>::read(path)?),
            ArrayDataType::Int16 => AnyDenseArray::I16(DenseArray::<i16, _>::read(path)?),
            ArrayDataType::Uint16 => AnyDenseArray::U16(DenseArray::<u16, _>::read(path)?),
            ArrayDataType::Int32 => AnyDenseArray::I32(DenseArray::<i32, _>::read(path)?),
            ArrayDataType::Uint32 => AnyDenseArray::U32(DenseArray::<u32, _>::read(path)?),
            ArrayDataType::Int64 => AnyDenseArray::I64(DenseArray::<i64, _>::read(path)?),
            ArrayDataType::Uint64 => AnyDenseArray::U64(DenseArray::<u64, _>::read(path)?),
            ArrayDataType::Float32 => AnyDenseArray::F32(DenseArray::<f32, _>::read(path)?),
            ArrayDataType::Float64 => AnyDenseArray::F64(DenseArray::<f64, _>::read(path)?),
        })
    }
}

macro_rules! impl_try_from_dense_raster {
    ( $data_type:path, $data_type_enum:ident ) => {
        impl<Metadata: ArrayMetadata> TryFrom<AnyDenseArray<Metadata>> for DenseArray<$data_type, Metadata> {
            type Error = Error;

            fn try_from(value: AnyDenseArray<Metadata>) -> Result<Self> {
                match value {
                    AnyDenseArray::$data_type_enum(raster) => Ok(raster),
                    _ => Err(Error::InvalidArgument(format!("Expected {} raster", stringify!($data_type),))),
                }
            }
        }
    };
}

macro_rules! impl_try_from_dense_raster_ref {
    ( $data_type:path, $data_type_enum:ident ) => {
        impl<'a, Metadata: ArrayMetadata> TryFrom<&'a AnyDenseArray<Metadata>> for &'a DenseArray<$data_type, Metadata> {
            type Error = Error;

            fn try_from(value: &'a AnyDenseArray<Metadata>) -> Result<Self> {
                match value {
                    AnyDenseArray::$data_type_enum(raster) => Ok(&raster),
                    _ => Err(Error::InvalidArgument(format!("Expected {} raster", stringify!($data_type),))),
                }
            }
        }
    };
}

impl_try_from_dense_raster!(u8, U8);
impl_try_from_dense_raster!(i8, I8);
impl_try_from_dense_raster!(u16, U16);
impl_try_from_dense_raster!(i16, I16);
impl_try_from_dense_raster!(u32, U32);
impl_try_from_dense_raster!(i32, I32);
impl_try_from_dense_raster!(u64, U64);
impl_try_from_dense_raster!(i64, I64);
impl_try_from_dense_raster!(f32, F32);
impl_try_from_dense_raster!(f64, F64);

impl_try_from_dense_raster_ref!(u8, U8);
impl_try_from_dense_raster_ref!(i8, I8);
impl_try_from_dense_raster_ref!(u16, U16);
impl_try_from_dense_raster_ref!(i16, I16);
impl_try_from_dense_raster_ref!(u32, U32);
impl_try_from_dense_raster_ref!(i32, I32);
impl_try_from_dense_raster_ref!(u64, U64);
impl_try_from_dense_raster_ref!(i64, I64);
impl_try_from_dense_raster_ref!(f32, F32);
impl_try_from_dense_raster_ref!(f64, F64);

#[cfg(test)]
mod tests {

    use crate::array::{Columns, Rows};

    use super::*;

    #[test]
    fn try_from() {
        const TILE_WIDTH: Columns = Columns(10);
        const TILE_HEIGHT: Rows = Rows(10);

        let raster = DenseArray::new(
            RasterSize::with_rows_cols(TILE_HEIGHT, TILE_WIDTH),
            (0..(TILE_WIDTH * TILE_HEIGHT) as u32).collect::<Vec<u32>>(),
        );

        let type_erased = AnyDenseArray::U32(raster);

        let _: DenseArray<u32> = type_erased.clone().try_into().expect("Cast failed");

        assert!(TryInto::<DenseArray<u8>>::try_into(type_erased.clone()).is_err());
        assert!(TryInto::<DenseArray<i8>>::try_into(type_erased.clone()).is_err());
        assert!(TryInto::<DenseArray<u16>>::try_into(type_erased.clone()).is_err());
        assert!(TryInto::<DenseArray<i16>>::try_into(type_erased.clone()).is_err());
        assert!(TryInto::<DenseArray<u32>>::try_into(type_erased.clone()).is_ok());
        assert!(TryInto::<DenseArray<i32>>::try_into(type_erased.clone()).is_err());
        assert!(TryInto::<DenseArray<f32>>::try_into(type_erased.clone()).is_err());
        assert!(TryInto::<DenseArray<f64>>::try_into(type_erased.clone()).is_err());
    }
}

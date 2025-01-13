use crate::{DenseRaster, Error, Raster, RasterDataType, Result};

/// Type erased `RasterTile`
#[cfg_attr(target_arch = "wasm32", wasm_bindgen)]
#[derive(Clone)]
pub enum AnyDenseRaster {
    U8(DenseRaster<u8>),
    U16(DenseRaster<u16>),
    U32(DenseRaster<u32>),
    U64(DenseRaster<u64>),
    I8(DenseRaster<i8>),
    I16(DenseRaster<i16>),
    I32(DenseRaster<i32>),
    I64(DenseRaster<i64>),
    F32(DenseRaster<f32>),
    F64(DenseRaster<f64>),
}

#[macro_export]
macro_rules! unerase_raster_type_op {
    ( $raster_op:ident ) => {
        pub fn $raster_op(&self) -> usize {
            match self {
                AnyDenseRaster::U8(raster) => raster.$raster_op(),
                AnyDenseRaster::U16(raster) => raster.$raster_op(),
                AnyDenseRaster::U32(raster) => raster.$raster_op(),
                AnyDenseRaster::U64(raster) => raster.$raster_op(),
                AnyDenseRaster::I8(raster) => raster.$raster_op(),
                AnyDenseRaster::I16(raster) => raster.$raster_op(),
                AnyDenseRaster::I32(raster) => raster.$raster_op(),
                AnyDenseRaster::I64(raster) => raster.$raster_op(),
                AnyDenseRaster::F32(raster) => raster.$raster_op(),
                AnyDenseRaster::F64(raster) => raster.$raster_op(),
            }
        }
    };
}

impl AnyDenseRaster {
    unerase_raster_type_op!(width);
    unerase_raster_type_op!(height);

    pub fn data_type(&self) -> RasterDataType {
        match self {
            AnyDenseRaster::U8(_) => RasterDataType::Uint8,
            AnyDenseRaster::U16(_) => RasterDataType::Uint16,
            AnyDenseRaster::U32(_) => RasterDataType::Uint32,
            AnyDenseRaster::U64(_) => RasterDataType::Uint64,
            AnyDenseRaster::I8(_) => RasterDataType::Int8,
            AnyDenseRaster::I16(_) => RasterDataType::Int16,
            AnyDenseRaster::I32(_) => RasterDataType::Int32,
            AnyDenseRaster::I64(_) => RasterDataType::Int64,
            AnyDenseRaster::F32(_) => RasterDataType::Float32,
            AnyDenseRaster::F64(_) => RasterDataType::Float64,
        }
    }
}

macro_rules! impl_try_from_dense_raster {
    ( $data_type:path, $data_type_enum:ident ) => {
        impl TryFrom<AnyDenseRaster> for DenseRaster<$data_type> {
            type Error = Error;

            fn try_from(value: AnyDenseRaster) -> Result<Self> {
                match value {
                    AnyDenseRaster::$data_type_enum(raster) => Ok(raster),
                    _ => Err(Error::InvalidArgument(format!(
                        "Expected {} raster",
                        stringify!($data_type),
                    ))),
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

#[cfg(test)]
mod tests {

    use crate::{raster::RasterCreation, RasterSize};

    use super::*;

    #[test]
    fn try_from() {
        const TILE_WIDTH: usize = 10;
        const TILE_HEIGHT: usize = 10;

        let raster = DenseRaster::new(
            RasterSize::with_rows_cols(TILE_HEIGHT, TILE_WIDTH),
            (0..(TILE_WIDTH * TILE_HEIGHT) as u32).collect::<Vec<u32>>(),
        );

        let type_erased = AnyDenseRaster::U32(raster);

        let _: DenseRaster<u32> = type_erased.clone().try_into().expect("Cast failed");

        assert!(TryInto::<DenseRaster<u8>>::try_into(type_erased.clone()).is_err());
        assert!(TryInto::<DenseRaster<i8>>::try_into(type_erased.clone()).is_err());
        assert!(TryInto::<DenseRaster<u16>>::try_into(type_erased.clone()).is_err());
        assert!(TryInto::<DenseRaster<i16>>::try_into(type_erased.clone()).is_err());
        assert!(TryInto::<DenseRaster<u32>>::try_into(type_erased.clone()).is_ok());
        assert!(TryInto::<DenseRaster<i32>>::try_into(type_erased.clone()).is_err());
        assert!(TryInto::<DenseRaster<f32>>::try_into(type_erased.clone()).is_err());
        assert!(TryInto::<DenseRaster<f64>>::try_into(type_erased.clone()).is_err());
    }
}

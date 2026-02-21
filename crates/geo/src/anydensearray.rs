use inf::{allocate, cast};

use crate::{
    Array, ArrayDataType, ArrayMetadata, ArrayNum, Cell, DenseArray, Error, Result,
    array::{Columns, Rows},
    raster::algo,
    rastermetadata::RasterMetadata,
};

/// Type erased `DenseArray`
/// Needed to cross boundaries to dynamically typed languages like Python or JavaScript
#[derive(Clone, Debug, PartialEq)]
pub enum AnyDenseArray<Metadata: ArrayMetadata = RasterMetadata> {
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

impl<Metadata: ArrayMetadata> AnyDenseArray<Metadata> {
    apply_anydensearray_method!(rows, Rows);
    apply_anydensearray_method!(columns, Columns);
    apply_anydensearray_method_ref!(metadata, Metadata);

    /// Applies a unary operation to each element of the array and returns a new array with the result.
    /// If the type of the array does not match the type of the operation, the internal raster is first cast
    /// to the type of the operation.
    /// The operation is applied to each element of the array, also the nodata cells.
    pub fn unary<T: ArrayNum>(&self, op: impl Fn(T) -> T) -> DenseArray<T, Metadata> {
        let lhs: Result<&DenseArray<T, Metadata>> = self.try_into();

        match lhs {
            Ok(lhs) => lhs.unary(op),
            Err(_) => self.cast_to::<T>().unary(op),
        }
    }

    /// Applies a unary operation to each element of the array and returns the array with the result.
    /// If the type of the array does not match the type of the operation, the raster gets cast to the operation type
    /// before the operation is applied.
    /// The operation is applied to each element of the array, also the nodata cells.
    pub fn unary_mut<T: ArrayNum>(mut self, op: impl Fn(T) -> T) -> DenseArray<T, Metadata> {
        if T::TYPE != self.data_type() {
            self = self.cast(T::TYPE);
        }

        let ras: Result<DenseArray<T, Metadata>> = self.try_into();
        match ras {
            Ok(ras) => ras.unary_mut(op),
            Err(e) => panic!("Unreachable code: {e}"),
        }
    }

    /// Applies a unary operation to each element of the arra.
    /// If the type of the array does not match the type of the operation an error is returned.
    /// The operation is applied to each element of the array, also the nodata cells.
    pub fn unary_inplace<T: ArrayNum>(&mut self, op: impl Fn(&mut T)) -> Result<()> {
        let lhs: &mut DenseArray<T, Metadata> = self
            .try_into()
            .map_err(|_| Error::InvalidArgument(format!("Invalid unary callback function for type {}", T::TYPE)))?;

        lhs.unary_inplace(op);
        Ok(())
    }

    pub fn binary_op<T: ArrayNum>(&self, other: &Self, op: impl Fn(T, T) -> T) -> DenseArray<T, Metadata> {
        let lhs: Result<&DenseArray<T, Metadata>> = self.try_into();
        let rhs: Result<&DenseArray<T, Metadata>> = other.try_into();

        match (lhs, rhs) {
            (Ok(lhs), Ok(rhs)) => lhs.binary(rhs, op),
            (Err(_), Err(_)) => self.cast_to::<T>().binary(&other.cast_to::<T>(), op),
            (Ok(lhs), Err(_)) => lhs.binary(&other.cast_to::<T>(), op),
            (Err(_), Ok(rhs)) => self.cast_to::<T>().binary(rhs, op),
        }
    }

    pub fn binary_op_to<TDest: ArrayNum, T: ArrayNum>(&self, other: &Self, op: impl Fn(T, T) -> TDest) -> DenseArray<TDest, Metadata> {
        let lhs: Result<&DenseArray<T, Metadata>> = self.try_into();
        let rhs: Result<&DenseArray<T, Metadata>> = other.try_into();

        match (lhs, rhs) {
            (Ok(lhs), Ok(rhs)) => lhs.binary_to(rhs, op),
            (Err(_), Err(_)) => self.cast_to::<T>().binary_to(&other.cast_to::<T>(), op),
            (Ok(lhs), Err(_)) => lhs.binary_to(&other.cast_to::<T>(), op),
            (Err(_), Ok(rhs)) => self.cast_to::<T>().binary_to(rhs, op),
        }
    }

    pub fn filled_with(fill: Option<f64>, metadata: Metadata, datatype: ArrayDataType) -> Self {
        dispatch_datatype!(datatype, T, DenseArray::filled_with(cast::option::<T>(fill), metadata))
    }

    pub fn empty(datatype: ArrayDataType) -> Self {
        dispatch_datatype!(datatype, T, DenseArray::<T, Metadata>::empty())
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

    pub fn cell_value<T: ArrayNum>(&self, cell: Cell) -> Option<T> {
        dispatch_anydensearray!(self, arr, arr.cell_value(cell).and_then(|v| T::from(v)))
    }

    pub fn with_metadata<M: ArrayMetadata>(self, meta: M) -> Result<AnyDenseArray<M>> {
        Ok(apply_to_anydensearray!(self, raster, raster.with_metadata(meta)?))
    }

    pub fn min_max(&self) -> Result<Option<std::ops::RangeInclusive<f64>>> {
        Ok(dispatch_anydensearray!(
            self,
            arr,
            algo::min_max(arr).and_then(|r| cast::inclusive_range::<f64>(r).ok())
        ))
    }

    pub fn filter(&mut self, values_to_include: &[f64]) -> Result<()> {
        dispatch_anydensearray!(self, arr, algo::filter(arr, &cast::slice(values_to_include)?));
        Ok(())
    }

    apply_anydensearray_method!(is_empty, bool);

    apply_anydensearray_method!(len, usize);

    #[cfg(feature = "gdal")]
    #[cfg_attr(docsrs, doc(cfg(feature = "gdal")))]
    pub fn write(&mut self, path: &std::path::Path) -> Result<()> {
        use crate::raster::RasterReadWrite;

        dispatch_anydensearray!(self, arr, arr.write(path))
    }

    /// # Safety
    /// Used for FFI with python where safety is not guaranteed anyway
    pub unsafe fn raw_data_u8_slice(&self) -> &[u8] {
        unsafe {
            dispatch_anydensearray!(self, arr, {
                let slice = arr.as_slice();
                std::slice::from_raw_parts(slice.as_ptr().cast::<u8>(), std::mem::size_of_val(slice))
            })
        }
    }
}

impl<'a, Metadata: ArrayMetadata> AnyDenseArray<Metadata> {
    /// This will panic if the type does not match, so make sure to check `data_type()` first.
    pub fn as_densearray_ref<T: ArrayNum>(&'a self) -> &'a DenseArray<T, Metadata> {
        if self.data_type() != T::TYPE {
            panic!("Type mismatch: {} != {}", T::TYPE, self.data_type());
        }

        self.try_into().unwrap()
    }
}

impl<Metadata: ArrayMetadata> AnyDenseArray<Metadata> {
    #[cfg(feature = "gdal")]
    #[cfg_attr(docsrs, doc(cfg(feature = "gdal")))]
    pub fn read(path: &std::path::Path) -> Result<Self> {
        use crate::raster;

        Self::read_as(raster::io::detect_data_type(path, 1)?, path)
    }

    #[cfg(feature = "gdal")]
    #[cfg_attr(docsrs, doc(cfg(feature = "gdal")))]
    pub fn read_as(data_type: ArrayDataType, path: &std::path::Path) -> Result<Self> {
        use crate::raster::RasterReadWrite;

        Ok(dispatch_datatype!(data_type, T, DenseArray::<T, _>::read(path)?))
    }
}

fn dense_array_as<TDest, T, Metadata>(raster: DenseArray<T, Metadata>) -> Result<DenseArray<TDest, Metadata>>
where
    TDest: ArrayNum,
    T: ArrayNum,
    Metadata: ArrayMetadata,
{
    if TDest::TYPE == T::TYPE {
        let (meta, data) = raster.into_raw_parts();
        Ok(DenseArray::new(meta, allocate::cast_aligned_vec::<T, TDest>(data))?)
    } else {
        Err(Error::InvalidArgument(format!("Type mismatch: {} != {}", TDest::TYPE, T::TYPE)))
    }
}

fn dense_array_as_ref<TDest, T, Metadata>(raster: &DenseArray<T, Metadata>) -> Result<&DenseArray<TDest, Metadata>>
where
    TDest: ArrayNum,
    T: ArrayNum,
    Metadata: ArrayMetadata,
{
    if TDest::TYPE == T::TYPE {
        let ptr = (raster as *const DenseArray<T, Metadata>).cast::<DenseArray<TDest, Metadata>>();
        // Safety: We just checked that TDest and T are the same type
        Ok(unsafe { &*ptr })
    } else {
        Err(Error::InvalidArgument(format!("Type mismatch: {} != {}", TDest::TYPE, T::TYPE)))
    }
}

fn dense_array_as_mut_ref<TDest, T, Metadata>(raster: &mut DenseArray<T, Metadata>) -> Result<&mut DenseArray<TDest, Metadata>>
where
    TDest: ArrayNum,
    T: ArrayNum,
    Metadata: ArrayMetadata,
{
    if TDest::TYPE == T::TYPE {
        let ptr = (raster as *mut DenseArray<T, Metadata>).cast::<DenseArray<TDest, Metadata>>();
        // Safety: We just checked that TDest and T are the same type
        Ok(unsafe { &mut *ptr })
    } else {
        Err(Error::InvalidArgument(format!("Type mismatch: {} != {}", TDest::TYPE, T::TYPE)))
    }
}

impl<T: ArrayNum, Metadata: ArrayMetadata> TryFrom<AnyDenseArray<Metadata>> for DenseArray<T, Metadata> {
    type Error = Error;

    fn try_from(value: AnyDenseArray<Metadata>) -> Result<Self> {
        dispatch_anydensearray!(value, arr, dense_array_as::<T, _, _>(arr))
    }
}

impl<'a, T: ArrayNum, Metadata: ArrayMetadata> TryFrom<&'a AnyDenseArray<Metadata>> for &'a DenseArray<T, Metadata> {
    type Error = Error;

    fn try_from(value: &'a AnyDenseArray<Metadata>) -> Result<Self> {
        dispatch_anydensearray!(value, arr, dense_array_as_ref::<T, _, _>(arr))
    }
}

impl<'a, T: ArrayNum, Metadata: ArrayMetadata> TryFrom<&'a mut AnyDenseArray<Metadata>> for &'a mut DenseArray<T, Metadata> {
    type Error = Error;

    fn try_from(value: &'a mut AnyDenseArray<Metadata>) -> Result<Self> {
        dispatch_anydensearray!(value, arr, dense_array_as_mut_ref::<T, _, _>(arr))
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        RasterSize,
        array::{Columns, Rows},
    };

    use super::*;

    #[test]
    fn try_from() {
        const TILE_WIDTH: Columns = Columns(10);
        const TILE_HEIGHT: Rows = Rows(10);

        let raster = DenseArray::new(
            RasterMetadata::sized(RasterSize::with_rows_cols(TILE_HEIGHT, TILE_WIDTH), ArrayDataType::Uint32),
            allocate::aligned_vec_from_iter(0..(TILE_WIDTH * TILE_HEIGHT) as u32),
        )
        .unwrap();

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

    #[test]
    fn try_from_ref() {
        const TILE_WIDTH: Columns = Columns(10);
        const TILE_HEIGHT: Rows = Rows(10);

        let raster = DenseArray::new(
            RasterMetadata::sized(RasterSize::with_rows_cols(TILE_HEIGHT, TILE_WIDTH), ArrayDataType::Uint32),
            allocate::aligned_vec_from_iter(0..(TILE_WIDTH * TILE_HEIGHT) as u32),
        )
        .unwrap();

        let type_erased = AnyDenseArray::U32(raster);

        let _: &DenseArray<u32> = (&type_erased).try_into().expect("Cast failed");

        assert!(TryInto::<&DenseArray<u8>>::try_into(&type_erased).is_err());
        assert!(TryInto::<&DenseArray<i8>>::try_into(&type_erased).is_err());
        assert!(TryInto::<&DenseArray<u16>>::try_into(&type_erased).is_err());
        assert!(TryInto::<&DenseArray<i16>>::try_into(&type_erased).is_err());
        assert!(TryInto::<&DenseArray<u32>>::try_into(&type_erased).is_ok());
        assert!(TryInto::<&DenseArray<i32>>::try_into(&type_erased).is_err());
        assert!(TryInto::<&DenseArray<f32>>::try_into(&type_erased).is_err());
        assert!(TryInto::<&DenseArray<f64>>::try_into(&type_erased).is_err());
    }

    #[test]
    fn apply_to_anydensearray_macro() {
        const TILE_WIDTH: Columns = Columns(5);
        const TILE_HEIGHT: Rows = Rows(5);

        // Test with U32 variant
        let raster_u32 = DenseArray::new(
            RasterMetadata::sized(RasterSize::with_rows_cols(TILE_HEIGHT, TILE_WIDTH), ArrayDataType::Uint32),
            allocate::aligned_vec_from_iter(0..(TILE_WIDTH * TILE_HEIGHT) as u32),
        )
        .unwrap();

        let any_array_u32 = AnyDenseArray::U32(raster_u32.clone());

        // Test that the macro correctly unpacks and repacks
        let result = apply_to_anydensearray!(any_array_u32, arr, arr.clone());

        match result {
            AnyDenseArray::U32(arr) => {
                assert_eq!(arr.rows(), raster_u32.rows());
                assert_eq!(arr.columns(), raster_u32.columns());
            }
            _ => panic!("Expected U32 variant"),
        }

        // Test with F64 variant
        let raster_f64 = DenseArray::new(
            RasterMetadata::sized(RasterSize::with_rows_cols(TILE_HEIGHT, TILE_WIDTH), ArrayDataType::Float64),
            allocate::aligned_vec_from_iter((0..(TILE_WIDTH * TILE_HEIGHT)).map(|i| i as f64)),
        )
        .unwrap();

        let any_array_f64 = AnyDenseArray::F64(raster_f64.clone());

        let result = apply_to_anydensearray!(any_array_f64, arr, arr.clone());

        match result {
            AnyDenseArray::F64(arr) => {
                assert_eq!(arr.rows(), raster_f64.rows());
                assert_eq!(arr.columns(), raster_f64.columns());
            }
            _ => panic!("Expected F64 variant"),
        }
    }
}

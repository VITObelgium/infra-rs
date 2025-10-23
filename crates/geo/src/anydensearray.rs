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

/// Macro for applying an operation to an `AnyDenseArray` that returns a non-`AnyDenseArray` type.
/// This is useful for methods like `rows()`, `columns()`, `is_empty()`, `len()`, etc.
///
/// # Examples
///
/// ```ignore
/// // Define a method that returns the same type regardless of variant
/// apply_anydensearray_method!(rows, Rows);
/// apply_anydensearray_method!(columns, Columns);
/// apply_anydensearray_method!(is_empty, bool);
/// apply_anydensearray_method!(len, usize);
/// ```
#[macro_export]
macro_rules! apply_anydensearray_method {
    ($method:ident, $ret:ty) => {
        pub fn $method(&self) -> $ret {
            match self {
                $crate::AnyDenseArray::U8(raster) => raster.$method(),
                $crate::AnyDenseArray::U16(raster) => raster.$method(),
                $crate::AnyDenseArray::U32(raster) => raster.$method(),
                $crate::AnyDenseArray::U64(raster) => raster.$method(),
                $crate::AnyDenseArray::I8(raster) => raster.$method(),
                $crate::AnyDenseArray::I16(raster) => raster.$method(),
                $crate::AnyDenseArray::I32(raster) => raster.$method(),
                $crate::AnyDenseArray::I64(raster) => raster.$method(),
                $crate::AnyDenseArray::F32(raster) => raster.$method(),
                $crate::AnyDenseArray::F64(raster) => raster.$method(),
            }
        }
    };
}

/// Macro for applying an operation to an `AnyDenseArray` that returns a reference to a non-`AnyDenseArray` type.
/// This is useful for methods like `metadata()` that return references.
///
/// # Examples
///
/// ```ignore
/// // Define a method that returns a reference to the same type regardless of variant
/// apply_anydensearray_method_ref!(metadata, Metadata);
/// ```
#[macro_export]
macro_rules! apply_anydensearray_method_ref {
    ($method:ident, $ret:ty) => {
        pub fn $method(&self) -> &$ret {
            match self {
                $crate::AnyDenseArray::U8(raster) => raster.$method(),
                $crate::AnyDenseArray::U16(raster) => raster.$method(),
                $crate::AnyDenseArray::U32(raster) => raster.$method(),
                $crate::AnyDenseArray::U64(raster) => raster.$method(),
                $crate::AnyDenseArray::I8(raster) => raster.$method(),
                $crate::AnyDenseArray::I16(raster) => raster.$method(),
                $crate::AnyDenseArray::I32(raster) => raster.$method(),
                $crate::AnyDenseArray::I64(raster) => raster.$method(),
                $crate::AnyDenseArray::F32(raster) => raster.$method(),
                $crate::AnyDenseArray::F64(raster) => raster.$method(),
            }
        }
    };
}

#[macro_export]
macro_rules! apply_to_anydensearray {
    ($array:expr, $var:ident, $expr:expr) => {
        match $array {
            $crate::AnyDenseArray::U8($var) => $crate::AnyDenseArray::U8($expr),
            $crate::AnyDenseArray::U16($var) => $crate::AnyDenseArray::U16($expr),
            $crate::AnyDenseArray::U32($var) => $crate::AnyDenseArray::U32($expr),
            $crate::AnyDenseArray::U64($var) => $crate::AnyDenseArray::U64($expr),
            $crate::AnyDenseArray::I8($var) => $crate::AnyDenseArray::I8($expr),
            $crate::AnyDenseArray::I16($var) => $crate::AnyDenseArray::I16($expr),
            $crate::AnyDenseArray::I32($var) => $crate::AnyDenseArray::I32($expr),
            $crate::AnyDenseArray::I64($var) => $crate::AnyDenseArray::I64($expr),
            $crate::AnyDenseArray::F32($var) => $crate::AnyDenseArray::F32($expr),
            $crate::AnyDenseArray::F64($var) => $crate::AnyDenseArray::F64($expr),
        }
    };
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

    pub fn cell_value<T: ArrayNum>(&self, cell: Cell) -> Option<T> {
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

    pub fn cast(&self, data_type: ArrayDataType) -> AnyDenseArray<Metadata> {
        match data_type {
            ArrayDataType::Uint8 => AnyDenseArray::U8(self.cast_to::<u8>()),
            ArrayDataType::Uint16 => AnyDenseArray::U16(self.cast_to::<u16>()),
            ArrayDataType::Uint32 => AnyDenseArray::U32(self.cast_to::<u32>()),
            ArrayDataType::Uint64 => AnyDenseArray::U64(self.cast_to::<u64>()),
            ArrayDataType::Int8 => AnyDenseArray::I8(self.cast_to::<i8>()),
            ArrayDataType::Int16 => AnyDenseArray::I16(self.cast_to::<i16>()),
            ArrayDataType::Int32 => AnyDenseArray::I32(self.cast_to::<i32>()),
            ArrayDataType::Int64 => AnyDenseArray::I64(self.cast_to::<i64>()),
            ArrayDataType::Float32 => AnyDenseArray::F32(self.cast_to::<f32>()),
            ArrayDataType::Float64 => AnyDenseArray::F64(self.cast_to::<f64>()),
        }
    }

    pub fn cast_to<T: ArrayNum>(&self) -> DenseArray<T, Metadata> {
        match self {
            AnyDenseArray::U8(raster) => algo::cast::<T, _>(raster),
            AnyDenseArray::U16(raster) => algo::cast::<T, _>(raster),
            AnyDenseArray::U32(raster) => algo::cast::<T, _>(raster),
            AnyDenseArray::U64(raster) => algo::cast::<T, _>(raster),
            AnyDenseArray::I8(raster) => algo::cast::<T, _>(raster),
            AnyDenseArray::I16(raster) => algo::cast::<T, _>(raster),
            AnyDenseArray::I32(raster) => algo::cast::<T, _>(raster),
            AnyDenseArray::I64(raster) => algo::cast::<T, _>(raster),
            AnyDenseArray::F32(raster) => algo::cast::<T, _>(raster),
            AnyDenseArray::F64(raster) => algo::cast::<T, _>(raster),
        }
    }

    pub fn with_metadata<M: ArrayMetadata>(self, meta: M) -> Result<AnyDenseArray<M>> {
        Ok(apply_to_anydensearray!(self, raster, raster.with_metadata(meta)?))
    }

    pub fn min_max(&self) -> Result<Option<std::ops::RangeInclusive<f64>>> {
        Ok(match self {
            AnyDenseArray::U8(raster) => algo::min_max(raster).and_then(|r| cast::inclusive_range::<f64>(r).ok()),
            AnyDenseArray::U16(raster) => algo::min_max(raster).and_then(|r| cast::inclusive_range::<f64>(r).ok()),
            AnyDenseArray::U32(raster) => algo::min_max(raster).and_then(|r| cast::inclusive_range::<f64>(r).ok()),
            AnyDenseArray::U64(raster) => algo::min_max(raster).and_then(|r| cast::inclusive_range::<f64>(r).ok()),
            AnyDenseArray::I8(raster) => algo::min_max(raster).and_then(|r| cast::inclusive_range::<f64>(r).ok()),
            AnyDenseArray::I16(raster) => algo::min_max(raster).and_then(|r| cast::inclusive_range::<f64>(r).ok()),
            AnyDenseArray::I32(raster) => algo::min_max(raster).and_then(|r| cast::inclusive_range::<f64>(r).ok()),
            AnyDenseArray::I64(raster) => algo::min_max(raster).and_then(|r| cast::inclusive_range::<f64>(r).ok()),
            AnyDenseArray::F32(raster) => algo::min_max(raster).and_then(|r| cast::inclusive_range::<f64>(r).ok()),
            AnyDenseArray::F64(raster) => algo::min_max(raster).and_then(|r| cast::inclusive_range::<f64>(r).ok()),
        })
    }

    pub fn filter(&mut self, values_to_include: &[f64]) -> Result<()> {
        match self {
            AnyDenseArray::U8(raster) => algo::filter(raster, &cast::slice::<u8>(values_to_include)?),
            AnyDenseArray::U16(raster) => algo::filter(raster, &cast::slice::<u16>(values_to_include)?),
            AnyDenseArray::U32(raster) => algo::filter(raster, &cast::slice::<u32>(values_to_include)?),
            AnyDenseArray::U64(raster) => algo::filter(raster, &cast::slice::<u64>(values_to_include)?),
            AnyDenseArray::I8(raster) => algo::filter(raster, &cast::slice::<i8>(values_to_include)?),
            AnyDenseArray::I16(raster) => algo::filter(raster, &cast::slice::<i16>(values_to_include)?),
            AnyDenseArray::I32(raster) => algo::filter(raster, &cast::slice::<i32>(values_to_include)?),
            AnyDenseArray::I64(raster) => algo::filter(raster, &cast::slice::<i64>(values_to_include)?),
            AnyDenseArray::F32(raster) => algo::filter(raster, &cast::slice::<f32>(values_to_include)?),
            AnyDenseArray::F64(raster) => algo::filter(raster, &cast::slice::<f64>(values_to_include)?),
        }

        Ok(())
    }

    apply_anydensearray_method!(is_empty, bool);

    apply_anydensearray_method!(len, usize);

    #[cfg(feature = "gdal")]
    #[cfg_attr(docsrs, doc(cfg(feature = "gdal")))]
    pub fn write(&mut self, path: &std::path::Path) -> Result<()> {
        use crate::raster::RasterReadWrite;

        match self {
            AnyDenseArray::U8(raster) => raster.write(path),
            AnyDenseArray::U16(raster) => raster.write(path),
            AnyDenseArray::U32(raster) => raster.write(path),
            AnyDenseArray::U64(raster) => raster.write(path),
            AnyDenseArray::I8(raster) => raster.write(path),
            AnyDenseArray::I16(raster) => raster.write(path),
            AnyDenseArray::I32(raster) => raster.write(path),
            AnyDenseArray::I64(raster) => raster.write(path),
            AnyDenseArray::F32(raster) => raster.write(path),
            AnyDenseArray::F64(raster) => raster.write(path),
        }
    }

    /// # Safety
    /// Used for FFI with python where safety is not guaranteed anyway
    pub unsafe fn raw_data_u8_slice(&self) -> &[u8] {
        unsafe {
            match self {
                AnyDenseArray::U8(raster) => raster.as_slice(),
                AnyDenseArray::U16(raster) => {
                    std::slice::from_raw_parts(raster.as_slice().as_ptr().cast::<u8>(), std::mem::size_of_val(raster.as_slice()))
                }
                AnyDenseArray::U32(raster) => {
                    std::slice::from_raw_parts(raster.as_slice().as_ptr().cast::<u8>(), std::mem::size_of_val(raster.as_slice()))
                }
                AnyDenseArray::U64(raster) => {
                    std::slice::from_raw_parts(raster.as_slice().as_ptr().cast::<u8>(), std::mem::size_of_val(raster.as_slice()))
                }
                AnyDenseArray::I8(raster) => {
                    std::slice::from_raw_parts(raster.as_slice().as_ptr().cast::<u8>(), std::mem::size_of_val(raster.as_slice()))
                }
                AnyDenseArray::I16(raster) => {
                    std::slice::from_raw_parts(raster.as_slice().as_ptr().cast::<u8>(), std::mem::size_of_val(raster.as_slice()))
                }
                AnyDenseArray::I32(raster) => {
                    std::slice::from_raw_parts(raster.as_slice().as_ptr().cast::<u8>(), std::mem::size_of_val(raster.as_slice()))
                }
                AnyDenseArray::I64(raster) => {
                    std::slice::from_raw_parts(raster.as_slice().as_ptr().cast::<u8>(), std::mem::size_of_val(raster.as_slice()))
                }
                AnyDenseArray::F32(raster) => {
                    std::slice::from_raw_parts(raster.as_slice().as_ptr().cast::<u8>(), std::mem::size_of_val(raster.as_slice()))
                }
                AnyDenseArray::F64(raster) => {
                    std::slice::from_raw_parts(raster.as_slice().as_ptr().cast::<u8>(), std::mem::size_of_val(raster.as_slice()))
                }
            }
        }
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
        match value {
            AnyDenseArray::U8(raster) => dense_array_as::<T, u8, _>(raster),
            AnyDenseArray::U16(raster) => dense_array_as::<T, u16, _>(raster),
            AnyDenseArray::U32(raster) => dense_array_as::<T, u32, _>(raster),
            AnyDenseArray::U64(raster) => dense_array_as::<T, u64, _>(raster),
            AnyDenseArray::I8(raster) => dense_array_as::<T, i8, _>(raster),
            AnyDenseArray::I16(raster) => dense_array_as::<T, i16, _>(raster),
            AnyDenseArray::I32(raster) => dense_array_as::<T, i32, _>(raster),
            AnyDenseArray::I64(raster) => dense_array_as::<T, i64, _>(raster),
            AnyDenseArray::F32(raster) => dense_array_as::<T, f32, _>(raster),
            AnyDenseArray::F64(raster) => dense_array_as::<T, f64, _>(raster),
        }
    }
}

impl<'a, T: ArrayNum, Metadata: ArrayMetadata> TryFrom<&'a AnyDenseArray<Metadata>> for &'a DenseArray<T, Metadata> {
    type Error = Error;

    fn try_from(value: &'a AnyDenseArray<Metadata>) -> Result<Self> {
        match value {
            AnyDenseArray::U8(raster) => dense_array_as_ref::<T, _, _>(raster),
            AnyDenseArray::U16(raster) => dense_array_as_ref::<T, _, _>(raster),
            AnyDenseArray::U32(raster) => dense_array_as_ref::<T, _, _>(raster),
            AnyDenseArray::U64(raster) => dense_array_as_ref::<T, _, _>(raster),
            AnyDenseArray::I8(raster) => dense_array_as_ref::<T, _, _>(raster),
            AnyDenseArray::I16(raster) => dense_array_as_ref::<T, _, _>(raster),
            AnyDenseArray::I32(raster) => dense_array_as_ref::<T, _, _>(raster),
            AnyDenseArray::I64(raster) => dense_array_as_ref::<T, _, _>(raster),
            AnyDenseArray::F32(raster) => dense_array_as_ref::<T, _, _>(raster),
            AnyDenseArray::F64(raster) => dense_array_as_ref::<T, _, _>(raster),
        }
    }
}

impl<'a, T: ArrayNum, Metadata: ArrayMetadata> TryFrom<&'a mut AnyDenseArray<Metadata>> for &'a mut DenseArray<T, Metadata> {
    type Error = Error;

    fn try_from(value: &'a mut AnyDenseArray<Metadata>) -> Result<Self> {
        match value {
            AnyDenseArray::U8(raster) => dense_array_as_mut_ref::<T, _, _>(raster),
            AnyDenseArray::U16(raster) => dense_array_as_mut_ref::<T, _, _>(raster),
            AnyDenseArray::U32(raster) => dense_array_as_mut_ref::<T, _, _>(raster),
            AnyDenseArray::U64(raster) => dense_array_as_mut_ref::<T, _, _>(raster),
            AnyDenseArray::I8(raster) => dense_array_as_mut_ref::<T, _, _>(raster),
            AnyDenseArray::I16(raster) => dense_array_as_mut_ref::<T, _, _>(raster),
            AnyDenseArray::I32(raster) => dense_array_as_mut_ref::<T, _, _>(raster),
            AnyDenseArray::I64(raster) => dense_array_as_mut_ref::<T, _, _>(raster),
            AnyDenseArray::F32(raster) => dense_array_as_mut_ref::<T, _, _>(raster),
            AnyDenseArray::F64(raster) => dense_array_as_mut_ref::<T, _, _>(raster),
        }
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

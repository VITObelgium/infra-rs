//! Macros for dispatching on `AnyDenseArray` and `ArrayDataType` variants.
//!
//! These macros reduce boilerplate when working with type-erased dense arrays.

/// Macro to dispatch on `AnyDenseArray` variants and apply an expression to the inner array.
///
/// This macro does not re-wrap the result in `AnyDenseArray`, making it suitable for
/// operations that return a concrete type (not dependent on the variant).
///
/// # Example
///
/// ```ignore
/// let len = dispatch_anydensearray!(any_array, arr, arr.len());
/// ```
#[macro_export]
macro_rules! dispatch_anydensearray {
    ($array:expr, $var:ident, $expr:expr) => {
        match $array {
            $crate::AnyDenseArray::U8($var) => $expr,
            $crate::AnyDenseArray::U16($var) => $expr,
            $crate::AnyDenseArray::U32($var) => $expr,
            $crate::AnyDenseArray::U64($var) => $expr,
            $crate::AnyDenseArray::I8($var) => $expr,
            $crate::AnyDenseArray::I16($var) => $expr,
            $crate::AnyDenseArray::I32($var) => $expr,
            $crate::AnyDenseArray::I64($var) => $expr,
            $crate::AnyDenseArray::F32($var) => $expr,
            $crate::AnyDenseArray::F64($var) => $expr,
        }
    };
}

/// Macro to dispatch on `AnyDenseArray` variants and wrap the result back in the same variant.
///
/// This is useful when the operation returns the same element type as the input.
///
/// # Example
///
/// ```ignore
/// let cropped = apply_to_anydensearray!(any_array, arr, algo::crop(arr));
/// ```
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

/// Macro to dispatch on `ArrayDataType` and execute an expression with the corresponding Rust type.
///
/// The expression `$expr` is evaluated with `$t` bound to the concrete type (u8, i32, f64, etc.)
/// and the result is wrapped in the corresponding `AnyDenseArray` variant.
///
/// # Example
///
/// ```ignore
/// let empty = dispatch_datatype!(data_type, T, DenseArray::<T, _>::empty());
/// ```
#[macro_export]
macro_rules! dispatch_datatype {
    ($data_type:expr, $t:ident, $expr:expr) => {
        match $data_type {
            $crate::ArrayDataType::Uint8 => {
                type $t = u8;
                $crate::AnyDenseArray::U8($expr)
            }
            $crate::ArrayDataType::Uint16 => {
                type $t = u16;
                $crate::AnyDenseArray::U16($expr)
            }
            $crate::ArrayDataType::Uint32 => {
                type $t = u32;
                $crate::AnyDenseArray::U32($expr)
            }
            $crate::ArrayDataType::Uint64 => {
                type $t = u64;
                $crate::AnyDenseArray::U64($expr)
            }
            $crate::ArrayDataType::Int8 => {
                type $t = i8;
                $crate::AnyDenseArray::I8($expr)
            }
            $crate::ArrayDataType::Int16 => {
                type $t = i16;
                $crate::AnyDenseArray::I16($expr)
            }
            $crate::ArrayDataType::Int32 => {
                type $t = i32;
                $crate::AnyDenseArray::I32($expr)
            }
            $crate::ArrayDataType::Int64 => {
                type $t = i64;
                $crate::AnyDenseArray::I64($expr)
            }
            $crate::ArrayDataType::Float32 => {
                type $t = f32;
                $crate::AnyDenseArray::F32($expr)
            }
            $crate::ArrayDataType::Float64 => {
                type $t = f64;
                $crate::AnyDenseArray::F64($expr)
            }
        }
    };
}

/// Macro to dispatch on `ArrayDataType` and execute an expression with the corresponding Rust type.
///
/// Unlike `dispatch_datatype!`, this does not wrap the result in `AnyDenseArray`.
/// Use this when the operation returns a type that is not `AnyDenseArray`.
///
/// # Example
///
/// ```ignore
/// dispatch_datatype_nowrap!(data_type, T, {
///     let slice = bytemuck::cast_slice_mut::<u8, T>(output);
///     arr.cast_to_slice::<T>(slice)
/// })
/// ```
#[macro_export]
macro_rules! dispatch_datatype_nowrap {
    ($data_type:expr, $t:ident, $expr:expr) => {
        match $data_type {
            $crate::ArrayDataType::Uint8 => {
                type $t = u8;
                $expr
            }
            $crate::ArrayDataType::Uint16 => {
                type $t = u16;
                $expr
            }
            $crate::ArrayDataType::Uint32 => {
                type $t = u32;
                $expr
            }
            $crate::ArrayDataType::Uint64 => {
                type $t = u64;
                $expr
            }
            $crate::ArrayDataType::Int8 => {
                type $t = i8;
                $expr
            }
            $crate::ArrayDataType::Int16 => {
                type $t = i16;
                $expr
            }
            $crate::ArrayDataType::Int32 => {
                type $t = i32;
                $expr
            }
            $crate::ArrayDataType::Int64 => {
                type $t = i64;
                $expr
            }
            $crate::ArrayDataType::Float32 => {
                type $t = f32;
                $expr
            }
            $crate::ArrayDataType::Float64 => {
                type $t = f64;
                $expr
            }
        }
    };
}

/// Macro for defining a method on `AnyDenseArray` that returns a non-`AnyDenseArray` type.
///
/// This is useful for methods like `rows()`, `columns()`, `is_empty()`, `len()`, etc.
///
/// # Example
///
/// ```ignore
/// impl<Metadata: ArrayMetadata> AnyDenseArray<Metadata> {
///     apply_anydensearray_method!(rows, Rows);
///     apply_anydensearray_method!(columns, Columns);
///     apply_anydensearray_method!(is_empty, bool);
///     apply_anydensearray_method!(len, usize);
/// }
/// ```
#[macro_export]
macro_rules! apply_anydensearray_method {
    ($method:ident, $ret:ty) => {
        pub fn $method(&self) -> $ret {
            $crate::dispatch_anydensearray!(self, arr, arr.$method())
        }
    };
}

/// Macro for defining a method on `AnyDenseArray` that returns a reference to a non-`AnyDenseArray` type.
///
/// This is useful for methods like `metadata()` that return references.
///
/// # Example
///
/// ```ignore
/// impl<Metadata: ArrayMetadata> AnyDenseArray<Metadata> {
///     apply_anydensearray_method_ref!(metadata, Metadata);
/// }
/// ```
#[macro_export]
macro_rules! apply_anydensearray_method_ref {
    ($method:ident, $ret:ty) => {
        pub fn $method(&self) -> &$ret {
            $crate::dispatch_anydensearray!(self, arr, arr.$method())
        }
    };
}

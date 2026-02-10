use std::{
    ffi::{CStr, c_char, c_double, c_int, c_longlong},
    slice,
};

#[derive(PartialEq, Eq)]
pub enum ColumnType {
    Integer,
    Float,
    Blob,
    Null,
    Text,
}

#[derive(Clone, Copy)]
pub struct Row {
    stmt: *mut libsqlite3_sys::sqlite3_stmt,
}

impl Row {
    pub fn new(stmt: *mut libsqlite3_sys::sqlite3_stmt) -> Self {
        Self { stmt }
    }

    pub fn column_type(&self, index: c_int) -> ColumnType {
        let column_type = unsafe { libsqlite3_sys::sqlite3_column_type(self.stmt, index) };
        match column_type {
            libsqlite3_sys::SQLITE_INTEGER => ColumnType::Integer,
            libsqlite3_sys::SQLITE_FLOAT => ColumnType::Float,
            libsqlite3_sys::SQLITE_BLOB => ColumnType::Blob,
            libsqlite3_sys::SQLITE_NULL => ColumnType::Null,
            libsqlite3_sys::SQLITE_TEXT => ColumnType::Text,
            _ => panic!("Invalid column type"),
        }
    }

    pub fn column_is_null(&self, index: c_int) -> bool {
        self.column_type(index) == ColumnType::Null
    }

    pub fn column_string(&self, index: c_int) -> Option<&str> {
        let data = unsafe { libsqlite3_sys::sqlite3_column_text(self.stmt, index) };
        if !data.is_null() {
            let c_str = unsafe { CStr::from_ptr(data.cast::<c_char>()) };
            return c_str.to_str().ok();
        }
        None
    }

    pub fn column_blob(&self, index: c_int) -> Option<&[u8]> {
        let data = unsafe { libsqlite3_sys::sqlite3_column_blob(self.stmt, index) };
        let size = unsafe { libsqlite3_sys::sqlite3_column_bytes(self.stmt, index) };
        if !data.is_null() && size > 0 {
            let data_slice = unsafe { slice::from_raw_parts(data.cast::<u8>(), size as usize) };
            return Some(data_slice);
        }
        None
    }

    pub fn column_floating_point<T: From<c_double>>(&self, index: c_int) -> T {
        let value = unsafe { libsqlite3_sys::sqlite3_column_double(self.stmt, index) };
        T::from(value)
    }

    pub fn column_int(&self, index: c_int) -> c_int {
        unsafe { libsqlite3_sys::sqlite3_column_int(self.stmt, index) }
    }

    pub fn column_int64(&self, index: c_int) -> c_longlong {
        unsafe { libsqlite3_sys::sqlite3_column_int64(self.stmt, index) }
    }
}

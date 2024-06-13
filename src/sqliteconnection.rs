use crate::{Error, Result};
use std::ffi::CStr;
use std::os::raw::{c_char, c_double, c_int, c_longlong};
use std::path::Path;
use std::slice;
use std::thread::sleep;
use std::time::Duration;

#[derive(Clone, Copy)]
pub struct SqliteRow {
    stmt: *mut libsqlite3_sys::sqlite3_stmt,
}

impl SqliteRow {
    pub fn new(stmt: *mut libsqlite3_sys::sqlite3_stmt) -> Self {
        Self { stmt }
    }

    pub fn column_type(&self, index: c_int) -> RowType {
        let column_type = unsafe { libsqlite3_sys::sqlite3_column_type(self.stmt, index) };
        match column_type {
            libsqlite3_sys::SQLITE_INTEGER => RowType::Integer,
            libsqlite3_sys::SQLITE_FLOAT => RowType::Float,
            libsqlite3_sys::SQLITE_BLOB => RowType::Blob,
            libsqlite3_sys::SQLITE_NULL => RowType::Null,
            libsqlite3_sys::SQLITE_TEXT => RowType::Text,
            _ => panic!("Invalid column type"),
        }
    }

    pub fn column_is_null(&self, index: c_int) -> bool {
        self.column_type(index) == RowType::Null
    }

    pub fn column_string(&self, index: c_int) -> Option<&str> {
        let data = unsafe { libsqlite3_sys::sqlite3_column_text(self.stmt, index) };
        if !data.is_null() {
            let c_str = unsafe { CStr::from_ptr(data as *const c_char) };
            return Some(c_str.to_str().unwrap());
        }
        None
    }

    pub fn column_blob(&self, index: c_int) -> Option<&[u8]> {
        let data = unsafe { libsqlite3_sys::sqlite3_column_blob(self.stmt, index) };
        let size = unsafe { libsqlite3_sys::sqlite3_column_bytes(self.stmt, index) };
        if !data.is_null() && size > 0 {
            let data_slice = unsafe { slice::from_raw_parts(data as *const u8, size as usize) };
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

pub struct SqliteStatement {
    stmt: *mut libsqlite3_sys::sqlite3_stmt,
}

impl SqliteStatement {
    pub fn new(stmt: *mut libsqlite3_sys::sqlite3_stmt) -> Self {
        Self { stmt }
    }

    pub fn bind(&self, index: c_int, value: c_int) -> Result<()> {
        self.check_rc(unsafe { libsqlite3_sys::sqlite3_bind_int(self.stmt, index, value) })?;
        Ok(())
    }

    pub fn bind_int64(&self, index: c_int, value: c_longlong) -> Result<()> {
        self.check_rc(unsafe { libsqlite3_sys::sqlite3_bind_int64(self.stmt, index, value) })?;
        Ok(())
    }

    pub fn bind_double(&self, index: c_int, value: c_double) -> Result<()> {
        self.check_rc(unsafe { libsqlite3_sys::sqlite3_bind_double(self.stmt, index, value) })?;
        Ok(())
    }

    pub fn bind_text(&self, index: c_int, value: &str) -> Result<()> {
        let c_str = std::ffi::CString::new(value).unwrap();
        self.check_rc(unsafe { libsqlite3_sys::sqlite3_bind_text(self.stmt, index, c_str.as_ptr(), -1, None) })?;
        Ok(())
    }

    pub fn bind_null(&self, index: c_int) -> Result<()> {
        self.check_rc(unsafe { libsqlite3_sys::sqlite3_bind_null(self.stmt, index) })?;
        Ok(())
    }

    pub fn reset(&self) -> Result<()> {
        self.check_rc(unsafe { libsqlite3_sys::sqlite3_reset(self.stmt) })?;
        Ok(())
    }

    pub fn step(&self) -> c_int {
        let mut rc = unsafe { libsqlite3_sys::sqlite3_step(self.stmt) };
        while rc == libsqlite3_sys::SQLITE_BUSY {
            sleep(Duration::from_micros(1));
            rc = unsafe { libsqlite3_sys::sqlite3_step(self.stmt) };
        }
        rc
    }

    fn advance(&mut self) -> bool {
        if !self.stmt.is_null() {
            let rc = self.step();
            if rc == libsqlite3_sys::SQLITE_ROW {
                return true;
            } else {
                self.stmt = std::ptr::null_mut();
            }
        }

        false
    }

    fn error_message(&self) -> String {
        let error_message = unsafe { libsqlite3_sys::sqlite3_errmsg(libsqlite3_sys::sqlite3_db_handle(self.stmt)) };
        let c_str = unsafe { CStr::from_ptr(error_message) };

        c_str.to_string_lossy().into_owned()
    }

    fn check_rc(&self, rc: libc::c_int) -> Result<libc::c_int> {
        if rc != libsqlite3_sys::SQLITE_OK && (rc != libsqlite3_sys::SQLITE_ROW && rc != libsqlite3_sys::SQLITE_DONE) {
            return Err(Error::DatabaseError(self.error_message()));
        }

        Ok(rc)
    }
}

impl Iterator for SqliteStatement {
    type Item = SqliteRow;

    fn next(&mut self) -> Option<Self::Item> {
        if self.advance() {
            Some(SqliteRow::new(self.stmt))
        } else {
            None
        }
    }
}

pub struct SqliteConnection {
    db: *mut libsqlite3_sys::sqlite3,
}

impl SqliteConnection {
    pub fn new(db_path: &Path, mode: AccessMode) -> Result<Self> {
        let mut db: *mut libsqlite3_sys::sqlite3 = std::ptr::null_mut();
        let c_path = std::ffi::CString::new(db_path.as_os_str().to_str().unwrap()).unwrap();
        let flags = access_mode_flags(mode);
        let rc = unsafe { libsqlite3_sys::sqlite3_open_v2(c_path.as_ptr(), &mut db, flags, std::ptr::null()) };
        if rc != libsqlite3_sys::SQLITE_OK {
            let error_message = SqliteConnection::last_sqlite_error(db);
            unsafe { libsqlite3_sys::sqlite3_close_v2(db) };
            return Err(Error::DatabaseError(error_message));
        }
        Ok(Self { db })
    }

    pub fn path(&self) -> Option<String> {
        let filename = unsafe { libsqlite3_sys::sqlite3_db_filename(self.db, std::ptr::null()) };
        if !filename.is_null() {
            let c_str = unsafe { CStr::from_ptr(filename as *const c_char) };
            return Some(c_str.to_str().unwrap().to_owned());
        }
        None
    }

    pub fn execute(&self, sql: &str) -> Result<()> {
        let stmt = self.prepare_statement(sql)?;
        let rc = stmt.step();
        if rc == libsqlite3_sys::SQLITE_DONE {
            Ok(())
        } else {
            Err(Error::DatabaseError(self.last_error()))
        }
    }

    pub fn prepare_statement(&self, sql: &str) -> Result<SqliteStatement> {
        let mut stmt: *mut libsqlite3_sys::sqlite3_stmt = std::ptr::null_mut();
        let c_sql = std::ffi::CString::new(sql).unwrap();
        let mut rc = unsafe { libsqlite3_sys::sqlite3_prepare_v2(self.db, c_sql.as_ptr(), -1, &mut stmt, std::ptr::null_mut()) };
        while rc == libsqlite3_sys::SQLITE_BUSY {
            sleep(Duration::from_micros(1));
            rc = unsafe { libsqlite3_sys::sqlite3_prepare_v2(self.db, c_sql.as_ptr(), -1, &mut stmt, std::ptr::null_mut()) };
        }
        if rc != libsqlite3_sys::SQLITE_OK {
            return Err(Error::DatabaseError(self.last_error()));
        }

        Ok(SqliteStatement::new(stmt))
    }

    pub fn execute_sql_statements(&self, sql_contents: &str) -> Result<()> {
        let lines = sql_contents
            .split(';')
            .map(|line| line.trim())
            .filter(|line| !line.is_empty() && !line.starts_with("--"))
            .collect::<Vec<&str>>();
        for line in lines {
            self.execute(line)?;
        }
        Ok(())
    }

    pub fn execute_sql_file(&self, sql_path: &str) -> Result<()> {
        let sql_contents = std::fs::read_to_string(sql_path).unwrap();
        self.execute_sql_statements(&sql_contents)
    }

    pub fn last_error(&self) -> String {
        SqliteConnection::last_sqlite_error(self.db)
    }

    fn last_sqlite_error(db: *mut libsqlite3_sys::sqlite3) -> String {
        let error_message = unsafe { libsqlite3_sys::sqlite3_errmsg(db) };
        let c_str = unsafe { CStr::from_ptr(error_message) };
        c_str.to_string_lossy().into_owned()
    }
}

impl Drop for SqliteConnection {
    fn drop(&mut self) {
        unsafe { libsqlite3_sys::sqlite3_close_v2(self.db) };
    }
}

#[derive(PartialEq, Eq)]
pub enum RowType {
    Integer,
    Float,
    Blob,
    Null,
    Text,
}

#[derive(Debug, Copy, Clone)]
pub enum AccessMode {
    ReadOnly,
    ReadWrite,
    Create,
}

fn access_mode_flags(mode: AccessMode) -> c_int {
    match mode {
        AccessMode::ReadOnly => libsqlite3_sys::SQLITE_OPEN_READONLY,
        AccessMode::ReadWrite => libsqlite3_sys::SQLITE_OPEN_READWRITE,
        AccessMode::Create => libsqlite3_sys::SQLITE_OPEN_READWRITE | libsqlite3_sys::SQLITE_OPEN_CREATE,
    }
}

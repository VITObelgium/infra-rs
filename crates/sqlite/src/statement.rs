use crate::{Error, Result, Row};
use std::ffi::{CStr, c_double, c_int, c_longlong};

pub struct Statement {
    stmt: *mut libsqlite3_sys::sqlite3_stmt,
}

impl Statement {
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
        let c_str = std::ffi::CString::new(value)?;
        self.check_rc(unsafe {
            libsqlite3_sys::sqlite3_bind_text(self.stmt, index, c_str.as_ptr(), -1, libsqlite3_sys::SQLITE_TRANSIENT())
        })?;
        Ok(())
    }

    pub fn bind_null(&self, index: c_int) -> Result<()> {
        self.check_rc(unsafe { libsqlite3_sys::sqlite3_bind_null(self.stmt, index) })?;
        Ok(())
    }

    pub fn bind_blob(&self, index: c_int, value: &[u8]) -> Result<()> {
        self.check_rc(unsafe {
            libsqlite3_sys::sqlite3_bind_blob(
                self.stmt,
                index,
                value.as_ptr().cast::<std::ffi::c_void>(),
                value.len() as c_int,
                libsqlite3_sys::SQLITE_STATIC(),
            )
        })?;
        Ok(())
    }

    pub fn reset(&self) -> Result<()> {
        self.check_rc(unsafe { libsqlite3_sys::sqlite3_reset(self.stmt) })?;
        Ok(())
    }

    pub fn step(&self) -> c_int {
        let mut rc = unsafe { libsqlite3_sys::sqlite3_step(self.stmt) };
        while rc == libsqlite3_sys::SQLITE_BUSY {
            std::thread::sleep(std::time::Duration::from_micros(1));
            rc = unsafe { libsqlite3_sys::sqlite3_step(self.stmt) };
        }
        rc
    }

    fn advance(&mut self) -> bool {
        if !self.stmt.is_null() {
            let rc = self.step();
            if rc == libsqlite3_sys::SQLITE_ROW {
                return true;
            }
        }

        false
    }

    fn error_message(&self) -> String {
        let error_message = unsafe { libsqlite3_sys::sqlite3_errmsg(libsqlite3_sys::sqlite3_db_handle(self.stmt)) };
        let c_str = unsafe { CStr::from_ptr(error_message) };

        c_str.to_string_lossy().into_owned()
    }

    fn check_rc(&self, rc: c_int) -> Result<c_int> {
        if rc != libsqlite3_sys::SQLITE_OK && (rc != libsqlite3_sys::SQLITE_ROW && rc != libsqlite3_sys::SQLITE_DONE) {
            return Err(Error::DatabaseError(self.error_message()));
        }

        Ok(rc)
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        unsafe { libsqlite3_sys::sqlite3_finalize(self.stmt) };
    }
}

impl Iterator for Statement {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        if self.advance() { Some(Row::new(self.stmt)) } else { None }
    }
}

use crate::{AccessMode, Error, Result, Statement};
use std::ffi::CStr;
use std::os::raw::{c_char, c_int};
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

pub struct Connection {
    db: *mut libsqlite3_sys::sqlite3,
}

impl Connection {
    pub fn new(db_path: &Path, mode: AccessMode) -> Result<Self> {
        let mut db: *mut libsqlite3_sys::sqlite3 = std::ptr::null_mut();
        let c_path = std::ffi::CString::new(db_path.to_string_lossy().to_string())?;
        let flags = access_mode_flags(mode);
        let rc = unsafe { libsqlite3_sys::sqlite3_open_v2(c_path.as_ptr(), &mut db, flags, std::ptr::null()) };
        if rc != libsqlite3_sys::SQLITE_OK {
            let error_message = Connection::last_sqlite_error(db);
            unsafe { libsqlite3_sys::sqlite3_close(db) };
            return Err(Error::DatabaseError(error_message));
        }
        Ok(Self { db })
    }

    pub fn path(&self) -> Option<String> {
        let filename = unsafe { libsqlite3_sys::sqlite3_db_filename(self.db, std::ptr::null()) };
        if !filename.is_null() {
            let c_str = unsafe { CStr::from_ptr(filename.cast::<c_char>()) };
            return Some(c_str.to_string_lossy().to_string());
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

    pub fn prepare_statement(&self, sql: &str) -> Result<Statement> {
        let mut stmt: *mut libsqlite3_sys::sqlite3_stmt = std::ptr::null_mut();
        let c_sql = std::ffi::CString::new(sql)?;
        let mut rc =
            unsafe { libsqlite3_sys::sqlite3_prepare_v2(self.db, c_sql.as_ptr(), -1, &mut stmt, std::ptr::null_mut()) };
        while rc == libsqlite3_sys::SQLITE_BUSY {
            sleep(Duration::from_micros(1));
            rc = unsafe {
                libsqlite3_sys::sqlite3_prepare_v2(self.db, c_sql.as_ptr(), -1, &mut stmt, std::ptr::null_mut())
            };
        }
        if rc != libsqlite3_sys::SQLITE_OK {
            return Err(Error::DatabaseError(self.last_error()));
        }

        Ok(Statement::new(stmt))
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

    pub fn execute_sql_file(&self, sql_path: &Path) -> Result<()> {
        let sql_contents =
            std::fs::read_to_string(sql_path).map_err(|e| Error::Runtime(format!("Failed to open sql file: {}", e)))?;
        self.execute_sql_statements(&sql_contents)
    }

    pub fn last_error(&self) -> String {
        Connection::last_sqlite_error(self.db)
    }

    fn last_sqlite_error(db: *mut libsqlite3_sys::sqlite3) -> String {
        let error_message = unsafe { libsqlite3_sys::sqlite3_errmsg(db) };
        let c_str = unsafe { CStr::from_ptr(error_message) };
        c_str.to_string_lossy().into_owned()
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        unsafe { libsqlite3_sys::sqlite3_close(self.db) };
    }
}

fn access_mode_flags(mode: AccessMode) -> c_int {
    match mode {
        AccessMode::ReadOnly => libsqlite3_sys::SQLITE_OPEN_READONLY,
        AccessMode::ReadWrite => libsqlite3_sys::SQLITE_OPEN_READWRITE,
        AccessMode::Create => libsqlite3_sys::SQLITE_OPEN_READWRITE | libsqlite3_sys::SQLITE_OPEN_CREATE,
    }
}

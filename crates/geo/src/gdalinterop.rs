use std::{
    ffi::CString,
    path::{Path, PathBuf},
};

use crate::{Error, Result};
use gdal::{cpl::CslStringList, errors::GdalError};
use inf::fs;

pub const FALSE: std::ffi::c_int = 0;
pub const TRUE: std::ffi::c_int = 1;

pub struct Config {
    pub debug_logging: bool,
    pub proj_db_search_location: PathBuf,
    pub config_options: Vec<(String, String)>,
}

impl Config {
    pub fn apply(&self) -> Result<()> {
        setup_logging(self.debug_logging);
        let proj_db_path = self.proj_db_search_location.to_string_lossy().to_string();
        if !proj_db_path.is_empty() {
            gdal::config::set_config_option("PROJ_DATA", proj_db_path.as_str())?;

            // Also set the environment variable unless it is already set by the user
            // e.g. Spatialite library does not use gdal settings
            if std::env::var_os("PROJ_DATA").is_none() {
                // # Safety
                // The config is applied at te beginning of the program
                // so not race conditions should occur
                unsafe {
                    std::env::set_var("PROJ_DATA", proj_db_path.as_str());
                }
            }
        }

        for (key, value) in &self.config_options {
            gdal::config::set_config_option(key, value)?;
        }

        Ok(())
    }
}

pub fn setup_logging(debug: bool) {
    if debug && gdal::config::set_config_option("CPL_DEBUG", "ON").is_err() {
        log::debug!("Failed to set GDAL debug level");
    }

    gdal::config::set_config_option("CPL_LOG_ERRORS", "ON").unwrap_or_else(|_| {
        log::warn!("Failed to set CPL_LOG_ERRORS, GDAL error logging may not work as expected");
    });

    gdal::config::set_error_handler(|sev, _ec, msg| {
        use gdal::errors::CplErrType;
        match sev {
            CplErrType::Debug => log::debug!("GDAL: {msg}"),
            CplErrType::Warning => log::warn!("GDAL: {msg}"),
            CplErrType::Failure | CplErrType::Fatal => log::error!("GDAL: {msg}"),
            CplErrType::None => {}
        }
    });
}

pub fn create_string_list(options: &[String]) -> Result<CslStringList> {
    let mut result = CslStringList::new();
    for opt in options {
        result.add_string(opt)?;
    }

    Ok(result)
}

pub fn create_output_directory_if_needed(p: &Path) -> Result<()> {
    if p.starts_with("/vsi") {
        // this is a gdal virtual filesystem path
        return Ok(());
    }

    Ok(fs::create_directory_for_file(p)?)
}

pub fn check_rc(rc: gdal_sys::CPLErr::Type) -> std::result::Result<(), GdalError> {
    if rc != gdal_sys::CPLErr::CE_None {
        let msg = last_error_message();
        let last_err_no = unsafe { gdal_sys::CPLGetLastErrorNo() };
        Err(GdalError::CplError {
            class: rc,
            number: last_err_no,
            msg,
        })
    } else {
        Ok(())
    }
}

pub fn check_pointer<T>(ptr: *mut T, method_name: &'static str) -> std::result::Result<*mut T, GdalError> {
    if ptr.is_null() {
        let msg = last_error_message();
        unsafe { gdal_sys::CPLErrorReset() };
        Err(GdalError::NullPointer { method_name, msg })
    } else {
        Ok(ptr)
    }
}

fn raw_string_to_string(raw_ptr: *const std::ffi::c_char) -> String {
    let c_str = unsafe { std::ffi::CStr::from_ptr(raw_ptr) };
    c_str.to_string_lossy().into_owned()
}

pub fn last_error_message() -> String {
    raw_string_to_string(unsafe { gdal_sys::CPLGetLastErrorMsg() })
}

/// In memory file for GDAL
/// Use this to create a file in memory that can be used by GDAL
/// This is useful for creating temporary files that do not need to be persisted to disk
pub struct MemoryFile {
    path: std::path::PathBuf,
    file_ptr: *mut gdal_sys::VSILFILE,
}

impl MemoryFile {
    #[allow(dead_code)]
    pub fn with_data(path: &Path, data: &[u8]) -> Result<Self> {
        let path_str = CString::new(path.to_string_lossy().as_ref())?;
        let file_ptr = unsafe {
            gdal_sys::VSIFileFromMemBuffer(
                path_str.as_ptr(),
                data.as_ptr() as *mut gdal_sys::GByte,
                data.len() as gdal_sys::vsi_l_offset,
                FALSE, /*do not take ownership*/
            )
        };

        Ok(MemoryFile {
            path: path.to_path_buf(),
            file_ptr,
        })
    }

    pub fn empty(path: &Path) -> Result<Self> {
        let path_str = CString::new(path.to_string_lossy().as_ref())?;
        let mode = CString::new("w")?;
        let file_ptr = check_pointer(unsafe { gdal_sys::VSIFOpenL(path_str.as_ptr(), mode.as_ptr()) }, "Open memory file")?;

        Ok(MemoryFile {
            path: path.to_path_buf(),
            file_ptr,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn as_slice(&self) -> Result<&[u8]> {
        let mut len: gdal_sys::vsi_l_offset = 0;
        let path_str = CString::new(self.path.to_string_lossy().as_ref())?;
        unsafe {
            let data = check_pointer(
                gdal_sys::VSIGetMemFileBuffer(path_str.as_ptr(), &mut len, FALSE /*do not take ownership*/),
                "VSIGetMemFileBuffer",
            )?;
            Ok(std::slice::from_raw_parts(data as *const u8, len as usize))
        }
    }

    #[allow(dead_code)]
    pub fn write(&mut self, data: &[u8]) -> Result<()> {
        let bytes_written = unsafe { gdal_sys::VSIFWriteL(data.as_ptr().cast::<std::ffi::c_void>(), 1, data.len(), self.file_ptr) };
        if bytes_written != data.len() {
            Err(Error::Runtime("Failed to write to memory file".to_string()))
        } else {
            Ok(())
        }
    }
}

impl Drop for MemoryFile {
    fn drop(&mut self) {
        unsafe {
            gdal_sys::VSIFCloseL(self.file_ptr);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MemoryFile;

    #[test]
    fn empty_memory_file() {
        let path = std::path::Path::new("/vsimem/test");

        let mut mem_file = MemoryFile::empty(path).unwrap();
        assert_eq!(mem_file.path(), path);

        let test_data = b"test data";
        mem_file.write(test_data).unwrap();

        assert!(!mem_file.as_slice().unwrap().is_empty());
    }

    #[test]
    fn filled_memory_file() {
        let path = std::path::Path::new("/vsimem/test_filled");

        let test_data = b"test data";
        let mem_file = MemoryFile::with_data(path, test_data).unwrap();
        assert_eq!(mem_file.path(), path);
        assert_eq!(mem_file.as_slice().unwrap(), test_data);

        // let test_data = b"test data";
        // mem_file.write(test_data).unwrap();

        // assert!(!mem_file.as_slice().unwrap().is_empty());
    }
}

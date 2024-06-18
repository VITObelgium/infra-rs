use std::path::PathBuf;

use crate::Result;
use gdal::{cpl::CslStringList, errors::GdalError};

pub struct Config {
    pub debug_logging: bool,
    pub proj_db_search_location: PathBuf,
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
                std::env::set_var("PROJ_DATA", proj_db_path.as_str());
            }
        }

        Ok(())
    }
}

pub fn setup_logging(debug: bool) {
    if debug && gdal::config::set_config_option("CPL_DEBUG", "ON").is_err() {
        log::debug!("Failed to set GDAL debug level")
    }

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

pub fn check_gdal_rc(rc: gdal_sys::CPLErr::Type) -> std::result::Result<(), GdalError> {
    if rc != 0 {
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

pub fn check_gdal_pointer(
    ptr: *mut libc::c_void,
    method_name: &'static str,
) -> std::result::Result<*mut libc::c_void, GdalError> {
    if ptr.is_null() {
        let msg = last_error_message();
        unsafe { gdal_sys::CPLErrorReset() };
        Err(GdalError::NullPointer { method_name, msg })
    } else {
        Ok(ptr)
    }
}

fn raw_string_to_string(raw_ptr: *const libc::c_char) -> String {
    let c_str = unsafe { std::ffi::CStr::from_ptr(raw_ptr) };
    c_str.to_string_lossy().into_owned()
}

fn last_error_message() -> String {
    raw_string_to_string(unsafe { gdal_sys::CPLGetLastErrorMsg() })
}

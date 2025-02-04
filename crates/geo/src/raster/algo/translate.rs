use std::ffi::{c_int, CString};

use crate::{gdalinterop, Error, Result};

struct TranslateOptionsWrapper {
    options: *mut gdal_sys::GDALTranslateOptions,
}

impl TranslateOptionsWrapper {
    fn new(opts: &[String]) -> Result<Self> {
        let option_values = gdalinterop::create_string_list(opts)?;

        unsafe {
            Ok(TranslateOptionsWrapper {
                options: gdal_sys::GDALTranslateOptionsNew(option_values.as_ptr(), core::ptr::null_mut()),
            })
        }
    }
}

impl Drop for TranslateOptionsWrapper {
    fn drop(&mut self) {
        unsafe {
            gdal_sys::GDALTranslateOptionsFree(self.options);
        }
    }
}

pub fn translate_file(
    input_path: &std::path::Path,
    output_path: &std::path::Path,
    options: &[String],
) -> Result<gdal::Dataset> {
    let ds = gdal::Dataset::open(input_path)?;
    translate(&ds, output_path, options)
}

pub fn translate(ds: &gdal::Dataset, output_path: &std::path::Path, options: &[String]) -> Result<gdal::Dataset> {
    let opts = TranslateOptionsWrapper::new(options)?;
    let mut user_error: c_int = 0;
    let ds = unsafe {
        let path_str = CString::new(output_path.to_string_lossy().as_ref())?;
        gdal::Dataset::from_c_dataset(gdalinterop::check_pointer(
            gdal_sys::GDALTranslate(path_str.as_ptr(), ds.c_dataset(), opts.options, &mut user_error),
            "GDALTranslate",
        )?)
    };

    if user_error != 0 {
        return Err(Error::Runtime("GDAL Translate: invalid arguments".to_string()));
    }

    Ok(ds)
}

use std::path::Path;

use inf::gdalinterop;

use crate::{io, Error, Result};

/// Translate a GDAL vector dataset using the provided translate options
/// The options are passed as a list of strings in the form `["-option1", "value1", "-option2", "value2"]`
/// and match the options of the gdal ogr2ogr command line tool
/// The translated dataset is returned
pub fn translate_vector(ds: &gdal::Dataset, options: &[String]) -> Result<gdal::Dataset> {
    let mem_ds = io::dataset::create_in_memory()?;
    let mut opts = VectorTranslateOptions::new(options)?;

    let mut usage_error: std::ffi::c_int = 0;
    unsafe {
        gdal_sys::GDALVectorTranslate(
            std::ptr::null_mut(),
            mem_ds.c_dataset(),
            1,
            &mut ds.c_dataset(),
            opts.c_options(),
            &mut usage_error,
        );
    }

    if usage_error == gdalinterop::TRUE {
        return Err(Error::InvalidArgument(
            "Vector translate: invalid arguments".to_string(),
        ));
    }

    Ok(mem_ds)
}

/// Translate a GDAL vector dataset to disk using the provided translate options
/// The options are passed as a list of strings in the form `["-option1", "value1", "-option2", "value2"]`
/// and match the options of the gdal ogr2ogr command line tool
/// The dataset is returned in case the user wants to continue working with it but can also be ignored
pub fn translate_ds_to_disk(ds: &gdal::Dataset, path: &Path, options: &[String]) -> Result<gdal::Dataset> {
    gdalinterop::create_output_directory_if_needed(path)?;
    let path_str = std::ffi::CString::new(path.to_string_lossy().as_ref())?;
    let mut opts = VectorTranslateOptions::new(options)?;
    let mut usage_error: std::ffi::c_int = 0;

    let handle = unsafe {
        gdal_sys::GDALVectorTranslate(
            path_str.as_ptr(),
            std::ptr::null_mut(),
            1,
            &mut ds.c_dataset(),
            opts.c_options(),
            &mut usage_error,
        )
    };

    if usage_error == gdalinterop::TRUE {
        return Err(Error::InvalidArgument(
            "Vector translate: invalid arguments".to_string(),
        ));
    }

    gdalinterop::check_pointer(handle, "GDALVectorTranslate")?;

    Ok(unsafe { gdal::Dataset::from_c_dataset(handle) })
}

struct VectorTranslateOptions {
    options: *mut gdal_sys::GDALVectorTranslateOptions,
}

impl VectorTranslateOptions {
    fn new(opts: &[String]) -> Result<Self> {
        let mut c_opts = gdal::cpl::CslStringList::new();
        for opt in opts {
            c_opts.add_string(opt)?;
        }

        let options = unsafe { gdal_sys::GDALVectorTranslateOptionsNew(c_opts.as_ptr(), std::ptr::null_mut()) };
        if options.is_null() {
            return Err(Error::InvalidArgument(
                "Failed to create vector translate options".to_string(),
            ));
        }

        Ok(Self { options })
    }

    fn c_options(&mut self) -> *mut gdal_sys::GDALVectorTranslateOptions {
        self.options
    }
}

impl Drop for VectorTranslateOptions {
    fn drop(&mut self) {
        unsafe { gdal_sys::GDALVectorTranslateOptionsFree(self.c_options()) };
    }
}

use std::path::Path;

use gdal::raster::GdalType;
use inf::allocate::{self, AlignedVec};

use crate::{
    ArrayNum, Error, GeoReference, Result, gdalinterop,
    raster::{self, formats},
    vector::{BurnValue, gdalio},
};

#[derive(Debug, Default)]
pub struct RasterizeOptions<T: num::One> {
    /// the attribute field used to burn the values, or a fixed value
    pub burn_value: BurnValue<T>,
    /// If Some the raster will be initialized with this value for rasterization
    pub init_value: Option<T>,
    pub add: bool,
    pub all_touched: bool,
    pub meta: GeoReference,
    /// if none, the first layer will be used
    pub input_layer: Option<String>,
    pub target_aligned_pixels: bool,
    /// Additional cli options to pass to the rasterize command
    /// in the form `["-option1", "value1", "-option2", "value2"]`
    /// and match the options of the gdal `gdal_rasterize` command line tool
    /// Use when the provided options are not sufficient or new command line options are not supported yet
    pub cli_options: Vec<String>,
}

impl<T: num::One + ToString> From<RasterizeOptions<T>> for Vec<String> {
    fn from(options: RasterizeOptions<T>) -> Vec<String> {
        let mut options_vec = Vec::new();
        if let Some(input_layer) = options.input_layer {
            options_vec.push("-l".to_string());
            options_vec.push(input_layer);
        }

        if options.add {
            options_vec.push("-add".to_string());
        }

        if options.all_touched {
            options_vec.push("-at".to_string());
        }

        if options.target_aligned_pixels {
            options_vec.push("-tap".to_string());
        }

        match options.burn_value {
            BurnValue::Field(field_name) => {
                options_vec.push("-a".to_string());
                options_vec.push(field_name);
            }
            BurnValue::Value(value) => {
                options_vec.push("-burn".to_string());
                options_vec.push(value.to_string());
            }
        }

        if let Some(init_value) = options.init_value {
            options_vec.push("-init".to_string());
            options_vec.push(init_value.to_string());
        }

        options_vec.extend(options.cli_options);

        options_vec
    }
}

pub fn rasterize_ds<T: ArrayNum + GdalType + ToString>(
    ds: &gdal::Dataset,
    meta: &GeoReference,
    options: RasterizeOptions<T>,
) -> Result<(GeoReference, AlignedVec<T>)> {
    if options.add
        && let Some(nodata_value) = options.meta.nodata()
        && nodata_value.is_nan()
    {
        return Err(Error::InvalidArgument(
            "Rasterize output nodata is nan, this is not compatible with the add algorithm".to_string(),
        ));
    }

    let cli_options: Vec<String> = options.into();
    rasterize_ds_with_cli_options(ds, meta, &cli_options)
}

pub fn rasterize<T: ArrayNum + GdalType + ToString>(
    vector_path: &Path,
    meta: &GeoReference,
    options: RasterizeOptions<T>,
) -> Result<(GeoReference, AlignedVec<T>)> {
    let ds = gdalio::dataset::open_read_only(vector_path)?;
    rasterize_ds(&ds, meta, options)
}

/// Rasterize a GDAL vector dataset using the provided rasterize options
/// The options are passed as a list of strings in the form `["-option1", "value1", "-option2", "value2"]`
/// and match the options of the gdal `gdal_rasterize` command line tool
/// The rasterized dataset is returned
pub fn rasterize_ds_with_cli_options<T: ArrayNum + GdalType>(
    ds: &gdal::Dataset,
    meta: &GeoReference,
    options: &[String],
) -> Result<(GeoReference, AlignedVec<T>)> {
    let gdal_options = RasterizeOptionsWrapper::new(options)?;

    let data = allocate::aligned_vec_filled_with(meta.nodata_as::<T>()?.unwrap_or(T::zero()), meta.rows() * meta.columns());
    let mut mem_ds = raster::formats::gdal::create_in_memory_dataset_with_data::<T>(meta, &data)?;

    raster::formats::gdal::metadata_to_dataset_band(&mut mem_ds, meta, 1)?;

    let mut usage_error: std::ffi::c_int = gdal_sys::CPLErr::CE_None as std::ffi::c_int;
    unsafe {
        gdal_sys::GDALRasterize(
            std::ptr::null_mut(),
            mem_ds.c_dataset(),
            ds.c_dataset(),
            gdal_options.c_options(),
            &mut usage_error,
        );
    }

    if usage_error == gdalinterop::TRUE {
        return Err(Error::InvalidArgument("Vector rasterize: invalid arguments".to_string()));
    }

    let meta = formats::gdal::read_band_metadata(&mem_ds, 1)?;
    Ok((meta, data))
}

/// Rasterize a GDAL vector dataset using the provided rasterize options
/// The options are passed as a list of strings in the form `["-option1", "value1", "-option2", "value2"]`
/// and match the options of the gdal `gdal_rasterize` command line tool
/// The rasterized dataset is returned
pub fn rasterize_with_cli_options<T: ArrayNum + GdalType>(
    vector_path: &Path,
    meta: &GeoReference,
    options: &[String],
) -> Result<(GeoReference, AlignedVec<T>)> {
    let ds = gdalio::dataset::open_read_only(vector_path)?;
    rasterize_ds_with_cli_options(&ds, meta, options)
}

/// Convenience function to rasterize a vector dataset to disk
/// Avoids creating an in-memory dataset that then needs to be written to disk
pub fn rasterize_ds_to_disk_with_cli_options(ds: &gdal::Dataset, output_path: &Path, options: &[String]) -> Result<gdal::Dataset> {
    let gdal_options = RasterizeOptionsWrapper::new(options)?;
    let path_cstr = std::ffi::CString::new(output_path.to_string_lossy().to_string())?;

    let mut usage_error: std::ffi::c_int = 0;
    let handle = unsafe {
        gdal_sys::GDALRasterize(
            path_cstr.as_ptr(),
            std::ptr::null_mut(),
            ds.c_dataset(),
            gdal_options.c_options(),
            &mut usage_error,
        )
    };

    if usage_error == gdalinterop::TRUE {
        return Err(Error::InvalidArgument("Vector rasterize: invalid arguments".to_string()));
    }

    gdalinterop::check_pointer(handle, "GDALRasterize")?;

    Ok(unsafe { gdal::Dataset::from_c_dataset(handle) })
}

/// Convenience function to rasterize a vector dataset to disk
/// Avoids creating an in-memory dataset that then needs to be written to disk
pub fn rasterize_to_disk_with_cli_options(input_vector_path: &Path, output_path: &Path, options: &[String]) -> Result<gdal::Dataset> {
    let ds = gdalio::dataset::open_read_only(input_vector_path)?;
    rasterize_ds_to_disk_with_cli_options(&ds, output_path, options)
}

struct RasterizeOptionsWrapper {
    options: *mut gdal_sys::GDALRasterizeOptions,
}

impl RasterizeOptionsWrapper {
    fn new(opts: &[String]) -> Result<Self> {
        let mut c_opts = gdal::cpl::CslStringList::new();
        for opt in opts {
            c_opts.add_string(opt)?;
        }

        let options = unsafe { gdal_sys::GDALRasterizeOptionsNew(c_opts.as_ptr(), std::ptr::null_mut()) };
        if options.is_null() {
            return Err(Error::InvalidArgument("Failed to create rasterize options".to_string()));
        }

        Ok(Self { options })
    }

    fn c_options(&self) -> *mut gdal_sys::GDALRasterizeOptions {
        self.options
    }
}

impl Drop for RasterizeOptionsWrapper {
    fn drop(&mut self) {
        unsafe { gdal_sys::GDALRasterizeOptionsFree(self.c_options()) };
    }
}

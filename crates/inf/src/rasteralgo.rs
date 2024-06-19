use gdal::cpl::CslStringList;
use std::ffi::CString;

use crate::{
    gdalinterop::{check_gdal_pointer, check_gdal_rc, create_string_list},
    Error, Result,
};

struct TranslateOptionsWrapper {
    options: *mut gdal_sys::GDALTranslateOptions,
}

impl TranslateOptionsWrapper {
    fn new(opts: &[String]) -> Result<Self> {
        let option_values = create_string_list(opts)?;

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
    let mut user_error: libc::c_int = 0;
    let ds = unsafe {
        gdal::Dataset::from_c_dataset(check_gdal_pointer(
            gdal_sys::GDALTranslate(
                output_path.to_string_lossy().to_string().as_ptr() as *const i8,
                ds.c_dataset(),
                opts.options,
                &mut user_error,
            ),
            "GDALTranslate",
        )?)
    };

    if user_error != 0 {
        return Err(Error::Runtime("GDAL Translate: invalid arguments".to_string()));
    }

    Ok(ds)
}

pub struct WarpOptions {
    pub resample_algo: gdal::raster::ResampleAlg,
    //PolygonCRef clipPolygon TODO
    pub clip_blend_distance: Option<f64>,
    pub additional_options: Option<Vec<String>>,
}

impl Default for WarpOptions {
    fn default() -> Self {
        WarpOptions {
            resample_algo: gdal::raster::ResampleAlg::NearestNeighbour,
            clip_blend_distance: None,
            additional_options: None,
        }
    }
}

pub fn warp(src_ds: &gdal::Dataset, dst_ds: &gdal::Dataset, options: &WarpOptions) -> Result<()> {
    let mut str_options = CslStringList::new();
    str_options.add_string("NUM_THREADS=ALL_CPUS")?;

    if let Some(opts) = &options.additional_options {
        for opt in opts {
            str_options.add_string(opt)?;
        }
    }

    unsafe {
        let warp_options = gdal_sys::GDALCreateWarpOptions();
        (*warp_options).papszWarpOptions = gdal_sys::CSLDuplicate(str_options.as_ptr());
        (*warp_options).hSrcDS = src_ds.c_dataset();
        (*warp_options).hDstDS = dst_ds.c_dataset();
        (*warp_options).hDstDS = dst_ds.c_dataset();
        (*warp_options).nBandCount = 1;
        (*warp_options).panSrcBands =
            gdal_sys::CPLMalloc(std::mem::size_of::<libc::c_int>() * (*warp_options).nBandCount as usize)
                as *mut libc::c_int;
        (*warp_options).panSrcBands.wrapping_add(0).write(1); // warpOptions->panSrcBands[0]   = 1;
        (*warp_options).panDstBands =
            gdal_sys::CPLMalloc(std::mem::size_of::<libc::c_int>() * (*warp_options).nBandCount as usize)
                as *mut libc::c_int;
        (*warp_options).panDstBands.wrapping_add(0).write(1); // warpOptions->panDstBands[0]   = 1;
        (*warp_options).pfnTransformer = Some(gdal_sys::GDALGenImgProjTransform);
        (*warp_options).eResampleAlg = options.resample_algo.to_gdal();

        // if (options.clipPolygon.get() != nullptr) {
        //     warpOptions->hCutline = options.clipPolygon.get()->clone();
        // }

        if let Some(clip_dist) = options.clip_blend_distance {
            (*warp_options).dfCutlineBlendDist = clip_dist;
        }

        let dst_band = src_ds.rasterband(1)?;

        let band_size = (*warp_options).nBandCount as usize * std::mem::size_of::<libc::c_double>();
        if let Some(src_nodata_value) = dst_band.no_data_value() {
            // will get freed by gdal
            (*warp_options).padfSrcNoDataReal = gdal_sys::CPLMalloc(band_size) as *mut libc::c_double;
            // C++ equivalent: padfSrcNoDataReal[0] = src_nodata_value;
            (*warp_options)
                .padfSrcNoDataReal
                .wrapping_add(0)
                .write(src_nodata_value);
        }

        if let Some(dst_nodata_value) = dst_ds.rasterband(1)?.no_data_value() {
            // will get freed by gdal
            (*warp_options).padfDstNoDataReal = gdal_sys::CPLMalloc(band_size) as *mut libc::c_double;
            // C++ equivalent: padfDstNoDataReal[0] = dstNodataValue.value();
            (*warp_options)
                .padfDstNoDataReal
                .wrapping_add(0)
                .write(dst_nodata_value);
        }

        const FALSE: i32 = 0;

        (*warp_options).pTransformerArg = gdal_sys::GDALCreateGenImgProjTransformer(
            src_ds.c_dataset(),
            std::ptr::null_mut(),
            dst_ds.c_dataset(),
            std::ptr::null_mut(),
            FALSE,
            0.0,
            0,
        );

        if (*warp_options).pTransformerArg.is_null() {
            return Err(Error::Runtime("Failed to create transformer".to_string()));
        }

        let operation = gdal_sys::GDALCreateWarpOperation(warp_options);
        if operation.is_null() {
            return Err(Error::Runtime("Failed to create warp operation".to_string()));
        }

        check_gdal_rc(gdal_sys::GDALChunkAndWarpImage(
            operation,
            0,
            0,
            dst_band.x_size() as i32,
            dst_band.y_size() as i32,
        ))?;

        gdal_sys::GDALDestroyGenImgProjTransformer((*warp_options).pTransformerArg);
        gdal_sys::GDALDestroyWarpOptions(warp_options);
    }

    Ok(())
}

struct WarpAppOptionsWrapper {
    options: *mut gdal_sys::GDALWarpAppOptions,
}

impl WarpAppOptionsWrapper {
    fn new(opts: &[String]) -> Result<Self> {
        let mut c_opts = CslStringList::new();
        for opt in opts {
            c_opts.add_string(opt)?;
        }

        Ok(WarpAppOptionsWrapper {
            options: unsafe { gdal_sys::GDALWarpAppOptionsNew(c_opts.as_ptr(), core::ptr::null_mut()) },
        })
    }

    fn set_warp_options(&mut self, key_value_options: &Vec<(String, String)>) -> Result<()> {
        for (key, value) in key_value_options {
            self.set_warp_option(key, value)?;
        }

        Ok(())
    }

    fn set_warp_option(&mut self, key: &str, value: &str) -> Result<()> {
        let key = CString::new(key)?;
        let val = CString::new(value)?;
        unsafe {
            gdal_sys::GDALWarpAppOptionsSetWarpOption(self.options, key.as_ptr(), val.as_ptr());
        }

        Ok(())
    }
}

impl Drop for WarpAppOptionsWrapper {
    fn drop(&mut self) {
        unsafe {
            gdal_sys::GDALWarpAppOptionsFree(self.options);
        }
    }
}

pub fn warp_cli(
    src_ds: &gdal::Dataset,
    dst_ds: &mut gdal::Dataset,
    options: &[String],
    key_value_options: &Vec<(String, String)>,
) -> Result<()> {
    let mut warp_options = WarpAppOptionsWrapper::new(options)?;
    warp_options.set_warp_options(key_value_options)?;

    unsafe {
        let mut user_error: libc::c_int = 0;
        gdal_sys::GDALWarp(
            std::ptr::null(),
            dst_ds.c_dataset(),
            1,
            &mut src_ds.c_dataset(),
            warp_options.options,
            &mut user_error,
        );

        if user_error != 0 {
            return Err(Error::Runtime("GDAL Warp: invalid arguments".to_string()));
        }
    }

    Ok(())
}

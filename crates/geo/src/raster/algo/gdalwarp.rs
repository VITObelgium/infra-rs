use std::{
    ffi::{CString, c_double, c_int},
    path::Path,
};

use crate::{
    Columns, Error, GeoReference, RasterSize, Result, Rows,
    gdalinterop::{self, check_rc},
    raster::algo::{TargetSrs, WarpOptions},
    srs::SpatialReference,
};
use gdal::cpl::CslStringList;

pub struct GdalWarpOptions {
    pub resample_algo: gdal::raster::ResampleAlg,
    pub clip_polygon: Option<gdal::vector::Geometry>,
    pub clip_blend_distance: Option<f64>,
    pub additional_options: Option<Vec<String>>,
    pub all_cpus: bool,
}

impl Default for GdalWarpOptions {
    fn default() -> Self {
        GdalWarpOptions {
            resample_algo: gdal::raster::ResampleAlg::NearestNeighbour,
            clip_polygon: None,
            clip_blend_distance: None,
            additional_options: None,
            all_cpus: true,
        }
    }
}

pub fn warp(src_ds: &gdal::Dataset, dst_ds: &gdal::Dataset, options: &GdalWarpOptions) -> Result<()> {
    let mut str_options = CslStringList::new();
    if options.all_cpus {
        str_options.add_string("NUM_THREADS=ALL_CPUS")?;
    }

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
            gdal_sys::CPLMalloc(std::mem::size_of::<c_int>() * (*warp_options).nBandCount as usize).cast::<c_int>();
        (*warp_options).panSrcBands.wrapping_add(0).write(1); // warpOptions->panSrcBands[0]   = 1;
        (*warp_options).panDstBands =
            gdal_sys::CPLMalloc(std::mem::size_of::<c_int>() * (*warp_options).nBandCount as usize).cast::<c_int>();
        (*warp_options).panDstBands.wrapping_add(0).write(1); // warpOptions->panDstBands[0]   = 1;
        (*warp_options).pfnTransformer = Some(gdal_sys::GDALGenImgProjTransform);
        (*warp_options).eResampleAlg = options.resample_algo.to_gdal();

        if let Some(poly) = options.clip_polygon.as_ref() {
            if poly.geometry_type() != gdal_sys::OGRwkbGeometryType::wkbPolygon {
                return Err(Error::InvalidArgument(
                    "Warp clip polygon geometry type must be a polygon".to_string(),
                ));
            }
            (*warp_options).hCutline = poly.c_geometry();
        }

        if let Some(clip_dist) = options.clip_blend_distance {
            (*warp_options).dfCutlineBlendDist = clip_dist;
        }

        let dst_band = src_ds.rasterband(1)?;

        let band_size = (*warp_options).nBandCount as usize * std::mem::size_of::<c_double>();
        if let Some(src_nodata_value) = dst_band.no_data_value() {
            // will get freed by gdal
            (*warp_options).padfSrcNoDataReal = gdal_sys::CPLMalloc(band_size).cast::<c_double>();
            // C++ equivalent: padfSrcNoDataReal[0] = src_nodata_value;
            (*warp_options).padfSrcNoDataReal.wrapping_add(0).write(src_nodata_value);
        }

        if let Some(dst_nodata_value) = dst_ds.rasterband(1)?.no_data_value() {
            // will get freed by gdal
            (*warp_options).padfDstNoDataReal = gdal_sys::CPLMalloc(band_size).cast::<c_double>();
            // C++ equivalent: padfDstNoDataReal[0] = dstNodataValue.value();
            (*warp_options).padfDstNoDataReal.wrapping_add(0).write(dst_nodata_value);
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

        check_rc(gdal_sys::GDALChunkAndWarpImage(
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

pub fn warp_to_disk_cli(
    src_ds: &gdal::Dataset,
    dest_path: &Path,
    options: &[String],
    key_value_options: &Vec<(String, String)>,
) -> Result<()> {
    let mut warp_options = WarpAppOptionsWrapper::new(options)?;
    warp_options.set_warp_options(key_value_options)?;

    gdalinterop::create_output_directory_if_needed(dest_path)?;

    let path_str = CString::new(dest_path.to_string_lossy().to_string())?;

    unsafe {
        let mut user_error: c_int = 0;
        let handle = gdal_sys::GDALWarp(
            path_str.as_ptr(),
            std::ptr::null_mut(),
            1,
            &mut src_ds.c_dataset(),
            warp_options.options,
            &mut user_error,
        );

        if user_error != 0 {
            return Err(Error::Runtime("GDAL Warp: invalid arguments".to_string()));
        }

        gdal::Dataset::from_c_dataset(gdalinterop::check_pointer(handle, "GDALWarp")?);
    }

    Ok(())
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
        let mut user_error: c_int = 0;
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

pub fn warp_georeference(georef: &GeoReference, opts: &WarpOptions) -> Result<GeoReference> {
    if georef.projection().is_empty() {
        return Err(Error::InvalidArgument(
            "Cannot warp metadata without projection information".to_string(),
        ));
    }

    let target_projection = match &opts.target_srs {
        TargetSrs::Epsg(epsg) => SpatialReference::from_epsg(*epsg)?.to_wkt()?,
        TargetSrs::Proj4(proj4) => proj4.clone(),
    };

    let mem_driver = gdal::DriverManager::get_driver_by_name("MEM")?;
    let mut src_ds = mem_driver.create("in-mem", georef.columns().count() as usize, georef.rows().count() as usize, 0)?;
    src_ds.set_geo_transform(&georef.geo_transform().into())?;
    src_ds.set_projection(georef.projection())?;

    // Create a transformer that maps from source pixel/line coordinates
    // to destination georeferenced coordinates (not destination pixel line).
    // We do that by omitting the destination dataset handle (setting it to nullptr).
    unsafe {
        let target_srs = std::ffi::CString::new(target_projection.clone())?;
        let transformer_arg = gdalinterop::check_pointer(
            gdal_sys::GDALCreateGenImgProjTransformer(
                src_ds.c_dataset(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                target_srs.as_ptr(),
                gdalinterop::FALSE,
                0.0,
                0,
            ),
            "Failed to create projection transformer",
        )?;

        let mut target_transform: gdal::GeoTransform = [0.0; 6];
        let mut rows: std::ffi::c_int = 0;
        let mut cols: std::ffi::c_int = 0;

        let warp_rc = gdal_sys::GDALSuggestedWarpOutput(
            src_ds.c_dataset(),
            Some(gdal_sys::GDALGenImgProjTransform),
            transformer_arg,
            target_transform.as_mut_ptr(),
            &mut cols,
            &mut rows,
        );

        gdal_sys::GDALDestroyGenImgProjTransformer(transformer_arg);

        match crate::gdalinterop::check_rc(warp_rc) {
            Ok(_) => Ok(GeoReference::new(
                target_projection,
                RasterSize {
                    rows: Rows(rows),
                    cols: Columns(cols),
                },
                target_transform.into(),
                georef.nodata(),
                None,
            )),
            Err(e) => {
                gdal_sys::GDALDestroyGenImgProjTransformer(transformer_arg);
                Err(e.into())
            }
        }
    }
}

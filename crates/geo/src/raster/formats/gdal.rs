use gdal::{Metadata as _, cpl::CslStringList, errors::GdalError, raster::GdalType};

use num::NumCast;
use std::{
    ffi::{CString, c_void},
    mem::MaybeUninit,
    path::{Path, PathBuf},
};

use crate::{
    ArrayDataType, ArrayNum, Columns, Error, GeoReference, Nodata, RasterScale, RasterSize, Result, Rows,
    gdalinterop::{FALSE, check_pointer, check_rc, create_output_directory_if_needed},
    raster::{
        Compression, Predictor, TiffChunkType, WriteRasterOptions,
        formats::{RasterFileFormat, RasterFormat, RasterFormatDyn, RasterOpenOptions},
        intersection::{CutOut, intersect_georeference},
        utils::{cast_uninit_byte_slice_mut, cast_uninit_slice_to_byte},
    },
};

pub struct GdalRasterIO {
    ds: gdal::Dataset,
}

impl RasterFileFormat {
    pub fn gdal_driver_name(&self) -> &str {
        match self {
            RasterFileFormat::Memory => "MEM",
            RasterFileFormat::ArcAscii => "AAIGrid",
            RasterFileFormat::GeoTiff => "GTiff",
            RasterFileFormat::Gif => "GIF",
            RasterFileFormat::Png => "PNG",
            RasterFileFormat::PcRaster => "PCRaster",
            RasterFileFormat::Netcdf => "netCDF",
            RasterFileFormat::MBTiles => "MBTiles",
            RasterFileFormat::GeoPackage => "GPKG",
            RasterFileFormat::Grib => "GRIB",
            RasterFileFormat::Postgis => "PostGISRaster",
            RasterFileFormat::Vrt => "VRT",
            RasterFileFormat::Unknown => "Unknown",
        }
    }
}

impl GdalRasterIO {
    pub fn read_raster_band_as<T: ArrayNum>(
        &mut self,
        band_index: usize,
        data_type: ArrayDataType,
        dst_data: &mut [MaybeUninit<T>],
    ) -> Result<GeoReference> {
        let meta = self.georeference(band_index)?;

        debug_assert_eq!(dst_data.len(), meta.raster_size().cell_count());
        check_if_metadata_fits(meta.nodata(), self.data_type(band_index)?, data_type)?;

        let cut_out = CutOut {
            rows: meta.rows().count(),
            cols: meta.columns().count(),
            ..Default::default()
        };

        let dst_data = unsafe {
            std::slice::from_raw_parts_mut(
                dst_data.as_mut_ptr().cast::<MaybeUninit<u8>>(),
                dst_data.len() * std::mem::size_of::<T>(),
            )
        };
        read_region_from_dataset(band_index, &cut_out, &self.ds, dst_data, meta.columns().count(), data_type)?;

        Ok(meta)
    }

    pub fn read_raster_band_region_as<T: ArrayNum>(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        data_type: ArrayDataType,
        dst_data: &mut [MaybeUninit<T>],
    ) -> Result<GeoReference> {
        let meta = self.georeference(band_index)?;
        let cut_out = intersect_georeference(&meta, region)?;

        // Error if the requeated data type can not hold the nodata value of the raster
        check_if_metadata_fits(meta.nodata(), self.data_type(band_index)?, data_type)?;

        let cut_out_smaller_than_extent = (region.rows() * region.columns()) != (cut_out.rows * cut_out.cols) as usize;
        let mut dst_meta = region.clone();
        if let Some(nodata) = meta.nodata() {
            dst_meta.set_nodata(Some(nodata));
        }

        if cut_out_smaller_than_extent && dst_meta.nodata().is_none() {
            dst_meta.set_nodata(Some(NumCast::from(data_type.default_nodata_value()).unwrap_or(-9999.0)));
        }

        let expected_buffer_size = dst_meta.rows() * dst_meta.columns();
        if dst_data.len() != expected_buffer_size {
            return Err(Error::InvalidArgument(format!(
                "Invalid data buffer provided: incorrect size (got {} pixels but should be {expected_buffer_size} pixels)",
                dst_data.len(),
            )));
        }

        if cut_out_smaller_than_extent && let Some(nodata) = dst_meta.nodata() {
            let nodata = NumCast::from(nodata).unwrap_or(T::NODATA);
            for dst_data in dst_data.iter_mut() {
                let _ = *dst_data.write(nodata);
            }
        }

        if cut_out.cols * cut_out.rows > 0 {
            read_region_from_dataset(
                band_index,
                &cut_out,
                &self.ds,
                cast_uninit_slice_to_byte(dst_data),
                dst_meta.columns().count(),
                data_type,
            )?;
        }

        Ok(dst_meta)
    }
}

fn open_with_options(path: impl AsRef<Path>, options: gdal::DatasetOptions) -> Result<gdal::Dataset> {
    let path = path.as_ref();
    gdal::Dataset::open_ex(path, options).map_err(|err| match err {
        // Match on the error to give a cleaner error message when the file does not exist
        GdalError::NullPointer { method_name: _, msg: _ } => {
            if !path.exists() {
                Error::InvalidPath(PathBuf::from(path))
            } else {
                let ras_type = RasterFileFormat::guess_from_path(path);
                if ras_type != RasterFileFormat::Unknown && gdal::DriverManager::get_driver_by_name(ras_type.gdal_driver_name()).is_err() {
                    return Error::Runtime(format!("Gdal driver not supported: {}", ras_type.gdal_driver_name()));
                }

                Error::Runtime(format!(
                    "Failed to open raster dataset ({}), check file correctness or driver configuration ({})",
                    path.to_string_lossy(),
                    err
                ))
            }
        }
        _ => Error::Runtime(format!("Failed to open raster dataset: {} ({})", path.to_string_lossy(), err)),
    })
}

impl GdalRasterIO {
    pub fn from_dataset(ds: gdal::Dataset) -> Self {
        Self { ds }
    }
}

impl RasterFormatDyn for GdalRasterIO {
    fn band_count(&self) -> Result<usize> {
        Ok(self.ds.raster_count())
    }

    fn raster_size(&self) -> Result<RasterSize> {
        let (width, height) = self.ds.raster_size();
        Ok(RasterSize {
            rows: Rows(height as i32),
            cols: Columns(width as i32),
        })
    }

    fn georeference(&mut self, band_index: usize) -> Result<GeoReference> {
        read_band_metadata(&self.ds, band_index)
    }

    fn data_type(&self, band_index: usize) -> Result<crate::ArrayDataType> {
        self.ds.rasterband(band_index)?.band_type().try_into()
    }

    fn overview_count(&self, band_index: usize) -> Result<usize> {
        Ok(self.ds.rasterband(band_index)?.overview_count()? as usize)
    }

    fn read_band_into_byte_buffer(
        &mut self,
        band: usize,
        data_type: crate::ArrayDataType,
        data: &mut [std::mem::MaybeUninit<u8>],
    ) -> Result<GeoReference> {
        match data_type {
            ArrayDataType::Uint8 => self.read_raster_band_as::<u8>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Uint16 => self.read_raster_band_as::<u16>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Uint32 => self.read_raster_band_as::<u32>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Uint64 => self.read_raster_band_as::<u64>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Int8 => self.read_raster_band_as::<i8>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Int16 => self.read_raster_band_as::<i16>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Int32 => self.read_raster_band_as::<i32>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Int64 => self.read_raster_band_as::<i64>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Float32 => self.read_raster_band_as::<f32>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Float64 => self.read_raster_band_as::<f64>(band, data_type, cast_uninit_byte_slice_mut(data)),
        }
    }

    fn read_band_region_into_byte_buffer(
        &mut self,
        band: usize,
        region: &GeoReference,
        data_type: ArrayDataType,
        data: &mut [MaybeUninit<u8>],
    ) -> Result<GeoReference> {
        match data_type {
            ArrayDataType::Uint8 => self.read_raster_band_region_as::<u8>(band, region, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Uint16 => self.read_raster_band_region_as::<u16>(band, region, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Uint32 => self.read_raster_band_region_as::<u32>(band, region, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Uint64 => self.read_raster_band_region_as::<u64>(band, region, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Int8 => self.read_raster_band_region_as::<i8>(band, region, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Int16 => self.read_raster_band_region_as::<i16>(band, region, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Int32 => self.read_raster_band_region_as::<i32>(band, region, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Int64 => self.read_raster_band_region_as::<i64>(band, region, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Float32 => self.read_raster_band_region_as::<f32>(band, region, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Float64 => self.read_raster_band_region_as::<f64>(band, region, data_type, cast_uninit_byte_slice_mut(data)),
        }
    }
}

impl RasterFormat for GdalRasterIO {
    /// Open a GDAL raster dataset for reading
    fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            ds: open_dataset_read_only(path)?,
        })
    }

    /// Open a GDAL raster dataset for reading with driver open options
    fn open_read_only_with_options(path: impl AsRef<Path>, options: &RasterOpenOptions) -> Result<Self> {
        Ok(Self {
            ds: open_dataset_read_only_with_options(path, options)?,
        })
    }

    fn write_band<T: ArrayNum>(
        path: impl AsRef<Path>,
        geo_reference: &GeoReference,
        data: &[T],
        options: crate::raster::WriteRasterOptions,
    ) -> Result<()> {
        let path = path.as_ref();
        create_output_directory_if_needed(path)?;

        // To write a raster to disk we need a dataset that contains the data
        // Create a memory dataset with 0 bands, then assign a band given the pointer of our vector
        // Creating a dataset with 1 band would casuse unnecessary memory allocation
        let mut ds = create_in_memory_dataset_with_data(geo_reference, data)?;
        write_to_disk(&mut ds, path, &write_raster_options_to_gdal(options), &[])?;

        Ok(())
    }
}

/// Open a GDAL raster dataset for reading
pub fn open_dataset_read_only(path: impl AsRef<Path>) -> Result<gdal::Dataset> {
    let options = gdal::DatasetOptions {
        open_flags: gdal::GdalOpenFlags::GDAL_OF_READONLY | gdal::GdalOpenFlags::GDAL_OF_RASTER,
        ..Default::default()
    };

    open_with_options(path, options)
}

fn write_raster_options_to_gdal(options: WriteRasterOptions) -> Vec<String> {
    match options {
        WriteRasterOptions::Default => Vec::default(),
        WriteRasterOptions::GeoTiff(tiff_opts) => {
            let mut opts = Vec::default();
            opts.push(format!(
                "TILED={}",
                match tiff_opts.chunk_type {
                    TiffChunkType::Tiled => "YES",
                    TiffChunkType::Striped => "NO",
                }
            ));

            opts.push(format!(
                "COMPRESS={}",
                match tiff_opts.compression {
                    Some(Compression::Lzw) => "LZW",
                    Some(Compression::Zstd) => "ZSTD",
                    Some(Compression::Deflate) => "DEFLATE",
                    None => "NONE",
                }
            ));

            opts.push(format!(
                "PREDICTOR={}",
                match tiff_opts.predictor {
                    None => "1",
                    Some(Predictor::Horizontal) => "2",
                    Some(Predictor::FloatingPoint) => "3",
                }
            ));

            opts.push(format!(
                "SPARSE_OK={}",
                match tiff_opts.sparse_ok {
                    true => "TRUE",
                    false => "FALSE",
                }
            ));

            opts
        }
    }
}

/// Open a GDAL raster dataset for reading with driver open options
pub fn open_dataset_read_only_with_options(path: impl AsRef<Path>, open_options: &RasterOpenOptions) -> Result<gdal::Dataset> {
    let raster_format = RasterFileFormat::guess_from_path(path.as_ref());
    let open_options = create_gdal_open_options(raster_format, open_options);
    let open_options = open_options.iter().map(|s| s.as_str()).collect::<Vec<_>>();

    let options = gdal::DatasetOptions {
        open_flags: gdal::GdalOpenFlags::GDAL_OF_READONLY | gdal::GdalOpenFlags::GDAL_OF_RASTER,
        open_options: Some(&open_options),
        ..Default::default()
    };

    open_with_options(path, options)
}

/// Reads the [`crate::GeoReference`] from the provided band of a raster file
/// The band index is 1-based
pub fn read_band_metadata(ds: &gdal::Dataset, band_index: usize) -> Result<GeoReference> {
    let rasterband = ds.rasterband(band_index)?;

    let offset = rasterband.offset();
    let scale = rasterband.scale();
    let raster_scale = match (offset, scale) {
        (Some(offset), Some(scale)) => Some(RasterScale { offset, scale }),
        (Some(offset), None) => Some(RasterScale { offset, scale: 1.0 }),
        (None, Some(scale)) => Some(RasterScale { offset: 0.0, scale }),
        _ => None,
    };

    let (width, height) = ds.raster_size();
    Ok(GeoReference::new(
        ds.projection(),
        RasterSize {
            rows: Rows(height as i32),
            cols: Columns(width as i32),
        },
        ds.geo_transform()?.into(),
        rasterband.no_data_value(),
        raster_scale,
    ))
}

/// Creates an in-memory dataset without any bands
pub fn create_in_memory_dataset(size: RasterSize) -> Result<gdal::Dataset> {
    let mem_driver = gdal::DriverManager::get_driver_by_name("MEM")?;
    Ok(mem_driver.create(PathBuf::from("in_mem"), size.cols.count() as usize, size.rows.count() as usize, 0)?)
}

/// Creates an in-memory dataset with the provided metadata.
/// The array passed data will be used as the dataset band.
/// Make sure the data array is the correct size and will live as long as the dataset.
pub fn create_in_memory_dataset_with_data<T: GdalType + Nodata>(meta: &GeoReference, data: &[T]) -> Result<gdal::Dataset> {
    let mut ds = create_in_memory_dataset(meta.raster_size())?;
    add_band_from_data_ptr(&mut ds, data)?;
    metadata_to_dataset_band(&mut ds, meta, 1)?;
    Ok(ds)
}

pub(crate) fn metadata_to_dataset_band(ds: &mut gdal::Dataset, meta: &GeoReference, band_index: usize) -> Result<()> {
    ds.set_geo_transform(&meta.geo_transform().into())?;
    ds.set_projection(meta.projection())?;
    let mut band = ds.rasterband(band_index)?;
    band.set_no_data_value(meta.nodata())?;
    if let Some(scale) = meta.scale() {
        band.set_offset(scale.offset)?;
        band.set_scale(scale.scale)?;
    }

    Ok(())
}

fn add_band_from_data_ptr<T: GdalType>(ds: &mut gdal::Dataset, data: &[T]) -> Result<()> {
    // convert the data pointer to a string
    let data_ptr = format!("DATAPOINTER={:p}", data.as_ptr());

    let mut str_options = gdal::cpl::CslStringList::new();
    str_options.add_string(data_ptr.as_str())?;
    let rc = unsafe { gdal_sys::GDALAddBand(ds.c_dataset(), T::gdal_ordinal(), str_options.as_ptr()) };
    check_rc(rc)?;

    Ok(())
}

fn create_raster_driver_for_path(path: impl AsRef<Path>) -> Result<gdal::Driver> {
    let path = path.as_ref();
    let raster_format = RasterFileFormat::guess_from_path(path);
    if raster_format == RasterFileFormat::Unknown {
        return Err(Error::Runtime(format!(
            "Could not detect raster type from filename: {}",
            path.to_string_lossy()
        )));
    }

    Ok(gdal::DriverManager::get_driver_by_name(raster_format.gdal_driver_name())?)
}

// Write dataset to disk using the Drivers CreateCopy method
fn write_to_disk(
    ds: &mut gdal::Dataset,
    path: impl AsRef<Path>,
    driver_options: &[String],
    metadata_values: &[(String, String)],
) -> Result<()> {
    let path = path.as_ref();
    let driver = create_raster_driver_for_path(path)?;

    let mut c_opts = CslStringList::new();
    for opt in driver_options {
        c_opts.add_string(opt)?;
    }

    if driver_options.is_empty() && driver.description().unwrap_or_default() == RasterFileFormat::GeoTiff.gdal_driver_name() {
        // Provide sane default for GeoTIFF files
        c_opts.add_string("COMPRESS=LZW")?;
        c_opts.add_string("NUM_THREADS=ALL_CPUS")?;
    }

    for (key, value) in metadata_values {
        ds.set_metadata_item(key, value, "")?;
    }

    let path_str = path.to_string_lossy();
    let path_str = CString::new(path_str.to_string())?;

    let ds_handle = check_pointer(
        unsafe {
            gdal_sys::GDALCreateCopy(
                driver.c_driver(),
                path_str.as_ptr(),
                ds.c_dataset(),
                FALSE,
                c_opts.as_ptr(),
                Some(gdal_sys::GDALDummyProgress),
                std::ptr::null_mut(),
            )
        },
        "GDALCreateCopy",
    )
    .map_err(|err| Error::Runtime(format!("Failed to write raster to disk: {err}")))?;

    unsafe { gdal_sys::GDALClose(ds_handle) };

    Ok(())
}

fn create_gdal_open_options(raster_format: RasterFileFormat, open_options: &RasterOpenOptions) -> Vec<String> {
    let mut options = Vec::new();

    if let Some(layer_name) = &open_options.layer_name
        && raster_format == RasterFileFormat::GeoPackage
    {
        options.push(format!("TABLE={}", layer_name));
    }

    options
}

fn check_if_metadata_fits(nodata: Option<f64>, source_data_type: ArrayDataType, target_data_type: ArrayDataType) -> Result {
    if nodata.is_some_and(|nod| !fits_in_type(target_data_type, nod)) {
        return Err(Error::InvalidArgument(format!(
            "Trying to read a raster with native data type {} into a buffer with data type {}, but the rasters nodata value {} does not fit",
            source_data_type,
            target_data_type,
            nodata.unwrap_or_default()
        )));
    }
    Ok(())
}

/// Read a subregion into the provided data buffer.
/// The buffer should be allocated and have the correct size.
/// The band index is 1-based.
fn read_region_from_dataset(
    band_nr: usize,
    cut: &CutOut,
    ds: &gdal::Dataset,
    data: &mut [MaybeUninit<u8>],
    data_cols: i32,
    data_type: ArrayDataType,
) -> Result<()> {
    let mut data_ptr = data.as_mut_ptr();
    if cut.dst_row_offset > 0 {
        data_ptr = unsafe { data_ptr.add((cut.dst_row_offset * data_cols) as usize * data_type.bytes() as usize) };
    }

    if cut.dst_col_offset > 0 {
        data_ptr = unsafe { data_ptr.add(cut.dst_col_offset as usize * data_type.bytes() as usize) };
    }

    let raster_band = ds.rasterband(band_nr)?;
    let window = (cut.src_col_offset, cut.src_row_offset);
    let window_size = (cut.cols, cut.rows);
    let size = window_size;

    unsafe {
        check_rc(gdal_sys::GDALRasterIOEx(
            raster_band.c_rasterband(),
            gdal_sys::GDALRWFlag::GF_Read,
            window.0,
            window.1,
            window_size.0,
            window_size.1,
            data_ptr.cast::<c_void>(),
            size.0,
            size.1,
            crate::gdalinterop::gdal_ordinal_for_data_type(data_type),
            0,
            data_cols as gdal_sys::GSpacing * data_type.bytes() as gdal_sys::GSpacing,
            core::ptr::null_mut(),
        ))?;
    }

    Ok(())
}

fn fits_in_type(data_type: ArrayDataType, value: f64) -> bool {
    match data_type {
        ArrayDataType::Uint8 => inf::cast::fits_in_type::<u8>(value),
        ArrayDataType::Uint16 => inf::cast::fits_in_type::<u16>(value),
        ArrayDataType::Uint32 => inf::cast::fits_in_type::<u32>(value),
        ArrayDataType::Uint64 => inf::cast::fits_in_type::<u64>(value),
        ArrayDataType::Int8 => inf::cast::fits_in_type::<i8>(value),
        ArrayDataType::Int16 => inf::cast::fits_in_type::<i16>(value),
        ArrayDataType::Int32 => inf::cast::fits_in_type::<i32>(value),
        ArrayDataType::Int64 => inf::cast::fits_in_type::<i64>(value),
        ArrayDataType::Float32 => inf::cast::fits_in_type::<f32>(value),
        ArrayDataType::Float64 => inf::cast::fits_in_type::<f64>(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_read_only_invalid_path() {
        let path = PathBuf::from("/this/does/not/exist.tif");
        let res = open_dataset_read_only(path.as_path());
        assert!(res.is_err());
        assert!(matches!(res.err().unwrap(), Error::InvalidPath(p) if p == path));
    }
}

use gdal::errors::GdalError;

use num::NumCast;
use std::{
    ffi::c_void,
    mem::MaybeUninit,
    path::{Path, PathBuf},
};

use crate::{
    ArrayDataType, ArrayNum, Columns, Error, GeoReference, RasterSize, Result, Rows,
    gdalinterop::check_rc,
    raster::{
        intersection::{CutOut, intersect_georeference},
        io::RasterFormat,
        reader::{RasterOpenOptions, RasterReader, RasterReaderDyn},
        utils::reinterpret_uninit_slice_to_byte,
    },
};

pub struct GdalRasterIO {
    ds: gdal::Dataset,
}

impl TryFrom<gdal::raster::GdalDataType> for ArrayDataType {
    type Error = Error;

    fn try_from(value: gdal::raster::GdalDataType) -> std::result::Result<Self, Self::Error> {
        match value {
            gdal::raster::GdalDataType::UInt8 => Ok(ArrayDataType::Uint8),
            gdal::raster::GdalDataType::UInt16 => Ok(ArrayDataType::Uint16),
            gdal::raster::GdalDataType::UInt32 => Ok(ArrayDataType::Uint32),
            gdal::raster::GdalDataType::UInt64 => Ok(ArrayDataType::Uint64),
            gdal::raster::GdalDataType::Int8 => Ok(ArrayDataType::Int8),
            gdal::raster::GdalDataType::Int16 => Ok(ArrayDataType::Int16),
            gdal::raster::GdalDataType::Int32 => Ok(ArrayDataType::Int32),
            gdal::raster::GdalDataType::Int64 => Ok(ArrayDataType::Int64),
            gdal::raster::GdalDataType::Float32 => Ok(ArrayDataType::Float32),
            gdal::raster::GdalDataType::Float64 => Ok(ArrayDataType::Float64),
            gdal::raster::GdalDataType::Unknown => Err(Error::Runtime(format!("Unknown GDAL data type: {:?}", value))),
        }
    }
}

impl RasterFormat {
    pub fn gdal_driver_name(&self) -> &str {
        match self {
            RasterFormat::Memory => "MEM",
            RasterFormat::ArcAscii => "AAIGrid",
            RasterFormat::GeoTiff => "GTiff",
            RasterFormat::Gif => "GIF",
            RasterFormat::Png => "PNG",
            RasterFormat::PcRaster => "PCRaster",
            RasterFormat::Netcdf => "netCDF",
            RasterFormat::MBTiles => "MBTiles",
            RasterFormat::GeoPackage => "GPKG",
            RasterFormat::Grib => "GRIB",
            RasterFormat::Postgis => "PostGISRaster",
            RasterFormat::Vrt => "VRT",
            RasterFormat::Unknown => "Unknown",
        }
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
                let ras_type = RasterFormat::guess_from_path(path);
                if ras_type != RasterFormat::Unknown && gdal::DriverManager::get_driver_by_name(ras_type.gdal_driver_name()).is_err() {
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

impl RasterReaderDyn for GdalRasterIO {
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

    fn read_raster_band_u8(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<u8>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Uint8, dst_data)
    }

    fn read_raster_band_u16(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<u16>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Uint16, dst_data)
    }

    fn read_raster_band_u32(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<u32>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Uint32, dst_data)
    }

    fn read_raster_band_u64(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<u64>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Uint64, dst_data)
    }

    fn read_raster_band_i8(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<i8>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Int8, dst_data)
    }

    fn read_raster_band_i16(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<i16>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Int16, dst_data)
    }

    fn read_raster_band_i32(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<i32>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Int32, dst_data)
    }

    fn read_raster_band_i64(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<i64>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Int64, dst_data)
    }

    fn read_raster_band_f32(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<f32>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Float32, dst_data)
    }

    fn read_raster_band_f64(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<f64>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Float64, dst_data)
    }

    fn read_raster_band_region(
        &mut self,
        band_index: usize,
        extent: &GeoReference,
        data_type: ArrayDataType,
        dst_data: &mut [MaybeUninit<u8>],
    ) -> Result<GeoReference> {
        let meta = self.georeference(band_index)?;
        let cut_out = intersect_georeference(&meta, extent)?;

        // Error if the requeated data type can not hold the nodata value of the raster
        check_if_metadata_fits(meta.nodata(), self.data_type(band_index)?, data_type)?;

        let cut_out_smaller_than_extent = (extent.rows() * extent.columns()) != (cut_out.rows * cut_out.cols) as usize;
        let mut dst_meta = extent.clone();
        if let Some(nodata) = meta.nodata() {
            dst_meta.set_nodata(Some(nodata));
        }

        if cut_out_smaller_than_extent && dst_meta.nodata().is_none() {
            dst_meta.set_nodata(Some(NumCast::from(data_type.default_nodata_value()).unwrap_or(-9999.0)));
        }

        let expected_buffer_size = dst_meta.rows() * dst_meta.columns() * data_type.bytes() as usize;
        if dst_data.len() != expected_buffer_size {
            return Err(Error::InvalidArgument(format!(
                "Invalid data buffer provided: incorrect size (got {} bytes but should be {expected_buffer_size} bytes)",
                dst_data.len(),
            )));
        }

        if cut_out_smaller_than_extent && let Some(nodata) = dst_meta.nodata() {
            let nodata = NumCast::from(nodata).unwrap_or_default();
            for dst_data in dst_data.iter_mut() {
                let _ = *dst_data.write(nodata);
            }
        }

        if cut_out.cols * cut_out.rows > 0 {
            read_region_from_dataset(band_index, &cut_out, &self.ds, dst_data, dst_meta.columns().count(), data_type)?;
        }

        Ok(dst_meta)
    }

    fn read_raster_band_region_u8(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [MaybeUninit<u8>],
    ) -> Result<GeoReference> {
        self.read_raster_band_region(band_index, region, ArrayDataType::Uint8, dst_data)
    }

    fn read_raster_band_region_u16(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [MaybeUninit<u16>],
    ) -> Result<GeoReference> {
        self.read_raster_band_region(
            band_index,
            region,
            ArrayDataType::Uint16,
            reinterpret_uninit_slice_to_byte(dst_data),
        )
    }

    fn read_raster_band_region_u32(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [MaybeUninit<u32>],
    ) -> Result<GeoReference> {
        self.read_raster_band_region(
            band_index,
            region,
            ArrayDataType::Uint32,
            reinterpret_uninit_slice_to_byte(dst_data),
        )
    }

    fn read_raster_band_region_u64(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [MaybeUninit<u64>],
    ) -> Result<GeoReference> {
        self.read_raster_band_region(
            band_index,
            region,
            ArrayDataType::Uint64,
            reinterpret_uninit_slice_to_byte(dst_data),
        )
    }

    fn read_raster_band_region_i8(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [MaybeUninit<i8>],
    ) -> Result<GeoReference> {
        self.read_raster_band_region(band_index, region, ArrayDataType::Int8, reinterpret_uninit_slice_to_byte(dst_data))
    }

    fn read_raster_band_region_i16(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [MaybeUninit<i16>],
    ) -> Result<GeoReference> {
        self.read_raster_band_region(band_index, region, ArrayDataType::Int16, reinterpret_uninit_slice_to_byte(dst_data))
    }

    fn read_raster_band_region_i32(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [MaybeUninit<i32>],
    ) -> Result<GeoReference> {
        self.read_raster_band_region(band_index, region, ArrayDataType::Int32, reinterpret_uninit_slice_to_byte(dst_data))
    }

    fn read_raster_band_region_i64(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [MaybeUninit<i64>],
    ) -> Result<GeoReference> {
        self.read_raster_band_region(band_index, region, ArrayDataType::Int64, reinterpret_uninit_slice_to_byte(dst_data))
    }

    fn read_raster_band_region_f32(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [MaybeUninit<f32>],
    ) -> Result<GeoReference> {
        self.read_raster_band_region(
            band_index,
            region,
            ArrayDataType::Float32,
            reinterpret_uninit_slice_to_byte(dst_data),
        )
    }

    fn read_raster_band_region_f64(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [MaybeUninit<f64>],
    ) -> Result<GeoReference> {
        self.read_raster_band_region(
            band_index,
            region,
            ArrayDataType::Float64,
            reinterpret_uninit_slice_to_byte(dst_data),
        )
    }
}

impl RasterReader for GdalRasterIO {
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

    fn read_raster_band<T: ArrayNum>(
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
}

/// Open a GDAL raster dataset for reading
pub fn open_dataset_read_only(path: impl AsRef<Path>) -> Result<gdal::Dataset> {
    let options = gdal::DatasetOptions {
        open_flags: gdal::GdalOpenFlags::GDAL_OF_READONLY | gdal::GdalOpenFlags::GDAL_OF_RASTER,
        ..Default::default()
    };

    open_with_options(path, options)
}

/// Open a GDAL raster dataset for reading with driver open options
pub fn open_dataset_read_only_with_options(path: impl AsRef<Path>, open_options: &RasterOpenOptions) -> Result<gdal::Dataset> {
    let raster_format = RasterFormat::guess_from_path(path.as_ref());
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

    let (width, height) = ds.raster_size();
    Ok(GeoReference::new(
        ds.projection(),
        RasterSize {
            rows: Rows(height as i32),
            cols: Columns(width as i32),
        },
        ds.geo_transform()?.into(),
        rasterband.no_data_value(),
    ))
}

fn create_gdal_open_options(raster_format: RasterFormat, open_options: &RasterOpenOptions) -> Vec<String> {
    let mut options = Vec::new();

    if let Some(layer_name) = &open_options.layer_name
        && raster_format == RasterFormat::GeoPackage
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

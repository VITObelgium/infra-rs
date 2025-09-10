//! Contains low-level functions to read and write raster data using the GDAL library.
//! These functions should only be used for specific use-cases.
//! For general use, the [`crate::Array`] and [`crate::raster::RasterReadWrite`] traits should be used.

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

#[cfg(any(feature = "gdal", feature = "raster-io-geotiff"))]
use crate::raster::formats::{self, RasterFormat as _};
use crate::{
    raster::{
        formats::{FormatProvider, RasterFileFormat, RasterFormatDyn, RasterOpenOptions},
        utils, WriteRasterOptions,
    },
    ArrayDataType, ArrayMetadata, ArrayNum, Error, GeoReference, RasterSize, Result,
};
use bytemuck::cast_slice;
use inf::allocate::{AlignedVec, AlignedVecUnderConstruction};
use num::NumCast;
use simd_macro::simd_bounds;
use std::{mem::MaybeUninit, path::Path};

//////////////////////////////////////////////////////////////////////////////////////////////
// Some convenience functions to quickly read raster data without dealing with RasterIO struct
//////////////////////////////////////////////////////////////////////////////////////////////

pub fn read_raster_georeference(path: impl AsRef<Path>, band_nr: usize) -> Result<GeoReference> {
    RasterIO::open_read_only(path)?.georeference(band_nr)
}

pub fn read_raster_band<T: ArrayNum>(path: impl AsRef<Path>, band_nr: usize) -> Result<(GeoReference, AlignedVec<T>)> {
    RasterIO::open_read_only(path)?.read_raster_band(band_nr)
}

pub fn write_raster_band_as<TStore: ArrayNum, T: ArrayNum>(
    path: impl AsRef<Path>,
    georef: &GeoReference,
    data: &[T],
    options: WriteRasterOptions,
) -> Result<()> {
    if T::TYPE == TStore::TYPE {
        write_raster_band(path, georef, data, options)
    } else {
        let converted: Vec<TStore> = data.iter().map(|&v| NumCast::from(v).unwrap_or(TStore::NODATA)).collect();
        write_raster_band(path, georef, &converted, options)
    }
}

pub fn write_raster_band<T: ArrayNum>(
    path: impl AsRef<Path>,
    georef: &GeoReference,
    _data: &[T],
    options: WriteRasterOptions,
) -> Result<()> {
    match T::TYPE {
        ArrayDataType::Uint8 | ArrayDataType::Uint16 | ArrayDataType::Uint32 | ArrayDataType::Uint64 => {
            if georef.nodata().is_some_and(|v| v < 0.0) {
                return Err(Error::InvalidArgument(
                    "Trying to store a raster with unsigned data type using a negative nodata value".to_string(),
                ));
            }
        }
        _ => {}
    }

    let format = match &options {
        WriteRasterOptions::GeoTiff(_) => RasterFileFormat::GeoTiff,
        WriteRasterOptions::Default => RasterFileFormat::guess_from_path(path.as_ref()),
    };

    if format == RasterFileFormat::GeoTiff {
        cfg_if::cfg_if! {
            if #[cfg(feature = "gdal")] {
                return formats::gdal::GdalRasterIO::write_band::<T>(path, georef, _data, options);
            } else if #[cfg(feature = "raster-io-geotiff")] {
                return formats::geotiff::GeotiffRasterIO::write_band::<T>(path, georef, _data, options);
            } else {
                return Err(Error::Runtime(
                    "GeoTiff format support not compiled in".into()
                ));
            }
        }
    };

    cfg_if::cfg_if! {
        if #[cfg(feature = "gdal")] {
            formats::gdal::GdalRasterIO::write_band::<T>(path, georef, _data, options)
        } else {
            Err(Error::Runtime(format!(
                "Unsupported raster file type for writing: {}",
                path.as_ref().display()
            )))
        }
    }
}

#[simd_bounds]
pub fn read_raster_band_region<T: ArrayNum>(
    path: impl AsRef<Path>,
    band_nr: usize,
    bounds: &GeoReference,
) -> Result<(GeoReference, AlignedVec<T>)> {
    RasterIO::open_read_only(path)?.read_raster_band_region(band_nr, bounds)
}

/// Detect the data type of the raster band at the provided path
pub fn detect_data_type(path: impl AsRef<Path>, band_index: usize) -> Result<ArrayDataType> {
    RasterIO::open_read_only(path)?.data_type(band_index)
}

//////////////////////////////////////////////////////////////////////////////////////////////
// RasterIO struct for more fine grained raster format access
// Most of the reading function are implemented in terms of reading into byte buffers
// This way the necessary conversion logic only has to be implemented once
// The rest of the functions are just convenience wrappers around the byte buffer functions
// Which are discouraged to be used directly
//////////////////////////////////////////////////////////////////////////////////////////////

/// Main struct to read raster data from various formats
pub struct RasterIO {
    io: Box<dyn RasterFormatDyn>,
}

impl RasterIO {
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            io: create_raster_impl_with_options(path, &RasterOpenOptions::default())?,
        })
    }

    pub fn open_read_only_force_format(path: impl AsRef<Path>, format: FormatProvider) -> Result<Self> {
        Ok(Self {
            io: create_raster_impl_with_options_for_format(path, &RasterOpenOptions::default(), format)?,
        })
    }

    pub fn open_read_only_with_options(path: impl AsRef<Path>, open_options: &RasterOpenOptions) -> Result<Self> {
        Ok(Self {
            io: create_raster_impl_with_options(path, open_options)?,
        })
    }

    pub fn open_read_only_with_options_force_format(
        path: impl AsRef<Path>,
        open_options: &RasterOpenOptions,
        format: FormatProvider,
    ) -> Result<Self> {
        Ok(Self {
            io: create_raster_impl_with_options_for_format(path, open_options, format)?,
        })
    }

    pub fn band_count(&self) -> Result<usize> {
        self.io.band_count()
    }

    pub fn raster_size(&self) -> Result<RasterSize> {
        self.io.raster_size()
    }

    pub fn georeference(&mut self, band_index: usize) -> Result<GeoReference> {
        self.io.georeference(band_index)
    }

    pub fn data_type(&self, band_index: usize) -> Result<ArrayDataType> {
        self.io.data_type(band_index)
    }

    pub fn overview_count(&self, band_index: usize) -> Result<usize> {
        self.io.overview_count(band_index)
    }

    /// Read the raster band from the file into a newly allocated buffer.
    /// Returns the georeference information and the data buffer.
    pub fn read_raster_band<T: ArrayNum>(&mut self, band_index: usize) -> Result<(GeoReference, AlignedVec<T>)> {
        let raster_size = self.io.raster_size()?;
        let mut dst_data = AlignedVecUnderConstruction::<T>::new(raster_size.cell_count());

        let georef = self.read_raster_band_into_byte_buffer(band_index, T::TYPE, dst_data.as_uninit_byte_slice_mut())?;
        Ok((georef, unsafe { dst_data.assume_init() }))
    }

    /// Read the raster band region from the file into a newly allocated buffer.
    /// The region is not required to be fully contained within the raster extent.
    /// If the region extends beyond the raster extent, the areas outside the raster will be filled with nodata values.
    /// Returns the georeference information and the data buffer.
    pub fn read_raster_band_region<T: ArrayNum>(
        &mut self,
        band_index: usize,
        region: &GeoReference,
    ) -> Result<(GeoReference, AlignedVec<T>)> {
        let mut dst_data = AlignedVecUnderConstruction::<T>::new(region.raster_size().cell_count());
        let georef = self.read_raster_band_region_into_byte_buffer(band_index, region, T::TYPE, dst_data.as_uninit_byte_slice_mut())?;
        Ok((georef, unsafe { dst_data.assume_init() }))
    }

    /// Read the raster band into an already allocated buffer.
    /// The buffer must have the exact size to hold all the data.
    /// To know the required size, first call `raster_size()` and allocate a buffer of that size.
    pub fn read_raster_band_into_buffer<T: ArrayNum>(&mut self, band_index: usize, buffer: &mut [MaybeUninit<T>]) -> Result<GeoReference> {
        self.read_raster_band_into_byte_buffer(band_index, T::TYPE, utils::cast_uninit_slice_to_byte(buffer))
    }

    /// Read the raster band region into an already allocated buffer.
    /// The region is not required to be fully contained within the raster extent.
    /// If the region extends beyond the raster extent, the areas outside the raster will be filled with nodata values.
    /// The buffer must have the exact size to hold all the data.
    /// The data size is determined by the provided bounds.
    pub fn read_raster_band_region_into_buffer<T: ArrayNum>(
        &mut self,
        band_index: usize,
        bounds: &GeoReference,
        buffer: &mut [MaybeUninit<T>],
    ) -> Result<GeoReference> {
        self.read_raster_band_region_into_byte_buffer(band_index, bounds, T::TYPE, utils::cast_uninit_slice_to_byte(buffer))
    }

    /// Read the raster band into an already allocated buffer.
    /// The buffer must have the exact size to hold all the data as the specified data type.
    /// To know the required size, first call `raster_size()` and allocate a buffer of that size.
    /// If the data type does not match the native data type of the raster band, a conversion will be performed.
    /// This method should not generally be used, except for special use-cases where a generic context is not available
    pub fn read_raster_band_into_byte_buffer(
        &mut self,
        band_index: usize,
        data_type: ArrayDataType,
        buffer: &mut [MaybeUninit<u8>],
    ) -> Result<GeoReference> {
        let src_data_type = self.data_type(band_index)?;

        if src_data_type == data_type {
            // We can read directly into the destination buffer
            self.io.read_band_into_byte_buffer(band_index, data_type, buffer)
        } else {
            // First read into a temporary buffer of the native data type
            let buffer_size = buffer.len() / data_type.bytes() as usize * src_data_type.bytes() as usize;
            let mut tmp_buf = AlignedVecUnderConstruction::<u8>::new(buffer_size);
            let georef = self
                .io
                .read_band_into_byte_buffer(band_index, src_data_type, tmp_buf.as_uninit_slice_mut())?;

            cast_to_buffer(
                data_type,
                src_data_type,
                tmp_buf,
                utils::cast_away_uninit_mut(buffer),
                georef.nodata(),
            );

            Ok(georef)
        }
    }

    /// Read the raster band region into an already allocated buffer.
    /// The region is not required to be fully contained within the raster extent.
    /// If the region extends beyond the raster extent, the areas outside the raster will be filled with nodata values.
    /// The buffer must have the exact size to hold all the data.
    /// The data size is determined by the provided bounds.
    /// This method should not generally be used, except for special use-cases where a generic context is not available
    pub fn read_raster_band_region_into_byte_buffer(
        &mut self,
        band_index: usize,
        bounds: &GeoReference,
        data_type: ArrayDataType,
        buffer: &mut [MaybeUninit<u8>],
    ) -> Result<GeoReference> {
        let src_data_type = self.data_type(band_index)?;
        if src_data_type == data_type {
            self.io.read_band_region_into_byte_buffer(band_index, bounds, src_data_type, buffer)
        } else {
            // First read into a temporary buffer of the native data type
            let mut tmp_buf = AlignedVecUnderConstruction::<u8>::new(bounds.size().cell_count() * src_data_type.bytes() as usize);
            let georef = self
                .io
                .read_band_region_into_byte_buffer(band_index, bounds, src_data_type, tmp_buf.as_uninit_slice_mut())?;

            cast_to_buffer(
                data_type,
                src_data_type,
                tmp_buf,
                utils::cast_away_uninit_mut(buffer),
                georef.nodata(),
            );

            Ok(georef)
        }
    }
}

fn cast_to_buffer(
    data_type: ArrayDataType,
    src_data_type: ArrayDataType,
    mut tmp_buf: AlignedVecUnderConstruction<u8>,
    dst: &mut [u8],
    nodata: Option<f64>,
) {
    match src_data_type {
        ArrayDataType::Uint8 => cast_into_byte_array(unsafe { tmp_buf.as_slice() }, data_type, dst, nodata),
        ArrayDataType::Uint16 => cast_into_byte_array::<u16>(cast_slice(unsafe { tmp_buf.as_slice() }), data_type, dst, nodata),
        ArrayDataType::Uint32 => cast_into_byte_array::<u32>(cast_slice(unsafe { tmp_buf.as_slice() }), data_type, dst, nodata),
        ArrayDataType::Uint64 => cast_into_byte_array::<u64>(cast_slice(unsafe { tmp_buf.as_slice() }), data_type, dst, nodata),
        ArrayDataType::Int8 => cast_into_byte_array::<i8>(cast_slice(unsafe { tmp_buf.as_slice() }), data_type, dst, nodata),
        ArrayDataType::Int16 => cast_into_byte_array::<i16>(cast_slice(unsafe { tmp_buf.as_slice() }), data_type, dst, nodata),
        ArrayDataType::Int32 => cast_into_byte_array::<i32>(cast_slice(unsafe { tmp_buf.as_slice() }), data_type, dst, nodata),
        ArrayDataType::Int64 => cast_into_byte_array::<i64>(cast_slice(unsafe { tmp_buf.as_slice() }), data_type, dst, nodata),
        ArrayDataType::Float32 => cast_into_byte_array::<f32>(cast_slice(unsafe { tmp_buf.as_slice() }), data_type, dst, nodata),
        ArrayDataType::Float64 => cast_into_byte_array::<f64>(cast_slice(unsafe { tmp_buf.as_slice() }), data_type, dst, nodata),
    }
}

fn create_raster_impl_with_options_for_format(
    _path: impl AsRef<Path>,
    _options: &RasterOpenOptions,
    format: FormatProvider,
) -> Result<Box<dyn RasterFormatDyn>> {
    match format {
        FormatProvider::GeoTiff => {
            #[cfg(feature = "raster-io-geotiff")]
            {
                use crate::raster::formats::RasterFormat as _;
                Ok(Box::new(formats::geotiff::GeotiffRasterIO::open_read_only_with_options(
                    _path.as_ref(),
                    _options,
                )?))
            }
            #[cfg(not(feature = "raster-io-geotiff"))]
            {
                #[cfg(feature = "gdal")]
                {
                    return Ok(Box::new(formats::gdal::GdalRasterIO::open_read_only_with_options(
                        _path.as_ref(),
                        _options,
                    )?));
                }
                #[cfg(not(feature = "gdal"))]
                {
                    return Err(Error::Runtime("GeoTiff format support not compiled in".to_string()));
                }
            }
        }
        FormatProvider::Gdal => {
            #[cfg(feature = "gdal")]
            {
                use crate::raster::formats::RasterFormat as _;

                Ok(Box::new(formats::gdal::GdalRasterIO::open_read_only_with_options(
                    _path.as_ref(),
                    _options,
                )?))
            }
            #[cfg(not(feature = "gdal"))]
            {
                Err(Error::Runtime("GDAL format support not compiled in".to_string()))
            }
        }
    }
}

fn create_raster_impl_with_options(path: impl AsRef<Path>, _options: &RasterOpenOptions) -> Result<Box<dyn RasterFormatDyn>> {
    match RasterFileFormat::guess_from_path(path.as_ref()) {
        #[cfg(all(feature = "raster-io-geotiff", not(feature = "gdal")))]
        RasterFileFormat::GeoTiff => Ok(Box::new(formats::geotiff::GeotiffRasterIO::open_read_only_with_options(
            path.as_ref(),
            _options,
        )?)),
        #[cfg(feature = "gdal")]
        RasterFileFormat::ArcAscii
        | RasterFileFormat::Gif
        | RasterFileFormat::GeoTiff
        | RasterFileFormat::Png
        | RasterFileFormat::PcRaster
        | RasterFileFormat::Netcdf
        | RasterFileFormat::MBTiles
        | RasterFileFormat::GeoPackage
        | RasterFileFormat::Grib
        | RasterFileFormat::Postgis
        | RasterFileFormat::Vrt => Ok(Box::new(formats::gdal::GdalRasterIO::open_read_only_with_options(
            path.as_ref(),
            _options,
        )?)),
        _ => Err(Error::Runtime(format!("Unsupported raster file type: {}", path.as_ref().display()))),
    }
}

fn cast_into_array<TSrc: ArrayNum, TDest: ArrayNum>(src: &[TSrc], dst: &mut [TDest], nodata: TDest) {
    for (&value, dst_value) in src.iter().zip(dst) {
        *dst_value = NumCast::from(value).unwrap_or(nodata);
    }
}

fn nodata_for_data_type<T: ArrayNum>(nodata: Option<f64>) -> T {
    nodata.and_then(NumCast::from).unwrap_or(T::NODATA)
}

fn cast_into_byte_array<T: ArrayNum>(src: &[T], dst_data_type: ArrayDataType, dst: &mut [u8], nodata: Option<f64>) {
    match dst_data_type {
        ArrayDataType::Uint8 => cast_into_array(src, bytemuck::cast_slice_mut::<_, u8>(dst), nodata_for_data_type(nodata)),
        ArrayDataType::Uint16 => cast_into_array(src, bytemuck::cast_slice_mut::<_, u16>(dst), nodata_for_data_type(nodata)),
        ArrayDataType::Uint32 => cast_into_array(src, bytemuck::cast_slice_mut::<_, u32>(dst), nodata_for_data_type(nodata)),
        ArrayDataType::Uint64 => cast_into_array(src, bytemuck::cast_slice_mut::<_, u64>(dst), nodata_for_data_type(nodata)),
        ArrayDataType::Int8 => cast_into_array(src, bytemuck::cast_slice_mut::<_, i8>(dst), nodata_for_data_type(nodata)),
        ArrayDataType::Int16 => cast_into_array(src, bytemuck::cast_slice_mut::<_, i16>(dst), nodata_for_data_type(nodata)),
        ArrayDataType::Int32 => cast_into_array(src, bytemuck::cast_slice_mut::<_, i32>(dst), nodata_for_data_type(nodata)),
        ArrayDataType::Int64 => cast_into_array(src, bytemuck::cast_slice_mut::<_, i64>(dst), nodata_for_data_type(nodata)),
        ArrayDataType::Float32 => cast_into_array(src, bytemuck::cast_slice_mut::<_, f32>(dst), nodata_for_data_type(nodata)),
        ArrayDataType::Float64 => cast_into_array(src, bytemuck::cast_slice_mut::<_, f64>(dst), nodata_for_data_type(nodata)),
    }
}

#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use crate::crs;
    use path_macro::path;

    #[cfg(all(feature = "raster-io-geotiff", feature = "gdal"))]
    use std::path::Path;

    use crate::ArrayDataType;
    #[cfg(all(feature = "raster-io-geotiff", feature = "gdal"))]
    use crate::GeoReference;

    use crate::raster::formats::{RasterFormat as _, RasterFormatDyn as _};
    use crate::raster::RasterReadWrite as _;
    use crate::{raster::formats, Point, RasterSize};
    use inf::allocate::AlignedVecUnderConstruction;

    use super::*;

    #[test]
    fn projection_info_projected_31370() {
        let path = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data" / "epsg31370.tif");
        let meta = read_raster_georeference(path, 1).unwrap();
        assert!(!meta.projection().is_empty());
        assert!(meta.projected_epsg().is_some());
        assert_eq!(meta.projected_epsg(), Some(crs::epsg::BELGIAN_LAMBERT72));
        assert_eq!(meta.geographic_epsg(), Some(crs::epsg::BELGE72_GEO));
        assert_eq!(meta.projection_frienly_name(), "EPSG:31370");
    }

    #[test]
    fn projection_info_projected_3857() {
        let path = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data" / "epsg3857.tif");
        let meta = read_raster_georeference(path, 1).unwrap();
        assert!(!meta.projection().is_empty());
        assert!(meta.projected_epsg().is_some());
        assert_eq!(meta.projected_epsg().unwrap(), crs::epsg::WGS84_WEB_MERCATOR);
        assert_eq!(meta.geographic_epsg().unwrap(), crs::epsg::WGS84);
        assert_eq!(meta.projection_frienly_name(), "EPSG:3857");
    }

    #[allow(unused)]
    fn test_input_files() -> Vec<(&'static str, ArrayDataType)> {
        vec![
            ("landusebyte.tif", ArrayDataType::Uint8),
            ("landusebyte.tif", ArrayDataType::Float32),
            ("landusebyte_tiled.tif", ArrayDataType::Uint8),
            ("landusebyte_tiled.tif", ArrayDataType::Int16),
        ]
    }

    #[cfg(all(feature = "raster-io-geotiff", feature = "gdal"))]
    fn compare_geotiff_vs_gdal_read(
        input: &Path,
        band_index: usize,
        geo_reference: Option<&GeoReference>,
        data_type: ArrayDataType,
    ) -> Result<()> {
        let mut gdal_reader = RasterIO::open_read_only_force_format(input, FormatProvider::Gdal)?;
        let mut gtif_reader = RasterIO::open_read_only_force_format(input, FormatProvider::GeoTiff)?;

        assert_eq!(gdal_reader.band_count()?, gtif_reader.band_count()?);
        assert_eq!(gdal_reader.raster_size()?, gtif_reader.raster_size()?);
        assert_eq!(gdal_reader.data_type(band_index)?, gtif_reader.data_type(band_index)?);
        assert_eq!(gdal_reader.overview_count(band_index)?, gtif_reader.overview_count(band_index)?);
        assert_eq!(
            gdal_reader.georeference(band_index)?.geo_transform(),
            gtif_reader.georeference(band_index)?.geo_transform()
        );

        let geo_ref = match geo_reference {
            Some(geo) => geo.clone(),
            None => gdal_reader.georeference(band_index)?,
        };

        let mut gdal_region_data = AlignedVecUnderConstruction::<u8>::new(geo_ref.raster_size().cell_count() * data_type.bytes() as usize);
        let mut gtif_region_data = AlignedVecUnderConstruction::<u8>::new(geo_ref.raster_size().cell_count() * data_type.bytes() as usize);

        let (gdal_geo, gtif_geo) = if let Some(geo_reference) = geo_reference {
            (
                gdal_reader.read_raster_band_region_into_byte_buffer(
                    band_index,
                    geo_reference,
                    data_type,
                    gdal_region_data.as_uninit_slice_mut(),
                )?,
                gtif_reader.read_raster_band_region_into_byte_buffer(
                    band_index,
                    geo_reference,
                    data_type,
                    gtif_region_data.as_uninit_slice_mut(),
                )?,
            )
        } else {
            // Read the full raster band
            (
                gdal_reader.read_raster_band_into_byte_buffer(band_index, data_type, gdal_region_data.as_uninit_slice_mut())?,
                gtif_reader.read_raster_band_into_byte_buffer(band_index, data_type, gtif_region_data.as_uninit_slice_mut())?,
            )
        };

        // Validate GDAL region reading results
        assert_eq!(gdal_geo.raster_size(), gtif_geo.raster_size());
        assert_eq!(gdal_geo.projected_epsg(), gtif_geo.projected_epsg());
        assert_eq!(gdal_geo.geo_transform(), gtif_geo.geo_transform());
        assert_eq!(gdal_geo.nodata(), gtif_geo.nodata());
        assert_eq!(unsafe { gdal_region_data.assume_init() }, unsafe { gtif_region_data.assume_init() });

        Ok(())
    }

    #[test]
    #[cfg(all(feature = "raster-io-geotiff", feature = "gdal"))]
    fn compare_geotiff_gdal_read() -> Result<()> {
        for (input_file, data_type) in test_input_files() {
            let input = crate::testutils::workspace_test_data_dir().join(input_file);
            let band_index = 1;
            compare_geotiff_vs_gdal_read(&input, band_index, None, data_type)?;
        }

        Ok(())
    }

    #[test]
    #[cfg(all(feature = "raster-io-geotiff", feature = "gdal"))]
    fn compare_tiled_geotiff_gdal_read_region_within_extent() -> Result<()> {
        use crate::Point;

        for (input_file, data_type) in test_input_files() {
            use crate::raster::formats;

            let band_index = 1;
            let input = crate::testutils::workspace_test_data_dir().join(input_file);

            let full_georef = formats::gdal::GdalRasterIO::open_read_only(&input)?.georeference(band_index)?;
            let mut shifted_geo_trans = full_georef.geo_transform();
            shifted_geo_trans.set_top_left(
                shifted_geo_trans.top_left() + Point::new(shifted_geo_trans.cell_size_x() * 5.0, shifted_geo_trans.cell_size_y() * 10.0),
            ); // Shift the region a bit

            // Create a smaller region (upper-left quarter of the image)
            let region_rows = full_georef.rows().count() / 2;
            let region_cols = full_georef.columns().count() / 2;
            let region_georef = GeoReference::new(
                full_georef.projection(),
                RasterSize::with_rows_cols(region_rows.into(), region_cols.into()),
                shifted_geo_trans,
                full_georef.nodata(),
            );

            compare_geotiff_vs_gdal_read(&input, band_index, Some(&region_georef), data_type)?;
        }

        Ok(())
    }

    #[test]
    #[cfg(all(feature = "raster-io-geotiff", feature = "gdal"))]
    fn compare_tiled_geotiff_gdal_read_region_outside_extent_top_left() -> Result<()> {
        for (input_file, data_type) in test_input_files() {
            let band_index = 1;
            let input = crate::testutils::workspace_test_data_dir().join(input_file);

            let full_georef = formats::gdal::GdalRasterIO::open_read_only(&input)?.georeference(band_index)?;
            let mut shifted_geo_trans = full_georef.geo_transform();
            shifted_geo_trans.set_top_left(
                shifted_geo_trans.top_left() - Point::new(shifted_geo_trans.cell_size_x() * 5.0, shifted_geo_trans.cell_size_y() * 10.0),
            ); // Shift the region a bit

            // Create a smaller region (upper-left quarter of the image)
            let region_rows = full_georef.rows().count() / 2;
            let region_cols = full_georef.columns().count() / 2;
            let region_georef = GeoReference::new(
                full_georef.projection(),
                RasterSize::with_rows_cols(region_rows.into(), region_cols.into()),
                shifted_geo_trans,
                full_georef.nodata(),
            );

            compare_geotiff_vs_gdal_read(&input, band_index, Some(&region_georef), data_type)?;
        }

        Ok(())
    }

    #[test]
    #[cfg(all(feature = "raster-io-geotiff", feature = "gdal"))]
    fn compare_tiled_geotiff_gdal_read_region_outside_extent_bottom_right() -> Result<()> {
        for (input_file, data_type) in test_input_files() {
            let band_index = 1;
            let input = crate::testutils::workspace_test_data_dir().join(input_file);

            let full_georef = formats::gdal::GdalRasterIO::open_read_only(&input)?.georeference(band_index)?;
            let mut shifted_geo_trans = full_georef.geo_transform();
            shifted_geo_trans.set_top_left(
                shifted_geo_trans.top_left() + Point::new(shifted_geo_trans.cell_size_x() * 5.0, shifted_geo_trans.cell_size_y() * 10.0),
            ); // Shift the region a bit

            // Create a smaller region (upper-left quarter of the image)
            let region_rows = full_georef.rows().count();
            let region_cols = full_georef.columns().count();
            let region_georef = GeoReference::new(
                full_georef.projection(),
                RasterSize::with_rows_cols(region_rows.into(), region_cols.into()),
                shifted_geo_trans,
                full_georef.nodata(),
            );

            compare_geotiff_vs_gdal_read(&input, band_index, Some(&region_georef), data_type)?;
        }
        Ok(())
    }

    #[test]
    #[cfg(all(feature = "raster-io-geotiff", feature = "gdal"))]
    fn compare_tiled_geotiff_gdal_read_region_larger_then_raster() -> Result<()> {
        for (input_file, data_type) in test_input_files() {
            let band_index = 1;
            let input = crate::testutils::workspace_test_data_dir().join(input_file);

            let full_georef = formats::gdal::GdalRasterIO::open_read_only(&input)?.georeference(band_index)?;
            let mut shifted_geo_trans = full_georef.geo_transform();
            shifted_geo_trans.set_top_left(
                shifted_geo_trans.top_left() - Point::new(shifted_geo_trans.cell_size_x() * 5.0, shifted_geo_trans.cell_size_y() * 10.0),
            ); // Shift the region a bit

            // Create a smaller region (upper-left quarter of the image)
            let region_rows = full_georef.rows().count() + 20;
            let region_cols = full_georef.columns().count() + 10;
            let region_georef = GeoReference::new(
                full_georef.projection(),
                RasterSize::with_rows_cols(region_rows.into(), region_cols.into()),
                shifted_geo_trans,
                full_georef.nodata(),
            );

            compare_geotiff_vs_gdal_read(&input, band_index, Some(&region_georef), data_type)?;
        }
        Ok(())
    }

    #[test_log::test]
    #[cfg(all(feature = "raster-io-geotiff", feature = "gdal"))]
    fn read_raster_no_srs() -> Result<()> {
        use crate::raster::{formats::FormatProvider, io};

        let input = crate::testutils::geo_test_data_dir().join("reference/clusteridwithobstacles.tif");

        let mut geotiff = io::RasterIO::open_read_only_force_format(&input, FormatProvider::GeoTiff)?;
        let gtif_raster = {
            use crate::{raster::DenseRaster, ArrayInterop};
            let (geo_ref, vec) = geotiff.read_raster_band::<f32>(1)?;
            DenseRaster::new_init_nodata(geo_ref, vec)?
        };

        let mut gdal = io::RasterIO::open_read_only_force_format(&input, FormatProvider::Gdal)?;
        let gdal_raster = {
            use crate::{raster::DenseRaster, ArrayInterop};
            let (geo_ref, vec) = gdal.read_raster_band::<f32>(1)?;
            DenseRaster::new_init_nodata(geo_ref, vec)?
        };

        assert_eq!(gtif_raster, gdal_raster);

        Ok(())
    }

    #[test_log::test]
    #[cfg(all(feature = "raster-io-geotiff", feature = "gdal"))]
    fn read_raster_with_different_data_type() -> Result<()> {
        use crate::raster::{formats::FormatProvider, io};

        let input = crate::testutils::geo_test_data_dir().join("reference/clusteridwithobstacles.tif");

        let mut geotiff = io::RasterIO::open_read_only_force_format(&input, FormatProvider::GeoTiff)?;
        let gtif_raster = {
            use crate::{raster::DenseRaster, ArrayInterop};
            let (geo_ref, vec) = geotiff.read_raster_band::<f32>(1)?;
            DenseRaster::new_init_nodata(geo_ref, vec)?
        };

        let mut gdal = io::RasterIO::open_read_only_force_format(&input, FormatProvider::Gdal)?;
        let gdal_raster = {
            use crate::{raster::DenseRaster, ArrayInterop};
            let (geo_ref, vec) = gdal.read_raster_band::<f32>(1)?;
            DenseRaster::new_init_nodata(geo_ref, vec)?
        };

        assert_eq!(gtif_raster, gdal_raster);

        Ok(())
    }
}

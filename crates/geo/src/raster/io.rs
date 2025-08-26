//! Contains low-level functions to read and write raster data using the GDAL library.
//! These functions should only be used for specific use-cases.
//! For general use, the [`crate::Array`] and [`crate::raster::RasterIO`] traits should be used.

use std::{
    ffi::CString,
    mem::MaybeUninit,
    path::{Path, PathBuf},
};

use crate::{
    ArrayDataType, ArrayNum, Error, Result,
    raster::reader::{self, RasterOpenOptions, RasterReader},
};
use crate::{GeoReference, RasterSize};
use inf::allocate::{AlignedVec, AlignedVecUnderConstruction};
use num::NumCast;

pub fn read_raster_georeference(path: impl AsRef<Path>, band_nr: usize) -> Result<GeoReference> {
    RasterIO::open_read_only(path)?.georeference(band_nr)
}

pub fn read_raster_band<T: ArrayNum>(path: impl AsRef<Path>, band_nr: usize) -> Result<(GeoReference, AlignedVec<T>)> {
    RasterIO::open_read_only(path)?.read_raster_band(band_nr)
}

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

/// Main struct to read raster data from various formats
pub struct RasterIO {
    reader: Box<dyn RasterReader>,
}

impl RasterIO {
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            reader: reader::create_raster_reader(path)?,
        })
    }

    pub fn open_read_only_with_options(path: impl AsRef<Path>, open_options: &RasterOpenOptions) -> Result<Self> {
        Ok(Self {
            reader: reader::create_raster_reader_with_options(path, open_options)?,
        })
    }

    pub fn band_count(&self) -> Result<usize> {
        self.reader.band_count()
    }

    pub fn raster_size(&self) -> Result<RasterSize> {
        self.reader.raster_size()
    }

    pub fn georeference(&mut self, band_index: usize) -> Result<GeoReference> {
        self.reader.georeference(band_index)
    }

    pub fn data_type(&self, band_index: usize) -> Result<ArrayDataType> {
        self.reader.data_type(band_index)
    }

    pub fn overview_count(&self, band_index: usize) -> Result<usize> {
        self.reader.overview_count(band_index)
    }

    pub fn read_raster_band<T: ArrayNum>(&mut self, band_index: usize) -> Result<(GeoReference, AlignedVec<T>)> {
        let raster_size = self.reader.raster_size()?;
        let mut dst_data = AlignedVecUnderConstruction::<T>::new(raster_size.cell_count());
        let georef = self
            .reader
            .read_raster_band(band_index, T::TYPE, dst_data.as_uninit_byte_slice_mut())?;
        Ok((georef, unsafe { dst_data.assume_init() }))
    }

    pub fn read_raster_band_region<T: ArrayNum>(
        &mut self,
        band_index: usize,
        bounds: &GeoReference,
    ) -> Result<(GeoReference, AlignedVec<T>)> {
        let mut dst_data = AlignedVecUnderConstruction::<T>::new(bounds.raster_size().cell_count());
        let georef = self
            .reader
            .read_raster_band_region(band_index, bounds, T::TYPE, dst_data.as_uninit_byte_slice_mut())?;
        Ok((georef, unsafe { dst_data.assume_init() }))
    }

    /// Read the raster band into an already allocated buffer.
    /// The buffer must have the exact size to hold all the data.
    /// To know the required size, first call `raster_size()` and allocate a buffer of that size.
    pub fn read_raster_band_into_buffer<T: ArrayNum>(&mut self, band_index: usize, buffer: &mut [MaybeUninit<T>]) -> Result<GeoReference> {
        self.reader.read_raster_band(band_index, T::TYPE, unsafe {
            std::slice::from_raw_parts_mut(
                buffer.as_mut_ptr().cast::<MaybeUninit<u8>>(),
                buffer.len() * std::mem::size_of::<T>(),
            )
        })
    }

    /// Read the raster band into an already allocated buffer.
    /// The buffer must have the exact size to hold all the data.
    /// The data size is determined by the provided bounds.
    pub fn read_raster_band_region_into_buffer<T: ArrayNum>(
        &mut self,
        band_index: usize,
        bounds: &GeoReference,
        buffer: &mut [MaybeUninit<T>],
    ) -> Result<GeoReference> {
        self.reader.read_raster_band_region(band_index, bounds, T::TYPE, unsafe {
            std::slice::from_raw_parts_mut(
                buffer.as_mut_ptr().cast::<MaybeUninit<u8>>(),
                buffer.len() * std::mem::size_of::<T>(),
            )
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RasterFormat {
    Memory,
    ArcAscii,
    GeoTiff,
    Gif,
    Png,
    PcRaster,
    Netcdf,
    MBTiles,
    GeoPackage,
    Grib,
    Postgis,
    Vrt,
    Unknown,
}

impl RasterFormat {
    /// Given a file path, guess the raster type based on the file extension
    pub fn guess_from_path(file_path: impl AsRef<Path>) -> RasterFormat {
        let file_path = file_path.as_ref();
        let ext = file_path.extension().map(|ext| ext.to_string_lossy().to_lowercase());

        if let Some(ext) = ext {
            match ext.as_ref() {
                "asc" => return RasterFormat::ArcAscii,
                "tiff" | "tif" => return RasterFormat::GeoTiff,
                "gif" => return RasterFormat::Gif,
                "png" => return RasterFormat::Png,
                "map" => return RasterFormat::PcRaster,
                "nc" => return RasterFormat::Netcdf,
                "mbtiles" | "db" => return RasterFormat::MBTiles,
                "gpkg" => return RasterFormat::GeoPackage,
                "grib" => return RasterFormat::Grib,
                _ => {}
            }
        }

        let path = file_path.to_string_lossy();
        if path.starts_with("postgresql://") || path.starts_with("pg:") {
            RasterFormat::Postgis
        } else {
            RasterFormat::Unknown
        }
    }
}

#[cfg(feature = "gdal")]
#[cfg_attr(docsrs, doc(cfg(feature = "gdal")))]
pub mod dataset {

    use crate::{Nodata, RasterSize, gdalinterop::*};
    use gdal::{Metadata, cpl::CslStringList, raster::GdalType};

    use super::*;

    /// Write raster to disk using a different data type then present in the data buffer
    /// Driver options (as documented in the GDAL drivers) can be provided
    /// If no driver options are provided, some sane defaults will be used for geotiff files
    pub fn write_as<TStore, T>(data: &[T], meta: &GeoReference, path: impl AsRef<Path>, driver_options: &[String]) -> Result<()>
    where
        T: GdalType + Nodata + num::NumCast + Copy,
        TStore: GdalType + Nodata + num::NumCast,
    {
        let path = path.as_ref();
        create_output_directory_if_needed(path)?;

        // To write a raster to disk we need a dataset that contains the data
        // Create a memory dataset with 0 bands, then assign a band given the pointer of our vector
        // Creating a dataset with 1 band would casuse unnecessary memory allocation

        if T::datatype() == TStore::datatype() {
            let mut ds = create_in_memory_with_data(meta, data)?;
            write_to_disk(&mut ds, path, driver_options, &[])?;
        } else {
            // TODO: Investigate VRT driver to create a virtual dataset with different type without creating a copy
            let converted: Vec<TStore> = data
                .iter()
                .map(|&v| -> TStore { NumCast::from(v).unwrap_or(TStore::NODATA) })
                .collect();
            let mut ds = create_in_memory_with_data(meta, &converted)?;
            write_to_disk(&mut ds, path, driver_options, &[])?;
        }

        Ok(())
    }

    /// Write the raster to disk.
    /// Driver options (as documented in the GDAL drivers) can be provided.
    /// If no driver options are provided, some sane defaults will be used for geotiff files (compression, tiling).
    pub fn write<T>(data: &[T], meta: &GeoReference, path: impl AsRef<Path>, driver_options: &[String]) -> Result
    where
        T: GdalType + Nodata + num::NumCast + Copy,
    {
        match <T>::datatype() {
            gdal::raster::GdalDataType::UInt8
            | gdal::raster::GdalDataType::UInt16
            | gdal::raster::GdalDataType::UInt32
            | gdal::raster::GdalDataType::UInt64 => {
                if meta.nodata().is_some_and(|v| v < 0.0) {
                    return Err(Error::InvalidArgument(
                        "Trying to store a raster with unsigned data type using a negative nodata value".to_string(),
                    ));
                }
            }
            _ => {}
        }

        write_as::<T, _>(data, meta, path, driver_options)
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

        if driver_options.is_empty() && driver.description().unwrap_or_default() == RasterFormat::GeoTiff.gdal_driver_name() {
            // Provide sane default for GeoTIFF files
            c_opts.add_string("COMPRESS=LZW")?;
            c_opts.add_string("NUM_THREADS=ALL_CPUS")?;
        }

        for (key, value) in metadata_values {
            ds.set_metadata_item(key, value, "")?;
        }

        let path_str = path.to_string_lossy();
        let path_str = CString::new(path_str.as_ref())?;

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

    /// Creates an in-memory dataset without any bands
    pub fn create_in_memory(size: RasterSize) -> Result<gdal::Dataset> {
        let mem_driver = gdal::DriverManager::get_driver_by_name("MEM")?;
        Ok(mem_driver.create(PathBuf::from("in_mem"), size.cols.count() as usize, size.rows.count() as usize, 0)?)
    }

    /// Creates an in-memory dataset with the provided metadata.
    /// The array passed data will be used as the dataset band.
    /// Make sure the data array is the correct size and will live as long as the dataset.
    pub fn create_in_memory_with_data<T: GdalType + Nodata>(meta: &GeoReference, data: &[T]) -> Result<gdal::Dataset> {
        let mut ds = create_in_memory(meta.raster_size())?;
        add_band_from_data_ptr(&mut ds, data)?;
        metadata_to_dataset_band(&mut ds, meta, 1)?;
        Ok(ds)
    }

    pub(crate) fn metadata_to_dataset_band(ds: &mut gdal::Dataset, meta: &GeoReference, band_index: usize) -> Result<()> {
        ds.set_geo_transform(&meta.geo_transform().into())?;
        ds.set_projection(meta.projection())?;
        ds.rasterband(band_index)?.set_no_data_value(meta.nodata())?;
        Ok(())
    }

    fn create_raster_driver_for_path(path: impl AsRef<Path>) -> Result<gdal::Driver> {
        let path = path.as_ref();
        let raster_format = RasterFormat::guess_from_path(path);
        if raster_format == RasterFormat::Unknown {
            return Err(Error::Runtime(format!(
                "Could not detect raster type from filename: {}",
                path.to_string_lossy()
            )));
        }

        Ok(gdal::DriverManager::get_driver_by_name(raster_format.gdal_driver_name())?)
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
}

#[cfg(test)]
mod tests {
    use crate::crs;

    use super::*;

    use path_macro::path;

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
}

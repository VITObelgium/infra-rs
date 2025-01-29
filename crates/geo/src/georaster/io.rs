//! Contains low-level functions to read and write raster data using the GDAL library.
//! These functions should only be used for specfiic use-cases.
//! For general use, the [`crate::Raster`] and [`crate::RasterIO`] traits should be used.

use std::{
    ffi::{c_void, CString},
    path::{Path, PathBuf},
};

use crate::{gdalinterop::*, GeoReference};
use crate::{Error, Result};
use approx::relative_eq;
use gdal::{
    cpl::CslStringList,
    errors::GdalError::{self},
    raster::{GdalDataType, GdalType},
};
use num::NumCast;

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

    /// Given a file path, guess the raster type based on the file extension
    pub fn guess_from_path(file_path: &Path) -> RasterFormat {
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

#[derive(Default)]
struct CutOut {
    pub src_col_offset: i32,
    pub src_row_offset: i32,
    pub dst_col_offset: i32,
    pub dst_row_offset: i32,
    pub rows: i32,
    pub cols: i32,
}

pub mod dataset {
    use gdal::Metadata;
    use raster::{Nodata, RasterNum, RasterSize};

    use super::*;

    fn str_vec<T: AsRef<str>>(options: &[T]) -> Vec<&str> {
        options.iter().map(|s| s.as_ref()).collect()
    }

    fn open_with_options(path: &Path, options: gdal::DatasetOptions) -> Result<gdal::Dataset> {
        gdal::Dataset::open_ex(path, options).map_err(|err| match err {
            // Match on the error to give a cleaner error message when the file does not exist
            GdalError::NullPointer { method_name: _, msg: _ } => {
                if !path.exists() {
                    Error::InvalidPath(PathBuf::from(path))
                } else {
                    let ras_type = RasterFormat::guess_from_path(path);
                    if ras_type != RasterFormat::Unknown
                        && gdal::DriverManager::get_driver_by_name(ras_type.gdal_driver_name()).is_err()
                    {
                        return Error::Runtime(format!("Gdal driver not supported: {}", ras_type.gdal_driver_name()));
                    }

                    Error::Runtime(format!(
                        "Failed to open raster dataset ({}), check file correctness or driver configuration ({})",
                        path.to_string_lossy(),
                        err
                    ))
                }
            }
            _ => Error::Runtime(format!(
                "Failed to open raster dataset: {} ({})",
                path.to_string_lossy(),
                err
            )),
        })
    }

    /// Open a GDAL raster dataset for reading
    pub fn open_read_only(path: &Path) -> Result<gdal::Dataset> {
        let options = gdal::DatasetOptions {
            open_flags: gdal::GdalOpenFlags::GDAL_OF_READONLY | gdal::GdalOpenFlags::GDAL_OF_RASTER,
            ..Default::default()
        };

        open_with_options(path, options)
    }

    /// Open a GDAL raster dataset for reading with driver open options
    pub fn open_read_only_with_options(path: &Path, open_options: &[&str]) -> Result<gdal::Dataset> {
        let options = gdal::DatasetOptions {
            open_flags: gdal::GdalOpenFlags::GDAL_OF_READONLY | gdal::GdalOpenFlags::GDAL_OF_RASTER,
            open_options: Some(open_options),
            ..Default::default()
        };

        open_with_options(path, options)
    }

    /// Detect the data type of the raster band at the provided path
    pub fn detect_data_type(path: &Path, band_index: usize) -> Result<gdal::raster::GdalDataType> {
        Ok(open_read_only(path)?.rasterband(band_index)?.band_type())
    }

    /// Reads the [`inf::GeoMetadata`] from the first band of a raster file
    pub fn read_file_metadata(path: &Path) -> Result<GeoReference> {
        read_band_metadata(&open_read_only(path)?, 1)
    }

    /// Opens the raster dataset to read the spatial metadata with driver open options
    pub fn read_file_metadata_with_options<T: AsRef<str>>(path: &Path, open_options: &[T]) -> Result<GeoReference> {
        read_band_metadata(&open_read_only_with_options(path, str_vec(open_options).as_slice())?, 1)
    }

    /// Reads the [`inf::GeoMetadata`] from the provided band of a raster file
    /// The band index is 1-based
    pub fn read_band_metadata(ds: &gdal::Dataset, band_index: usize) -> Result<GeoReference> {
        let rasterband = ds.rasterband(band_index)?;

        let (width, height) = ds.raster_size();
        Ok(GeoReference::new(
            ds.projection(),
            RasterSize {
                rows: height,
                cols: width,
            },
            ds.geo_transform()?,
            rasterband.no_data_value(),
        ))
    }

    /// The provided extent will be the extent of the resulting raster.
    /// Areas outside the extent of the raster on disk will be filled with nodata.
    pub fn read_band_region<T: GdalType + RasterNum<T>>(
        dataset: &gdal::Dataset,
        band_nr: usize,
        extent: &GeoReference,
        dst_data: &mut [T],
    ) -> Result<GeoReference> {
        let meta = read_band_metadata(dataset, band_nr)?;
        let cut_out = intersect_metadata(&meta, extent)?;

        // Error if the requeated data type can not hold the nodata value of the raster
        check_if_metadata_fits::<T>(meta.nodata(), dataset.rasterband(band_nr)?.band_type())?;

        let cut_out_smaller_than_extent = (extent.rows() * extent.columns()) != (cut_out.rows * cut_out.cols) as usize;
        let mut dst_meta = extent.clone();
        if let Some(nodata) = meta.nodata() {
            dst_meta.set_nodata(Some(nodata));
        }

        if cut_out_smaller_than_extent && dst_meta.nodata().is_none() {
            dst_meta.set_nodata(Some(NumCast::from(T::max_value()).unwrap_or(-9999.0)));
        }

        if dst_data.len() != dst_meta.rows() * dst_meta.columns() {
            return Err(Error::InvalidArgument(
                "Invalid data buffer provided: incorrect size".to_string(),
            ));
        }

        if cut_out_smaller_than_extent {
            if let Some(nodata) = dst_meta.nodata() {
                dst_data.fill(NumCast::from(nodata).unwrap_or(T::zero()));
            }
        }

        if cut_out.cols * cut_out.rows > 0 {
            read_region_from_dataset(band_nr, &cut_out, dataset, dst_data, dst_meta.columns() as i32)?;
        }

        Ok(dst_meta)
    }

    /// Read the full band from the dataset into the provided data buffer.
    /// The data buffer should be pre-allocated and have the correct size.
    /// The band index is 1-based.
    pub fn read_band<T: GdalType + num::NumCast>(
        dataset: &gdal::Dataset,
        band_index: usize,
        dst_data: &mut [T],
    ) -> Result<GeoReference> {
        let raster_band = dataset.rasterband(band_index)?;
        let meta = read_band_metadata(dataset, band_index)?;

        check_if_metadata_fits::<T>(meta.nodata(), raster_band.band_type())?;
        if dst_data.len() != meta.rows() * meta.columns() {
            return Err(Error::InvalidArgument(
                "Invalid data buffer provided: incorrect size".to_string(),
            ));
        }

        let cut_out = CutOut {
            rows: meta.rows() as i32,
            cols: meta.columns() as i32,
            ..Default::default()
        };

        read_region_from_dataset(band_index, &cut_out, dataset, dst_data, meta.columns() as i32)?;
        Ok(meta)
    }

    fn read_region_from_dataset<T: GdalType>(
        band_nr: usize,
        cut: &CutOut,
        ds: &gdal::Dataset,
        data: &mut [T],
        data_cols: i32,
    ) -> Result<()> {
        let mut data_ptr = data.as_mut_ptr();
        if cut.dst_row_offset > 0 {
            data_ptr = unsafe { data_ptr.add((cut.dst_row_offset * data_cols) as usize) };
        }

        if cut.dst_col_offset > 0 {
            data_ptr = unsafe { data_ptr.add(cut.dst_col_offset as usize) };
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
                T::gdal_ordinal(),
                0,
                data_cols as gdal_sys::GSpacing * std::mem::size_of::<T>() as gdal_sys::GSpacing,
                core::ptr::null_mut(),
            ))?;
        }

        Ok(())
    }

    /// Write raster to disk using a different data type then present in the data buffer
    /// Driver options (as documented in the GDAL drivers) can be provided
    /// If no driver options are provided, some sane defaults will be used for geotiff files
    pub fn write_as<TStore, T>(data: &[T], meta: &GeoReference, path: &Path, driver_options: &[String]) -> Result<()>
    where
        T: GdalType + Nodata<T> + num::NumCast + Copy,
        TStore: GdalType + Nodata<TStore> + num::NumCast,
    {
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
                .map(|&v| -> TStore { NumCast::from(v).unwrap_or(TStore::nodata_value()) })
                .collect();
            let mut ds = create_in_memory_with_data(meta, &converted)?;
            write_to_disk(&mut ds, path, driver_options, &[])?;
        }

        Ok(())
    }

    /// Write the raster to disk.
    /// Driver options (as documented in the GDAL drivers) can be provided.
    /// If no driver options are provided, some sane defaults will be used for geotiff files (compression, tiling).
    pub fn write<T>(data: &[T], meta: &GeoReference, path: &Path, driver_options: &[String]) -> Result
    where
        T: GdalType + Nodata<T> + num::NumCast + Copy,
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
        path: &Path,
        driver_options: &[String],
        metadata_values: &[(String, String)],
    ) -> Result<()> {
        let driver = create_raster_driver_for_path(path)?;

        let mut c_opts = CslStringList::new();
        for opt in driver_options {
            c_opts.add_string(opt)?;
        }

        if driver_options.is_empty() && driver.description().unwrap_or_default() == "GTiff" {
            // Provide sane default for GeoTIFF files
            c_opts.add_string("COMPRESS=LZW")?;
            c_opts.add_string("TILED=YES")?;
            c_opts.add_string("NUM_THREADS=ALL_CPUS")?;
        }

        for (key, value) in metadata_values {
            ds.set_metadata_item(key, value, "")?;
        }

        let path_str = path.to_string_lossy();
        let path_str = CString::new(path_str.as_ref())?;

        let _ = check_pointer(
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
        .map_err(|err| Error::Runtime(format!("Failed to write raster to disk: {}", err)));

        Ok(())
    }

    /// Creates an in-memory dataset without any bands
    pub fn create_in_memory(meta: &GeoReference) -> Result<gdal::Dataset> {
        let mem_driver = gdal::DriverManager::get_driver_by_name("MEM")?;
        Ok(mem_driver.create(PathBuf::from("in_mem"), meta.columns(), meta.rows(), 0)?)
    }

    /// Creates an in-memory dataset with the provided metadata.
    /// The array passed data will be used as the dataset band.
    /// Make sure the data array is the correct size and will live as long as the dataset.
    pub fn create_in_memory_with_data<T: GdalType + Nodata<T>>(
        meta: &GeoReference,
        data: &[T],
    ) -> Result<gdal::Dataset> {
        let mut ds = create_in_memory(meta)?;
        add_band_from_data_ptr(&mut ds, data)?;
        metadata_to_dataset_band(&mut ds, meta, 1)?;
        Ok(ds)
    }

    pub(crate) fn metadata_to_dataset_band(
        ds: &mut gdal::Dataset,
        meta: &GeoReference,
        band_index: usize,
    ) -> Result<()> {
        ds.set_geo_transform(&meta.geo_transform())?;
        ds.set_projection(meta.projection())?;
        ds.rasterband(band_index)?.set_no_data_value(meta.nodata())?;
        Ok(())
    }

    fn intersect_metadata(src_meta: &GeoReference, dst_meta: &GeoReference) -> Result<CutOut> {
        // src_meta: the metadata of the raster that we are going to read as it ison disk
        // dst_meta: the metadata of the raster that will be returned to the user

        let src_cellsize = src_meta.cell_size();
        let dst_cellsize = dst_meta.cell_size();

        if !relative_eq!(src_cellsize, dst_cellsize, epsilon = 1e-10) {
            return Err(Error::InvalidArgument("Cell sizes do not match".to_string()));
        }

        if !src_cellsize.is_valid() {
            return Err(Error::InvalidArgument("Extent cellsize is zero".to_string()));
        }

        let cell_size = src_meta.cell_size();
        let src_bbox = src_meta.bounding_box();
        let dst_bbox = dst_meta.bounding_box();

        let intersect = src_bbox.intersection(&dst_bbox);

        // Calulate the cell in the source extent that corresponds to the top left cell of the intersect
        //let intersect_top_left_cell = src_meta.point_to_cell(*intersect.top_left() + Point::new(src_cellsize.x() / 2.0, src_cellsize.y() / 2.0));
        let intersect_top_left_cell = src_meta.point_to_cell(intersect.top_left());

        let result = CutOut {
            src_col_offset: intersect_top_left_cell.col,
            src_row_offset: intersect_top_left_cell.row,
            rows: (intersect.height() / cell_size.y()).abs().round() as i32,
            cols: (intersect.width() / cell_size.x()).round() as i32,
            dst_col_offset: ((intersect.top_left().x() - dst_bbox.top_left().x()) / cell_size.x()).round() as i32,
            dst_row_offset: ((dst_bbox.top_left().y() - intersect.top_left().y()) / cell_size.y().abs()).round() as i32,
        };

        Ok(result)
    }

    fn create_raster_driver_for_path(path: &Path) -> Result<gdal::Driver> {
        let raster_format = RasterFormat::guess_from_path(path);
        if raster_format == RasterFormat::Unknown {
            return Err(Error::Runtime(format!(
                "Could not detect raster type from filename: {}",
                path.to_string_lossy()
            )));
        }

        Ok(gdal::DriverManager::get_driver_by_name(
            raster_format.gdal_driver_name(),
        )?)
    }

    fn check_if_metadata_fits<T: num::NumCast + GdalType>(nodata: Option<f64>, source_type: GdalDataType) -> Result {
        if nodata.is_some_and(|nod| !inf::cast::fits_in_type::<T>(nod)) {
            return Err(Error::InvalidArgument(format!(
            "Trying to read a raster with data type {} into a buffer with data type {}, but the rasters nodata value {} does not fit",
            source_type,
            T::datatype(),
            nodata.unwrap_or_default()
        )));
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

    #[cfg(test)]
    mod tests {
        use crate::{crs, CellSize, Point};

        use super::*;
        use approx::assert_relative_eq;
        use path_macro::path;
        use raster::Cell;

        #[test]
        fn test_intersect_metadata() {
            let meta1 = GeoReference::with_origin(
                String::default(),
                RasterSize { rows: 3, cols: 5 },
                Point::new(1.0, -10.0),
                CellSize::square(4.0),
                Some(-10.0),
            );
            let meta2 = GeoReference::with_origin(
                String::default(),
                RasterSize { rows: 3, cols: 4 },
                Point::new(-3.0, -6.0),
                CellSize::square(4.0),
                Some(-6.0),
            );

            assert_eq!(meta2.cell_center(Cell::from_row_col(0, 0)), Point::new(-1.0, 4.0));
            assert_eq!(meta1.point_to_cell(Point::new(0.0, 4.0)), Cell::from_row_col(-1, -1));

            let cutout = intersect_metadata(&meta1, &meta2).unwrap();

            assert_eq!(cutout.rows, 2);
            assert_eq!(cutout.cols, 3);
            assert_eq!(cutout.src_col_offset, 0);
            assert_eq!(cutout.src_row_offset, 0);
            assert_eq!(cutout.dst_col_offset, 1);
            assert_eq!(cutout.dst_row_offset, 1);
        }

        #[test]
        fn intersect_meta_epsg_4326() {
            const TRANS: [f64; 6] = [
                -30.000_000_763_788_11,
                0.100_000_001_697_306_9,
                0.0,
                29.999999619212282,
                0.0,
                -0.049_999_998_635_984_29,
            ];

            let meta = GeoReference::new(
                "EPSG:4326".to_string(),
                RasterSize { rows: 840, cols: 900 },
                TRANS,
                None,
            );
            assert_relative_eq!(
                meta.cell_center(Cell::from_row_col(0, 0)),
                Point::new(TRANS[0] + (TRANS[1] / 2.0), TRANS[3] + (TRANS[5] / 2.0)),
                epsilon = 1e-6
            );

            // Cell to point and back
            let cell = Cell::from_row_col(0, 0);
            assert_eq!(meta.point_to_cell(meta.cell_center(cell)), cell);
            assert_eq!(meta.point_to_cell(meta.top_left()), Cell::from_row_col(0, 0));

            let cutout = intersect_metadata(&meta, &meta).unwrap();
            assert_eq!(cutout.cols, 900);
            assert_eq!(cutout.rows, 840);

            assert_eq!(cutout.src_col_offset, 0);
            assert_eq!(cutout.dst_col_offset, 0);

            assert_eq!(cutout.src_row_offset, 0);
            assert_eq!(cutout.dst_row_offset, 0);
        }

        #[test]
        fn projection_info_projected_31370() {
            let path = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data" / "epsg31370.tif");
            let meta = read_file_metadata(path.as_path()).unwrap();
            assert!(!meta.projection().is_empty());
            assert!(meta.projected_epsg().is_some());
            assert_eq!(meta.projected_epsg(), Some(crs::epsg::BELGIAN_LAMBERT72));
            assert_eq!(meta.geographic_epsg(), Some(crs::epsg::BELGE72_GEO));
            assert_eq!(meta.projection_frienly_name(), "EPSG:31370");
        }

        #[test]
        fn projection_info_projected_3857() {
            let path = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data" / "epsg3857.tif");
            let meta = read_file_metadata(path.as_path()).unwrap();
            assert!(!meta.projection().is_empty());
            assert!(meta.projected_epsg().is_some());
            assert_eq!(meta.projected_epsg().unwrap(), crs::epsg::WGS84_WEB_MERCATOR);
            assert_eq!(meta.geographic_epsg().unwrap(), crs::epsg::WGS84);
            assert_eq!(meta.projection_frienly_name(), "EPSG:3857");
        }

        #[test]
        fn open_read_only_invalid_path() {
            let path = PathBuf::from("/this/does/not/exist.tif");
            let res = open_read_only(path.as_path());
            assert!(res.is_err());
            assert!(matches!(res.err().unwrap(), Error::InvalidPath(p) if p == path));
        }
    }
}

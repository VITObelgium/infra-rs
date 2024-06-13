use std::{
    ffi::CStr,
    path::{Path, PathBuf},
};

use approx::relative_eq;
use gdal::{errors::GdalError, raster::GdalType};
use num::NumCast;

use crate::{rect::rectangle_intersection, Error, GeoMetadata, Nodata, RasterNum, RasterSize};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RasterType {
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

#[derive(Default)]
struct CutOut {
    pub src_col_offset: i32,
    pub src_row_offset: i32,
    pub dst_col_offset: i32,
    pub dst_row_offset: i32,
    pub rows: i32,
    pub cols: i32,
}

fn raw_string_to_string(raw_ptr: *const libc::c_char) -> String {
    let c_str = unsafe { std::ffi::CStr::from_ptr(raw_ptr) };
    c_str.to_string_lossy().into_owned()
}

pub fn check_gdal_rc(rc: gdal_sys::CPLErr::Type) -> Result<(), GdalError> {
    if rc != 0 {
        let last_err_no = unsafe { gdal_sys::CPLGetLastErrorNo() };
        let last_err_msg = raw_string_to_string(unsafe { gdal_sys::CPLGetLastErrorMsg() });
        Err(GdalError::CplError {
            class: rc,
            number: last_err_no,
            msg: last_err_msg,
        })
    } else {
        Ok(())
    }
}

pub fn guess_rastertype_from_filename(file_path: &Path) -> RasterType {
    let ext = file_path.extension().map(|ext| ext.to_string_lossy().to_lowercase());

    if let Some(ext) = ext {
        match ext.as_ref() {
            "asc" => RasterType::ArcAscii,
            "tiff" | "tif" => RasterType::GeoTiff,
            "gif" => RasterType::Gif,
            "png" => RasterType::Png,
            "map" => RasterType::PcRaster,
            "nc" => RasterType::Netcdf,
            "mbtiles" => RasterType::MBTiles,
            "gpkg" => RasterType::GeoPackage,
            "grib" => RasterType::Grib,
            _ => {
                let path = file_path.to_string_lossy();
                if path.starts_with("postgresql://") || path.starts_with("pg:") {
                    RasterType::Postgis
                } else {
                    RasterType::Unknown
                }
            }
        }
    } else {
        RasterType::Unknown
    }
}

fn str_vec<T: AsRef<str>>(options: &[T]) -> Vec<&str> {
    options.iter().map(|s| s.as_ref()).collect()
}

pub fn open_raster_read_only(path: &Path) -> Result<gdal::Dataset, Error> {
    let ds_opts = gdal::DatasetOptions {
        open_flags: gdal::GdalOpenFlags::GDAL_OF_READONLY | gdal::GdalOpenFlags::GDAL_OF_RASTER,
        ..Default::default()
    };
    Ok(gdal::Dataset::open_ex(path, ds_opts)?)
}

pub fn open_raster_read_only_with_options(path: &Path, open_options: &[&str]) -> Result<gdal::Dataset, Error> {
    let ds_opts = gdal::DatasetOptions {
        open_flags: gdal::GdalOpenFlags::GDAL_OF_READONLY | gdal::GdalOpenFlags::GDAL_OF_RASTER,
        open_options: Some(open_options),
        ..Default::default()
    };
    Ok(gdal::Dataset::open_ex(path, ds_opts)?)
}

pub fn metadata_from_file(path: &Path) -> Result<GeoMetadata, Error> {
    metadata_from_dataset_band(&open_raster_read_only(path)?, 1)
}

pub fn metadata_from_file_with_options<T: AsRef<str>>(path: &Path, open_options: &[T]) -> Result<GeoMetadata, Error> {
    metadata_from_dataset_band(&open_raster_read_only_with_options(path, str_vec(open_options).as_slice())?, 1)
}

pub fn metadata_from_dataset_band(ds: &gdal::Dataset, band_index: usize) -> Result<GeoMetadata, Error> {
    let rasterband = ds.rasterband(band_index)?;

    let (width, height) = ds.raster_size();
    Ok(GeoMetadata::new(
        ds.projection(),
        RasterSize { rows: height, cols: width },
        ds.geo_transform()?,
        rasterband.no_data_value(),
    ))
}

fn metadata_to_dataset_band(ds: &mut gdal::Dataset, meta: &GeoMetadata, band_index: usize) -> Result<(), Error> {
    ds.set_geo_transform(&meta.geo_transform())?;
    ds.set_projection(meta.projection())?;
    ds.rasterband(band_index)?.set_no_data_value(meta.nodata())?;
    Ok(())
}

fn intersect_metadata(src_meta: &GeoMetadata, dst_meta: &GeoMetadata) -> Result<CutOut, Error> {
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

    let intersect = rectangle_intersection(&src_bbox, &dst_bbox);

    // Calulate the cell in the source extent that corresponds to the top left cell of the intersect
    //let intersect_top_left_cell = src_meta.point_to_cell(*intersect.top_left() + Point::new(src_cellsize.x() / 2.0, src_cellsize.y() / 2.0));
    let intersect_top_left_cell = src_meta.point_to_cell(*intersect.top_left());

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

fn fits_in_type<T: NumCast>(v: f64) -> bool {
    let x: Option<T> = NumCast::from(v);
    x.is_some()
}

/// The provided extent will be the extent of the resulting raster
/// Areas outside the extent of the raster on disk will be filled with nodata
pub fn data_from_dataset_with_extent<T: GdalType + RasterNum<T>>(
    dataset: &gdal::Dataset,
    extent: &GeoMetadata,
    band_nr: usize,
    dst_data: &mut [T],
) -> Result<GeoMetadata, Error> {
    let meta = metadata_from_dataset_band(dataset, band_nr)?;
    let cut_out = intersect_metadata(&meta, extent)?;

    let cut_out_smaller_than_extent = (extent.rows() * extent.columns()) != (cut_out.rows * cut_out.cols) as usize;
    let mut dst_meta = extent.clone();
    if let Some(nodata) = meta.nodata() {
        dst_meta.set_nodata(Some(nodata));
    }

    if cut_out_smaller_than_extent && dst_meta.nodata().is_none() {
        dst_meta.set_nodata(Some(NumCast::from(T::max_value()).unwrap_or(-9999.0)));
    }

    if dst_data.len() != dst_meta.rows() * dst_meta.columns() {
        return Err(Error::InvalidArgument("Invalid data buffer provided: incorrect size".to_string()));
    }

    if cut_out_smaller_than_extent {
        if let Some(nodata) = dst_meta.nodata() {
            dst_data.fill(NumCast::from(nodata).unwrap_or(T::zero()));
        }
    }

    //let is_byte = <T>::datatype() == GdalDataType::UInt8;
    // let mut nodata_fit_in_type = true;
    // if let Some(nodata) = dst_meta.nodata {
    //     nodata_fit_in_type = fits_in_type::<T>(nodata);
    // }

    // TODO
    // let is_byte = std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>();
    // if is_byte && dst_meta.nodata.is_some() && !inf::fits_in_type(dst_meta.nodata.unwrap()) {
    //     let mut temp_data: Vec<f32> = vec![dst_meta.nodata.unwrap_or(0) as f32; extent.rows * extent.cols];
    //     read_raster_data(band_nr, &cut_out, dataset, &mut temp_data, extent.cols)?;
    //     let cast_meta = cast_raster::<f32, T>(dst_meta, temp_data, dst_data)?;
    //     Ok(cast_meta)
    // } else {
    if cut_out.cols * cut_out.rows > 0 {
        read_raster_data(band_nr, &cut_out, dataset, dst_data, dst_meta.columns() as i32)?;
    }

    let raster_band = dataset.rasterband(band_nr)?;
    let data_type = raster_band.band_type();
    if <T>::datatype() != data_type {
        if let Some(nodata) = raster_band.no_data_value() {
            if nodata.is_finite() && !fits_in_type::<T>(nodata) {
                dst_meta.set_nodata(Some(nodata));
            }
        }
    }
    Ok(dst_meta)
    //}
}

/// This version will read the full dataset and is used in cases where there is no geotransform info available
pub fn data_from_dataset<T: GdalType>(dataset: &gdal::Dataset, band_nr: usize, dst_data: &mut [T]) -> Result<GeoMetadata, Error> {
    let raster_band = dataset.rasterband(band_nr)?;
    let meta = GeoMetadata::without_spatial_reference(
        RasterSize {
            rows: raster_band.y_size(),
            cols: raster_band.x_size(),
        },
        raster_band.no_data_value(),
    );

    if dst_data.len() != meta.rows() * meta.columns() {
        return Err(Error::InvalidArgument("Invalid data buffer provided: incorrect size".to_string()));
    }

    let cut_out = CutOut {
        rows: meta.rows() as i32,
        cols: meta.columns() as i32,
        ..Default::default()
    };

    // let is_byte = std::any::TypeId::of::<T>() == std::any::TypeId::of::<u8>();
    // if is_byte && meta.nodata.is_some() && !inf::fits_in_type(meta.nodata.unwrap()) {
    //     let mut temp_data: Vec<f32> = vec![meta.nodata.unwrap_or(0) as f32; meta.rows * meta.cols];
    //     read_raster_data(band_nr, &cut_out, dataset, &mut temp_data, meta.cols)?;
    //     let cast_meta = cast_raster::<f32, T>(meta, temp_data, dst_data)?;
    //     Ok(cast_meta)
    // } else {
    read_raster_data(band_nr, &cut_out, dataset, dst_data, meta.columns() as i32)?;
    Ok(meta)
    //}
}

fn read_raster_data<T: GdalType>(band_nr: usize, cut: &CutOut, ds: &gdal::Dataset, data: &mut [T], data_cols: i32) -> Result<(), Error> {
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
        check_gdal_rc(gdal_sys::GDALRasterIOEx(
            raster_band.c_rasterband(),
            gdal_sys::GDALRWFlag::GF_Read,
            window.0,
            window.1,
            window_size.0,
            window_size.1,
            data_ptr as *mut libc::c_void,
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

fn add_band<T: GdalType>(ds: &mut gdal::Dataset, data: &[T]) -> Result<(), Error> {
    // convert the data pointer to a string
    let ptr: [libc::c_char; 32] = [0; 32];
    unsafe { gdal_sys::CPLPrintPointer(ptr.as_ptr() as *mut libc::c_char, data.as_ptr() as *mut std::ffi::c_void, ptr.len() as i32) };
    let c_str: &CStr = unsafe { CStr::from_ptr(ptr.as_ptr()) };

    let mut str_options = gdal::cpl::CslStringList::new();
    str_options.add_string(format!("DATAPOINTER={}", c_str.to_str().unwrap()).as_str())?;
    let rc = unsafe { gdal_sys::GDALAddBand(ds.c_dataset(), T::gdal_ordinal(), str_options.as_ptr()) };
    check_gdal_rc(rc)?;

    Ok(())
}

/// Creates an in-memory dataset with the provided metadata
/// The array passed data will be used as the dataset band
/// Make sure the data array is the correct size and will live as long as the dataset
pub fn create_memory_dataset<T: GdalType + Nodata<T>>(meta: &GeoMetadata, data: &mut [T]) -> Result<gdal::Dataset, Error> {
    let mem_driver = gdal::DriverManager::get_driver_by_name("MEM")?;
    let mut ds = mem_driver.create_with_band_type::<T, _>(&PathBuf::from("in_mem"), meta.columns(), meta.rows(), 0)?;
    add_band(&mut ds, data)?;
    metadata_to_dataset_band(&mut ds, meta, 1)?;

    Ok(ds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crs;
    use crate::Cell;
    use crate::CellSize;
    use crate::Point;
    use approx::assert_relative_eq;

    #[test]
    fn test_intersect_metadata() {
        let meta1 = GeoMetadata::with_origin(
            String::default(),
            RasterSize { rows: 3, cols: 5 },
            Point::new(1.0, -10.0),
            CellSize::square(4.0),
            Some(-10.0),
        );
        let meta2 = GeoMetadata::with_origin(
            String::default(),
            RasterSize { rows: 3, cols: 4 },
            Point::new(-3.0, -6.0),
            CellSize::square(4.0),
            Some(-6.0),
        );

        assert_eq!(meta2.cell_center(Cell::new(0, 0)), Point::new(-1.0, 4.0));
        assert_eq!(meta1.point_to_cell(Point::new(0.0, 4.0)), Cell::new(-1, -1));

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

        let meta = GeoMetadata::new("EPSG:4326".to_string(), RasterSize { rows: 840, cols: 900 }, TRANS, None);
        assert_relative_eq!(
            meta.cell_center(Cell::new(0, 0)),
            Point::new(TRANS[0] + (TRANS[1] / 2.0), TRANS[3] + (TRANS[5] / 2.0)),
            epsilon = 1e-6
        );

        // Cell to point and back
        let cell = Cell::new(0, 0);
        assert_eq!(meta.point_to_cell(meta.cell_center(cell)), cell);
        assert_eq!(meta.point_to_cell(meta.top_left()), Cell::new(0, 0));

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
        let path: std::path::PathBuf = [env!("CARGO_MANIFEST_DIR"), "test", "data", "epsg31370.tif"].iter().collect();
        let meta = metadata_from_file(path.as_path()).unwrap();
        assert!(!meta.projection().is_empty());
        log::info!("{}", meta.projection());
        assert!(meta.projected_epsg().is_some());
        assert_eq!(meta.projected_epsg(), Some(crs::epsg::BELGIAN_LAMBERT72));
        assert_eq!(meta.geographic_epsg(), Some(crs::epsg::BELGE72_GEO));
        assert_eq!(meta.projection_frienly_name(), "EPSG:31370");
    }

    #[test]
    fn projection_info_projected_3857() {
        let path: std::path::PathBuf = [env!("CARGO_MANIFEST_DIR"), "test", "data", "epsg3857.tif"].iter().collect();
        let meta = metadata_from_file(path.as_path()).unwrap();
        assert!(!meta.projection().is_empty());
        assert!(meta.projected_epsg().is_some());
        assert_eq!(meta.projected_epsg().unwrap(), crs::epsg::WGS84_WEB_MERCATOR);
        assert_eq!(meta.geographic_epsg().unwrap(), crs::epsg::WGS84);
        assert_eq!(meta.projection_frienly_name(), "EPSG:3857");
    }
}

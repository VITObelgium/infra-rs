use std::{mem::MaybeUninit, path::Path};

use simd_macro::simd_bounds;

use crate::{ArrayDataType, ArrayNum, Error, GeoReference, RasterSize, Result, raster::io::RasterFormat};

#[cfg(feature = "gdal")]
pub mod gdal;
#[cfg(feature = "raster-io-geotiff")]
pub mod geotiff;
#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

// Dyn compatible methods for raster reading
pub trait RasterReaderDyn {
    fn band_count(&self) -> Result<usize>;
    fn raster_size(&self) -> Result<RasterSize>;
    fn georeference(&mut self, band_index: usize) -> Result<GeoReference>;
    fn data_type(&self, band_index: usize) -> Result<ArrayDataType>;
    fn overview_count(&self, band_index: usize) -> Result<usize>;

    fn read_raster_band_u8(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<u8>]) -> Result<GeoReference>;
    fn read_raster_band_u16(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<u16>]) -> Result<GeoReference>;
    fn read_raster_band_u32(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<u32>]) -> Result<GeoReference>;
    fn read_raster_band_u64(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<u64>]) -> Result<GeoReference>;

    fn read_raster_band_i8(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<i8>]) -> Result<GeoReference>;
    fn read_raster_band_i16(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<i16>]) -> Result<GeoReference>;
    fn read_raster_band_i32(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<i32>]) -> Result<GeoReference>;
    fn read_raster_band_i64(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<i64>]) -> Result<GeoReference>;

    fn read_raster_band_f32(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<f32>]) -> Result<GeoReference>;
    fn read_raster_band_f64(&mut self, band_index: usize, dst_data: &mut [MaybeUninit<f64>]) -> Result<GeoReference>;

    fn read_raster_band_region(
        &mut self,
        band_index: usize,
        extent: &GeoReference,
        data_type: ArrayDataType,
        dst_data: &mut [MaybeUninit<u8>],
    ) -> Result<GeoReference>;
}

/// Trait for reading raster data from various formats.
/// Meant to be implemented by different raster format readers and not be used directly.
pub trait RasterReader: RasterReaderDyn + Sized {
    fn open_read_only(path: impl AsRef<Path>) -> Result<Self>;
    fn open_read_only_with_options(path: impl AsRef<Path>, open_options: &RasterOpenOptions) -> Result<Self>;

    #[simd_bounds]
    fn read_raster_band<T: ArrayNum>(
        &mut self,
        band_index: usize,
        data_type: ArrayDataType,
        dst_data: &mut [MaybeUninit<T>],
    ) -> Result<GeoReference>;
}

#[derive(Debug, Clone, Default)]
pub struct RasterOpenOptions {
    pub layer_name: Option<String>,
    pub driver_specific_options: Option<Vec<String>>,
}

/// Creates a `RasterReader` for the specified path based on the file extension.
pub fn create_raster_reader(path: impl AsRef<Path>) -> Result<Box<dyn RasterReaderDyn>> {
    create_raster_reader_with_options(path, &RasterOpenOptions::default())
}

/// Creates a `RasterReader` for the specified path based on the file extension.
pub fn create_raster_reader_with_options(path: impl AsRef<Path>, _options: &RasterOpenOptions) -> Result<Box<dyn RasterReaderDyn>> {
    match RasterFormat::guess_from_path(path.as_ref()) {
        // #[cfg(feature = "raster-io-geotiff")]
        // RasterFormat::GeoTiff => Ok(Box::new(geotiff::GeotiffRasterIO::open_read_only_with_options(
        //     path.as_ref(),
        //     _options,
        // )?)),
        // #[cfg(not(feature = "raster-io-geotiff"))]
        // RasterFormat::GeoTiff => Ok(Box::new(gdal::GdalRasterIO::open_read_only_with_options(path.as_ref(), options)?)),
        #[cfg(feature = "gdal")]
        RasterFormat::ArcAscii
        | RasterFormat::Gif
        | RasterFormat::GeoTiff
        | RasterFormat::Png
        | RasterFormat::PcRaster
        | RasterFormat::Netcdf
        | RasterFormat::MBTiles
        | RasterFormat::GeoPackage
        | RasterFormat::Grib
        | RasterFormat::Postgis
        | RasterFormat::Vrt => Ok(Box::new(gdal::GdalRasterIO::open_read_only_with_options(path.as_ref(), _options)?)),
        _ => Err(Error::Runtime(format!("Unsupported raster file type: {}", path.as_ref().display()))),
    }
}

fn reinterpret_slice<TDest, T>(data: &mut [MaybeUninit<T>]) -> &mut [MaybeUninit<TDest>] {
    debug_assert!(std::mem::size_of::<TDest>() == std::mem::size_of::<T>());
    let byte_len = data.len() * std::mem::size_of::<T>();
    unsafe { std::slice::from_raw_parts_mut(data.as_mut_ptr().cast::<MaybeUninit<TDest>>(), byte_len) }
}

/// Extension trait: generic convenience method
pub trait RasterReaderGeneric: RasterReaderDyn {
    fn read_band<T: crate::ArrayNum>(&mut self, band_index: usize, dst: &mut [MaybeUninit<T>]) -> Result<GeoReference> {
        use crate::ArrayDataType;

        match T::TYPE {
            ArrayDataType::Uint8 => self.read_raster_band_u8(band_index, reinterpret_slice(dst)),
            ArrayDataType::Uint16 => self.read_raster_band_u16(band_index, reinterpret_slice(dst)),
            ArrayDataType::Uint32 => self.read_raster_band_u32(band_index, reinterpret_slice(dst)),
            ArrayDataType::Uint64 => self.read_raster_band_u64(band_index, reinterpret_slice(dst)),
            ArrayDataType::Int8 => self.read_raster_band_i8(band_index, reinterpret_slice(dst)),
            ArrayDataType::Int16 => self.read_raster_band_i16(band_index, reinterpret_slice(dst)),
            ArrayDataType::Int32 => self.read_raster_band_i32(band_index, reinterpret_slice(dst)),
            ArrayDataType::Int64 => self.read_raster_band_i64(band_index, reinterpret_slice(dst)),
            ArrayDataType::Float32 => self.read_raster_band_f32(band_index, reinterpret_slice(dst)),
            ArrayDataType::Float64 => self.read_raster_band_f64(band_index, reinterpret_slice(dst)),
        }
    }
}

impl<R: RasterReaderDyn + ?Sized> RasterReaderGeneric for R {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(all(feature = "cog", feature = "gdal"))]
    fn compare_geotiff_gdal_read() -> Result<()> {
        use inf::allocate::AlignedVecUnderConstruction;

        use crate::testutils;

        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let mut gdal_reader = gdal::GdalRasterIO::open_read_only(&input)?;
        let mut gtif_reader = geotiff::GeotiffRasterIO::open_read_only(&input)?;

        let band_index = 1;

        assert_eq!(gdal_reader.band_count()?, gtif_reader.band_count()?);
        assert_eq!(gdal_reader.raster_size()?, gtif_reader.raster_size()?);
        assert_eq!(gdal_reader.data_type(band_index)?, gtif_reader.data_type(band_index)?);
        assert_eq!(gdal_reader.overview_count(band_index)?, gtif_reader.overview_count(band_index)?);
        assert_eq!(
            gdal_reader.georeference(band_index)?.geo_transform(),
            gtif_reader.georeference(band_index)?.geo_transform()
        );

        {
            // Read the full raster band
            let mut gdal_data = AlignedVecUnderConstruction::<u8>::new(gdal_reader.georeference(band_index)?.raster_size().cell_count());
            let mut gtif_data = AlignedVecUnderConstruction::<u8>::new(gtif_reader.georeference(band_index)?.raster_size().cell_count());

            let gdal_georef = gdal_reader.read_raster_band(band_index, ArrayDataType::Uint8, gdal_data.as_uninit_byte_slice_mut())?;
            let gtif_georef = gtif_reader.read_raster_band(band_index, ArrayDataType::Uint8, gtif_data.as_uninit_byte_slice_mut())?;

            assert_eq!(unsafe { gdal_data.assume_init() }, unsafe { gtif_data.assume_init() });
            assert_eq!(gdal_georef.geo_transform(), gtif_georef.geo_transform());
            assert_eq!(gdal_georef.projected_epsg(), gtif_georef.projected_epsg());
            // assert_eq!(gdal_georef.geographic_epsg(), gtif_georef.geographic_epsg()); TODO try to obtain the full wkt so this can be filled in
        }

        // {
        //     // Test region reading functionality
        //     let full_georef = gdal_reader.georeference(band_index)?;

        //     // Create a smaller region (upper-left quarter of the image)
        //     let region_rows = full_georef.rows().count() / 2;
        //     let region_cols = full_georef.columns().count() / 2;
        //     let region_georef = GeoReference::new(
        //         full_georef.projection(),
        //         RasterSize {
        //             rows: region_rows.into(),
        //             cols: region_cols.into(),
        //         },
        //         full_georef.geo_transform(),
        //         full_georef.nodata(),
        //     );

        //     let mut gdal_region_data = AlignedVecUnderConstruction::<u8>::new(region_georef.raster_size().cell_count());
        //     let mut gtif_region_data = AlignedVecUnderConstruction::<u8>::new(region_georef.raster_size().cell_count());

        //     let gdal_region_georef = gdal_reader.read_raster_band_region(
        //         band_index,
        //         &region_georef,
        //         ArrayDataType::Uint8,
        //         gdal_region_data.as_uninit_byte_slice_mut(),
        //     )?;

        //     let gtif_region_georef = gtif_reader.read_raster_band_region(
        //         band_index,
        //         &region_georef,
        //         ArrayDataType::Uint8,
        //         gtif_region_data.as_uninit_byte_slice_mut(),
        //     )?;

        //     // Validate GDAL region reading results
        //     assert_eq!(gdal_region_georef.raster_size(), region_georef.raster_size());
        //     assert_eq!(gdal_region_georef.geo_transform(), region_georef.geo_transform());

        //     assert_eq!(unsafe { gdal_region_data.assume_init() }, unsafe { gtif_region_data.assume_init() });
        //     assert_eq!(gdal_region_georef.geo_transform(), gtif_region_georef.geo_transform());
        //     assert_eq!(gdal_region_georef.projected_epsg(), gtif_region_georef.projected_epsg());
        // }

        Ok(())
    }
}

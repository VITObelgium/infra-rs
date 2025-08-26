use std::{mem::MaybeUninit, path::Path};

#[cfg(feature = "gdal")]
use crate::Error;
use crate::{ArrayDataType, GeoReference, RasterSize, Result, raster::io::RasterFormat};

#[cfg(feature = "gdal")]
pub mod gdal;
pub mod geotiff;

/// Trait for reading raster data from various formats.
/// Meant to be implemented by different raster format readers and not be used directly.
pub trait RasterReader {
    fn open_read_only(path: impl AsRef<Path>) -> Result<Self>
    where
        Self: Sized;
    fn open_read_only_with_options(path: impl AsRef<Path>, open_options: &RasterOpenOptions) -> Result<Self>
    where
        Self: Sized;

    fn band_count(&self) -> Result<usize>;
    fn raster_size(&self) -> Result<RasterSize>;
    fn georeference(&mut self, band_index: usize) -> Result<GeoReference>;
    fn data_type(&self, band_index: usize) -> Result<ArrayDataType>;
    fn overview_count(&self, band_index: usize) -> Result<usize>;

    fn read_raster_band(&mut self, band_index: usize, data_type: ArrayDataType, dst_data: &mut [MaybeUninit<u8>]) -> Result<GeoReference>;
    fn read_raster_band_region(
        &mut self,
        band_index: usize,
        extent: &GeoReference,
        data_type: ArrayDataType,
        dst_data: &mut [MaybeUninit<u8>],
    ) -> Result<GeoReference>;
}

#[derive(Debug, Clone, Default)]
pub struct RasterOpenOptions {
    pub layer_name: Option<String>,
    pub driver_specific_options: Option<Vec<String>>,
}

/// Creates a `RasterReader` for the specified path based on the file extension.
pub fn create_raster_reader(path: impl AsRef<Path>) -> Result<Box<dyn RasterReader>> {
    match RasterFormat::guess_from_path(path.as_ref()) {
        #[cfg(feature = "gdal")]
        RasterFormat::ArcAscii
        | RasterFormat::GeoTiff
        | RasterFormat::Gif
        | RasterFormat::Png
        | RasterFormat::PcRaster
        | RasterFormat::Netcdf
        | RasterFormat::MBTiles
        | RasterFormat::GeoPackage
        | RasterFormat::Grib
        | RasterFormat::Postgis
        | RasterFormat::Vrt => Ok(Box::new(gdal::GdalRasterIO::open_read_only(path.as_ref())?)),
        _ => Err(Error::Runtime(format!("Unsupported raster file type: {}", path.as_ref().display()))),
    }
}

/// Creates a `RasterReader` for the specified path based on the file extension.
pub fn create_raster_reader_with_options(path: impl AsRef<Path>, options: &RasterOpenOptions) -> Result<Box<dyn RasterReader>> {
    match RasterFormat::guess_from_path(path.as_ref()) {
        #[cfg(feature = "gdal")]
        RasterFormat::ArcAscii
        | RasterFormat::GeoTiff
        | RasterFormat::Gif
        | RasterFormat::Png
        | RasterFormat::PcRaster
        | RasterFormat::Netcdf
        | RasterFormat::MBTiles
        | RasterFormat::GeoPackage
        | RasterFormat::Grib
        | RasterFormat::Postgis
        | RasterFormat::Vrt => Ok(Box::new(gdal::GdalRasterIO::open_read_only_with_options(path.as_ref(), options)?)),
        _ => Err(Error::Runtime(format!("Unsupported raster file type: {}", path.as_ref().display()))),
    }
}

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

        Ok(())
    }
}

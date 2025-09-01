use std::{mem::MaybeUninit, path::Path};

use simd_macro::simd_bounds;

use crate::{
    ArrayDataType, ArrayNum, Error, GeoReference, RasterSize, Result,
    raster::{io::RasterFormat, utils::reinterpret_uninit_slice_to_byte},
};

#[cfg(feature = "gdal")]
pub mod gdal;
#[cfg(feature = "raster-io-geotiff")]
pub mod geotiff;
#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

// Dyn compatible methods for raster reading
// Dyn compatibility is obtained by passing the databuffer as a byte slice whuch is then reinterpreted to the desired type in the concrete implementation
pub trait RasterReaderDyn {
    fn band_count(&self) -> Result<usize>;
    fn raster_size(&self) -> Result<RasterSize>;
    fn georeference(&mut self, band_index: usize) -> Result<GeoReference>;
    fn data_type(&self, band_index: usize) -> Result<ArrayDataType>;
    fn overview_count(&self, band_index: usize) -> Result<usize>;

    fn read_raster_band(&mut self, band_index: usize, data_type: ArrayDataType, dst_data: &mut [MaybeUninit<u8>]) -> Result<GeoReference>;
    fn read_raster_band_region(
        &mut self,
        band: usize,
        region: &GeoReference,
        data_type: ArrayDataType,
        data: &mut [MaybeUninit<u8>],
    ) -> Result<GeoReference>;
}

/// Trait for reading raster data from various formats.
/// Meant to be implemented by different raster format readers and not be used directly.
pub trait RasterReader: RasterReaderDyn + Sized {
    fn open_read_only(path: impl AsRef<Path>) -> Result<Self>;
    fn open_read_only_with_options(path: impl AsRef<Path>, open_options: &RasterOpenOptions) -> Result<Self>;

    #[simd_bounds]
    fn read_raster_band_as<T: ArrayNum>(
        &mut self,
        band_index: usize,
        data_type: ArrayDataType,
        dst_data: &mut [MaybeUninit<T>],
    ) -> Result<GeoReference>;

    #[simd_bounds]
    fn read_raster_band_region_as<T: ArrayNum>(
        &mut self,
        band_index: usize,
        region: &GeoReference,
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

/// Extension trait: generic convenience method
pub trait RasterReaderGeneric: RasterReaderDyn {
    fn read_band<T: crate::ArrayNum>(&mut self, band_index: usize, dst: &mut [MaybeUninit<T>]) -> Result<GeoReference> {
        use crate::ArrayDataType;

        match T::TYPE {
            ArrayDataType::Uint8 => self.read_raster_band(band_index, ArrayDataType::Uint8, reinterpret_uninit_slice_to_byte(dst)),
            ArrayDataType::Uint16 => self.read_raster_band(band_index, ArrayDataType::Uint16, reinterpret_uninit_slice_to_byte(dst)),
            ArrayDataType::Uint32 => self.read_raster_band(band_index, ArrayDataType::Uint32, reinterpret_uninit_slice_to_byte(dst)),
            ArrayDataType::Uint64 => self.read_raster_band(band_index, ArrayDataType::Uint64, reinterpret_uninit_slice_to_byte(dst)),
            ArrayDataType::Int8 => self.read_raster_band(band_index, ArrayDataType::Int8, reinterpret_uninit_slice_to_byte(dst)),
            ArrayDataType::Int16 => self.read_raster_band(band_index, ArrayDataType::Int16, reinterpret_uninit_slice_to_byte(dst)),
            ArrayDataType::Int32 => self.read_raster_band(band_index, ArrayDataType::Int32, reinterpret_uninit_slice_to_byte(dst)),
            ArrayDataType::Int64 => self.read_raster_band(band_index, ArrayDataType::Int64, reinterpret_uninit_slice_to_byte(dst)),
            ArrayDataType::Float32 => self.read_raster_band(band_index, ArrayDataType::Float32, reinterpret_uninit_slice_to_byte(dst)),
            ArrayDataType::Float64 => self.read_raster_band(band_index, ArrayDataType::Float64, reinterpret_uninit_slice_to_byte(dst)),
        }
    }

    fn read_band_region<T: crate::ArrayNum>(
        &mut self,
        band: usize,
        region: &GeoReference,
        dst: &mut [MaybeUninit<T>],
    ) -> Result<GeoReference> {
        use crate::ArrayDataType;

        match T::TYPE {
            ArrayDataType::Uint8 => self.read_raster_band_region(band, region, ArrayDataType::Uint8, reinterpret_uninit_slice_to_byte(dst)),
            ArrayDataType::Uint16 => {
                self.read_raster_band_region(band, region, ArrayDataType::Uint16, reinterpret_uninit_slice_to_byte(dst))
            }
            ArrayDataType::Uint32 => {
                self.read_raster_band_region(band, region, ArrayDataType::Uint32, reinterpret_uninit_slice_to_byte(dst))
            }
            ArrayDataType::Uint64 => {
                self.read_raster_band_region(band, region, ArrayDataType::Uint64, reinterpret_uninit_slice_to_byte(dst))
            }
            ArrayDataType::Int8 => self.read_raster_band_region(band, region, ArrayDataType::Int8, reinterpret_uninit_slice_to_byte(dst)),
            ArrayDataType::Int16 => self.read_raster_band_region(band, region, ArrayDataType::Int16, reinterpret_uninit_slice_to_byte(dst)),
            ArrayDataType::Int32 => self.read_raster_band_region(band, region, ArrayDataType::Int32, reinterpret_uninit_slice_to_byte(dst)),
            ArrayDataType::Int64 => self.read_raster_band_region(band, region, ArrayDataType::Int64, reinterpret_uninit_slice_to_byte(dst)),
            ArrayDataType::Float32 => {
                self.read_raster_band_region(band, region, ArrayDataType::Float32, reinterpret_uninit_slice_to_byte(dst))
            }
            ArrayDataType::Float64 => {
                self.read_raster_band_region(band, region, ArrayDataType::Float64, reinterpret_uninit_slice_to_byte(dst))
            }
        }
    }
}

impl<R: RasterReaderDyn + ?Sized> RasterReaderGeneric for R {}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_input_files() -> Vec<(&'static str, ArrayDataType)> {
        vec![
            ("landusebyte.tif", ArrayDataType::Uint8),
            ("landusebyte_tiled.tif", ArrayDataType::Uint8),
        ]
    }

    #[cfg(all(feature = "cog", feature = "gdal"))]
    fn compare_geotiff_vs_gdal_read(
        input: &Path,
        band_index: usize,
        geo_reference: Option<&GeoReference>,
        data_type: ArrayDataType,
    ) -> Result<()> {
        use inf::allocate::AlignedVecUnderConstruction;

        use crate::{ArrayInterop, raster::DenseRaster};

        let mut gdal_reader = gdal::GdalRasterIO::open_read_only(input)?;
        let mut gtif_reader = geotiff::GeotiffRasterIO::open_read_only(input)?;

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

        let mut gdal_region_data = AlignedVecUnderConstruction::<u8>::new(geo_ref.raster_size().cell_count());
        let mut gtif_region_data = AlignedVecUnderConstruction::<u8>::new(geo_ref.raster_size().cell_count());

        let (gdal_geo, gtif_geo) = if let Some(geo_reference) = geo_reference {
            (
                gdal_reader.read_raster_band_region(band_index, geo_reference, data_type, gdal_region_data.as_uninit_slice_mut())?,
                gtif_reader.read_raster_band_region(band_index, geo_reference, data_type, gtif_region_data.as_uninit_slice_mut())?,
            )
        } else {
            // Read the full raster band
            (
                gdal_reader.read_raster_band_as(band_index, data_type, gdal_region_data.as_uninit_byte_slice_mut())?,
                gtif_reader.read_raster_band_as(band_index, data_type, gtif_region_data.as_uninit_byte_slice_mut())?,
            )
        };

        // Validate GDAL region reading results
        assert_eq!(gdal_geo.raster_size(), gtif_geo.raster_size());
        assert_eq!(gdal_geo.geo_transform(), gtif_geo.geo_transform());

        let gdal = DenseRaster::new_init_nodata(gdal_geo, unsafe { gdal_region_data.assume_init() })?;
        let gtif = DenseRaster::new_init_nodata(gtif_geo, unsafe { gtif_region_data.assume_init() })?;

        let rasters_equal = gdal == gtif;
        if !rasters_equal {
            //gdal.write("/Users/dirk/gdal.tif")?;
            //gtif.write("/Users/dirk/gtif.tif")?;
            assert_eq!(gdal, gtif);
        }

        Ok(())
    }

    #[test]
    #[cfg(all(feature = "cog", feature = "gdal"))]
    fn compare_geotiff_gdal_read() -> Result<()> {
        for (input_file, data_type) in test_input_files() {
            let input = crate::testutils::workspace_test_data_dir().join(input_file);
            let band_index = 1;
            compare_geotiff_vs_gdal_read(&input, band_index, None, data_type)?;
        }

        Ok(())
    }

    #[test]
    #[cfg(all(feature = "cog", feature = "gdal"))]
    fn compare_tiled_geotiff_gdal_read_region_within_extent() -> Result<()> {
        use crate::Point;

        for (input_file, data_type) in test_input_files() {
            let band_index = 1;
            let input = crate::testutils::workspace_test_data_dir().join(input_file);

            let full_georef = gdal::GdalRasterIO::open_read_only(&input)?.georeference(band_index)?;
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
    #[cfg(all(feature = "cog", feature = "gdal"))]
    fn compare_tiled_geotiff_gdal_read_region_outside_extent_top_left() -> Result<()> {
        use crate::Point;

        for (input_file, data_type) in test_input_files() {
            let band_index = 1;
            let input = crate::testutils::workspace_test_data_dir().join(input_file);

            let full_georef = gdal::GdalRasterIO::open_read_only(&input)?.georeference(band_index)?;
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
    #[cfg(all(feature = "cog", feature = "gdal"))]
    fn compare_tiled_geotiff_gdal_read_region_outside_extent_bottom_right() -> Result<()> {
        use crate::Point;

        for (input_file, data_type) in test_input_files() {
            let band_index = 1;
            let input = crate::testutils::workspace_test_data_dir().join(input_file);

            let full_georef = gdal::GdalRasterIO::open_read_only(&input)?.georeference(band_index)?;
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
    #[cfg(all(feature = "cog", feature = "gdal"))]
    fn compare_tiled_geotiff_gdal_read_region_larger_then_raster() -> Result<()> {
        use crate::Point;

        for (input_file, data_type) in test_input_files() {
            let band_index = 1;
            let input = crate::testutils::workspace_test_data_dir().join(input_file);

            let full_georef = gdal::GdalRasterIO::open_read_only(&input)?.georeference(band_index)?;
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
}

use std::path::Path;

use crate::{ArrayDataType, GeoReference, RasterSize, Result, geotiff::GeoTiffReader, raster::reader::RasterReader};

pub struct GeotiffRasterIO {
    reader: GeoTiffReader,
}

impl RasterReader for GeotiffRasterIO {
    fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            reader: GeoTiffReader::from_file(path.as_ref())?,
        })
    }

    fn open_read_only_with_options(path: impl AsRef<Path>, _open_options: &crate::raster::reader::RasterOpenOptions) -> Result<Self> {
        Ok(Self {
            reader: GeoTiffReader::from_file(path.as_ref())?,
        })
    }

    fn band_count(&self) -> Result<usize> {
        //self.reader.band_count()
        Ok(1)
    }

    fn raster_size(&self) -> Result<RasterSize> {
        Ok(self.reader.metadata().geo_reference.raster_size())
    }

    fn georeference(&mut self, band_index: usize) -> Result<GeoReference> {
        assert_eq!(1, band_index, "Geotiff format currently only supports single raster band");
        Ok(self.reader.metadata().geo_reference.clone())
    }

    fn data_type(&self, band_index: usize) -> Result<ArrayDataType> {
        assert_eq!(1, band_index, "Geotiff format currently only supports single raster band");
        Ok(self.reader.metadata().data_type)
    }

    fn overview_count(&self, band_index: usize) -> Result<usize> {
        assert_eq!(1, band_index, "Geotiff format currently only supports single raster band");
        let overview_count = self.reader.metadata().overviews.len();
        Ok(if overview_count > 0 { overview_count - 1 } else { 0 })
    }

    fn read_raster_band(
        &mut self,
        band_index: usize,
        data_type: crate::ArrayDataType,
        dst_data: &mut [std::mem::MaybeUninit<u8>],
    ) -> Result<GeoReference> {
        assert_eq!(1, band_index, "Geotiff format currently only supports single raster band");
        assert_eq!(
            data_type,
            self.reader.metadata().data_type,
            "Geotiff format currently does not support on-the-fly data type conversion"
        );
        self.reader.read_raster(dst_data)
    }

    fn read_raster_band_region(
        &mut self,
        band_index: usize,
        extent: &crate::GeoReference,
        data_type: crate::ArrayDataType,
        dst_data: &mut [std::mem::MaybeUninit<u8>],
    ) -> Result<GeoReference> {
        //self.reader.read_raster_band_region(band_index, extent, data_type, dst_data)
        todo!()
    }
}

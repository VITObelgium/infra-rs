use simd_macro::simd_bounds;
use std::path::Path;

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

use crate::{
    ArrayDataType, ArrayNum, GeoReference, RasterSize, Result,
    raster::reader::{RasterReader, RasterReaderDyn},
};

use crate::geotiff::GeoTiffReader;

pub struct GeotiffRasterIO {
    reader: GeoTiffReader,
}

impl RasterReaderDyn for GeotiffRasterIO {
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

    fn read_raster_band_u8(&mut self, band_index: usize, dst_data: &mut [std::mem::MaybeUninit<u8>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Uint8, dst_data)
    }

    fn read_raster_band_u16(&mut self, band_index: usize, dst_data: &mut [std::mem::MaybeUninit<u16>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Uint16, dst_data)
    }

    fn read_raster_band_u32(&mut self, band_index: usize, dst_data: &mut [std::mem::MaybeUninit<u32>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Uint32, dst_data)
    }

    fn read_raster_band_u64(&mut self, band_index: usize, dst_data: &mut [std::mem::MaybeUninit<u64>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Uint64, dst_data)
    }

    fn read_raster_band_i8(&mut self, band_index: usize, dst_data: &mut [std::mem::MaybeUninit<i8>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Int8, dst_data)
    }

    fn read_raster_band_i16(&mut self, band_index: usize, dst_data: &mut [std::mem::MaybeUninit<i16>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Int16, dst_data)
    }

    fn read_raster_band_i32(&mut self, band_index: usize, dst_data: &mut [std::mem::MaybeUninit<i32>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Int32, dst_data)
    }

    fn read_raster_band_i64(&mut self, band_index: usize, dst_data: &mut [std::mem::MaybeUninit<i64>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Int64, dst_data)
    }

    fn read_raster_band_f32(&mut self, band_index: usize, dst_data: &mut [std::mem::MaybeUninit<f32>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Float32, dst_data)
    }

    fn read_raster_band_f64(&mut self, band_index: usize, dst_data: &mut [std::mem::MaybeUninit<f64>]) -> Result<GeoReference> {
        self.read_raster_band(band_index, ArrayDataType::Float64, dst_data)
    }

    fn read_raster_band_region(
        &mut self,
        band_index: usize,
        extent: &crate::GeoReference,
        data_type: crate::ArrayDataType,
        dst_data: &mut [std::mem::MaybeUninit<u8>],
    ) -> Result<GeoReference> {
        self.reader.read_band_region_into_buffer(band_index, extent, dst_data)
    }

    fn read_raster_band_region_u8(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [std::mem::MaybeUninit<u8>],
    ) -> Result<GeoReference> {
        self.reader.read_band_region_into_buffer(band_index, region, dst_data)
    }

    fn read_raster_band_region_u16(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [std::mem::MaybeUninit<u16>],
    ) -> Result<GeoReference> {
        self.reader.read_band_region_into_buffer(band_index, region, dst_data)
    }

    fn read_raster_band_region_u32(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [std::mem::MaybeUninit<u32>],
    ) -> Result<GeoReference> {
        self.reader.read_band_region_into_buffer(band_index, region, dst_data)
    }

    fn read_raster_band_region_u64(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [std::mem::MaybeUninit<u64>],
    ) -> Result<GeoReference> {
        self.reader.read_band_region_into_buffer(band_index, region, dst_data)
    }

    fn read_raster_band_region_i8(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [std::mem::MaybeUninit<i8>],
    ) -> Result<GeoReference> {
        self.reader.read_band_region_into_buffer(band_index, region, dst_data)
    }

    fn read_raster_band_region_i16(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [std::mem::MaybeUninit<i16>],
    ) -> Result<GeoReference> {
        self.reader.read_band_region_into_buffer(band_index, region, dst_data)
    }

    fn read_raster_band_region_i32(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [std::mem::MaybeUninit<i32>],
    ) -> Result<GeoReference> {
        self.reader.read_band_region_into_buffer(band_index, region, dst_data)
    }

    fn read_raster_band_region_i64(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [std::mem::MaybeUninit<i64>],
    ) -> Result<GeoReference> {
        self.reader.read_band_region_into_buffer(band_index, region, dst_data)
    }

    fn read_raster_band_region_f32(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [std::mem::MaybeUninit<f32>],
    ) -> Result<GeoReference> {
        self.reader.read_band_region_into_buffer(band_index, region, dst_data)
    }

    fn read_raster_band_region_f64(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        dst_data: &mut [std::mem::MaybeUninit<f64>],
    ) -> Result<GeoReference> {
        self.reader.read_band_region_into_buffer(band_index, region, dst_data)
    }
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

    #[simd_bounds]
    fn read_raster_band<T: ArrayNum>(
        &mut self,
        band_index: usize,
        data_type: crate::ArrayDataType,
        dst_data: &mut [std::mem::MaybeUninit<T>],
    ) -> Result<GeoReference> {
        assert_eq!(1, band_index, "Geotiff format currently only supports single raster band");
        assert_eq!(
            data_type,
            self.reader.metadata().data_type,
            "Geotiff format currently does not support on-the-fly data type conversion"
        );

        self.reader.read_raster_into_buffer::<T, GeoReference>(dst_data)
    }
}

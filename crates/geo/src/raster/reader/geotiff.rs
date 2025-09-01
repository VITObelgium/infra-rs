use simd_macro::simd_bounds;
use std::{mem::MaybeUninit, path::Path};

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

use crate::{
    ArrayDataType, ArrayNum, GeoReference, RasterSize, Result,
    raster::{
        reader::{RasterReader, RasterReaderDyn},
        utils::reinterpret_uninit_byte_slice,
    },
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

    fn read_raster_band(
        &mut self,
        band: usize,
        data_type: crate::ArrayDataType,
        data: &mut [std::mem::MaybeUninit<u8>],
    ) -> Result<GeoReference> {
        match data_type {
            ArrayDataType::Uint8 => self.read_raster_band_as::<u8>(band, data_type, reinterpret_uninit_byte_slice(data)),
            ArrayDataType::Uint16 => self.read_raster_band_as::<u16>(band, data_type, reinterpret_uninit_byte_slice(data)),
            ArrayDataType::Uint32 => self.read_raster_band_as::<u32>(band, data_type, reinterpret_uninit_byte_slice(data)),
            ArrayDataType::Uint64 => self.read_raster_band_as::<u64>(band, data_type, reinterpret_uninit_byte_slice(data)),
            ArrayDataType::Int8 => self.read_raster_band_as::<i8>(band, data_type, reinterpret_uninit_byte_slice(data)),
            ArrayDataType::Int16 => self.read_raster_band_as::<i16>(band, data_type, reinterpret_uninit_byte_slice(data)),
            ArrayDataType::Int32 => self.read_raster_band_as::<i32>(band, data_type, reinterpret_uninit_byte_slice(data)),
            ArrayDataType::Int64 => self.read_raster_band_as::<i64>(band, data_type, reinterpret_uninit_byte_slice(data)),
            ArrayDataType::Float32 => self.read_raster_band_as::<f32>(band, data_type, reinterpret_uninit_byte_slice(data)),
            ArrayDataType::Float64 => self.read_raster_band_as::<f64>(band, data_type, reinterpret_uninit_byte_slice(data)),
        }
    }

    fn read_raster_band_region(
        &mut self,
        band_index: usize,
        extent: &crate::GeoReference,
        data_type: crate::ArrayDataType,
        dst_data: &mut [std::mem::MaybeUninit<u8>],
    ) -> Result<GeoReference> {
        match data_type {
            ArrayDataType::Uint8 => {
                self.reader
                    .read_band_region_into_buffer::<u8, _>(band_index, extent, reinterpret_uninit_byte_slice(dst_data))
            }
            ArrayDataType::Uint16 => {
                self.reader
                    .read_band_region_into_buffer::<u16, _>(band_index, extent, reinterpret_uninit_byte_slice(dst_data))
            }
            ArrayDataType::Uint32 => {
                self.reader
                    .read_band_region_into_buffer::<u32, _>(band_index, extent, reinterpret_uninit_byte_slice(dst_data))
            }
            ArrayDataType::Uint64 => {
                self.reader
                    .read_band_region_into_buffer::<u64, _>(band_index, extent, reinterpret_uninit_byte_slice(dst_data))
            }
            ArrayDataType::Int8 => {
                self.reader
                    .read_band_region_into_buffer::<i8, _>(band_index, extent, reinterpret_uninit_byte_slice(dst_data))
            }
            ArrayDataType::Int16 => {
                self.reader
                    .read_band_region_into_buffer::<i16, _>(band_index, extent, reinterpret_uninit_byte_slice(dst_data))
            }
            ArrayDataType::Int32 => {
                self.reader
                    .read_band_region_into_buffer::<i32, _>(band_index, extent, reinterpret_uninit_byte_slice(dst_data))
            }
            ArrayDataType::Int64 => {
                self.reader
                    .read_band_region_into_buffer::<i64, _>(band_index, extent, reinterpret_uninit_byte_slice(dst_data))
            }
            ArrayDataType::Float32 => {
                self.reader
                    .read_band_region_into_buffer::<f32, _>(band_index, extent, reinterpret_uninit_byte_slice(dst_data))
            }
            ArrayDataType::Float64 => {
                self.reader
                    .read_band_region_into_buffer::<f64, _>(band_index, extent, reinterpret_uninit_byte_slice(dst_data))
            }
        }
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
    fn read_raster_band_as<T: ArrayNum>(
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

    #[simd_bounds]
    fn read_raster_band_region_as<T: ArrayNum>(
        &mut self,
        band_index: usize,
        region: &GeoReference,
        data_type: ArrayDataType,
        dst_data: &mut [MaybeUninit<T>],
    ) -> Result<GeoReference> {
        assert_eq!(1, band_index, "Geotiff format currently only supports single raster band");
        assert_eq!(
            data_type,
            self.reader.metadata().data_type,
            "Geotiff format currently does not support on-the-fly data type conversion"
        );

        self.reader.read_band_region_into_buffer(band_index, region, dst_data)
    }
}

use simd_macro::simd_bounds;
use std::{mem::MaybeUninit, path::Path};

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

use crate::{
    ArrayDataType, ArrayNum, GeoReference, RasterSize, Result,
    raster::{
        formats::{RasterFormat, RasterFormatDyn},
        utils::cast_uninit_byte_slice_mut,
    },
};

use crate::geotiff::GeoTiffReader;

pub struct GeotiffRasterIO {
    reader: GeoTiffReader,
}

impl RasterFormatDyn for GeotiffRasterIO {
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

    fn read_band_into_byte_buffer(
        &mut self,
        band: usize,
        data_type: crate::ArrayDataType,
        data: &mut [std::mem::MaybeUninit<u8>],
    ) -> Result<GeoReference> {
        match data_type {
            ArrayDataType::Uint8 => self.read_raster_band_as::<u8>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Uint16 => self.read_raster_band_as::<u16>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Uint32 => self.read_raster_band_as::<u32>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Uint64 => self.read_raster_band_as::<u64>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Int8 => self.read_raster_band_as::<i8>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Int16 => self.read_raster_band_as::<i16>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Int32 => self.read_raster_band_as::<i32>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Int64 => self.read_raster_band_as::<i64>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Float32 => self.read_raster_band_as::<f32>(band, data_type, cast_uninit_byte_slice_mut(data)),
            ArrayDataType::Float64 => self.read_raster_band_as::<f64>(band, data_type, cast_uninit_byte_slice_mut(data)),
        }
    }

    fn read_band_region_into_byte_buffer(
        &mut self,
        band: usize,
        extent: &crate::GeoReference,
        data_type: crate::ArrayDataType,
        dst_data: &mut [std::mem::MaybeUninit<u8>],
    ) -> Result<GeoReference> {
        match data_type {
            ArrayDataType::Uint8 => self.read_raster_band_region_as::<u8>(band, extent, data_type, cast_uninit_byte_slice_mut(dst_data)),
            ArrayDataType::Uint16 => self.read_raster_band_region_as::<u16>(band, extent, data_type, cast_uninit_byte_slice_mut(dst_data)),
            ArrayDataType::Uint32 => self.read_raster_band_region_as::<u32>(band, extent, data_type, cast_uninit_byte_slice_mut(dst_data)),
            ArrayDataType::Uint64 => self.read_raster_band_region_as::<u64>(band, extent, data_type, cast_uninit_byte_slice_mut(dst_data)),
            ArrayDataType::Int8 => self.read_raster_band_region_as::<i8>(band, extent, data_type, cast_uninit_byte_slice_mut(dst_data)),
            ArrayDataType::Int16 => self.read_raster_band_region_as::<i16>(band, extent, data_type, cast_uninit_byte_slice_mut(dst_data)),
            ArrayDataType::Int32 => self.read_raster_band_region_as::<i32>(band, extent, data_type, cast_uninit_byte_slice_mut(dst_data)),
            ArrayDataType::Int64 => self.read_raster_band_region_as::<i64>(band, extent, data_type, cast_uninit_byte_slice_mut(dst_data)),
            ArrayDataType::Float32 => self.read_raster_band_region_as::<f32>(band, extent, data_type, cast_uninit_byte_slice_mut(dst_data)),
            ArrayDataType::Float64 => self.read_raster_band_region_as::<f64>(band, extent, data_type, cast_uninit_byte_slice_mut(dst_data)),
        }
    }
}

impl RasterFormat for GeotiffRasterIO {
    fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            reader: GeoTiffReader::from_file(path.as_ref())?,
        })
    }

    fn open_read_only_with_options(path: impl AsRef<Path>, _open_options: &crate::raster::formats::RasterOpenOptions) -> Result<Self> {
        Ok(Self {
            reader: GeoTiffReader::from_file(path.as_ref())?,
        })
    }
}

impl GeotiffRasterIO {
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
        debug_assert_eq!(
            data_type,
            self.reader.metadata().data_type,
            "Geotiff format currently does not support on-the-fly data type conversion"
        );

        self.reader.read_band_region_into_buffer(band_index, region, dst_data)
    }
}

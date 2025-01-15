use gdal::{raster::GdalType, vector::LayerAccess};
use raster::Nodata;

use crate::{gdalinterop, georaster, vector, Error, GeoReference, Result};

fn polygonize_dataset(ds: &gdal::Dataset) -> Result<gdal::Dataset> {
    let mut mem_ds = vector::io::dataset::create_in_memory()?;
    if ds.raster_count() == 0 {
        return Err(Error::InvalidArgument(
            "Polygonize should be called on a raster dataset".to_string(),
        ));
    }

    let srs = ds.spatial_ref()?;
    let layer_options = gdal::vector::LayerOptions {
        name: "Polygons",
        srs: Some(&srs),
        ..Default::default()
    };

    let layer = mem_ds.create_layer(layer_options)?;
    layer.create_defn_fields(&[("Value", gdal::vector::OGRFieldType::OFTInteger)])?;

    let raster_band = ds.rasterband(1)?;

    gdalinterop::check_rc(unsafe {
        match raster_band.band_type() {
            gdal::raster::GdalDataType::Float32 | gdal::raster::GdalDataType::Float64 => gdal_sys::GDALFPolygonize(
                raster_band.c_rasterband(),
                std::ptr::null_mut(),
                layer.c_layer(),
                0,
                std::ptr::null_mut(),
                None,
                std::ptr::null_mut(),
            ),
            _ => gdal_sys::GDALPolygonize(
                raster_band.c_rasterband(),
                std::ptr::null_mut(),
                layer.c_layer(),
                0,
                std::ptr::null_mut(),
                None,
                std::ptr::null_mut(),
            ),
        }
    })?;

    Ok(mem_ds)
}

pub fn polygonize<T: GdalType + Nodata<T>>(meta: &GeoReference, data: &[T]) -> Result<gdal::Dataset> {
    let ds = georaster::io::dataset::create_in_memory_with_data(meta, data)?;
    polygonize_dataset(&ds)
}

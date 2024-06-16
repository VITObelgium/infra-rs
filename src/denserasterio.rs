use gdal::raster::GdalType;

use crate::{
    raster::RasterNum,
    rasterio::{self},
    DenseRaster, Error, GeoMetadata, Raster,
};

/// Reads the full raster from disk into a DenseRaster
/// No processing (cutting, resampling) is done on the raster data, the original data is returned
pub fn read_dense_raster<T: RasterNum<T> + GdalType>(path: &std::path::Path) -> Result<DenseRaster<T>, Error> {
    use gdal::Dataset;
    let band_index = 1;
    let ds = Dataset::open(path)?;

    let metadata = rasterio::metadata_from_dataset_band(&ds, band_index)?;
    let rasterband = ds.rasterband(band_index)?;

    let mut data: Vec<T> = vec![T::zero(); metadata.rows() * metadata.columns()];
    rasterband.read_into_slice::<T>(
        (0, 0),
        rasterband.size(),
        (metadata.columns(), metadata.rows()),
        data.as_mut_slice(),
        None,
    )?;

    Ok(DenseRaster::new(metadata, data))
}

/// Reads a subset of the raster from disk into a DenseRaster
/// The provided extent does not have to be contained within the raster
/// Areas outside of the original raster will be filled with the nodata value
pub fn read_dense_raster_with_extent<T: RasterNum<T> + GdalType>(
    path: &std::path::Path,
    extent: &GeoMetadata,
) -> Result<DenseRaster<T>, Error> {
    use gdal::Dataset;
    let band_index = 1;
    let ds = Dataset::open(path)?;

    let src_meta = rasterio::metadata_from_dataset_band(&ds, band_index)?;
    let mut data: Vec<T> = vec![T::zero(); src_meta.rows() * src_meta.columns()];
    let dst_meta = rasterio::data_from_dataset_with_extent(&ds, extent, band_index, &mut data)?;

    Ok(DenseRaster::new(dst_meta, data))
}

#[cfg(test)]
mod tests {
    use crate::Point;

    use super::*;

    #[test]
    fn test_read_dense_raster_as_float() {
        let path: std::path::PathBuf = [env!("CARGO_MANIFEST_DIR"), "test", "data", "landusebyte.tif"]
            .iter()
            .collect();

        let ras = read_dense_raster::<f32>(path.as_path()).unwrap();
        let meta = ras.geo_metadata();

        assert_eq!(ras.width(), 2370);
        assert_eq!(ras.height(), 920);
        assert_eq!(ras.as_slice().len(), 2370 * 920);
        assert_eq!(ras.nodata_value(), Some(255.0));
        assert_eq!(ras.sum(), 163654749.0);
        assert_eq!(ras.nodata_count(), 805630);

        assert_eq!(meta.cell_size_x(), 100.0);
        assert_eq!(meta.cell_size_y(), -100.0);
        assert_eq!(meta.bottom_left(), Point::new(22000.0, 153000.0));
    }
}

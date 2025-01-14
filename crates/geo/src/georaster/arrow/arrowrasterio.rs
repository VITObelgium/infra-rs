use arrow::datatypes::ArrowPrimitiveType;
use gdal::raster::GdalType;

use crate::{
    georaster::{io, ArrowRaster, ArrowRasterNum, GeoRaster, GeoRasterCreation, Raster, RasterIO},
    GeoReference, Result,
};

impl<T: ArrowRasterNum<T> + GdalType> RasterIO<T, ArrowRaster<T>> for ArrowRaster<T>
where
    T::TArrow: ArrowPrimitiveType<Native = T>,
{
    fn read(path: &std::path::Path) -> Result<Self> {
        ArrowRaster::<T>::read_band(path, 1)
    }

    fn read_band(path: &std::path::Path, band_index: usize) -> Result<ArrowRaster<T>> {
        let ds = io::dataset::open_read_only(path)?;

        let metadata = io::dataset::read_band_metadata(&ds, band_index)?;
        let rasterband = ds.rasterband(band_index)?;

        let mut data: Vec<T> = vec![T::zero(); metadata.rows() * metadata.columns()];
        rasterband.read_into_slice::<T>(
            (0, 0),
            rasterband.size(),
            (metadata.columns(), metadata.rows()),
            data.as_mut_slice(),
            None,
        )?;

        Ok(ArrowRaster::new(metadata, data))
    }

    /// Reads a subset of the raster from disk into a `DenseRaster`
    /// The provided extent does not have to be contained within the raster
    /// Areas outside of the original raster will be filled with the nodata value
    fn read_bounds(path: &std::path::Path, bounds: &GeoReference, band_index: usize) -> Result<ArrowRaster<T>>
    where
        T::TArrow: ArrowPrimitiveType<Native = T>,
    {
        let ds = gdal::Dataset::open(path)?;
        let src_meta = io::dataset::read_band_metadata(&ds, band_index)?;
        let mut data: Vec<T> = vec![T::zero(); src_meta.rows() * src_meta.columns()];
        let dst_meta = io::dataset::read_band_region(&ds, band_index, bounds, &mut data)?;

        Ok(ArrowRaster::new(dst_meta, data))
    }

    fn write(&mut self, path: &std::path::Path) -> Result
    where
        T::TArrow: ArrowPrimitiveType<Native = T>,
    {
        self.flatten_nodata();
        io::dataset::write(self.as_slice(), self.geo_reference(), path, &[])
    }
}

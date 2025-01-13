use crate::GeoReference;
use crate::Result;
use gdal::raster::GdalType;

use super::GeoRaster;
use super::GeoRasterCreation;
use super::{io, DenseGeoRaster, Raster, RasterIO, RasterNum};

impl<T: RasterNum<T> + GdalType> RasterIO<T, DenseGeoRaster<T>> for DenseGeoRaster<T>
where
    Self: Raster<T>,
{
    fn read(path: &std::path::Path) -> Result<Self> {
        DenseGeoRaster::<T>::read_band(path, 1)
    }

    fn read_band(path: &std::path::Path, band_index: usize) -> Result<DenseGeoRaster<T>> {
        let ds = io::dataset::open_read_only(path)?;
        let (cols, rows) = ds.raster_size();

        let mut data: Vec<T> = vec![T::zero(); cols * rows];
        let metadata = io::dataset::read_band(&ds, band_index, data.as_mut_slice())?;
        Ok(DenseGeoRaster::new(metadata, data))
    }

    /// Reads a subset of the raster from disk into a `DenseRaster`
    /// The provided extent does not have to be contained within the raster
    /// Areas outside of the original raster will be filled with the nodata value
    fn read_bounds(path: &std::path::Path, bounds: &GeoReference, band_index: usize) -> Result<DenseGeoRaster<T>> {
        let ds = gdal::Dataset::open(path)?;
        let (cols, rows) = ds.raster_size();
        let mut data: Vec<T> = vec![T::zero(); rows * cols];
        let dst_meta = io::dataset::read_band_region(&ds, band_index, bounds, &mut data)?;
        Ok(DenseGeoRaster::new(dst_meta, data))
    }

    fn write(&mut self, path: &std::path::Path) -> Result {
        self.flatten_nodata();
        io::dataset::write(self.as_slice(), self.geo_metadata(), path, &[])
    }
}

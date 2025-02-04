use crate::Array;
use crate::GeoReference;
use crate::RasterNum;
use crate::Result;
use gdal::raster::GdalType;

use super::denseraster::process_nodata;
use super::{io, DenseRaster, RasterIO};

impl<T: RasterNum<T> + GdalType> RasterIO for DenseRaster<T>
where
    Self: Array<Pixel = T, Metadata = GeoReference>,
{
    fn read(path: &std::path::Path) -> Result<Self> {
        DenseRaster::<T>::read_band(path, 1)
    }

    fn read_band(path: &std::path::Path, band_index: usize) -> Result<DenseRaster<T>> {
        let ds = io::dataset::open_read_only(path)?;
        let (cols, rows) = ds.raster_size();

        let mut data: Vec<T> = vec![T::zero(); cols * rows];
        let metadata = io::dataset::read_band(&ds, band_index, data.as_mut_slice())?;
        process_nodata(&mut data, metadata.nodata());

        Ok(DenseRaster::new(metadata, data))
    }

    /// Reads a subset of the raster from disk into a `DenseRaster`
    /// The provided extent does not have to be contained within the raster
    /// Areas outside of the original raster will be filled with the nodata value
    fn read_bounds(path: &std::path::Path, bounds: &GeoReference, band_index: usize) -> Result<DenseRaster<T>> {
        let ds = gdal::Dataset::open(path)?;
        let (cols, rows) = ds.raster_size();
        let mut data: Vec<T> = vec![T::zero(); rows * cols];
        let dst_meta = io::dataset::read_band_region(&ds, band_index, bounds, &mut data)?;
        process_nodata(&mut data, dst_meta.nodata());

        Ok(DenseRaster::new(dst_meta, data))
    }

    fn write(&mut self, path: &std::path::Path) -> Result {
        self.flatten_nodata()?;
        io::dataset::write(self.as_slice(), self.metadata(), path, &[])
    }
}

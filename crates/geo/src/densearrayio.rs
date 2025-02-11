use crate::densearrayutil;
use crate::raster;
use crate::raster::RasterIO;
use crate::Array;
use crate::ArrayMetadata;
use crate::ArrayNum;
use crate::DenseArray;
use crate::GeoReference;
use crate::Result;
use gdal::raster::GdalType;

impl<T: ArrayNum<T> + GdalType, Metadata: ArrayMetadata> RasterIO for DenseArray<T, Metadata> {
    fn read(path: &std::path::Path) -> Result<Self> {
        Self::read_band(path, 1)
    }

    fn read_band(path: &std::path::Path, band_index: usize) -> Result<Self> {
        let ds = raster::io::dataset::open_read_only(path)?;
        let (cols, rows) = ds.raster_size();

        let mut data: Vec<T> = vec![T::zero(); cols * rows];
        let metadata = raster::io::dataset::read_band(&ds, band_index, data.as_mut_slice())?;
        densearrayutil::process_nodata(&mut data, metadata.nodata());

        Ok(Self::new(Metadata::with_geo_reference(metadata), data))
    }

    /// Reads a subset of the raster from disk into a `DenseRaster`
    /// The provided extent does not have to be contained within the raster
    /// Areas outside of the original raster will be filled with the nodata value
    fn read_bounds(path: &std::path::Path, bounds: &GeoReference, band_index: usize) -> Result<Self> {
        let ds = gdal::Dataset::open(path)?;
        let (cols, rows) = ds.raster_size();
        let mut data: Vec<T> = vec![T::zero(); rows * cols];
        let dst_meta = raster::io::dataset::read_band_region(&ds, band_index, bounds, &mut data)?;
        densearrayutil::process_nodata(&mut data, dst_meta.nodata());

        Ok(Self::new(Metadata::with_geo_reference(dst_meta), data))
    }

    fn write(&mut self, path: &std::path::Path) -> Result {
        let georef = self.metadata().geo_reference();
        densearrayutil::flatten_nodata(&mut self.data, georef.nodata())?;
        raster::io::dataset::write(self.as_slice(), &georef, path, &[])
    }
}

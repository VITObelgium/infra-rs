use crate::Array;
use crate::ArrayMetadata;
use crate::ArrayNum;
use crate::DenseArray;
use crate::GeoReference;
use crate::Result;
use crate::array::ArrayInterop as _;
use crate::raster;
use crate::raster::RasterIO;
use gdal::raster::GdalType;
use inf::allocate;

impl<T: ArrayNum + GdalType, Metadata: ArrayMetadata> RasterIO for DenseArray<T, Metadata> {
    fn read(path: &std::path::Path) -> Result<Self> {
        Self::read_band(path, 1)
    }

    fn read_band(path: &std::path::Path, band_index: usize) -> Result<Self> {
        let ds = raster::io::dataset::open_read_only(path)?;
        let (cols, rows) = ds.raster_size();

        // read_band will take care of setting the data len
        let mut data: Vec<T> = allocate::aligned_vec_with_capacity(rows * cols);
        let metadata = raster::io::dataset::read_band(&ds, band_index, &mut data)?;
        Self::new_init_nodata(Metadata::with_geo_reference(metadata), data)
    }

    /// Reads a subset of the raster from disk into a `DenseRaster`
    /// The provided extent does not have to be contained within the raster
    /// Areas outside of the original raster will be filled with the nodata value
    fn read_bounds(path: &std::path::Path, bounds: &GeoReference, band_index: usize) -> Result<Self> {
        let ds = gdal::Dataset::open(path)?;
        let (cols, rows) = ds.raster_size();
        let mut data: Vec<T> = allocate::aligned_vec_with_capacity(rows * cols);
        let dst_meta = raster::io::dataset::read_band_region(&ds, band_index, bounds, &mut data)?;
        unsafe {
            // Safety: if read_band_region succeeds, it has written all rows * cols elements to `data`
            data.set_len(rows * cols);
        }

        Self::new_init_nodata(Metadata::with_geo_reference(dst_meta), data)
    }

    fn write(&mut self, path: &std::path::Path) -> Result {
        let georef = self.metadata().geo_reference();
        self.restore_nodata(); // Ensure nodata values are restored to the metadata value before writing
        raster::io::dataset::write(self.as_slice(), &georef, path, &[])?;
        self.init_nodata();
        Ok(())
    }
}

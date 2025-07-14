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
use inf::allocate::AlignedVecUnderConstruction;

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

#[simd_macro::simd_bounds]
impl<T: ArrayNum + GdalType, Metadata: ArrayMetadata> RasterIO for DenseArray<T, Metadata> {
    fn read(path: &std::path::Path) -> Result<Self> {
        Self::read_band(path, 1)
    }

    fn read_band(path: &std::path::Path, band_index: usize) -> Result<Self> {
        let ds = raster::io::dataset::open_read_only(path)?;
        let (metadata, data) = raster::io::dataset::read_band(&ds, band_index)?;
        Self::new_init_nodata(Metadata::with_geo_reference(metadata), data)
    }

    /// Reads a subset of the raster from disk into a `DenseRaster`
    /// The provided extent does not have to be contained within the raster
    /// Areas outside of the original raster will be filled with the nodata value
    fn read_bounds(path: &std::path::Path, bounds: &GeoReference, band_index: usize) -> Result<Self> {
        let ds = gdal::Dataset::open(path)?;
        let (cols, rows) = ds.raster_size();
        let mut data = AlignedVecUnderConstruction::new(rows * cols);
        let dst_meta = raster::io::dataset::read_band_region(&ds, band_index, bounds, data.as_uninit_slice_mut())?;
        let data = unsafe { data.assume_init() };

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

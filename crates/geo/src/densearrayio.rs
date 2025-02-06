use crate::raster;
use crate::raster::RasterIO;
use crate::Array;
use crate::ArrayMetadata;
use crate::ArrayNum;
use crate::DenseArray;
use crate::GeoReference;
use crate::Result;
use gdal::raster::GdalType;
use inf::cast;
use num::NumCast;

impl<T: ArrayNum<T> + GdalType, Metadata: ArrayMetadata> RasterIO for DenseArray<T, Metadata> {
    fn read(path: &std::path::Path) -> Result<Self> {
        Self::read_band(path, 1)
    }

    fn read_band(path: &std::path::Path, band_index: usize) -> Result<Self> {
        let ds = raster::io::dataset::open_read_only(path)?;
        let (cols, rows) = ds.raster_size();

        let mut data: Vec<T> = vec![T::zero(); cols * rows];
        let metadata = raster::io::dataset::read_band(&ds, band_index, data.as_mut_slice())?;
        process_nodata(&mut data, metadata.nodata());

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
        process_nodata(&mut data, dst_meta.nodata());

        Ok(Self::new(Metadata::with_geo_reference(dst_meta), data))
    }

    fn write(&mut self, path: &std::path::Path) -> Result {
        let georef = self.metadata().geo_reference();
        flatten_nodata(&mut self.data, georef.nodata())?;
        raster::io::dataset::write(self.as_slice(), &georef, path, &[])
    }
}

/// Process nodata values in the data array
/// This means replacing all the values that match the nodata value with the default nodata value for the type T
/// as defined by the [`crate::Nodata`] trait
fn process_nodata<T: ArrayNum<T>>(data: &mut [T], nodata: Option<f64>) {
    if let Some(nodata) = nodata {
        if nodata.is_nan() || NumCast::from(nodata) == Some(T::nodata_value()) {
            // the nodata value for floats is also nan, so no processing required
            // or the nodata value matches the default nodata value for the type
            return;
        }

        let nodata = NumCast::from(nodata).unwrap_or(T::nodata_value());
        for v in data.iter_mut() {
            if *v == nodata {
                *v = T::nodata_value();
            }
        }
    }
}

fn flatten_nodata<T: ArrayNum<T>>(data: &mut [T], nodata: Option<f64>) -> Result<()> {
    let nodata_value = cast::option::<T>(nodata);

    if let Some(nodata) = nodata_value {
        for x in data.iter_mut() {
            if x.is_nodata() {
                *x = nodata;
            }
        }
    }

    Ok(())
}

use std::path::Path;

use crate::array::ArrayInterop as _;
use crate::raster::{self, Compression, Predictor, RasterReadWrite, TiffChunkType, WriteRasterOptions};
use crate::{Array, ArrayMetadata, ArrayNum, DenseArray, GeoReference, Result};
use gdal::raster::GdalType;

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

fn write_raster_options_to_gdal(options: WriteRasterOptions) -> Vec<String> {
    match options {
        WriteRasterOptions::Default => Vec::default(),
        WriteRasterOptions::GeoTiff(tiff_opts) => {
            let mut opts = Vec::default();
            opts.push(format!(
                "TILED={}",
                match tiff_opts.chunk_type {
                    TiffChunkType::Tiled => "YES",
                    TiffChunkType::Striped => "NO",
                }
            ));

            opts.push(format!(
                "COMPRESS={}",
                match tiff_opts.compression {
                    Some(Compression::Lzw) => "LZW",
                    Some(Compression::Zstd) => "ZSTD",
                    None => "NONE",
                }
            ));

            opts.push(format!(
                "PREDICTOR={}",
                match tiff_opts.predictor {
                    None => "1",
                    Some(Predictor::Horizontal) => "2",
                    Some(Predictor::FloatingPoint) => "3",
                }
            ));

            opts.push(format!(
                "SPARSE_OK={}",
                match tiff_opts.sparse_ok {
                    true => "TRUE",
                    false => "FALSE",
                }
            ));

            opts
        }
    }
}

#[simd_macro::simd_bounds]
impl<T: ArrayNum + GdalType, Metadata: ArrayMetadata> RasterReadWrite for DenseArray<T, Metadata> {
    fn read(path: impl AsRef<Path>) -> Result<Self> {
        Self::read_band(path, 1)
    }

    fn read_band(path: impl AsRef<Path>, band_index: usize) -> Result<Self> {
        let (metadata, data) = raster::io::read_raster_band(path, band_index)?;
        Self::new_init_nodata(Metadata::with_geo_reference(metadata), data)
    }

    /// Reads a subset of the raster from disk into a `DenseRaster`
    /// The provided extent does not have to be contained within the raster
    /// Areas outside of the original raster will be filled with the nodata value
    fn read_bounds(path: impl AsRef<Path>, bounds: &GeoReference, band_index: usize) -> Result<Self> {
        let (dst_meta, raster_data) = raster::io::read_raster_band_region(path, band_index, bounds)?;
        Self::new_init_nodata(Metadata::with_geo_reference(dst_meta), raster_data)
    }

    fn write(&mut self, path: impl AsRef<Path>) -> Result {
        let georef = self.metadata().geo_reference();
        self.restore_nodata(); // Ensure nodata values are restored to the metadata value before writing
        raster::io::dataset::write(self.as_slice(), &georef, path, &[])?;
        self.init_nodata();
        Ok(())
    }

    fn into_write(mut self, path: impl AsRef<Path>) -> Result {
        self.write(path)
    }

    fn write_with_options(&mut self, path: impl AsRef<Path>, options: WriteRasterOptions) -> Result {
        let georef = self.metadata().geo_reference();
        self.restore_nodata(); // Ensure nodata values are restored to the metadata value before writing
        raster::io::dataset::write(self.as_slice(), &georef, path, &write_raster_options_to_gdal(options))?;
        self.init_nodata();
        Ok(())
    }
}

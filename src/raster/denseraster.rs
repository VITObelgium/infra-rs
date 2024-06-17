use num::NumCast;

use crate::{raster::RasterNum, GeoMetadata, Raster};

pub struct DenseRaster<T: RasterNum<T>> {
    pub(super) metadata: GeoMetadata,
    pub(super) data: Vec<T>,
}

impl<T: RasterNum<T>> DenseRaster<T> {
    #[allow(dead_code)] // this function is not used when gdal support is disabled
    pub(super) fn flatten_nodata(&mut self) {
        if self.nodata_value().is_none() {
            return;
        }

        self.data.iter_mut().for_each(|x| {
            if T::is_nodata(*x) {
                *x = T::nodata_value()
            }
        });
    }
}

impl<T: RasterNum<T>> Raster<T> for DenseRaster<T> {
    fn new(metadata: GeoMetadata, mut data: Vec<T>) -> Self {
        process_nodata(&mut data, metadata.nodata());
        DenseRaster { metadata, data }
    }

    fn zeros(meta: GeoMetadata) -> Self {
        DenseRaster::filled_with(T::zero(), meta)
    }

    fn filled_with(val: T, meta: GeoMetadata) -> Self {
        let data_size = meta.rows() * meta.columns();
        DenseRaster::new(meta, vec![val; data_size])
    }

    fn geo_metadata(&self) -> &GeoMetadata {
        &self.metadata
    }

    fn width(&self) -> usize {
        self.metadata.columns()
    }

    fn height(&self) -> usize {
        self.metadata.rows()
    }

    fn as_mut_slice(&mut self) -> &mut [T] {
        self.data.as_mut_slice()
    }

    fn as_slice(&self) -> &[T] {
        self.data.as_slice()
    }

    fn nodata_value(&self) -> Option<T> {
        match self.metadata.nodata() {
            Some(nodata) => NumCast::from(nodata),
            None => None,
        }
    }

    fn nodata_count(&self) -> usize {
        if self.nodata_value().is_none() {
            return 0;
        }

        self.data.iter().filter(|&&x| T::is_nodata(x)).count()
    }

    fn index_has_data(&self, index: usize) -> bool {
        self.data[index] != T::nodata_value()
    }

    fn masked_data(&self) -> Vec<Option<T>> {
        self.data
            .iter()
            .map(|&v| if T::is_nodata(v) { None } else { Some(v) })
            .collect()
    }

    fn sum(&self) -> f64 {
        self.data
            .iter()
            .filter(|&&x| !T::is_nodata(x))
            .fold(0.0, |acc, x| acc + NumCast::from(*x).unwrap_or(0.0))
    }
}

fn process_nodata<T: RasterNum<T>>(data: &mut [T], nodata: Option<f64>) {
    if let Some(nodata) = nodata {
        if nodata.is_nan() || NumCast::from(nodata) == Some(T::nodata_value()) {
            // the nodata value for floats is also nan, so no processing required
            // or the nodata value matches the default nodata value for the type
            return;
        }

        let nodata = NumCast::from(nodata).unwrap_or(T::nodata_value());
        data.iter_mut().for_each(|v| {
            if *v == nodata {
                *v = T::nodata_value();
            }
        });
    }
}

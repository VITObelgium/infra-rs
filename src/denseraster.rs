use num::NumCast;

use crate::{
    raster::{self, RasterNum},
    GeoMetadata, Raster,
};

pub struct DenseRaster<T: RasterNum<T>> {
    metadata: GeoMetadata,
    data: Vec<T>,
}

impl<T: RasterNum<T>> DenseRaster<T> {
    pub fn flatten_nodata(&mut self) {
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

impl<T: RasterNum<T> + std::ops::Add<Output = T>> std::ops::Add for DenseRaster<T> {
    type Output = DenseRaster<T>;

    fn add(self, other: DenseRaster<T>) -> DenseRaster<T> {
        raster::assert_dimensions(&self, &other);

        // Create a new DenseRaster with the same metadata
        let metadata = self.metadata.clone();
        let mut data = Vec::with_capacity(self.data.len());

        // Perform element-wise addition
        for (x, y) in self.data.into_iter().zip(other.data.into_iter()) {
            if T::is_nodata(x) || T::is_nodata(y) {
                data.push(T::nodata_value());
            } else {
                data.push(x + y);
            }
        }

        DenseRaster { metadata, data }
    }
}

impl<T: RasterNum<T> + std::ops::Mul<Output = T>> std::ops::Mul<T> for DenseRaster<T> {
    type Output = DenseRaster<T>;

    fn mul(mut self, scalar: T) -> DenseRaster<T> {
        self.data
            .iter_mut()
            .filter(|&&mut x| !T::is_nodata(x))
            .for_each(|raster_val| *raster_val = *raster_val * NumCast::from(scalar).unwrap_or(T::zero()));

        self
    }
}

impl<T: RasterNum<T> + std::ops::Mul<Output = T>> std::ops::Mul<T> for &DenseRaster<T> {
    type Output = DenseRaster<T>;

    fn mul(self, scalar: T) -> DenseRaster<T> {
        let mut data = Vec::with_capacity(self.data.len());

        for x in self.data.iter() {
            if T::is_nodata(*x) {
                data.push(T::nodata_value());
            } else {
                data.push(*x * scalar);
            }
        }

        DenseRaster {
            metadata: self.metadata.clone(),
            data,
        }
    }
}

impl<T: RasterNum<T> + std::ops::Add<Output = T>> std::ops::Add for &DenseRaster<T> {
    type Output = DenseRaster<T>;

    fn add(self, other: &DenseRaster<T>) -> DenseRaster<T> {
        raster::assert_dimensions(self, other);

        // Create a new DenseRaster with the same metadata
        let metadata = self.metadata.clone();
        let mut data = Vec::with_capacity(self.data.len());

        // Perform element-wise addition
        for (x, y) in self.data.iter().zip(other.data.iter()) {
            if T::is_nodata(*x) || T::is_nodata(*y) {
                data.push(T::nodata_value());
            } else {
                data.push(*x + *y);
            }
        }

        DenseRaster { metadata, data }
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

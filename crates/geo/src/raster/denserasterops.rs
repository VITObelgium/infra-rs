use num::NumCast;

use crate::raster;

use super::{DenseRaster, Raster, RasterNum};

impl<T> std::ops::Add for DenseRaster<T>
where
    T: RasterNum<T> + std::ops::Add<Output = T> + std::ops::AddAssign,
{
    type Output = DenseRaster<T>;

    fn add(mut self, other: DenseRaster<T>) -> DenseRaster<T> {
        raster::assert_dimensions(&self, &other);

        for (x, &y) in self.as_mut_slice().iter_mut().zip(other.as_slice().iter()) {
            if T::is_nodata(*x) || T::is_nodata(y) {
                *x = T::nodata_value();
            } else {
                *x += y;
            }
        }

        self
    }
}

impl<T: RasterNum<T> + std::ops::Mul<Output = T>> std::ops::Mul<T> for DenseRaster<T> {
    type Output = DenseRaster<T>;

    fn mul(mut self, scalar: T) -> DenseRaster<T> {
        self.as_mut_slice()
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

        for x in self.as_slice() {
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

use num::NumCast;

use crate::{
    raster::{self, RasterNum},
    GeoMetadata, Raster,
};

fn result_nodata<T: Clone>(lhs: Option<T>, rhs: Option<T>) -> Option<T> {
    lhs.or_else(|| rhs.clone())
}

pub struct DenseRaster<T: RasterNum<T>> {
    metadata: GeoMetadata,
    data: Vec<T>,
}

impl<T: RasterNum<T> + std::ops::Add<Output = T>> std::ops::Add for DenseRaster<T> {
    type Output = DenseRaster<T>;

    fn add(self, other: DenseRaster<T>) -> DenseRaster<T> {
        raster::assert_dimensions(&self, &other);

        // Create a new DenseRaster with the same metadata
        let metadata = self.metadata.clone();
        let mut data = Vec::with_capacity(self.data.len());

        let nod1 = self.nodata_value();
        let nod2 = other.nodata_value();
        let nod = if nod1.is_some() { nod1 } else { nod2 };

        let lhs_nodata = |val: T| nod1.map_or(false, |nodata| val == nodata);
        let rhs_nodata = |val: T| nod2.map_or(false, |nodata| val == nodata);

        // Perform element-wise addition
        for (x, y) in self.data.into_iter().zip(other.data.into_iter()) {
            if lhs_nodata(x) || rhs_nodata(y) {
                data.push(nod.unwrap());
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
        let nod = self.nodata_value();
        let is_nodata = |val: T| nod.map_or(false, |nodata| val == nodata);

        // let data = self.data_mut();
        // for value in data {
        //     if is_nodata(*value) {
        //         continue;
        //     }
        //     *value = *value * scalar;
        // }

        self.data
            .iter_mut()
            .filter(|x| !is_nodata(**x))
            .for_each(|raster_val| *raster_val = *raster_val * NumCast::from(scalar).unwrap_or(T::zero()));

        self
    }
}

impl<T: RasterNum<T> + std::ops::Mul<Output = T>> std::ops::Mul<T> for &DenseRaster<T> {
    type Output = DenseRaster<T>;

    fn mul(self, scalar: T) -> DenseRaster<T> {
        let nod = self.nodata_value();
        let is_nodata = |val: T| nod.map_or(false, |nodata| val == nodata);

        // let data = self.data_mut();
        // for value in data {
        //     if is_nodata(*value) {
        //         continue;
        //     }
        //     *value = *value * scalar;
        // }

        let mut data = Vec::with_capacity(self.data.len());

        for x in self.data.iter() {
            if is_nodata(*x) {
                data.push(nod.unwrap());
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

        let nod = result_nodata(self.nodata_value(), other.nodata_value());
        let lhs_nodata = |val: T| self.nodata_value().map_or(false, |nodata| val == nodata);

        // Perform element-wise addition
        for (x, y) in self.data.iter().zip(other.data.iter()) {
            if lhs_nodata(*x) || other.is_nodata(*y) {
                data.push(nod.unwrap());
            } else {
                data.push(*x + *y);
            }
        }

        DenseRaster { metadata, data }
    }
}

impl<T: RasterNum<T> + std::ops::AddAssign> DenseRaster<T> {
    pub fn sum(&self) -> f64 {
        self.data
            .iter()
            .filter(|x| self.nodata_value().map_or(true, |nodata| **x != nodata))
            .fold(0.0, |acc, x| acc + NumCast::from(*x).unwrap_or(0.0))
    }

    pub fn nodata_count(&self) -> usize {
        self.data
            .iter()
            .filter(|x| self.nodata_value().map_or(false, |nodata| **x == nodata))
            .count()
    }
}

impl<T: RasterNum<T>> Raster<T> for DenseRaster<T> {
    fn new(metadata: GeoMetadata, data: Vec<T>) -> Self {
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

    fn index_has_data(&self, index: usize) -> bool {
        self.data[index] != T::value()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GeoMetadata, RasterSize};

    #[test]
    fn test_add_rasters() {
        let metadata = GeoMetadata::new(
            "EPSG:4326".to_string(),
            RasterSize { rows: 2, cols: 2 },
            [0.0, 0.0, 1.0, 1.0, 0.0, 0.0],
            Some(-9999.0),
        );

        let data1 = vec![1, 2, -9999, 4];
        let data2 = vec![-9999, 6, 7, 8];
        let raster1 = DenseRaster::new(metadata.clone(), data1);
        let raster2 = DenseRaster::new(metadata.clone(), data2);

        {
            let result = &raster1 + &raster2;
            assert_eq!(result.as_slice(), &[-9999, 8, -9999, 12]);
        }

        {
            let result = raster1 + raster2;
            assert_eq!(result.as_slice(), &[-9999, 8, -9999, 12]);
        }
    }

    #[test]
    fn test_multiply_scalar() {
        let metadata = GeoMetadata::new(
            "EPSG:4326".to_string(),
            RasterSize { rows: 2, cols: 2 },
            [0.0, 0.0, 1.0, 1.0, 0.0, 0.0],
            Some(-9999.0),
        );

        let raster = DenseRaster::new(metadata.clone(), vec![1, 2, -9999, 4]);

        {
            let result = &raster * 2;
            assert_eq!(result.as_slice(), &[2, 4, -9999, 8]);
        }

        {
            let result = raster * 2;
            assert_eq!(result.as_slice(), &[2, 4, -9999, 8]);
        }
    }
}

use std::sync::Arc;

use arrow::{
    array::{downcast_array, PrimitiveArray},
    compute,
    datatypes::ArrowPrimitiveType,
};
use num::NumCast;

use crate::{
    arrowutil::{self, ArrowType},
    raster::{self, RasterNum},
    GeoMetadata, Raster,
};

fn result_nodata<T: Clone>(lhs: Option<T>, rhs: Option<T>) -> Option<T> {
    lhs.or_else(|| rhs.clone())
}

pub trait ArrowRasterNum<T: num::ToPrimitive>: RasterNum<T> + ArrowType + ArrowPrimitiveType {}

pub struct ArrowRaster<T: ArrowRasterNum<T> + ArrowType> {
    metadata: GeoMetadata,
    data: Arc<PrimitiveArray<T::TArrow>>,
}

impl<T: ArrowRasterNum<T> + std::ops::Add<Output = T>> std::ops::Add for ArrowRaster<T> {
    type Output = ArrowRaster<T>;

    fn add(self, other: ArrowRaster<T>) -> ArrowRaster<T> {
        raster::assert_dimensions(&self, &other);

        // Create a new ArrowRaster with the same metadata
        let metadata = self.metadata.clone();

        match compute::kernels::numeric::add_wrapping(&*self.data, &*other.data) {
            Ok(data) => {
                let data = downcast_array::<PrimitiveArray<T::TArrow>>(&*data);
                ArrowRaster {
                    metadata,
                    data: Arc::new(data),
                }
            }
            Err(e) => panic!("Error adding rasters: {:?}", e),
        }
    }
}

// impl<T: ArrowRasterNum<T> + std::ops::Mul<Output = T>> std::ops::Mul<T> for ArrowRaster<T> {
//     type Output = ArrowRaster<T>;

//     fn mul(mut self, scalar: T) -> ArrowRaster<T> {
//         let nod = self.nodata_value();
//         let is_nodata = |val: T| nod.map_or(false, |nodata| val == nodata);

//         // let data = self.data_mut();
//         // for value in data {
//         //     if is_nodata(*value) {
//         //         continue;
//         //     }
//         //     *value = *value * scalar;
//         // }

//         self.data
//             .iter_mut()
//             .filter(|x| !is_nodata(**x))
//             .for_each(|raster_val| *raster_val = *raster_val * NumCast::from(scalar).unwrap_or(T::zero()));

//         self
//     }
// }

// impl<T: ArrowRasterNum<T> + std::ops::Mul<Output = T>> std::ops::Mul<T> for &ArrowRaster<T> {
//     type Output = ArrowRaster<T>;

//     fn mul(self, scalar: T) -> ArrowRaster<T> {
//         let nod = self.nodata_value();
//         let is_nodata = |val: T| nod.map_or(false, |nodata| val == nodata);

//         // let data = self.data_mut();
//         // for value in data {
//         //     if is_nodata(*value) {
//         //         continue;
//         //     }
//         //     *value = *value * scalar;
//         // }

//         let mut data = Vec::with_capacity(self.data.len());

//         for x in self.data.iter() {
//             if is_nodata(*x) {
//                 data.push(nod.unwrap());
//             } else {
//                 data.push(*x * scalar);
//             }
//         }

//         ArrowRaster {
//             metadata: self.metadata.clone(),
//             data,
//         }
//     }
// }

// impl<T: ArrowRasterNum<T> + std::ops::Add<Output = T>> std::ops::Add for &ArrowRaster<T> {
//     type Output = ArrowRaster<T>;

//     fn add(self, other: &ArrowRaster<T>) -> ArrowRaster<T> {
//         raster::assert_dimensions(self, other);

//         // Create a new ArrowRaster with the same metadata
//         let metadata = self.metadata.clone();
//         let mut data = Vec::with_capacity(self.data.len());

//         let nod = result_nodata(self.nodata_value(), other.nodata_value());
//         let lhs_nodata = |val: T| self.nodata_value().map_or(false, |nodata| val == nodata);

//         // Perform element-wise addition
//         for (x, y) in self.data.iter().zip(other.data.iter()) {
//             if lhs_nodata(*x) || other.is_nodata(*y) {
//                 data.push(nod.unwrap());
//             } else {
//                 data.push(*x + *y);
//             }
//         }

//         ArrowRaster { metadata, data }
//     }
// }

// impl<T: ArrowRasterNum<T> + std::ops::AddAssign> ArrowRaster<T> {
//     pub fn sum(&self) -> f64 {
//         self.data
//             .iter()
//             .filter(|x| self.nodata_value().map_or(true, |nodata| **x != nodata))
//             .fold(0.0, |acc, x| acc + NumCast::from(*x).unwrap_or(0.0))
//     }
// }

impl<T: ArrowRasterNum<T>> Raster<T> for ArrowRaster<T>
where
    arrow::array::PrimitiveArray<<T as arrowutil::ArrowType>::TArrow>: std::convert::From<std::vec::Vec<T>>,
{
    fn new(metadata: GeoMetadata, data: Vec<T>) -> Self {
        let data: Arc<PrimitiveArray<T::TArrow>> = Arc::new(data.into());
        ArrowRaster { metadata, data }
    }

    fn zeros(meta: GeoMetadata) -> Self {
        ArrowRaster::filled_with(T::zero(), meta)
    }

    fn filled_with(val: T, meta: GeoMetadata) -> Self {
        let data_size = meta.rows() * meta.columns();
        ArrowRaster::new(meta, vec![val; data_size])
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

    fn data_mut(&mut self) -> &mut Vec<T> {
        &mut self.data
    }

    fn data(&self) -> &Vec<T> {
        &self.data
    }

    fn nodata_value(&self) -> Option<T> {
        match self.metadata.nodata() {
            Some(nodata) => NumCast::from(nodata),
            None => None,
        }
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
        let raster1 = ArrowRaster::new(metadata.clone(), data1);
        let raster2 = ArrowRaster::new(metadata.clone(), data2);

        {
            let result = &raster1 + &raster2;
            assert_eq!(result.data(), &[-9999, 8, -9999, 12]);
        }

        {
            let result = raster1 + raster2;
            assert_eq!(result.data(), &[-9999, 8, -9999, 12]);
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

        let raster = ArrowRaster::new(metadata.clone(), vec![1, 2, -9999, 4]);

        {
            let result = &raster * 2;
            assert_eq!(result.data(), &[2, 4, -9999, 8]);
        }

        {
            let result = raster * 2;
            assert_eq!(result.data(), &[2, 4, -9999, 8]);
        }
    }
}

use std::path::Path;

use crate::{fieldtype::VectorFieldType, io::open_read_only, Result};

pub trait DataRow {
    fn field_names() -> Vec<&'static str>;
    fn from_feature(feature: gdal::vector::Feature) -> Result<Self>
    where
        Self: Sized;
}

pub struct VectorDataframeIterator<TRow: DataRow> {
    features: gdal::vector::OwnedFeatureIterator,
    phantom: std::marker::PhantomData<TRow>,
}

impl<TRow: DataRow> VectorDataframeIterator<TRow> {
    pub fn new<P: AsRef<Path>>(path: &P) -> Result<Self> {
        let ds_layer = open_read_only(path.as_ref())?.into_layer(0)?;

        // let field_names = TRow::field_names();
        // let mut field_indices = Vec::with_capacity(field_names.len());
        // for &field_name in TRow::field_names() {
        //     let col = unsafe {
        //         let cdef = ds_layer.defn().c_defn();
        //         gdal_sys::OGR_FD_GetFieldIndex(cdef, CString::new(field_name)?.as_ptr())
        //     };

        //     field_indices.push(col);
        // }

        Ok(Self {
            features: ds_layer.owned_features(),
            phantom: std::marker::PhantomData,
        })
    }
}

impl<TRow: DataRow> Iterator for VectorDataframeIterator<TRow> {
    type Item = Result<TRow>;

    fn next(&mut self) -> Option<Self::Item> {
        self.features.into_iter().next().map(TRow::from_feature)
    }
}

fn read_feature_val<T: VectorFieldType<T>>(feature: &gdal::vector::Feature, field_name: &str) -> Result<Option<T>> {
    match feature.field(field_name)? {
        Some(field) => T::read_from_field(&field),
        None => Ok(None),
    }
}

#[cfg(feature = "derive")]
#[cfg(test)]
mod tests {
    use path_macro::path;
    use vector_derive::DataRow;

    use super::*;

    #[derive(DataRow)]
    struct PollutantData {
        #[vector(column = "Pollutant")]
        pollutant: String,
        #[vector(column = "Sector")]
        sector: String,
        value: f64,
    }

    // #[derive(DataRow)]
    // struct PollutantOptionalData {
    //     #[vector(column = "Pollutant")]
    //     pollutant: String,
    //     #[vector(column = "Sector")]
    //     sector: String,
    //     value: Option<f64>,
    // }

    #[test]
    fn test_iterate_features() {
        assert_eq!(PollutantData::field_names(), vec!["Pollutant", "Sector", "value"]);
    }

    #[test]
    fn test_row_data_derive() {
        let path = path!(env!("CARGO_MANIFEST_DIR") / "test" / "data" / "road.csv");
        let mut iter = VectorDataframeIterator::<PollutantData>::new(&path).unwrap();

        {
            let row = iter.next().unwrap().unwrap();
            assert_eq!(row.pollutant, "NO2");
            assert_eq!(row.sector, "A_PublicTransport");
            assert_eq!(row.value, 10.0);
        }

        {
            let row = iter.next().unwrap().unwrap();
            assert_eq!(row.pollutant, "NO2");
            assert_eq!(row.sector, "B_RoadTransport");
            assert_eq!(row.value, 11.5);
        }

        {
            let row = iter.next().unwrap().unwrap();
            assert_eq!(row.pollutant, "PM10");
            assert_eq!(row.sector, "B_RoadTransport");
            assert_eq!(row.value, 13.0);
        }

        assert!(iter.next().is_none());
    }

    #[test]
    fn test_row_data_derive_missing() {
        let path = path!(env!("CARGO_MANIFEST_DIR") / "test" / "data" / "road_missing_data.csv");
        let mut iter = VectorDataframeIterator::<PollutantData>::new(&path).unwrap();
        assert!(iter.nth(1).unwrap().is_err()); // The second line is incomplete (missing value)
        assert!(iter.next().unwrap().is_ok());
        assert!(iter.next().unwrap().is_ok());
        assert!(iter.next().is_none());
    }

    // #[test]
    // fn test_row_data_derive_missing_optionals() {
    //     let path = path!(env!("CARGO_MANIFEST_DIR") / "test" / "data" / "road_missing_data.csv");
    //     let mut iter = VectorDataframeIterator::<PollutantOptionalData>::new(&path).unwrap();
    //     let second = iter.nth(1).unwrap().unwrap();

    //     assert_eq!(row.pollutant, "PM10");
    //     assert_eq!(row.sector, "B_RoadTransport");
    //     assert_eq!(row.value, 13.0);
    // }
}

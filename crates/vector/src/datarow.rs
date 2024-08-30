use std::path::Path;

use gdal::vector::FieldValue;

use crate::{
    fieldtype::VectorFieldType,
    io::{self, open_read_only},
    Result,
};

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
    pub fn new<P: AsRef<Path>>(path: &P, layer: Option<&str>) -> Result<Self> {
        let ds = open_read_only(path.as_ref())?;
        let ds_layer = if let Some(layer_name) = layer {
            ds.into_layer_by_name(layer_name)?
        } else {
            ds.into_layer(0)?
        };

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
    let index = io::field_index_from_name(feature, field_name)?;

    let field_is_valid = unsafe { gdal_sys::OGR_F_IsFieldSetAndNotNull(feature.c_feature(), index) == 1 };

    if !field_is_valid {
        return Ok(None);
    }

    match feature.field(field_name)? {
        Some(field) => {
            if !T::empty_value_is_valid() {
                if let FieldValue::StringValue(val) = &field {
                    // Don't try to parse empty strings (empty strings are not considered as null values by GDAL for csv files)
                    if val.is_empty() {
                        return Ok(None);
                    }
                }
            }

            T::read_from_field(&field)
        }
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

    #[derive(DataRow)]
    struct PollutantOptionalData {
        #[vector(column = "Pollutant")]
        pollutant: String,
        #[vector(column = "Sector")]
        sector: String,
        value: Option<f64>,
    }

    #[test]
    fn test_iterate_features() {
        assert_eq!(PollutantData::field_names(), vec!["Pollutant", "Sector", "value"]);
    }

    #[test]
    fn test_row_data_derive() {
        let path = path!(env!("CARGO_MANIFEST_DIR") / "test" / "data" / "road.csv");
        let mut iter = VectorDataframeIterator::<PollutantData>::new(&path, None).unwrap();

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
        let mut iter = VectorDataframeIterator::<PollutantData>::new(&path, None).unwrap();
        assert!(iter.nth(1).unwrap().is_err()); // The second line is incomplete (missing value)
        assert!(iter.next().unwrap().is_ok());
        assert!(iter.next().unwrap().is_ok());
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_row_data_derive_missing_optionals() {
        let path = path!(env!("CARGO_MANIFEST_DIR") / "test" / "data" / "road_missing_data.csv");
        let mut iter = VectorDataframeIterator::<PollutantOptionalData>::new(&path, None).unwrap();

        {
            let row = iter.next().unwrap().unwrap();
            assert_eq!(row.pollutant, "NO2");
            assert_eq!(row.sector, "A_PublicTransport");
            assert_eq!(row.value, Some(10.0));
        }

        {
            let row = iter.next().unwrap().unwrap();
            assert_eq!(row.pollutant, "PM10");
            assert_eq!(row.sector, "A_PublicTransport");
            assert_eq!(row.value, None);
        }
    }
}

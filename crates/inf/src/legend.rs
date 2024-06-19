use num::NumCast;
use serde::Serialize;

use crate::{
    color::Color,
    colormap::{cmap, ColorMap},
    Result,
};
use std::{collections::HashMap, ops::Range};

/// Options for mapping values that can not be mapped by the legend mapper
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Default, Clone, Debug)]
pub struct MappingConfig {
    /// The color of the nodata pixels
    pub unmappable_nodata: Color,
    /// The color of the values below the lowest value in the colormap
    pub unmappable_low: Color,
    /// The color of the values above the highest value in the colormap
    pub unmappable_high: Color,
}

impl MappingConfig {
    pub fn new(nodata: Color, low: Color, high: Color) -> Self {
        MappingConfig {
            unmappable_nodata: nodata,
            unmappable_low: low,
            unmappable_high: high,
        }
    }
}

/// Trait for implementing color mappers
pub trait ColorMapper: Default {
    fn color_for_numeric_value(&self, value: f64, config: &MappingConfig) -> Color;
    fn color_for_string_value(&self, value: &str, config: &MappingConfig) -> Color;
    fn category_count(&self) -> usize;
}

pub mod mapper {
    use num::ToPrimitive;

    use crate::interpolate::linear_map_to_float;

    use super::*;
    use std::ops::Range;

    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Clone, Debug)]
    pub struct LegendBand {
        range: Range<f64>,
        color: Color,
        name: String,
    }

    impl PartialEq for LegendBand {
        fn eq(&self, other: &Self) -> bool {
            self.color == other.color
                && self.name == other.name
                && (self.range.start - other.range.start).abs() <= f64::EPSILON
                && (self.range.end - other.range.end).abs() <= f64::EPSILON
        }
    }

    impl LegendBand {
        pub fn new(range: Range<f64>, color: Color, name: String) -> Self {
            LegendBand { range, color, name }
        }
    }

    /// Linear color mapper
    /// each value gets its color based on the position in the configured value range
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Default, Clone, Debug)]
    pub struct Linear {
        value_range: Range<f64>,
        color_map: ColorMap,
    }

    impl Linear {
        pub fn new(value_range: Range<f64>, color_map: ColorMap) -> Self {
            Linear {
                color_map: color_map,
                value_range,
            }
        }
    }

    impl ColorMapper for Linear {
        fn color_for_numeric_value(&self, value: f64, config: &MappingConfig) -> Color {
            const EDGE_TOLERANCE: f64 = 1e-4;

            if value < self.value_range.start - EDGE_TOLERANCE {
                config.unmappable_low
            } else if value > self.value_range.end + EDGE_TOLERANCE {
                config.unmappable_high
            } else {
                let value_0_1 = linear_map_to_float::<f64, f64>(value, self.value_range.start, self.value_range.end);
                self.color_map.get_color(value_0_1)
            }
        }

        fn color_for_string_value(&self, value: &str, config: &MappingConfig) -> Color {
            if let Ok(num_value) = value.parse::<f64>() {
                self.color_for_numeric_value(num_value, config)
            } else {
                // Linear legend does not support string values
                config.unmappable_nodata
            }
        }

        fn category_count(&self) -> usize {
            1
        }
    }

    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Default, Clone, Debug)]
    pub struct LegendCategory {
        pub color: Color,
        pub name: String,
    }

    /// Categoric numeric color mapper (single numeric value → color)
    /// Contains a number of categories that map to a color
    /// each value gets its color based on the exact category match
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Default, Clone, Debug)]
    pub struct CategoricNumeric {
        categories: HashMap<i64, LegendCategory>,
    }

    impl CategoricNumeric {
        pub fn new(categories: HashMap<i64, LegendCategory>) -> Self {
            CategoricNumeric { categories }
        }

        pub fn for_value_range(value_range: Range<i64>, color_map: ColorMap) -> Self {
            let category_count = value_range.end - value_range.start + 1;
            let color_offset = if category_count == 1 {
                0.0
            } else {
                1.0 / (category_count as f64 - 1.0)
            };
            let mut color_pos = 0.0;

            let mut categories = HashMap::new();
            for cat in value_range {
                categories.insert(
                    cat,
                    LegendCategory {
                        color: color_map.get_color(color_pos),
                        name: String::default(),
                    },
                );

                color_pos += color_offset;
            }

            CategoricNumeric { categories }
        }
    }

    impl ColorMapper for CategoricNumeric {
        fn color_for_numeric_value(&self, value: f64, config: &MappingConfig) -> Color {
            if let Some(cat) = value.to_i64() {
                return self
                    .categories
                    .get(&cat)
                    .map_or(config.unmappable_nodata, |cat| cat.color);
            }

            config.unmappable_nodata
        }

        fn color_for_string_value(&self, value: &str, config: &MappingConfig) -> Color {
            // No string value support, so convert to numeric value if possible or return nodata color
            if let Ok(num_value) = value.parse::<f64>() {
                self.color_for_numeric_value(num_value, config)
            } else {
                config.unmappable_nodata
            }
        }

        fn category_count(&self) -> usize {
            self.categories.len()
        }
    }

    /// Categoric string color mapper (single string value → color)
    /// Contains a number of categories that map to a color
    /// each value gets its color based on the exact category match
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Default, Clone, Debug)]
    pub struct CategoricString {
        categories: HashMap<String, LegendCategory>,
    }

    impl CategoricString {
        pub fn new(string_map: HashMap<String, LegendCategory>) -> Self {
            CategoricString { categories: string_map }
        }
    }

    impl ColorMapper for CategoricString {
        fn color_for_numeric_value(&self, value: f64, config: &MappingConfig) -> Color {
            // Convert to string and match if possible
            self.color_for_string_value(value.to_string().as_str(), config)
        }

        fn color_for_string_value(&self, value: &str, config: &MappingConfig) -> Color {
            self.categories
                .get(value)
                .map_or(config.unmappable_nodata, |cat| cat.color)
        }

        fn category_count(&self) -> usize {
            self.categories.len()
        }
    }

    /// Banded color mapper (value range -> color)
    /// Contains a number of configured bands with a value range and a color
    /// each value gets its color based on the band it belongs to
    #[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
    #[derive(Default, Clone, Debug)]
    pub struct Banded {
        bands: Vec<LegendBand>,
    }

    impl Banded {
        pub fn new(bands: Vec<LegendBand>) -> Self {
            Banded { bands }
        }

        pub fn with_equal_bands(band_count: usize, value_range: Range<f64>, color_map: ColorMap) -> Self {
            let color_offset = if band_count == 1 {
                0.0
            } else {
                1.0 / (band_count as f64 - 1.0)
            };
            let band_offset: f64 = (value_range.end - value_range.start) / (band_count as f64 - 1.0);
            let mut color_pos = 0.0;
            let mut band_pos = value_range.start;

            let mut entries = Vec::with_capacity(band_count);
            for _band in 0..band_count {
                entries.push(LegendBand::new(
                    Range {
                        start: band_pos,
                        end: band_pos + band_offset,
                    },
                    color_map.get_color(color_pos),
                    String::default(),
                ));

                band_pos += band_offset;
                color_pos += color_offset;
            }

            Banded { bands: entries }
        }
    }

    impl ColorMapper for Banded {
        fn color_for_numeric_value(&self, value: f64, config: &MappingConfig) -> Color {
            const EDGE_TOLERANCE: f64 = 1e-4;

            for entry in &self.bands {
                if entry.range.contains(&value) {
                    return entry.color;
                }
            }

            if let Some(first_entry) = self.bands.first() {
                if (value - first_entry.range.start).abs() < EDGE_TOLERANCE {
                    return first_entry.color;
                } else if value < first_entry.range.start {
                    return config.unmappable_low;
                } else if let Some(last_entry) = self.bands.last() {
                    if (value - last_entry.range.end).abs() < EDGE_TOLERANCE {
                        return last_entry.color;
                    } else if value > last_entry.range.end {
                        return config.unmappable_high;
                    }
                }
            }

            config.unmappable_nodata
        }

        fn color_for_string_value(&self, value: &str, config: &MappingConfig) -> Color {
            // No string value support, so convert to numeric value if possible or return nodata color
            if let Ok(num_value) = value.parse::<f64>() {
                self.color_for_numeric_value(num_value, config)
            } else {
                config.unmappable_nodata
            }
        }

        fn category_count(&self) -> usize {
            self.bands.len()
        }
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Default, Clone, Debug)]
pub struct MappedLegend<TMapper: ColorMapper> {
    pub title: String,
    pub color_map_name: String,
    /// Render zero values as nodata
    pub zero_is_nodata: bool,
    pub mapper: TMapper,
    pub mapping_config: MappingConfig,
}

impl<TMapper: ColorMapper> MappedLegend<TMapper> {
    pub fn with_mapper(mapper: TMapper, mapping_config: MappingConfig) -> Self {
        MappedLegend {
            mapper,
            mapping_config,
            ..Default::default()
        }
    }

    fn is_unmappable(&self, value: f64, nodata: Option<f64>) -> bool {
        value.is_nan() || Some(value) == nodata || (self.zero_is_nodata && value == 0.0)
    }

    pub fn color_for_value<T: Copy + num::NumCast>(&self, value: T, nodata: Option<f64>) -> Color {
        let value = value.to_f64().unwrap_or(f64::NAN);
        if self.is_unmappable(value, nodata) {
            return self.mapping_config.unmappable_nodata;
        }

        self.mapper.color_for_numeric_value(value, &self.mapping_config)
    }

    pub fn color_for_string_value(&self, value: &str) -> Color {
        self.mapper.color_for_string_value(value, &self.mapping_config)
    }

    pub fn apply_to_data<T: Copy + num::NumCast, TNodata: Copy + num::NumCast>(
        &self,
        data: &[T],
        nodata: Option<TNodata>,
    ) -> Vec<Color> {
        let nodata = nodata.map(|v| v.to_f64().unwrap_or(f64::NAN));

        data.iter().map(|&value| self.color_for_value(value, nodata)).collect()
    }
}

pub type LinearLegend = MappedLegend<mapper::Linear>;
pub type BandedLegend = MappedLegend<mapper::Banded>;
pub type CategoricNumericLegend = MappedLegend<mapper::CategoricNumeric>;
pub type CategoricStringLegend = MappedLegend<mapper::CategoricString>;

/// Legend for mapping values to colors, can be linear, banded or categoric
/// Use this when you need to store a legend that can be of any mapping type
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Legend {
    Linear(LinearLegend),
    Banded(BandedLegend),
    CategoricNumeric(CategoricNumericLegend),
    CategoricString(CategoricStringLegend),
}

/// Default legend is a linear grayscale legend for the range [0-255]
impl Default for Legend {
    fn default() -> Self {
        Legend::Linear(LinearLegend::with_mapper(
            mapper::Linear::new(Range { start: 0.0, end: 255.0 }, ColorMap::new(&cmap::gray(), false)),
            MappingConfig::default(),
        ))
    }
}

impl Legend {
    pub fn linear(cmap_name: &str, value_range: Range<f64>) -> Result<Self> {
        Ok(Legend::Linear(create_linear(cmap_name, value_range)?))
    }

    pub fn banded(category_count: usize, cmap_name: &str, value_range: Range<f64>) -> Result<Self> {
        Ok(Legend::Banded(create_banded(category_count, cmap_name, value_range)?))
    }

    pub fn categoric_numeric(cmap_name: &str, value_range: Range<i64>) -> Result<Self> {
        Ok(Legend::CategoricNumeric(create_categoric_numeric(
            cmap_name,
            value_range,
        )?))
    }

    pub fn categoric_string(string_map: HashMap<String, mapper::LegendCategory>) -> Result<Self> {
        Ok(Legend::CategoricString(create_categoric_string(string_map)?))
    }

    pub fn apply<T: Copy + NumCast, TNodata: Copy + num::NumCast>(
        &self,
        data: &[T],
        nodata: Option<TNodata>,
    ) -> Vec<Color> {
        match self {
            Legend::Linear(legend) => legend.apply_to_data(data, nodata),
            Legend::Banded(legend) => legend.apply_to_data(data, nodata),
            Legend::CategoricNumeric(legend) => legend.apply_to_data(data, nodata),
            Legend::CategoricString(legend) => legend.apply_to_data(data, nodata),
        }
    }

    pub fn color_for_value<T: Copy + num::NumCast>(&self, value: T, nodata: Option<f64>) -> Color {
        match self {
            Legend::Linear(legend) => legend.color_for_value(value, nodata),
            Legend::Banded(legend) => legend.color_for_value(value, nodata),
            Legend::CategoricNumeric(legend) => legend.color_for_value(value, nodata),
            Legend::CategoricString(legend) => legend.color_for_value(value, nodata),
        }
    }

    pub fn color_for_string_value(&self, value: &str) -> Color {
        match self {
            Legend::Linear(legend) => legend.color_for_string_value(value),
            Legend::Banded(legend) => legend.color_for_string_value(value),
            Legend::CategoricNumeric(legend) => legend.color_for_string_value(value),
            Legend::CategoricString(legend) => legend.color_for_string_value(value),
        }
    }

    pub fn title(&self) -> &str {
        match self {
            Legend::Linear(legend) => legend.title.as_str(),
            Legend::Banded(legend) => legend.title.as_str(),
            Legend::CategoricNumeric(legend) => legend.title.as_str(),
            Legend::CategoricString(legend) => legend.title.as_str(),
        }
    }
}

/// Create a legend with linear color mapping
pub fn create_linear(cmap_name: &str, value_range: Range<f64>) -> Result<LinearLegend> {
    Ok(MappedLegend {
        mapper: mapper::Linear::new(value_range, ColorMap::create(cmap_name)?),
        color_map_name: cmap_name.to_string(),
        ..Default::default()
    })
}

/// Create a banded legend where the categories are equally spaced between the value range
pub fn create_banded(category_count: usize, cmap_name: &str, value_range: Range<f64>) -> Result<BandedLegend> {
    Ok(MappedLegend {
        mapper: mapper::Banded::with_equal_bands(category_count, value_range, ColorMap::create(cmap_name)?),
        color_map_name: cmap_name.to_string(),
        ..Default::default()
    })
}

/// Create a categoric legend where each value in the value range is a category
pub fn create_categoric_numeric(cmap_name: &str, value_range: Range<i64>) -> Result<CategoricNumericLegend> {
    Ok(MappedLegend {
        mapper: mapper::CategoricNumeric::for_value_range(value_range, ColorMap::create(cmap_name)?),
        color_map_name: cmap_name.to_string(),
        ..Default::default()
    })
}

/// Create a categoric legend with string value mapping
pub fn create_categoric_string(string_map: HashMap<String, mapper::LegendCategory>) -> Result<CategoricStringLegend> {
    Ok(MappedLegend {
        mapper: mapper::CategoricString::new(string_map),
        ..Default::default()
    })
}

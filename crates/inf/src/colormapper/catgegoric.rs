use std::{collections::HashMap, ops::Range};

use num::ToPrimitive as _;

use crate::{
    Color, Error, Result,
    colormap::{ColorMap, ProcessedColorMap},
    legend::{LegendCategory, MappingConfig},
};

use super::ColorMapper;

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

    pub fn for_values(category_values: &[i64], color_map: &ColorMap) -> Result<Self> {
        let category_count = category_values.len();
        let mut categories = HashMap::new();
        if let ColorMap::ColorList(colors) = color_map {
            if category_count != colors.len() {
                return Err(Error::InvalidArgument(
                    "Color list length does not match the number of category values".into(),
                ));
            }

            for (cat, color) in category_values.iter().zip(colors.iter()) {
                categories.insert(
                    *cat,
                    LegendCategory {
                        color: *color,
                        name: String::default(),
                    },
                );
            }
        } else {
            let processed_color_map = ProcessedColorMap::create(color_map)?;
            let color_offset = if category_count == 1 {
                0.0
            } else {
                1.0 / (category_count as f64 - 1.0)
            };

            let mut color_pos = 0.0;

            for cat in category_values {
                categories.insert(
                    *cat,
                    LegendCategory {
                        color: processed_color_map.get_color(color_pos),
                        name: String::default(),
                    },
                );

                color_pos += color_offset;
            }
        }

        Ok(CategoricNumeric { categories })
    }

    pub fn for_value_range(value_range: Range<i64>, color_map: &ColorMap) -> Result<Self> {
        let category_count = value_range.end - value_range.start + 1;
        let mut categories = HashMap::new();

        if let ColorMap::ColorList(colors) = color_map {
            if category_count != colors.len() as i64 {
                return Err(Error::InvalidArgument(
                    "Color list length does not match the number of categories in the range".into(),
                ));
            }

            for (cat, color) in value_range.zip(colors.iter()) {
                categories.insert(
                    cat,
                    LegendCategory {
                        color: *color,
                        name: String::default(),
                    },
                );
            }
        } else {
            let processed_color_map = ProcessedColorMap::create(color_map)?;
            let color_offset = if category_count == 1 {
                0.0
            } else {
                1.0 / (category_count as f64 - 1.0)
            };
            let mut color_pos = 0.0;

            for cat in value_range {
                categories.insert(
                    cat,
                    LegendCategory {
                        color: processed_color_map.get_color(color_pos),
                        name: String::default(),
                    },
                );

                color_pos += color_offset;
            }
        }

        Ok(CategoricNumeric { categories })
    }
}

impl ColorMapper for CategoricNumeric {
    fn color_for_numeric_value(&self, value: f64, config: &MappingConfig) -> Color {
        if let Some(cat) = value.to_i64() {
            return self.categories.get(&cat).map_or(config.nodata_color, |cat| cat.color);
        }

        config.nodata_color
    }

    fn color_for_string_value(&self, value: &str, config: &MappingConfig) -> Color {
        // No string value support, so convert to numeric value if possible or return nodata color
        if let Ok(num_value) = value.parse::<f64>() {
            self.color_for_numeric_value(num_value, config)
        } else {
            config.nodata_color
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
        self.categories.get(value).map_or(config.nodata_color, |cat| cat.color)
    }

    fn category_count(&self) -> usize {
        self.categories.len()
    }
}

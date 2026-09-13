use std::{
    collections::HashMap,
    ops::{Range, RangeInclusive},
};

use num::ToPrimitive as _;

use crate::{
    Color, Error, Result,
    colormap::{ColorMap, ProcessedColorMap},
    legend::{LegendCategory, MappingConfig},
};

use super::{ColorMapper, UnmappableColors};

/// Categoric numeric color mapper (single numeric value → color)
/// Contains a number of categories that map to a color
/// each value gets its color based on the exact category match
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Default, Clone, Debug)]
pub struct CategoricNumeric {
    categories: HashMap<i64, LegendCategory>,
    fast_lookup: Option<Vec<u32>>, // Fast lookup for numeric values
}

impl CategoricNumeric {
    fn create_fast_lookup(categories: &HashMap<i64, LegendCategory>) -> Option<Vec<u32>> {
        if categories.is_empty() {
            return None;
        }

        const MAX_LOOKUP_SIZE: i64 = 512; // Maximum size for fast lookup table

        let max_cat = *categories.keys().max().expect("Categories should not be empty");
        let min_cat = *categories.keys().min().expect("Categories should not be empty");
        if min_cat < 0 || max_cat > MAX_LOOKUP_SIZE {
            return None;
        }

        let mut lookup = vec![crate::color::TRANSPARENT.to_bits(); (max_cat + 1) as usize];
        for (cat, legend_cat) in categories {
            lookup[*cat as usize] = legend_cat.color.to_bits();
        }
        Some(lookup)
    }

    pub fn new(categories: HashMap<i64, LegendCategory>) -> Self {
        let fast_lookup = CategoricNumeric::create_fast_lookup(&categories);
        CategoricNumeric { categories, fast_lookup }
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
            let processed_color_map = ProcessedColorMap::<256>::create(color_map)?;
            let color_offset = if category_count == 1 {
                0.0
            } else {
                1.0 / (category_count as f32 - 1.0)
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

        let fast_lookup = CategoricNumeric::create_fast_lookup(&categories);
        Ok(CategoricNumeric { categories, fast_lookup })
    }

    pub fn for_value_range(value_range: RangeInclusive<i64>, color_map: &ColorMap) -> Result<Self> {
        let category_count = value_range.end() - value_range.start() + 1;
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
            let processed_color_map = ProcessedColorMap::<256>::create(color_map)?;
            let color_offset = if category_count == 1 {
                0.0
            } else {
                1.0 / (category_count as f32 - 1.0)
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

        let fast_lookup = CategoricNumeric::create_fast_lookup(&categories);
        Ok(CategoricNumeric { categories, fast_lookup })
    }
}

impl ColorMapper for CategoricNumeric {
    fn simd_supported(&self) -> bool {
        // SIMD support is available only if the fast lookup table is created
        self.fast_lookup.is_some()
    }

    #[inline]
    fn color_for_numeric_value(&self, value: f32, unmappable_colors: &UnmappableColors) -> Color {
        if let Some(cat) = value.to_i64() {
            if let Some(lookup) = &self.fast_lookup {
                // Fast lookup for numeric values
                if cat >= 0 && (cat as usize) < lookup.len() {
                    return Color::from(lookup[cat as usize]);
                }
            } else {
                return self.categories.get(&cat).map_or(unmappable_colors.nodata, |cat| cat.color);
            }
        }

        unmappable_colors.nodata
    }

    #[inline]
    fn color_for_numeric_value_simd<S: fearless_simd::Simd>(
        &self,
        simd: S,
        value: S::f32s,
        unmappable_colors: &UnmappableColors,
    ) -> S::u32s {
        use fearless_simd::*;

        let lookup = self.fast_lookup.as_ref().expect("fast lookup table must be present for SIMD");
        let nodata = unmappable_colors.nodata.to_bits();

        // There is no SIMD gather instruction available, so perform a per-lane lookup. Lanes whose
        // category falls outside the lookup table fall back to the nodata color.
        S::u32s::from_fn(simd, |i| match value[i].to_i64() {
            Some(cat) if cat >= 0 && (cat as usize) < lookup.len() => lookup[cat as usize],
            _ => nodata,
        })
    }

    fn color_for_string_value(&self, value: &str, unmappable_colors: &UnmappableColors) -> Color {
        // No string value support, so convert to numeric value if possible or return nodata color
        if let Ok(num_value) = value.parse::<f32>() {
            self.color_for_numeric_value(num_value, unmappable_colors)
        } else {
            unmappable_colors.nodata
        }
    }

    fn category_count(&self) -> usize {
        self.categories.len()
    }

    fn value_range(&self) -> RangeInclusive<f32> {
        let mut categories = self.categories.keys().collect::<Vec<_>>();
        categories.sort();

        let start = **categories.first().unwrap_or(&&0) as f32;
        let end = **categories.last().unwrap_or(&&0) as f32;

        start..=end
    }

    fn legend_entries(&self) -> Vec<(Range<f32>, Color)> {
        self.categories
            .iter()
            .map(|(cat, legend_cat)| ((*cat as f32)..(*cat as f32), legend_cat.color))
            .collect()
    }

    fn compute_unmappable_colors(&self, config: &MappingConfig) -> UnmappableColors {
        UnmappableColors {
            nodata: config.nodata_color,
            low: config.out_of_range_low_color.unwrap_or(config.nodata_color),
            high: config.out_of_range_high_color.unwrap_or(config.nodata_color),
        }
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
    fn simd_supported(&self) -> bool {
        false // No SIMD support for string values
    }

    #[inline]
    fn color_for_numeric_value(&self, value: f32, unmappable_colors: &UnmappableColors) -> Color {
        // Convert to string and match if possible
        self.color_for_string_value(value.to_string().as_str(), unmappable_colors)
    }

    fn color_for_string_value(&self, value: &str, unmappable_colors: &UnmappableColors) -> Color {
        self.categories.get(value).map_or(unmappable_colors.nodata, |cat| cat.color)
    }

    fn category_count(&self) -> usize {
        self.categories.len()
    }

    fn value_range(&self) -> RangeInclusive<f32> {
        0.0..=0.0
    }

    fn legend_entries(&self) -> Vec<(Range<f32>, Color)> {
        Vec::default()
    }

    fn compute_unmappable_colors(&self, config: &MappingConfig) -> UnmappableColors {
        UnmappableColors {
            nodata: config.nodata_color,
            low: config.out_of_range_low_color.unwrap_or(config.nodata_color),
            high: config.out_of_range_high_color.unwrap_or(config.nodata_color),
        }
    }
}

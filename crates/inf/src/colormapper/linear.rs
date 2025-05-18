use crate::Color;
use crate::colormap::ProcessedColorMap;
use crate::interpolate::linear_map_to_float;
use crate::legend::MappingConfig;
use std::ops::Range;

use super::ColorMapper;

/// Linear color mapper
/// each value gets its color based on the position in the configured value range
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Default, Clone, Debug)]
pub struct Linear {
    value_range: Range<f64>,
    color_map: ProcessedColorMap,
}

impl Linear {
    pub fn new(value_range: Range<f64>, color_map: ProcessedColorMap) -> Self {
        Linear { color_map, value_range }
    }
}

impl ColorMapper for Linear {
    fn color_for_numeric_value(&self, value: f64, config: &MappingConfig) -> Color {
        const EDGE_TOLERANCE: f64 = 1e-4;

        if value < self.value_range.start - EDGE_TOLERANCE {
            config.out_of_range_low_color.unwrap_or(self.color_map.get_color(0.0))
        } else if value > self.value_range.end + EDGE_TOLERANCE {
            config.out_of_range_high_color.unwrap_or(self.color_map.get_color(1.0))
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
            config.nodata_color
        }
    }

    fn category_count(&self) -> usize {
        1
    }
}

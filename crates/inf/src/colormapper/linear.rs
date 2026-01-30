use crate::colormap::ProcessedColorMap;
use crate::interpolate::linear_map_to_float;
use crate::legend::MappingConfig;
use crate::{Color, Error, Result};
use std::ops::{Range, RangeInclusive};

use super::ColorMapper;
use super::UnmappableColors;
#[cfg(feature = "simd")]
use super::UnmappableColorsSimd;

#[cfg(feature = "simd")]
use std::simd::Select;

#[cfg(feature = "simd")]
const LANES: usize = crate::simd::LANES;

/// Linear color mapper
/// each value gets its color based on the position in the configured value range
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Default, Clone, Debug)]
pub struct Linear {
    value_range: Range<f32>,
    color_map: ProcessedColorMap,
}

impl Linear {
    pub fn new(value_range: Range<f32>, color_map: ProcessedColorMap) -> Result<Self> {
        if value_range.start > value_range.end {
            return Err(Error::InvalidArgument(format!(
                "Invalid linear color mapper value range: start ({}) must be less than end ({})",
                value_range.start, value_range.end
            )));
        }

        Ok(Linear { color_map, value_range })
    }
}

impl ColorMapper for Linear {
    fn compute_unmappable_colors(&self, config: &MappingConfig) -> UnmappableColors {
        UnmappableColors {
            nodata: config.nodata_color,
            low: config.out_of_range_low_color.unwrap_or_else(|| self.color_map.get_color(0.0)),
            high: config.out_of_range_high_color.unwrap_or_else(|| self.color_map.get_color(1.0)),
        }
    }

    #[inline]
    fn color_for_numeric_value(&self, value: f32, unmappable_colors: &UnmappableColors) -> Color {
        const EDGE_TOLERANCE: f32 = 1e-4;

        if value < self.value_range.start - EDGE_TOLERANCE {
            unmappable_colors.low
        } else if value > self.value_range.end + EDGE_TOLERANCE {
            unmappable_colors.high
        } else {
            let value_0_1 = linear_map_to_float::<f32, f32>(value, self.value_range.start, self.value_range.end);
            self.color_map.get_color(value_0_1)
        }
    }

    #[cfg(feature = "simd")]
    #[inline]
    fn color_for_numeric_value_simd(
        &self,
        value: std::simd::Simd<f32, LANES>,
        unmappable: &UnmappableColorsSimd,
    ) -> std::simd::Simd<u32, LANES> {
        use std::simd::{Simd, cmp::SimdPartialOrd};

        use crate::interpolate::linear_map_to_float_simd;

        const EDGE_TOLERANCE: f32 = 1e-4;
        let start = Simd::splat(self.value_range.start - EDGE_TOLERANCE);
        let end = Simd::splat(self.value_range.end + EDGE_TOLERANCE);

        let value_0_1 = linear_map_to_float_simd(value, self.value_range.start, self.value_range.end);
        let colors = self.color_map.get_color_simd(value_0_1);

        value
            .simd_lt(start)
            .select(unmappable.low, value.simd_gt(end).select(unmappable.high, colors))
    }

    fn color_for_string_value(&self, value: &str, unmappable: &UnmappableColors) -> Color {
        if let Ok(num_value) = value.parse::<f32>() {
            self.color_for_numeric_value(num_value, unmappable)
        } else {
            // Linear legend does not support string values
            unmappable.nodata
        }
    }

    fn category_count(&self) -> usize {
        1
    }

    fn value_range(&self) -> RangeInclusive<f32> {
        RangeInclusive::new(self.value_range.start, self.value_range.end)
    }

    fn legend_entries(&self) -> Vec<(Range<f32>, Color)> {
        Vec::default()
    }
}

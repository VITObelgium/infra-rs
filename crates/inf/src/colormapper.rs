use crate::Color;
use crate::legend::MappingConfig;

mod banded;
mod catgegoric;
mod linear;

pub(crate) use banded::Banded;
pub(crate) use catgegoric::CategoricNumeric;
pub(crate) use catgegoric::CategoricString;
use fearless_simd::Simd;
pub(crate) use linear::Linear;
use std::ops::Range;
use std::ops::RangeInclusive;

pub struct UnmappableColors {
    pub nodata: Color,
    pub low: Color,
    pub high: Color,
}

/// Trait for implementing color mappers
pub trait ColorMapper: Default {
    fn color_for_numeric_value(&self, value: f32, unmappable_colors: &UnmappableColors) -> Color;

    fn compute_unmappable_colors(&self, config: &MappingConfig) -> UnmappableColors;

    fn color_for_numeric_value_simd<S: Simd>(&self, _simd: S, _value: S::f32s, _unmappable_colors: &UnmappableColors) -> S::u32s {
        panic!("No SIMD support for this color mapper");
    }

    fn color_for_string_value(&self, value: &str, unmappable_colors: &UnmappableColors) -> Color;
    fn category_count(&self) -> usize;
    fn value_range(&self) -> RangeInclusive<f32>;
    fn legend_entries(&self) -> Vec<(Range<f32>, Color)>;

    fn simd_supported(&self) -> bool {
        true
    }
}

use crate::Color;
use crate::legend::MappingConfig;

mod banded;
mod catgegoric;
mod linear;

pub(crate) use banded::Banded;
pub(crate) use catgegoric::CategoricNumeric;
pub(crate) use catgegoric::CategoricString;
pub(crate) use linear::Linear;
use std::ops::Range;
use std::ops::RangeInclusive;

#[cfg(feature = "simd")]
const LANES: usize = crate::simd::LANES;

pub struct UnmappableColors {
    pub nodata: Color,
    pub low: Color,
    pub high: Color,
}

#[cfg(feature = "simd")]
pub struct UnmappableColorsSimd {
    pub nodata: std::simd::Simd<u32, LANES>,
    pub low: std::simd::Simd<u32, LANES>,
    pub high: std::simd::Simd<u32, LANES>,
}

/// Trait for implementing color mappers
pub trait ColorMapper: Default {
    fn color_for_numeric_value(&self, value: f32, unmappable_colors: &UnmappableColors) -> Color;

    fn compute_unmappable_colors(&self, config: &MappingConfig) -> UnmappableColors;
    #[cfg(feature = "simd")]
    fn compute_unmappable_colors_simd(&self, config: &MappingConfig) -> UnmappableColorsSimd {
        use std::simd::Simd;

        let edge_colors = self.compute_unmappable_colors(config);

        UnmappableColorsSimd {
            nodata: Simd::splat(edge_colors.nodata.to_bits()),
            low: Simd::splat(edge_colors.low.to_bits()),
            high: Simd::splat(edge_colors.high.to_bits()),
        }
    }

    #[cfg(feature = "simd")]
    fn color_for_numeric_value_simd(
        &self,
        _value: std::simd::Simd<f32, LANES>,
        _unmappable_colors: &UnmappableColorsSimd,
    ) -> std::simd::Simd<u32, LANES> {
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

use crate::Color;
use crate::legend::MappingConfig;

mod banded;
mod catgegoric;
mod linear;

pub(crate) use banded::Banded;
pub(crate) use catgegoric::CategoricNumeric;
pub(crate) use catgegoric::CategoricString;
pub(crate) use linear::Linear;

/// Trait for implementing color mappers
pub trait ColorMapper: Default {
    fn color_for_numeric_value(&self, value: f32, config: &MappingConfig) -> Color;

    #[cfg(feature = "simd")]
    fn color_for_numeric_value_simd<const N: usize>(
        &self,
        value: &std::simd::Simd<f32, N>,
        config: &MappingConfig,
    ) -> std::simd::Simd<u32, N>
    where
        std::simd::LaneCount<N>: std::simd::SupportedLaneCount;

    fn color_for_string_value(&self, value: &str, config: &MappingConfig) -> Color;
    fn category_count(&self) -> usize;
}

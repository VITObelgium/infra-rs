use std::ops::{Range, RangeInclusive};

use crate::{
    Color, Error, Result,
    colormap::{ColorMap, ProcessedColorMap},
    legend::MappingConfig,
};

#[cfg(feature = "simd")]
use super::UnmappableColorsSimd;
use super::{ColorMapper, UnmappableColors};

#[cfg(feature = "simd")]
use std::simd::Select;

#[cfg(feature = "simd")]
const LANES: usize = crate::simd::LANES;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug)]
pub struct LegendBand {
    pub range: Range<f32>,
    pub color: Color,
    pub name: String,
}

impl PartialEq for LegendBand {
    fn eq(&self, other: &Self) -> bool {
        self.color == other.color
            && self.name == other.name
            && (self.range.start - other.range.start).abs() <= f32::EPSILON
            && (self.range.end - other.range.end).abs() <= f32::EPSILON
    }
}

impl LegendBand {
    pub fn new(range: Range<f32>, color: Color, name: String) -> Self {
        LegendBand { range, color, name }
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

    pub fn with_equal_bands(band_count: usize, value_range: RangeInclusive<f32>, color_map: &ColorMap) -> Result<Self> {
        if value_range.start() > value_range.end() {
            return Err(Error::InvalidArgument(format!(
                "Invalid banded color mapper value range: start ({}) must be less than end ({})",
                value_range.start(),
                value_range.end()
            )));
        }

        let mut entries = Vec::with_capacity(band_count);
        let band_offset: f32 = (value_range.end() - value_range.start()) / (band_count as f32);
        let mut band_pos = *value_range.start();

        if let ColorMap::ColorList(colors) = color_map {
            if colors.len() != band_count {
                return Err(Error::InvalidArgument("Color list length does not match band count".into()));
            }

            for color in colors.iter().take(band_count) {
                entries.push(LegendBand::new(
                    Range {
                        start: band_pos,
                        end: band_pos + band_offset,
                    },
                    *color,
                    String::default(),
                ));

                band_pos += band_offset;
            }
        } else {
            let cmap = ProcessedColorMap::create(color_map)?;

            let color_offset = if band_count == 1 { 0.0 } else { 1.0 / (band_count as f32 - 1.0) };
            let mut color_pos = 0.0;

            for _ in 0..band_count {
                entries.push(LegendBand::new(
                    Range {
                        start: band_pos,
                        end: band_pos + band_offset,
                    },
                    cmap.get_color(color_pos),
                    String::default(),
                ));

                band_pos += band_offset;
                color_pos += color_offset;
            }
        }

        Ok(Banded { bands: entries })
    }

    pub fn with_manual_ranges(value_ranges: Vec<Range<f32>>, color_map: &ColorMap) -> Result<Self> {
        let band_count = value_ranges.len();
        let mut entries = Vec::with_capacity(band_count);

        if let Some(invalid_range) = value_ranges.iter().find(|r| r.start >= r.end) {
            return Err(Error::InvalidArgument(format!(
                "Invalid range: start ({}) must be less than end ({})",
                invalid_range.start, invalid_range.end
            )));
        }

        if let ColorMap::ColorList(colors) = color_map {
            if colors.len() != value_ranges.len() {
                return Err(Error::InvalidArgument(
                    "Color list length does not match the number of ranges".into(),
                ));
            }

            for (range, color) in value_ranges.into_iter().zip(colors.iter()) {
                entries.push(LegendBand::new(range, *color, String::default()));
            }
        } else {
            let cmap = ProcessedColorMap::create(color_map)?;
            let color_offset = if band_count == 1 { 0.0 } else { 1.0 / (band_count as f32 - 1.0) };
            let mut color_pos = 0.0;

            for range in value_ranges {
                entries.push(LegendBand::new(range, cmap.get_color(color_pos), String::default()));
                color_pos += color_offset;
            }
        }

        Ok(Banded { bands: entries })
    }
}

impl ColorMapper for Banded {
    #[inline]
    fn color_for_numeric_value(&self, value: f32, unmappable_colors: &UnmappableColors) -> Color {
        const EDGE_TOLERANCE: f32 = 1e-4;

        for entry in &self.bands {
            if entry.range.contains(&value) {
                return entry.color;
            }
        }

        if let Some(first_entry) = self.bands.first() {
            if (value - first_entry.range.start).abs() < EDGE_TOLERANCE {
                return first_entry.color;
            } else if value < first_entry.range.start {
                return unmappable_colors.low;
            } else if let Some(last_entry) = self.bands.last() {
                if (value - last_entry.range.end).abs() < EDGE_TOLERANCE {
                    return last_entry.color;
                } else if value > last_entry.range.end {
                    return unmappable_colors.high;
                }
            }
        }

        unmappable_colors.nodata
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
        self.bands.len()
    }

    #[cfg(feature = "simd")]
    #[inline]
    fn color_for_numeric_value_simd(
        &self,
        value: std::simd::Simd<f32, LANES>,
        unmappable_colors: &UnmappableColorsSimd,
    ) -> std::simd::Simd<u32, LANES> {
        use std::simd::{Mask, Simd, cmp::SimdPartialOrd, num::SimdFloat};

        let mut in_range_total = Mask::splat(false);
        let mut colors = unmappable_colors.nodata;

        for entry in &self.bands {
            let start = entry.range.start;
            let end = entry.range.end;

            let in_range = value.simd_ge(Simd::splat(start)) & value.simd_lt(Simd::splat(end));
            let band_color = Simd::splat(entry.color.to_bits());

            in_range_total |= in_range;
            colors = in_range.select(band_color, colors);

            if in_range_total.all() {
                return colors;
            }
        }

        if let Some(first_entry) = self.bands.first() {
            let last_entry = self.bands.last().unwrap_or(first_entry);
            let edge_tolerance = Simd::splat(1e-4);

            let start = first_entry.range.start;
            let end = last_entry.range.end;

            let lower_edge = (value - Simd::splat(start)).abs().simd_lt(edge_tolerance).cast::<i32>();
            let upper_edge = (value - Simd::splat(end)).abs().simd_lt(edge_tolerance).cast::<i32>();
            let out_of_range_low = value.simd_lt(Simd::splat(start)).cast::<i32>();
            let out_of_range_high = value.simd_gt(Simd::splat(end)).cast::<i32>();

            colors = out_of_range_low.select(unmappable_colors.low, colors);
            colors = out_of_range_high.select(unmappable_colors.high, colors);
            colors = lower_edge.select(Simd::splat(first_entry.color.to_bits()), colors);
            colors = upper_edge.select(Simd::splat(last_entry.color.to_bits()), colors);
        }

        colors
    }

    fn value_range(&self) -> RangeInclusive<f32> {
        let start = self.bands.first().map_or(0.0, |b| b.range.start);
        let end = self.bands.last().map_or(0.0, |b| b.range.end);

        start..=end
    }

    fn legend_entries(&self) -> Vec<(Range<f32>, Color)> {
        self.bands.iter().map(|band| (band.range.clone(), band.color)).collect()
    }

    fn compute_unmappable_colors(&self, config: &MappingConfig) -> UnmappableColors {
        let low = config.out_of_range_low_color.unwrap_or_else(|| match self.bands.first() {
            Some(first_entry) => first_entry.color,
            None => config.nodata_color,
        });

        let high = config.out_of_range_high_color.unwrap_or_else(|| match self.bands.last() {
            Some(last_entry) => last_entry.color,
            None => config.nodata_color,
        });

        UnmappableColors {
            nodata: config.nodata_color,
            low,
            high,
        }
    }
}

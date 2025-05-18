use std::ops::Range;

use crate::{
    Color, Result,
    colormap::{ColorMap, ProcessedColorMap},
    legend::{LegendBand, MappingConfig},
};

use super::ColorMapper;

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

    pub fn with_equal_bands(band_count: usize, value_range: Range<f64>, color_map: &ProcessedColorMap) -> Self {
        let color_offset = if band_count == 1 { 0.0 } else { 1.0 / (band_count as f64 - 1.0) };
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

    pub fn with_manual_ranges(value_ranges: Vec<Range<f64>>, color_map: &ColorMap) -> Result<Self> {
        let band_count = value_ranges.len();
        let mut entries = Vec::with_capacity(band_count);

        if let ColorMap::ColorList(colors) = color_map {
            for (range, color) in value_ranges.into_iter().zip(colors.iter()) {
                entries.push(LegendBand::new(range, *color, String::default()));
            }
        } else {
            let cmap = ProcessedColorMap::create(color_map)?;
            let color_offset = if band_count == 1 { 0.0 } else { 1.0 / (band_count as f64 - 1.0) };
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
                return config.out_of_range_low_color.unwrap_or(first_entry.color);
            } else if let Some(last_entry) = self.bands.last() {
                if (value - last_entry.range.end).abs() < EDGE_TOLERANCE {
                    return last_entry.color;
                } else if value > last_entry.range.end {
                    return config.out_of_range_high_color.unwrap_or(last_entry.color);
                }
            }
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
        self.bands.len()
    }
}

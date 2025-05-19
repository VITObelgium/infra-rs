use num::NumCast;

use crate::{
    Result,
    color::Color,
    colormap::{ColorMap, ColorMapDirection, ColorMapPreset, ProcessedColorMap},
    colormapper::{self, ColorMapper},
};
use std::{collections::HashMap, ops::Range};

/// Options for mapping values that can not be mapped by the legend mapper
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Default, Clone, Copy, Debug)]
pub struct MappingConfig {
    /// The color of the nodata pixels
    pub nodata_color: Color,
    /// The color of the values below the lowest value in the colormap
    /// By default, this will be the color of the lower bound of the colormap
    pub out_of_range_low_color: Option<Color>,
    /// The color of the values above the highest value in the colormap
    /// By default, this will be the color of the upper bound of the colormap
    pub out_of_range_high_color: Option<Color>,
    /// Render 0 values as nodata
    pub zero_is_nodata: bool,
}

impl MappingConfig {
    pub fn new(nodata: Color, low: Option<Color>, high: Option<Color>, zero_is_nodata: bool) -> Self {
        MappingConfig {
            nodata_color: nodata,
            out_of_range_low_color: low,
            out_of_range_high_color: high,
            zero_is_nodata,
        }
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Default, Clone, Debug)]
pub struct LegendCategory {
    pub color: Color,
    pub name: String,
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug)]
pub struct LegendBand {
    pub range: Range<f64>,
    pub color: Color,
    pub name: String,
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

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Default, Clone, Debug)]
pub struct MappedLegend<TMapper: ColorMapper> {
    pub title: String,
    pub color_map_name: String,
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
        value.is_nan() || Some(value) == nodata || (self.mapping_config.zero_is_nodata && value == 0.0)
    }

    pub fn color_for_value<T: Copy + num::NumCast>(&self, value: T, nodata: Option<f64>) -> Color {
        let value = value.to_f64().unwrap_or(f64::NAN);
        if self.is_unmappable(value, nodata) {
            return self.mapping_config.nodata_color;
        }

        self.mapper.color_for_numeric_value(value, &self.mapping_config)
    }

    pub fn color_for_opt_value<T: Copy + num::NumCast>(&self, value: Option<T>) -> Color {
        match value {
            Some(v) => self.color_for_value(v, None),
            None => self.mapping_config.nodata_color,
        }
    }

    pub fn color_for_string_value(&self, value: &str) -> Color {
        self.mapper.color_for_string_value(value, &self.mapping_config)
    }

    pub fn apply_to_data<T: Copy + num::NumCast, TNodata: Copy + num::NumCast>(&self, data: &[T], nodata: Option<TNodata>) -> Vec<Color> {
        let nodata = nodata.map(|v| v.to_f64().unwrap_or(f64::NAN));

        data.iter().map(|&value| self.color_for_value(value, nodata)).collect()
    }
}

pub type LinearLegend = MappedLegend<colormapper::Linear>;
pub type BandedLegend = MappedLegend<colormapper::Banded>;
pub type CategoricNumericLegend = MappedLegend<colormapper::CategoricNumeric>;
pub type CategoricStringLegend = MappedLegend<colormapper::CategoricString>;

/// Legend for mapping values to colors, can be linear, banded or categoric
/// Use this when you need to store a legend that can be of any mapping type
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[allow(clippy::large_enum_variant)]
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
            colormapper::Linear::new(
                Range { start: 0.0, end: 255.0 },
                ProcessedColorMap::create_for_preset(ColorMapPreset::Gray, ColorMapDirection::Regular),
            ),
            MappingConfig::default(),
        ))
    }
}

impl Legend {
    pub fn linear(cmap_def: &ColorMap, value_range: Range<f64>, mapping_config: Option<MappingConfig>) -> Result<Self> {
        Ok(Legend::Linear(create_linear(cmap_def, value_range, mapping_config)?))
    }

    pub fn banded(
        category_count: usize,
        cmap_def: &ColorMap,
        value_range: Range<f64>,
        mapping_config: Option<MappingConfig>,
    ) -> Result<Self> {
        Ok(Legend::Banded(create_banded(
            category_count,
            cmap_def,
            value_range,
            mapping_config,
        )?))
    }

    pub fn banded_manual_ranges(cmap_def: &ColorMap, value_range: Vec<Range<f64>>, mapping_config: Option<MappingConfig>) -> Result<Self> {
        Ok(Legend::Banded(create_banded_manual_ranges(cmap_def, value_range, mapping_config)?))
    }

    pub fn categoric_value_range(cmap_def: &ColorMap, value_range: Range<i64>, mapping_config: Option<MappingConfig>) -> Result<Self> {
        Ok(Legend::CategoricNumeric(create_categoric_for_value_range(
            cmap_def,
            value_range,
            mapping_config,
        )?))
    }

    pub fn categoric_value_list(cmap_def: &ColorMap, values: &[i64], mapping_config: Option<MappingConfig>) -> Result<Self> {
        Ok(Legend::CategoricNumeric(create_categoric_for_value_list(
            cmap_def,
            values,
            mapping_config,
        )?))
    }

    pub fn categoric_string(string_map: HashMap<String, LegendCategory>, mapping_config: Option<MappingConfig>) -> Result<Self> {
        Ok(Legend::CategoricString(create_categoric_string(string_map, mapping_config)?))
    }

    pub fn apply<T: Copy + NumCast, TNodata: Copy + num::NumCast>(&self, data: &[T], nodata: Option<TNodata>) -> Vec<Color> {
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

    pub fn color_for_opt_value<T: Copy + num::NumCast>(&self, value: Option<T>) -> Color {
        match self {
            Legend::Linear(legend) => legend.color_for_opt_value(value),
            Legend::Banded(legend) => legend.color_for_opt_value(value),
            Legend::CategoricNumeric(legend) => legend.color_for_opt_value(value),
            Legend::CategoricString(legend) => legend.color_for_opt_value(value),
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
pub fn create_linear(cmap_def: &ColorMap, value_range: Range<f64>, mapping_config: Option<MappingConfig>) -> Result<LinearLegend> {
    Ok(MappedLegend {
        mapper: colormapper::Linear::new(value_range, ProcessedColorMap::create(cmap_def)?),
        color_map_name: cmap_def.name(),
        mapping_config: mapping_config.unwrap_or_default(),
        ..Default::default()
    })
}

/// Create a banded legend where the categories are equally spaced between the value range
/// If the the `ColorMap` is a `ColorMap::ColorList`, the length of the list must match `category_count`
/// Otherwise, the colors will be taken linearly from the colormap
pub fn create_banded(
    category_count: usize,
    cmap_def: &ColorMap,
    value_range: Range<f64>,
    mapping_config: Option<MappingConfig>,
) -> Result<BandedLegend> {
    Ok(MappedLegend {
        mapper: colormapper::Banded::with_equal_bands(category_count, value_range, cmap_def)?,
        color_map_name: cmap_def.name(),
        mapping_config: mapping_config.unwrap_or_default(),
        ..Default::default()
    })
}

/// Create a banded legend where the value ranges are manually configured
/// If the the `ColorMap` is a `ColorMap::ColorList`, the length of the list must match the number of bands
/// Otherwise, the colors will be taken linearly from the colormap
pub fn create_banded_manual_ranges(
    cmap_def: &ColorMap,
    value_ranges: Vec<Range<f64>>,
    mapping_config: Option<MappingConfig>,
) -> Result<BandedLegend> {
    let mapper = colormapper::Banded::with_manual_ranges(value_ranges, cmap_def)?;

    Ok(MappedLegend {
        mapper,
        color_map_name: cmap_def.name(),
        mapping_config: mapping_config.unwrap_or_default(),
        ..Default::default()
    })
}

/// Create a categoric legend where each value in the value range is a category
/// If the the `ColorMap` is a `ColorMap::ColorList`, the length of the list must match the number of values in the range
/// Otherwise, the colors will be taken linearly from the colormap
pub fn create_categoric_for_value_range(
    cmap_def: &ColorMap,
    value_range: Range<i64>,
    mapping_config: Option<MappingConfig>,
) -> Result<CategoricNumericLegend> {
    Ok(MappedLegend {
        mapper: colormapper::CategoricNumeric::for_value_range(value_range, cmap_def)?,
        color_map_name: cmap_def.name(),
        mapping_config: mapping_config.unwrap_or_default(),
        ..Default::default()
    })
}

/// Create a categoric legend based on the provided list of values
/// If the the `ColorMap` is a `ColorMap::ColorList`, the length of the list must match the number of values
/// Otherwise, the colors will be taken linearly from the colormap
pub fn create_categoric_for_value_list(
    cmap_def: &ColorMap,
    values: &[i64],
    mapping_config: Option<MappingConfig>,
) -> Result<CategoricNumericLegend> {
    Ok(MappedLegend {
        mapper: colormapper::CategoricNumeric::for_values(values, cmap_def)?,
        color_map_name: cmap_def.name(),
        mapping_config: mapping_config.unwrap_or_default(),
        ..Default::default()
    })
}

/// Create a categoric legend with string value mapping
pub fn create_categoric_string(
    string_map: HashMap<String, LegendCategory>,
    mapping_config: Option<MappingConfig>,
) -> Result<CategoricStringLegend> {
    Ok(MappedLegend {
        mapper: colormapper::CategoricString::new(string_map),
        mapping_config: mapping_config.unwrap_or_default(),
        ..Default::default()
    })
}

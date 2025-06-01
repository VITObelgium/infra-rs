use num::NumCast;

use crate::{
    Result, cast,
    color::Color,
    colormap::{ColorMap, ColorMapDirection, ColorMapPreset, ProcessedColorMap},
    colormapper::{self, ColorMapper},
};
use std::{
    collections::HashMap,
    ops::{Range, RangeInclusive},
};

#[cfg(feature = "simd")]
use std::simd::{LaneCount, Simd, SimdCast, SimdElement, SupportedLaneCount, cmp::SimdPartialEq, num::SimdFloat};

#[cfg(feature = "simd")]
pub const LANES: usize = crate::simd::LANES;

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

    fn is_unmappable<T: num::Float>(&self, value: T, nodata: Option<T>) -> bool {
        value.is_nan() || Some(value) == nodata || (self.mapping_config.zero_is_nodata && value == T::zero())
    }

    #[cfg(feature = "simd")]
    #[inline]
    fn is_unmappable_simd<const N: usize>(
        &self,
        value: Simd<f32, N>,
        nodata: Option<f32>,
    ) -> <std::simd::Simd<f32, N> as std::simd::cmp::SimdPartialEq>::Mask
    where
        LaneCount<N>: SupportedLaneCount,
    {
        let mut mask = value.is_nan();
        if let Some(nodata) = nodata {
            mask |= value.simd_eq(Simd::splat(nodata));
        }

        if self.mapping_config.zero_is_nodata {
            mask |= value.simd_eq(Simd::splat(0.0));
        }

        mask
    }

    pub fn color_for_value<T: num::NumCast>(&self, value: T, nodata: Option<T>) -> Color {
        let value = value.to_f32().unwrap_or(f32::NAN);
        if self.is_unmappable(value, cast::option(nodata)) {
            return self.mapping_config.nodata_color;
        }

        self.mapper.color_for_numeric_value(value, &self.mapping_config)
    }

    #[cfg(feature = "simd")]
    #[inline]
    pub fn color_for_value_simd<T: num::NumCast + Copy + num::Zero + SimdElement + SimdCast>(
        &self,
        value: &std::simd::Simd<T, LANES>,
        nodata: Option<T>,
        color_buffer: &mut std::simd::Simd<u32, LANES>,
    ) where
        std::simd::Simd<T, LANES>: crate::simd::SimdCastPl<LANES>,
    {
        use crate::simd::SimdCastPl;

        let value: Simd<T, LANES> = value.simd_cast();
        let mappable_mask = !self.is_unmappable_simd(value.simd_cast(), cast::option::<f32>(nodata));
        let colors = self.mapper.color_for_numeric_value_simd(value.simd_cast(), &self.mapping_config);
        colors.store_select(color_buffer.as_mut_array(), mappable_mask);
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

    pub fn apply_to_data<T: Copy + num::NumCast>(&self, data: &[T], nodata: Option<T>) -> Vec<Color> {
        data.iter().map(|&value| self.color_for_value(value, nodata)).collect()
    }

    #[cfg(feature = "simd")]
    pub fn apply_to_data_simd<T: num::NumCast + num::Zero + SimdElement + SimdCast>(&self, data: &[T], nodata: Option<T>) -> Vec<Color>
    where
        std::simd::Simd<T, LANES>: crate::simd::SimdCastPl<LANES>,
    {
        use crate::allocate;

        if !self.mapper.simd_supported() {
            // Not all color mappers can support SIMD, so fall back to scalar processing
            return self.apply_to_data(data, nodata);
        }

        let mut colors = allocate::aligned_vec_filled_with(self.mapping_config.nodata_color.to_bits(), data.len());

        let (head, simd_vals, tail) = data.as_simd();
        let (head_colors, simd_colors, tail_colors) = colors.as_simd_mut();

        assert!(head.len() == head_colors.len(), "Data alignment error");

        // scalar head
        for val in head.iter().zip(head_colors) {
            let color = self.color_for_value(*val.0, nodata);
            *val.1 = color.to_bits();
        }

        // simd body
        for (val_chunk, color_chunk) in simd_vals.iter().zip(simd_colors) {
            self.color_for_value_simd(val_chunk, nodata, color_chunk);
        }

        // scalar tail
        for val in tail.iter().zip(tail_colors) {
            let color = self.color_for_value(*val.0, nodata);
            *val.1 = color.to_bits();
        }

        // SAFETY: colors and data have the same length, and colors is already filled with u32 color bits
        let colors_ptr = colors.as_mut_ptr().cast::<Color>();
        let len = colors.len();
        let capacity = colors.capacity();
        std::mem::forget(colors); // prevent drop of colors Vec<u32>
        unsafe { Vec::from_raw_parts(colors_ptr, len, capacity) }
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
    pub fn linear(cmap_def: &ColorMap, value_range: Range<f32>, mapping_config: Option<MappingConfig>) -> Result<Self> {
        Ok(Legend::Linear(create_linear(cmap_def, value_range, mapping_config)?))
    }

    pub fn banded(
        category_count: usize,
        cmap_def: &ColorMap,
        value_range: RangeInclusive<f32>,
        mapping_config: Option<MappingConfig>,
    ) -> Result<Self> {
        Ok(Legend::Banded(create_banded(
            category_count,
            cmap_def,
            value_range,
            mapping_config,
        )?))
    }

    pub fn banded_manual_ranges(cmap_def: &ColorMap, value_range: Vec<Range<f32>>, mapping_config: Option<MappingConfig>) -> Result<Self> {
        Ok(Legend::Banded(create_banded_manual_ranges(cmap_def, value_range, mapping_config)?))
    }

    pub fn categoric_value_range(
        cmap_def: &ColorMap,
        value_range: RangeInclusive<i64>,
        mapping_config: Option<MappingConfig>,
    ) -> Result<Self> {
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

    pub fn apply<T: Copy + NumCast>(&self, data: &[T], nodata: Option<T>) -> Vec<Color> {
        match self {
            Legend::Linear(legend) => legend.apply_to_data(data, nodata),
            Legend::Banded(legend) => legend.apply_to_data(data, nodata),
            Legend::CategoricNumeric(legend) => legend.apply_to_data(data, nodata),
            Legend::CategoricString(legend) => legend.apply_to_data(data, nodata),
        }
    }

    #[cfg(feature = "simd")]
    pub fn apply_simd<T: Copy + num::Zero + NumCast + std::simd::SimdElement + std::simd::SimdCast>(
        &self,
        data: &[T],
        nodata: Option<T>,
    ) -> Vec<Color>
    where
        std::simd::Simd<T, LANES>: crate::simd::SimdCastPl<LANES>,
    {
        match self {
            Legend::Linear(legend) => legend.apply_to_data_simd(data, nodata),
            Legend::Banded(legend) => legend.apply_to_data_simd(data, nodata),
            Legend::CategoricNumeric(legend) => legend.apply_to_data_simd(data, nodata),
            Legend::CategoricString(legend) => legend.apply_to_data_simd(data, nodata),
        }
    }

    pub fn color_for_value<T: Copy + num::NumCast>(&self, value: T, nodata: Option<T>) -> Color {
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
pub fn create_linear(cmap_def: &ColorMap, value_range: Range<f32>, mapping_config: Option<MappingConfig>) -> Result<LinearLegend> {
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
    value_range: RangeInclusive<f32>,
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
    value_ranges: Vec<Range<f32>>,
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
    value_range: RangeInclusive<i64>,
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

#[cfg(test)]
mod tests {

    use super::*;
    use crate::colormap::ColorMapPreset;

    #[test]
    fn compare_banded_categoric() -> Result<()> {
        const RANGE_WIDTH: i64 = 100;

        // Create a banded and categoric legend which should have the same colors for the same values
        // and verify that the colors match
        let cmap_def = ColorMap::Preset(ColorMapPreset::Blues, ColorMapDirection::Regular);
        let banded = create_banded(RANGE_WIDTH as usize, &cmap_def, 1.0..=RANGE_WIDTH as f32, None)?;
        let categoric = create_categoric_for_value_range(&cmap_def, 1..=RANGE_WIDTH, None)?;

        for value in 1..=RANGE_WIDTH {
            assert_eq!(
                banded.color_for_value(value, None),
                categoric.color_for_value(value, None),
                "Color mismatch for value {value}"
            );
        }

        Ok(())
    }

    #[cfg(feature = "simd")]
    #[test]
    fn banded_legend() -> Result<()> {
        use aligned_vec::AVec;

        const RASTER_SIZE: usize = 34;
        use aligned_vec::CACHELINE_ALIGN;

        let input_data = AVec::<f32, aligned_vec::ConstAlign<CACHELINE_ALIGN>>::from_iter(
            CACHELINE_ALIGN,
            (0..RASTER_SIZE * RASTER_SIZE).map(|v| v as f32),
        );
        let cmap_def = ColorMap::Preset(ColorMapPreset::Blues, ColorMapDirection::Regular);

        let banded = create_banded(10, &cmap_def, 1.0..=(RASTER_SIZE * RASTER_SIZE) as f32, None)?;

        let colors = banded.apply_to_data(&input_data, None);
        let simd_colors = banded.apply_to_data_simd(&input_data, None);

        assert_eq!(colors.len(), input_data.len());
        assert_eq!(simd_colors, colors);

        Ok(())
    }

    #[cfg(feature = "simd")]
    #[test]
    fn linear_legend() -> Result<()> {
        use aligned_vec::AVec;

        const RASTER_SIZE: usize = 4;
        use aligned_vec::CACHELINE_ALIGN;

        let input_data = AVec::<f32, aligned_vec::ConstAlign<CACHELINE_ALIGN>>::from_iter(
            CACHELINE_ALIGN,
            (0..RASTER_SIZE * RASTER_SIZE).map(|v| v as f32),
        );
        let cmap_def = ColorMap::Preset(ColorMapPreset::Blues, ColorMapDirection::Regular);

        let linear = create_linear(&cmap_def, 1.0..(RASTER_SIZE * RASTER_SIZE) as f32, None)?;

        let colors = linear.apply_to_data(&input_data, None);
        let simd_colors = linear.apply_to_data_simd(&input_data, None);

        assert_eq!(colors.len(), input_data.len());
        assert_eq!(simd_colors, colors);

        Ok(())
    }

    #[cfg(feature = "simd")]
    #[test]
    fn categoric_legend() -> Result<()> {
        use aligned_vec::AVec;

        const RASTER_SIZE: usize = 4;
        use aligned_vec::CACHELINE_ALIGN;

        let input_data = AVec::<f32, aligned_vec::ConstAlign<CACHELINE_ALIGN>>::from_iter(
            CACHELINE_ALIGN,
            (0..RASTER_SIZE * RASTER_SIZE).map(|v| v as f32),
        );
        let cmap_def = ColorMap::Preset(ColorMapPreset::Blues, ColorMapDirection::Regular);

        let categoric = create_categoric_for_value_range(&cmap_def, 1..=(RASTER_SIZE * RASTER_SIZE) as i64, None)?;

        let colors = categoric.apply_to_data(&input_data, None);
        let simd_colors = categoric.apply_to_data_simd(&input_data, None);

        assert_eq!(colors.len(), input_data.len());
        assert_eq!(simd_colors, colors);

        Ok(())
    }
}

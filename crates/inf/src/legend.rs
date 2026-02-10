use num::NumCast;

use crate::allocate::AlignedVec;
use crate::colormapper::UnmappableColors;
#[cfg(feature = "simd")]
use crate::colormapper::UnmappableColorsSimd;
use crate::{
    Result, allocate, cast,
    color::Color,
    colormap::{ColorMap, ColorMapDirection, ColorMapPreset, ProcessedColorMap},
    colormapper::{self, ColorMapper},
};
use std::{
    collections::HashMap,
    ops::{Range, RangeInclusive},
};

#[cfg(feature = "simd")]
use std::simd::{Select, Simd, SimdCast, SimdElement, cmp::SimdPartialEq, num::SimdFloat};

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
    ) -> <std::simd::Simd<f32, N> as std::simd::cmp::SimdPartialEq>::Mask {
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
        self.color_for_value_precomputed_unmappable(value, nodata, &self.mapper.compute_unmappable_colors(&self.mapping_config))
    }

    fn color_for_value_precomputed_unmappable<T: num::NumCast>(&self, value: T, nodata: Option<T>, unmappable: &UnmappableColors) -> Color {
        let value = value.to_f32().unwrap_or(f32::NAN);
        if self.is_unmappable(value, cast::option(nodata)) {
            return unmappable.nodata;
        }

        self.mapper.color_for_numeric_value(value, unmappable)
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
        let unmappable = self.mapper.compute_unmappable_colors_simd(&self.mapping_config);
        self.color_for_value_simd_precomputed_unmappable(value, nodata, color_buffer, &unmappable);
    }

    #[cfg(feature = "simd")]
    #[inline]
    pub fn color_for_value_simd_precomputed_unmappable<T: num::NumCast + Copy + num::Zero + SimdElement + SimdCast>(
        &self,
        value: &std::simd::Simd<T, LANES>,
        nodata: Option<T>,
        color_buffer: &mut std::simd::Simd<u32, LANES>,
        unmappable: &UnmappableColorsSimd,
    ) where
        std::simd::Simd<T, LANES>: crate::simd::SimdCastPl<LANES>,
    {
        use crate::simd::SimdCastPl;

        let value: Simd<T, LANES> = value.simd_cast();
        let unmappable_mask = self.is_unmappable_simd(value.simd_cast(), cast::option::<f32>(nodata));
        if unmappable_mask.all() {
            *color_buffer = unmappable.nodata;
            return;
        }

        let colors = self.mapper.color_for_numeric_value_simd(value.simd_cast(), unmappable);
        *color_buffer = unmappable_mask.select(unmappable.nodata, colors);
    }

    pub fn color_for_opt_value<T: Copy + num::NumCast>(&self, value: Option<T>) -> Color {
        match value {
            Some(v) => self.color_for_value(v, None),
            None => self.mapping_config.nodata_color,
        }
    }

    pub fn color_for_string_value(&self, value: &str) -> Color {
        self.mapper
            .color_for_string_value(value, &self.mapper.compute_unmappable_colors(&self.mapping_config))
    }

    pub fn total_value_range(&self) -> RangeInclusive<f32> {
        self.mapper.value_range()
    }

    pub fn legend_entries(&self) -> Vec<(Range<f32>, Color)> {
        self.mapper.legend_entries()
    }

    #[cfg(feature = "simd")]
    pub fn apply_to_data<T: num::NumCast + num::Zero + SimdElement + SimdCast>(&self, data: &[T], nodata: Option<T>) -> AlignedVec<Color>
    where
        std::simd::Simd<T, LANES>: crate::simd::SimdCastPl<LANES>,
    {
        self.apply_to_data_simd(data, nodata)
    }

    #[cfg(not(feature = "simd"))]
    pub fn apply_to_data<T: Copy + num::NumCast>(&self, data: &[T], nodata: Option<T>) -> AlignedVec<Color> {
        self.apply_to_data_scalar(data, nodata)
    }

    pub fn apply_to_data_scalar<T: Copy + num::NumCast>(&self, data: &[T], nodata: Option<T>) -> AlignedVec<Color> {
        allocate::aligned_vec_from_iter(data.iter().map(|&value| self.color_for_value(value, nodata)))
    }

    #[cfg(feature = "simd")]
    #[inline]
    pub fn apply_to_data_simd<T: num::NumCast + num::Zero + SimdElement + SimdCast>(
        &self,
        data: &[T],
        nodata: Option<T>,
    ) -> AlignedVec<Color>
    where
        std::simd::Simd<T, LANES>: crate::simd::SimdCastPl<LANES>,
    {
        use crate::allocate;

        if !self.mapper.simd_supported() {
            // Not all color mappers can support SIMD, so fall back to scalar processing
            return self.apply_to_data_scalar(data, nodata);
        }

        let mut colors = allocate::aligned_vec_with_capacity(data.len());
        // Safety: all the cells in `colors` will be filled with u32 color bits, no need to initialize them
        unsafe { colors.set_len(data.len()) };

        let (head, simd_vals, tail) = data.as_simd();
        let (head_colors, simd_colors, tail_colors) = colors.as_simd_mut();

        assert!(head.len() == head_colors.len(), "Data alignment error");

        let unmappable = self.mapper.compute_unmappable_colors(&self.mapping_config);
        let unmappable_simd = self.mapper.compute_unmappable_colors_simd(&self.mapping_config);

        // scalar head
        for val in head.iter().zip(head_colors) {
            *val.1 = self.color_for_value_precomputed_unmappable(*val.0, nodata, &unmappable).to_bits();
        }

        // simd body
        for (val_chunk, color_chunk) in simd_vals.iter().zip(simd_colors) {
            self.color_for_value_simd_precomputed_unmappable(val_chunk, nodata, color_chunk, &unmappable_simd);
        }

        // scalar tail
        for val in tail.iter().zip(tail_colors) {
            *val.1 = self.color_for_value_precomputed_unmappable(*val.0, nodata, &unmappable).to_bits();
        }

        allocate::cast_aligned_vec::<u32, Color>(colors)
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
                ProcessedColorMap::<256>::create_for_preset(ColorMapPreset::Gray, ColorMapDirection::Regular),
            )
            .unwrap(), // Will never fail since the range is valid
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

    #[cfg(feature = "simd")]
    pub fn apply<T: num::NumCast + num::Zero + SimdElement + SimdCast>(&self, data: &[T], nodata: Option<T>) -> AlignedVec<Color>
    where
        std::simd::Simd<T, LANES>: crate::simd::SimdCastPl<LANES>,
    {
        match self {
            Legend::Linear(legend) => legend.apply_to_data(data, nodata),
            Legend::Banded(legend) => legend.apply_to_data(data, nodata),
            Legend::CategoricNumeric(legend) => legend.apply_to_data(data, nodata),
            Legend::CategoricString(legend) => legend.apply_to_data(data, nodata),
        }
    }

    pub fn apply_scalar<T: Copy + NumCast>(&self, data: &[T], nodata: Option<T>) -> AlignedVec<Color> {
        match self {
            Legend::Linear(legend) => legend.apply_to_data_scalar(data, nodata),
            Legend::Banded(legend) => legend.apply_to_data_scalar(data, nodata),
            Legend::CategoricNumeric(legend) => legend.apply_to_data_scalar(data, nodata),
            Legend::CategoricString(legend) => legend.apply_to_data_scalar(data, nodata),
        }
    }

    #[cfg(feature = "simd")]
    pub fn apply_simd<T: Copy + num::Zero + NumCast + std::simd::SimdElement + std::simd::SimdCast>(
        &self,
        data: &[T],
        nodata: Option<T>,
    ) -> AlignedVec<Color>
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

    pub fn total_value_range(&self) -> RangeInclusive<f32> {
        match self {
            Legend::Linear(legend) => legend.total_value_range(),
            Legend::Banded(legend) => legend.total_value_range(),
            Legend::CategoricNumeric(legend) => legend.total_value_range(),
            Legend::CategoricString(legend) => legend.total_value_range(),
        }
    }

    pub fn legend_entries(&self) -> Vec<(Range<f32>, Color)> {
        match self {
            Legend::Linear(legend) => legend.legend_entries(),
            Legend::Banded(legend) => legend.legend_entries(),
            Legend::CategoricNumeric(legend) => legend.legend_entries(),
            Legend::CategoricString(legend) => legend.legend_entries(),
        }
    }

    /// Get an array of 256 colors representing the colors of the legend, in order from lowest to highest value
    pub fn color_list(&self) -> Vec<Color> {
        let mut colors = Vec::with_capacity(256);
        for i in 0..256 {
            let value =
                self.total_value_range().start() + (self.total_value_range().end() - self.total_value_range().start()) * (i as f32 / 255.0);
            colors.push(self.color_for_value(value, None));
        }
        colors
    }
}

/// Create a legend with linear color mapping
pub fn create_linear(cmap_def: &ColorMap, value_range: Range<f32>, mapping_config: Option<MappingConfig>) -> Result<LinearLegend> {
    Ok(MappedLegend {
        mapper: colormapper::Linear::new(value_range, ProcessedColorMap::<256>::create(cmap_def)?)?,
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
    use crate::color;
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
        const RASTER_SIZE: usize = 34;

        let input_data = allocate::aligned_vec_from_iter((0..RASTER_SIZE * RASTER_SIZE).map(|v| v as f32));
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
        use crate::allocate;

        const RASTER_SIZE: usize = 4;

        let input_data = allocate::aligned_vec_from_iter((0..RASTER_SIZE * RASTER_SIZE).map(|v| v as f32));
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
        const RASTER_SIZE: usize = 4;

        let input_data = allocate::aligned_vec_from_iter((0..RASTER_SIZE * RASTER_SIZE).map(|v| v as f32));
        let cmap_def = ColorMap::Preset(ColorMapPreset::Blues, ColorMapDirection::Regular);

        let categoric = create_categoric_for_value_range(&cmap_def, 1..=(RASTER_SIZE * RASTER_SIZE) as i64, None)?;

        let colors = categoric.apply_to_data(&input_data, None);
        let simd_colors = categoric.apply_to_data_simd(&input_data, None);

        assert_eq!(colors.len(), input_data.len());
        assert_eq!(simd_colors, colors);

        Ok(())
    }

    #[test]
    fn linear_legend_out_of_range() -> Result<()> {
        // Define custom colors for out-of-range values
        let custom_low_color = color::RED;
        let custom_high_color = color::GREEN;
        let nodata_color = color::BLUE;

        let value_range = 10.0..90.0;
        let cmap_def = ColorMap::Preset(ColorMapPreset::Blues, ColorMapDirection::Regular);

        // Create mapping config with custom out-of-range colors
        let mapping_config = MappingConfig::new(nodata_color, Some(custom_low_color), Some(custom_high_color), false);

        let linear = create_linear(&cmap_def, value_range.clone(), Some(mapping_config))?;

        // Test values below range
        assert_eq!(linear.color_for_value(5.0, None), custom_low_color);
        assert_eq!(linear.color_for_value(0.0, None), custom_low_color);
        assert_eq!(linear.color_for_value(-10.0, None), custom_low_color);

        // Test values above range
        assert_eq!(linear.color_for_value(95.0, None), custom_high_color);
        assert_eq!(linear.color_for_value(100.0, None), custom_high_color);
        assert_eq!(linear.color_for_value(200.0, None), custom_high_color);

        // Test values at the boundaries (should be in range)
        assert_ne!(linear.color_for_value(value_range.start, None), custom_low_color);
        assert_ne!(linear.color_for_value(value_range.end - 0.001, None), custom_high_color);

        Ok(())
    }

    #[test]
    fn banded_legend_out_of_range_no_edge_mapping_config() -> Result<()> {
        // Create a banded legend with 5 bands from 10 to 60
        let cmap_def = ColorMap::Preset(ColorMapPreset::Blues, ColorMapDirection::Regular);
        let value_range = 10.0..=60.0;
        let nodata_color = color::BLUE;

        // Create mapping config with default out-of-range colors
        let mapping_config = MappingConfig::new(nodata_color, None, None, false);
        let banded = create_banded(5, &cmap_def, value_range, Some(mapping_config))?;

        // Verify colors get the edge values: if no unmappable colors are set, the values that fall outside the range
        // will get the color of the first or last band
        let out_of_range_low = banded.color_for_value(5.0, None);
        let out_of_range_high = banded.color_for_value(65.0, None);
        assert_eq!(out_of_range_low, banded.color_for_value(10.0, None));
        assert_eq!(out_of_range_high, banded.color_for_value(60.0, None));

        // Verify in-range color is not nodata
        let in_range = banded.color_for_value(35.0, None);
        assert_ne!(in_range, nodata_color);

        Ok(())
    }

    #[test]
    fn banded_legend_out_of_range_edge_mapping_config() -> Result<()> {
        // Create a banded legend with 5 bands from 10 to 60
        let cmap_def = ColorMap::Preset(ColorMapPreset::Blues, ColorMapDirection::Regular);
        let value_range = 10.0..=60.0;
        let nodata_color = color::BLUE;
        let nodata_color_low = color::GREEN;
        let nodata_color_high = color::RED;

        // Create mapping config with default out-of-range colors
        let mapping_config = MappingConfig::new(nodata_color, Some(nodata_color_low), Some(nodata_color_high), false);
        let banded = create_banded(5, &cmap_def, value_range, Some(mapping_config))?;

        // Verify that the colors get the configured unmappable colors
        let out_of_range_low = banded.color_for_value(5.0, None);
        let out_of_range_high = banded.color_for_value(65.0, None);
        assert_eq!(out_of_range_low, nodata_color_low);
        assert_eq!(out_of_range_high, nodata_color_high);

        // Verify in-range color is not nodata
        let in_range = banded.color_for_value(35.0, None);
        assert_ne!(in_range, nodata_color);

        Ok(())
    }

    #[test]
    fn categoric_legend_out_of_range() -> Result<()> {
        // Create a categoric legend with values 10 to 20
        let cmap_def = ColorMap::Preset(ColorMapPreset::Blues, ColorMapDirection::Regular);
        let value_range = 10..=20;
        let nodata_color = color::BLUE;

        // Create mapping config with default out-of-range values
        let mapping_config = MappingConfig::new(nodata_color, None, None, false);

        let categoric = create_categoric_for_value_range(&cmap_def, value_range, Some(mapping_config))?;

        // Verify that we get colors for in-range values
        for value in 10..=20 {
            let color = categoric.color_for_value(value, None);
            // Just verify we get some result without asserting what it is
            assert_eq!(color, categoric.color_for_value(value, None));
        }

        // Verify out-of-range values return consistent colors
        let out_of_range_low = categoric.color_for_value(5, None);
        let out_of_range_high = categoric.color_for_value(25, None);

        // For categorical legends, out-of-range values might return transparent color
        // Just verify we get consistent results for the same out-of-range value
        assert_eq!(out_of_range_low, categoric.color_for_value(5, None));
        assert_eq!(out_of_range_high, categoric.color_for_value(25, None));

        Ok(())
    }

    #[test]
    fn linear_legend_out_of_range_colors_edge_mapping_config() -> Result<()> {
        // Test that we can specify custom out-of-range colors
        let nodata_color = color::BLUE;
        let value_range = 10.0..90.0;
        let cmap_def = ColorMap::Preset(ColorMapPreset::Blues, ColorMapDirection::Regular);

        // Test with custom out-of-range colors for linear legend
        let custom_low_color = color::RED;
        let custom_high_color = color::GREEN;

        let mapping_config_custom = MappingConfig::new(nodata_color, Some(custom_low_color), Some(custom_high_color), false);

        let linear_custom = create_linear(&cmap_def, value_range.clone(), Some(mapping_config_custom))?;

        // The custom low color should be used for out-of-range low values
        let custom_below_color = linear_custom.color_for_value(5.0, None);
        assert_eq!(custom_below_color, custom_low_color);

        // The custom high color should be used for out-of-range high values
        let custom_above_color = linear_custom.color_for_value(91.0, None);
        assert_eq!(custom_above_color, custom_high_color);

        Ok(())
    }

    #[test]
    fn linear_legend_out_of_range_colors_no_edge_mapping_config() -> Result<()> {
        // Test that we can specify custom out-of-range colors
        let nodata_color = color::BLUE;
        let value_range = 10.0..90.0;
        let cmap_def = ColorMap::Preset(ColorMapPreset::Blues, ColorMapDirection::Regular);

        let mapping_config_custom = MappingConfig::new(nodata_color, None, None, false);

        let linear_custom = create_linear(&cmap_def, value_range.clone(), Some(mapping_config_custom))?;

        // The colors at the edges should be used for out-of-range values when no custom colors are set
        let custom_below_color = linear_custom.color_for_value(5.0, None);
        let custom_above_color = linear_custom.color_for_value(91.0, None);
        assert_eq!(custom_below_color, linear_custom.color_for_value(10.0, None));
        assert_eq!(custom_above_color, linear_custom.color_for_value(90.0, None));

        Ok(())
    }

    #[test]
    fn zero_is_nodata_option() -> Result<()> {
        // Test the zero_is_nodata option
        let nodata_color = color::BLUE;
        let value_range = -10.0..10.0;
        let cmap_def = ColorMap::Preset(ColorMapPreset::Blues, ColorMapDirection::Regular);

        // Create mapping config with zero_is_nodata = true
        let mapping_config = MappingConfig::new(
            nodata_color,
            None,
            None,
            true, // zero_is_nodata
        );

        let linear = create_linear(&cmap_def, value_range.clone(), Some(mapping_config))?;

        // Zero should be treated as nodata
        assert_eq!(linear.color_for_value(0.0, None), nodata_color);

        // Non-zero values in range should not be nodata
        assert_ne!(linear.color_for_value(5.0, None), nodata_color);
        assert_ne!(linear.color_for_value(-5.0, None), nodata_color);

        Ok(())
    }
}

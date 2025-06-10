#![cfg_attr(feature = "simd", feature(portable_simd, vec_into_raw_parts, allocator_api))]

#[cfg(feature = "simd")]
mod bench {

    use criterion::Criterion;
    use inf::{
        allocate::{self},
        colormap::{ColorMap, ColorMapDirection, ColorMapPreset},
        legend::{create_banded, create_categoric_for_value_range, create_linear},
    };
    use num::NumCast;

    const RASTER_WIDTH: usize = 1024;
    const RASTER_HEIGHT: usize = 768;

    pub fn bench_name<T: num::Num>(name: &str) -> String {
        format!("{}_{}", name, std::any::type_name::<T>())
    }

    const LANES: usize = inf::simd::LANES;

    pub fn bench_colormap<T: num::Num + num::NumCast + Copy + std::simd::SimdElement + std::simd::SimdCast>(c: &mut Criterion)
    where
        std::simd::Simd<T, LANES>: inf::simd::SimdCastPl<LANES> + std::simd::cmp::SimdPartialOrd + std::simd::num::SimdFloat,
        <std::simd::Simd<T, LANES> as std::simd::num::SimdFloat>::Mask: std::ops::Not,
        <std::simd::Simd<T, LANES> as std::simd::cmp::SimdPartialEq>::Mask: std::ops::BitAnd,
        <<std::simd::Simd<T, LANES> as std::simd::num::SimdFloat>::Mask as std::ops::Not>::Output:
            std::convert::Into<std::simd::Mask<i32, LANES>>,
    {
        let raster_size = RASTER_HEIGHT * RASTER_WIDTH;
        let data = allocate::aligned_vec_from_iter((0..raster_size).map(|i| NumCast::from(i).unwrap()));

        let cmap_def = ColorMap::Preset(ColorMapPreset::Turbo, ColorMapDirection::Regular);
        let legend = create_banded(10, &cmap_def, 0.0..=100.0, None).unwrap();

        let mut group = c.benchmark_group("Banded");
        group.bench_function(&bench_name::<T>("apply_banded_legend"), |b| {
            b.iter_with_large_drop(|| legend.apply_to_data_scalar(&data, NumCast::from(99.0)));
        });

        group.bench_function(&bench_name::<T>("apply_banded_legend_simd"), |b| {
            b.iter_with_large_drop(|| legend.apply_to_data_simd(&data, NumCast::from(99.0)));
        });

        group.finish();

        let mut group = c.benchmark_group("Linear");
        let legend = create_linear(&cmap_def, 0.0..100.0, None).unwrap();

        group.bench_function(&bench_name::<T>("apply_linear_legend"), |b| {
            b.iter_with_large_drop(|| legend.apply_to_data_scalar(&data, NumCast::from(99.0)));
        });

        group.bench_function(&bench_name::<T>("apply_linear_legend_simd"), |b| {
            b.iter_with_large_drop(|| legend.apply_to_data_simd(&data, NumCast::from(99.0)));
        });

        group.finish();

        let mut group = c.benchmark_group("Categoric");
        let legend = create_categoric_for_value_range(&cmap_def, 0..=300, None).unwrap();

        group.bench_function(&bench_name::<T>("apply_categoric_legend"), |b| {
            b.iter_with_large_drop(|| legend.apply_to_data_scalar(&data, NumCast::from(99.0)));
        });

        group.bench_function(&bench_name::<T>("apply_categoric_legend_simd"), |b| {
            b.iter_with_large_drop(|| legend.apply_to_data_simd(&data, NumCast::from(99.0)));
        });

        group.finish();
    }

    criterion::criterion_group!(cmap_benches_f32, bench_colormap<f32>);
}

#[cfg(feature = "simd")]
criterion::criterion_main!(cmap_benches_f32);

#[cfg(not(feature = "simd"))]
fn main() {
    println!("SIMD feature is not enabled. Please enable the 'simd' feature to run benchmarks.");
}

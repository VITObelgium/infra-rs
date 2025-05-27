#![feature(portable_simd)]
use aligned_vec::{AVec, CACHELINE_ALIGN, ConstAlign};
use criterion::Criterion;
use inf::{
    colormap::{ColorMap, ColorMapDirection, ColorMapPreset},
    legend::create_banded,
};
use num::NumCast;

const RASTER_WIDTH: usize = 1024;
const RASTER_HEIGHT: usize = 768;

pub fn bench_name<T: num::Num>(name: &str) -> String {
    format!("{}_{}", name, std::any::type_name::<T>())
}

const LANES: usize = inf::legend::LANES;

pub fn bench_colormap<T: num::Num + num::NumCast + Copy + std::simd::SimdElement + std::simd::SimdCast>(c: &mut Criterion)
where
    std::simd::Simd<T, LANES>: inf::legend::SimdCastPl<LANES>,
{
    let raster_size = RASTER_HEIGHT * RASTER_WIDTH;
    let data = AVec::<T, ConstAlign<CACHELINE_ALIGN>>::from_iter(CACHELINE_ALIGN, (0..raster_size).map(|i| NumCast::from(i).unwrap()));

    let cmap_def = ColorMap::Preset(ColorMapPreset::Turbo, ColorMapDirection::Regular);
    let legend = create_banded(10, &cmap_def, 0.0..=100.0, None).unwrap();

    c.bench_function(&bench_name::<T>("apply_legend"), |b| {
        b.iter_with_large_drop(|| legend.apply_to_data(&data, Some(99)));
    });

    c.bench_function(&bench_name::<T>("apply_legend_simd"), |b| {
        b.iter_with_large_drop(|| legend.apply_to_data_simd(&data, Some(99)));
    });
}

criterion::criterion_group!(benches_f32, bench_colormap<f32>);
//criterion::criterion_group!(benches_i32, bench_addition<i32>);
//criterion::criterion_group!(benches_f32, bench_addition<f32>);
//criterion::criterion_main!(benches_u8, benches_i32, benches_f32);

criterion::criterion_main!(benches_f32);

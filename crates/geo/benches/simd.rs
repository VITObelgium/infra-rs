#![cfg_attr(feature = "simd", feature(portable_simd))]
use criterion::{BatchSize, Criterion};

#[cfg(feature = "simd")]
use geo::NodataSimd;
use geo::{
    Array, ArrayInterop as _, ArrayNum, Columns, GeoReference, RasterSize, Rows,
    raster::{DenseRaster, algo},
};
use num::NumCast;

const RASTER_WIDTH: Columns = Columns(1024);
const RASTER_HEIGHT: Rows = Rows(768);

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

pub fn bench_name<T: ArrayNum>(name: &str) -> String {
    #[cfg(feature = "simd")]
    return format!("{}_{:?}_simd", name, T::TYPE);
    #[cfg(not(feature = "simd"))]
    return format!("{}_{:?}", name, T::TYPE);
}

#[simd_macro::simd_bounds]
pub fn simd<T: ArrayNum>(c: &mut Criterion) {
    let raster_size = RasterSize::with_rows_cols(RASTER_HEIGHT, RASTER_WIDTH);
    let geo_ref = GeoReference::without_spatial_reference(raster_size, Some(5.0));

    let create_raster = || DenseRaster::<T>::filled_with(NumCast::from(4.0), geo_ref.clone());

    c.bench_function(&bench_name::<T>("init_nodata"), |b| {
        b.iter_batched_ref(create_raster, |lhs| lhs.init_nodata(), BatchSize::LargeInput);
    });

    c.bench_function(&bench_name::<T>("restore_nodata"), |b| {
        b.iter_batched_ref(create_raster, |lhs| lhs.restore_nodata(), BatchSize::LargeInput);
    });
}

pub fn min_max_f32(c: &mut Criterion) {
    let raster_size = RasterSize::with_rows_cols(RASTER_HEIGHT, RASTER_WIDTH);
    let geo_ref = GeoReference::without_spatial_reference(raster_size, Some(5.0));

    let create_raster =
        || DenseRaster::<f32>::from_iter_opt(geo_ref.clone(), (0..RASTER_WIDTH * RASTER_HEIGHT).map(|x| Some(x as f32))).unwrap();
    let mut group = c.benchmark_group("MinMax");

    group.bench_function("min_max", |b| {
        b.iter_batched_ref(
            create_raster,
            |lhs| {
                let min_max = algo::min_max(lhs);
                assert!(min_max.start < min_max.end);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("min_max_simd", |b| {
        b.iter_batched_ref(
            create_raster,
            |lhs| {
                let min_max = algo::simd::min_max(lhs);
                assert!(min_max.start < min_max.end);
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("min_simd", |b| {
        b.iter_batched_ref(
            create_raster,
            |lhs| {
                let min = algo::simd::min(lhs);
                assert!(min == 0.0);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion::criterion_group!(benches_i32, simd<i32>);
criterion::criterion_group!(benches_f32, simd<f32>);
criterion::criterion_group!(algobenches_f32, min_max_f32);
criterion::criterion_main!(algobenches_f32);

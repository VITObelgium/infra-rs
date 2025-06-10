#![cfg_attr(feature = "simd", feature(portable_simd))]

#[cfg(feature = "simd")]
mod bench {
    use criterion::{BatchSize, Criterion};

    #[cfg(feature = "simd")]
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

    #[simd_macro::geo_simd_bounds]
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

    pub fn min_max(c: &mut Criterion) {
        let raster_size = RasterSize::with_rows_cols(RASTER_HEIGHT, RASTER_WIDTH);
        let geo_ref = GeoReference::without_spatial_reference(raster_size, Some(5.0));

        let create_f32_raster =
            || DenseRaster::<f32>::from_iter_opt(geo_ref.clone(), (0..RASTER_WIDTH * RASTER_HEIGHT).map(|x| Some(x as f32))).unwrap();
        let create_i32_raster =
            || DenseRaster::<i32>::from_iter_opt(geo_ref.clone(), (0..RASTER_WIDTH * RASTER_HEIGHT).map(|x| Some(x as i32))).unwrap();
        let mut group = c.benchmark_group("MinMax");

        group.bench_function("min_max", |b| {
            b.iter_batched_ref(
                create_f32_raster,
                |lhs| {
                    let min_max = algo::min_max(lhs).unwrap();
                    assert!(min_max.start() < min_max.end());
                },
                BatchSize::LargeInput,
            );
        });

        group.bench_function("min_max_simd_f32", |b| {
            b.iter_batched_ref(
                create_f32_raster,
                |lhs| {
                    if let Some(min_max) = algo::simd::min_max(lhs) {
                        assert!(min_max.start() < min_max.end());
                    }
                },
                BatchSize::LargeInput,
            );
        });

        group.bench_function("min_max_simd_i32", |b| {
            b.iter_batched_ref(
                create_i32_raster,
                |lhs| {
                    if let Some(min_max) = algo::simd::min_max(lhs) {
                        assert!(min_max.start() < min_max.end());
                    }
                },
                BatchSize::LargeInput,
            );
        });

        group.bench_function("min_simd_f32", |b| {
            b.iter_batched_ref(
                create_f32_raster,
                |lhs| {
                    let min = algo::simd::min(lhs);
                    assert!(min == Some(0.0));
                },
                BatchSize::LargeInput,
            );
        });

        group.bench_function("min_simd_i32", |b| {
            b.iter_batched_ref(
                create_i32_raster,
                |lhs| {
                    let min = algo::simd::min(lhs);
                    assert!(min == Some(0));
                },
                BatchSize::LargeInput,
            );
        });

        group.finish();
    }

    pub fn filter(c: &mut Criterion) {
        let raster_size = RasterSize::with_rows_cols(RASTER_HEIGHT, RASTER_WIDTH);
        let geo_ref = GeoReference::without_spatial_reference(raster_size, Some(5.0));

        let create_f32_raster =
            || DenseRaster::<f32>::from_iter_opt(geo_ref.clone(), (0..RASTER_WIDTH * RASTER_HEIGHT).map(|x| Some(x as f32))).unwrap();
        let mut group = c.benchmark_group("Filter single value");

        group.bench_function("filter_value", |b| {
            b.iter_batched_ref(
                create_f32_raster,
                |lhs| {
                    algo::filter_value(lhs, 3.0);
                },
                BatchSize::LargeInput,
            );
        });

        group.bench_function("filter_value_simd", |b| {
            b.iter_batched_ref(
                create_f32_raster,
                |lhs| {
                    algo::simd::filter_value(lhs, 3.0);
                },
                BatchSize::LargeInput,
            );
        });

        group.finish();
        let mut group = c.benchmark_group("Filter multiple value");

        group.bench_function("filter", |b| {
            b.iter_batched_ref(
                create_f32_raster,
                |lhs| {
                    algo::filter(lhs, &[1.0, 2.0, 3.0, 10.0, 11.0]);
                },
                BatchSize::LargeInput,
            );
        });

        group.bench_function("filter_simd", |b| {
            b.iter_batched_ref(
                create_f32_raster,
                |lhs| {
                    algo::simd::filter(lhs, &[1.0, 2.0, 3.0, 10.0, 11.0]);
                },
                BatchSize::LargeInput,
            );
        });

        group.finish();
    }

    criterion::criterion_group!(benches_i32, simd<i32>);
    criterion::criterion_group!(benches_f32, simd<f32>);
    criterion::criterion_group!(algobenches_f32, min_max, filter);
    criterion::criterion_main!(algobenches_f32);
}

#[cfg(feature = "simd")]
criterion::criterion_main!(bench::algobenches_f32);

#[cfg(not(feature = "simd"))]
fn main() {
    println!("SIMD feature is not enabled. Please enable the 'simd' feature to run benchmarks.");
}

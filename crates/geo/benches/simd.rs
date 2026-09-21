mod bench {
    use criterion::{BatchSize, Criterion};

    use std::ops::RangeInclusive;

    use geo::{
        Array, ArrayMetadata as _, ArrayNum, Columns, GeoReference, Nodata as _, RasterScale, RasterSize, Rows,
        raster::algo::Scale as _,
        raster::{DenseRaster, algo},
    };
    use num::NumCast;

    const RASTER_WIDTH: Columns = Columns(1024);
    const RASTER_HEIGHT: Rows = Rows(768);

    fn min_max(c: &mut Criterion) {
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

    fn filter(c: &mut Criterion) {
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

    macro_rules! scalar_scale {
        ($fn_name:ident, $src_type:ty, $dest_type:ty) => {
            fn $fn_name(raster: &DenseRaster<$src_type>, input_range: Option<RangeInclusive<$src_type>>) -> DenseRaster<$dest_type> {
                let range = input_range
                    .or_else(|| algo::min_max(raster))
                    .expect("benchmark raster contains data");
                let range = inf::cast::inclusive_range::<f64>(range).unwrap();
                let dest_min = 0.0;
                let dest_max = <$dest_type as ArrayNum>::TYPE.default_nodata_value() - 1.0;
                let input_range = range.end() - range.start();
                let output_range = dest_max - dest_min;
                let scale = if input_range > 0.0 { input_range / output_range } else { 1.0 };
                let offset = range.start() - (dest_min * scale);

                let mut output = inf::allocate::AlignedVecUnderConstruction::<$dest_type>::new(raster.len());
                for (value, output) in raster.iter_opt().zip(unsafe { output.as_slice_mut() }) {
                    *output = match value {
                        Some(value) => {
                            let value: f64 = NumCast::from(value).unwrap();
                            ((value - offset) / scale).max(dest_min).min(dest_max).round() as $dest_type
                        }
                        None => <$dest_type>::NODATA,
                    };
                }

                let metadata = raster.metadata().clone().with_scale(RasterScale { scale, offset });
                DenseRaster::new(metadata, unsafe { output.assume_init() }).unwrap()
            }
        };
    }

    scalar_scale!(scale_f64_to_u8_scalar, f64, u8);
    scalar_scale!(scale_f32_to_u8_scalar, f32, u8);
    scalar_scale!(scale_f64_to_u16_scalar, f64, u16);
    scalar_scale!(scale_f32_to_u16_scalar, f32, u16);

    fn scale(c: &mut Criterion) {
        let raster_size = RasterSize::with_rows_cols(RASTER_HEIGHT, RASTER_WIDTH);
        let geo_ref = GeoReference::without_spatial_reference(raster_size, Some(f64::NAN));

        let create_f64_raster = || {
            DenseRaster::<f64>::from_iter_opt(
                geo_ref.clone(),
                (0..RASTER_WIDTH * RASTER_HEIGHT).map(|x| if x % 100 == 0 { None } else { Some((x as f64) * 0.1 - 5000.0) }),
            )
            .unwrap()
        };

        let create_f32_raster = || {
            DenseRaster::<f32>::from_iter_opt(
                geo_ref.clone(),
                (0..RASTER_WIDTH * RASTER_HEIGHT).map(|x| if x % 100 == 0 { None } else { Some((x as f32) * 0.1 - 5000.0) }),
            )
            .unwrap()
        };

        let mut group = c.benchmark_group("Scale");

        // f64 -> u8 benchmarks
        group.bench_function("scale_to_u8_f64_scalar", |b| {
            b.iter_batched_ref(
                create_f64_raster,
                |raster| scale_f64_to_u8_scalar(raster, None),
                BatchSize::LargeInput,
            );
        });

        group.bench_function("scale_to_u8_f64_simd", |b| {
            b.iter_batched_ref(
                create_f64_raster,
                |raster| {
                    let _: DenseRaster<u8> = raster.scale(None).unwrap();
                },
                BatchSize::LargeInput,
            );
        });

        // f32 -> u8 benchmarks
        group.bench_function("scale_to_u8_f32_scalar", |b| {
            b.iter_batched_ref(
                create_f32_raster,
                |raster| scale_f32_to_u8_scalar(raster, None),
                BatchSize::LargeInput,
            );
        });

        let range = algo::min_max(&create_f32_raster()).unwrap();

        group.bench_function("scale_to_u8_f32_rangeinput_scalar", |b| {
            b.iter_batched_ref(
                create_f32_raster,
                |raster| scale_f32_to_u8_scalar(raster, Some(range.clone())),
                BatchSize::LargeInput,
            );
        });

        group.bench_function("scale_to_u8_f32_simd", |b| {
            b.iter_batched_ref(
                create_f32_raster,
                |raster| {
                    let _: DenseRaster<u8> = raster.scale(None).unwrap();
                },
                BatchSize::LargeInput,
            );
        });

        group.bench_function("scale_to_u8_f32_rangeinput_simd", |b| {
            b.iter_batched_ref(
                create_f32_raster,
                |raster| {
                    let _: DenseRaster<u8> = raster.scale(Some(range.clone())).unwrap();
                },
                BatchSize::LargeInput,
            );
        });

        // f64 -> u16 benchmarks
        group.bench_function("scale_to_u16_f64_scalar", |b| {
            b.iter_batched_ref(
                create_f64_raster,
                |raster| scale_f64_to_u16_scalar(raster, None),
                BatchSize::LargeInput,
            );
        });

        group.bench_function("scale_to_u16_f64_simd", |b| {
            b.iter_batched_ref(
                create_f64_raster,
                |raster| {
                    let _: DenseRaster<u16> = raster.scale(None).unwrap();
                },
                BatchSize::LargeInput,
            );
        });

        // f32 -> u16 benchmarks
        group.bench_function("scale_to_u16_f32_scalar", |b| {
            b.iter_batched_ref(
                create_f32_raster,
                |raster| scale_f32_to_u16_scalar(raster, None),
                BatchSize::LargeInput,
            );
        });

        group.bench_function("scale_to_u16_f32_simd", |b| {
            b.iter_batched_ref(
                create_f32_raster,
                |raster| {
                    let _: DenseRaster<u16> = raster.scale(None).unwrap();
                },
                BatchSize::LargeInput,
            );
        });

        group.finish();
    }

    criterion::criterion_group!(algobenches_f32, min_max, filter, scale);
}

criterion::criterion_main!(bench::algobenches_f32);

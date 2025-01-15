use criterion::{BatchSize, Criterion};
use num::NumCast;
use raster::{DenseRaster, Raster, RasterCreation, RasterNum, RasterSize};

const RASTER_WIDTH: usize = 1024;
const RASTER_HEIGHT: usize = 768;

pub fn bench_name<T: RasterNum<T>>(name: &str) -> String {
    format!("{}_{:?}", name, T::TYPE)
}

pub fn bench_addition<T: RasterNum<T>>(c: &mut Criterion) {
    let raster_size = RasterSize::with_rows_cols(RASTER_HEIGHT, RASTER_WIDTH);
    let rhs = DenseRaster::<T>::filled_with(NumCast::from(9.0).unwrap(), raster_size);

    let create_raster = || DenseRaster::<T>::filled_with(NumCast::from(4.0).unwrap(), raster_size);

    c.bench_function(&bench_name::<T>("raster_ops_add"), |b| {
        b.iter_batched_ref(create_raster, |lhs| *lhs += &rhs, BatchSize::LargeInput);
    });

    c.bench_function(&bench_name::<T>("raster_ops_mul"), |b| {
        b.iter_batched_ref(create_raster, |lhs| *lhs *= &rhs, BatchSize::LargeInput);
    });

    c.bench_function(&bench_name::<T>("raw_ops_mul"), |b| {
        b.iter_batched_ref(
            create_raster,
            |lhs| lhs.iter_mut().zip(rhs.iter()).for_each(|(l, r)| *l *= *r),
            BatchSize::LargeInput,
        );
    });

    c.bench_function(&bench_name::<T>("rawer_ops_mul"), |b| {
        b.iter_batched_ref(
            create_raster,
            |lhs| {
                let lhs = lhs.as_mut_slice();
                let rhs = rhs.as_slice();
                for i in 0..lhs.len() {
                    lhs[i] *= rhs[i];
                }
            },
            BatchSize::LargeInput,
        );
    });
}

criterion::criterion_group!(benches_i32, bench_addition<i32>);
criterion::criterion_group!(benches_f32, bench_addition<f32>);
criterion::criterion_main!(benches_i32, benches_f32);

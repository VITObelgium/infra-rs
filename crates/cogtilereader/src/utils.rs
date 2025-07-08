pub trait HorizontalUnpredictable {
    fn unpredict_horizontal<T>(&self, v2: Self) -> Self;
}

macro_rules! impl_horizontal_unpredictable_for_int {
    ($($t:ty),*) => {
        $(
            impl HorizontalUnpredictable for $t {
                fn unpredict_horizontal<T>(&self, prev: Self) -> Self {
                    self.wrapping_add(prev)
                }
            }
        )*
    };
}

macro_rules! impl_horizontal_unpredictable_for_fp {
    ($($t:ty),*) => {
        $(
            impl HorizontalUnpredictable for $t {
                fn unpredict_horizontal<T>(&self, prev: Self) -> Self {
                    self + prev
                }
            }
        )*
    };
}

impl_horizontal_unpredictable_for_int!(u8, u16, u32, u64, i8, i16, i32, i64);
impl_horizontal_unpredictable_for_fp!(f32, f64);

pub fn unpredict_horizontal<T: HorizontalUnpredictable + Copy>(data: &mut [T], tile_size: i32) {
    for row in data.chunks_mut(tile_size as usize) {
        for i in 1..row.len() {
            row[i] = row[i].unpredict_horizontal::<T>(row[i - 1]);
        }
    }
}

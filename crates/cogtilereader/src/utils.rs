use inf::allocate::{self, AlignedVec};

pub trait HorizontalUnpredictable {
    fn unpredict_horizontal(&self, v2: Self) -> Self;
}

macro_rules! impl_horizontal_unpredictable_for_int {
    ($($t:ty),*) => {
        $(
            impl HorizontalUnpredictable for $t {
                fn unpredict_horizontal(&self, prev: Self) -> Self {
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
                fn unpredict_horizontal(&self, prev: Self) -> Self {
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
            row[i] = row[i].unpredict_horizontal(row[i - 1]);
        }
    }
}

fn decode_delta_bytes(data: &mut [u8], bytes_per_pixel: usize, tile_size: i32) {
    for row in data.chunks_mut(bytes_per_pixel * tile_size as usize) {
        for i in 1..row.len() {
            row[i] = row[i].unpredict_horizontal(row[i - 1]);
        }
    }
}

pub fn unpredict_fp32(data: &mut [f32], tile_size: i32) -> AlignedVec<f32> {
    let bytes = unsafe { std::slice::from_raw_parts_mut(data.as_mut_ptr().cast::<u8>(), std::mem::size_of_val(data)) };
    debug_assert_eq!(bytes.len(), (tile_size * tile_size) as usize * std::mem::size_of::<f32>());
    decode_delta_bytes(bytes, std::mem::size_of::<f32>(), tile_size);

    let tile_size = tile_size as usize;
    let mut output = allocate::aligned_vec_with_capacity(data.len());

    for row in bytes.chunks_mut(std::mem::size_of::<f32>() * tile_size) {
        for i in 0..tile_size {
            output.push(f32::from_be_bytes([
                row[i],
                row[tile_size + i],
                row[tile_size * 2 + i],
                row[tile_size * 3 + i],
            ]));
        }
    }

    output
}

pub fn unpredict_fp64(data: &mut [f64], tile_size: i32) -> AlignedVec<f64> {
    let bytes = unsafe { std::slice::from_raw_parts_mut(data.as_mut_ptr().cast::<u8>(), std::mem::size_of_val(data)) };
    debug_assert_eq!(bytes.len(), (tile_size * tile_size) as usize * std::mem::size_of::<f64>());
    decode_delta_bytes(bytes, std::mem::size_of::<f64>(), tile_size);

    let tile_size = tile_size as usize;
    let mut output = allocate::aligned_vec_with_capacity(data.len());
    for row in bytes.chunks_mut(std::mem::size_of::<f64>() * tile_size) {
        for i in 0..tile_size {
            output.push(f64::from_be_bytes([
                row[i],
                row[tile_size + i],
                row[tile_size * 2 + i],
                row[tile_size * 3 + i],
                row[tile_size * 4 + i],
                row[tile_size * 5 + i],
                row[tile_size * 6 + i],
                row[tile_size * 7 + i],
            ]));
        }
    }

    output
}

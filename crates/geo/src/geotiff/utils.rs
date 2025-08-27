use crate::ArrayNum;

macro_rules! impl_horizontal_unpredictable_for_int {
    ($($t:ty),*) => {
        $(
            paste::paste! {
                fn [<unpredict_horizontal_ $t>](data: &mut [$t], row_size: u32) {
                    for row in data.chunks_mut(row_size as usize) {
                        for i in 1..row.len() {
                            row[i] = row[i].wrapping_add(row[i - 1]);
                        }
                    }
                }
            }
        )*
    };
}

macro_rules! impl_horizontal_unpredictable_for_fp {
    ($($t:ty),*) => {
        $(
            paste::paste! {
                fn [<unpredict_horizontal_ $t>](data: &mut [$t], row_size: u32) {
                    for row in data.chunks_mut(row_size as usize) {
                        for i in 1..row.len() {
                            row[i] += row[i - 1];
                        }
                    }
                }
            }
        )*
    };
}

impl_horizontal_unpredictable_for_int!(u8, u16, u32, u64, i8, i16, i32, i64);
impl_horizontal_unpredictable_for_fp!(f32, f64);

pub fn unpredict_horizontal<T: ArrayNum + Copy>(data: &mut [T], row_size: u32) {
    // Macro based dispatch to avoid an extra trait bound on T which pollutes the entire call stack.
    match T::TYPE {
        crate::ArrayDataType::Uint8 => unpredict_horizontal_u8(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Uint16 => unpredict_horizontal_u16(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Uint32 => unpredict_horizontal_u32(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Uint64 => unpredict_horizontal_u64(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Int8 => unpredict_horizontal_i8(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Int16 => unpredict_horizontal_i16(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Int32 => unpredict_horizontal_i32(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Int64 => unpredict_horizontal_i64(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Float32 => unpredict_horizontal_f32(bytemuck::cast_slice_mut(data), row_size),
        crate::ArrayDataType::Float64 => unpredict_horizontal_f64(bytemuck::cast_slice_mut(data), row_size),
    }
}

fn decode_delta_bytes(data: &mut [u8], bytes_per_pixel: usize, row_size: u32) {
    unpredict_horizontal_u8(data, bytes_per_pixel as u32 * row_size);
}

pub fn unpredict_fp32(data: &mut [f32], row_size: u32) {
    let mut bytes: Vec<u8> = bytemuck::cast_slice(data).to_vec();

    debug_assert_eq!(bytes.len() % row_size as usize, 0);
    decode_delta_bytes(&mut bytes, std::mem::size_of::<f32>(), row_size);

    let tile_size = row_size as usize;
    for (row_nr, row) in bytes.chunks_mut(std::mem::size_of::<f32>() * tile_size).enumerate() {
        for i in 0..tile_size {
            data[row_nr * tile_size + i] = f32::from_be_bytes([row[i], row[tile_size + i], row[tile_size * 2 + i], row[tile_size * 3 + i]]);
        }
    }
}

pub fn unpredict_fp64(data: &mut [f64], row_size: u32) {
    let mut bytes: Vec<u8> = bytemuck::cast_slice(data).to_vec();
    debug_assert_eq!(bytes.len() % row_size as usize, 0);
    decode_delta_bytes(&mut bytes, std::mem::size_of::<f64>(), row_size);

    let tile_size = row_size as usize;
    for (row_nr, row) in bytes.chunks_mut(std::mem::size_of::<f64>() * tile_size).enumerate() {
        for i in 0..tile_size {
            data[row_nr * tile_size + i] = f64::from_be_bytes([
                row[i],
                row[tile_size + i],
                row[tile_size * 2 + i],
                row[tile_size * 3 + i],
                row[tile_size * 4 + i],
                row[tile_size * 5 + i],
                row[tile_size * 6 + i],
                row[tile_size * 7 + i],
            ]);
        }
    }
}

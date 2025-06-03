use geo::ArrayNum;
use inf::allocate::{self, AlignedVec};

use crate::{Error, Result};

pub(crate) fn compress_tile_data<T: ArrayNum>(source: &[T]) -> Result<Vec<u8>> {
    // Safety: The T type is a RasterNum, so it is safe to transmute the slice to a byte slice
    let source_bytes = unsafe { std::slice::from_raw_parts(source.as_ptr().cast::<u8>(), std::mem::size_of_val(source)) };

    Ok(lz4_flex::compress(source_bytes))
}

pub(crate) fn decompress_tile_data<T: ArrayNum>(element_count: usize, source: &[u8]) -> Result<AlignedVec<T>> {
    let mut data = allocate::aligned_vec_with_capacity::<T>(element_count);

    // Safety: The T array is initialized with the capacity of element_count, so it is safe to transmute the slice to a byte slice
    let data_bytes = unsafe { std::slice::from_raw_parts_mut(data.as_mut_ptr().cast::<u8>(), element_count * std::mem::size_of::<T>()) };

    match lz4_flex::decompress_into(source, data_bytes) {
        Ok(size) => {
            if size != element_count * std::mem::size_of::<T>() {
                return Err(Error::InvalidArgument(format!(
                    "Decompressed tile data size mismatch: expected {}, got {}",
                    element_count * std::mem::size_of::<T>(),
                    size
                )));
            }

            // Safety: The decompression size was checked so it is safe to set the length of the data vector
            unsafe {
                data.set_len(element_count);
            }
            Ok(data)
        }
        Err(err) => Err(Error::InvalidArgument(format!("Failed to decompress tile data: {err}"))),
    }
}

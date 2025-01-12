use crate::Error;

use bytemuck::{must_cast_slice, must_cast_slice_mut, AnyBitPattern, NoUninit};

use crate::Result;

pub(crate) fn compress_tile_data<T: NoUninit + AnyBitPattern>(source: &[T]) -> Result<Vec<u8>> {
    Ok(lz4_flex::compress(must_cast_slice(source)))
}

pub(crate) fn decompress_tile_data<T: NoUninit + AnyBitPattern>(element_count: usize, source: &[u8]) -> Result<Vec<T>> {
    let mut data: Vec<T> = vec![T::zeroed(); element_count];

    match lz4_flex::decompress_into(source, must_cast_slice_mut(&mut data)) {
        Ok(size) => {
            if size != element_count * std::mem::size_of::<T>() {
                return Err(Error::InvalidArgument(format!(
                    "Decompressed tile data size mismatch: expected {}, got {}",
                    element_count * std::mem::size_of::<T>(),
                    size
                )));
            }
            Ok(data)
        }
        Err(err) => Err(Error::InvalidArgument(format!(
            "Failed to decompress tile data: {}",
            err
        ))),
    }
}

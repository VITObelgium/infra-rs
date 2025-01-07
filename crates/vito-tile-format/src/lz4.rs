use std::io::Write;

use bytemuck::{must_cast_slice, must_cast_slice_mut, AnyBitPattern, NoUninit};
use lz4::EncoderBuilder;

use crate::Result;

pub(crate) fn compress_tile_data<T: NoUninit + AnyBitPattern>(source: &[T]) -> Result<Vec<u8>> {
    let mut data: Vec<u8> = Vec::new();

    {
        let dest_writer = std::io::BufWriter::new(&mut data);
        let mut encoder = EncoderBuilder::new().level(4).build(dest_writer)?;
        encoder.write_all(must_cast_slice(source))?;

        let (_output, result) = encoder.finish();
        if let Err(err) = result {
            return Err(err.into());
        }
    }

    Ok(data)
}

pub(crate) fn decompress_tile_data<T: NoUninit + AnyBitPattern>(data_size: usize, source: &[u8]) -> Result<Vec<T>> {
    let mut data: Vec<T> = Vec::with_capacity(data_size);

    {
        let mut dest_writer = std::io::BufWriter::new(must_cast_slice_mut(&mut data));
        let mut decoder = lz4::Decoder::new(source)?;
        std::io::copy(&mut decoder, &mut dest_writer)?;
    }

    Ok(data)
}

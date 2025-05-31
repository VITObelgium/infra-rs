use geo::{Columns, RasterSize, Rows};
use inf::allocate;

use crate::Result;

use std::io::Cursor;

/// Decodes PNG data from a &[u8] slice.
///
/// # Arguments
/// - `data`: A byte slice containing the raw PNG bytes.
///
/// # Returns
/// - `Ok((Vec<f32>, (u32, u32)))`: The decoded image data as a vector of floats and its dimensions (width, height).
/// - `Err(DecodingError)`: An error if the decoding fails.
pub fn decode_png(data: &[u8]) -> Result<(Vec<f32>, RasterSize, png::ColorType)> {
    let mut decoder_options = png::DecodeOptions::default();
    decoder_options.set_ignore_checksums(true);
    decoder_options.set_ignore_text_chunk(true);
    decoder_options.set_ignore_iccp_chunk(true);

    let cursor = Cursor::new(data);
    let decoder = png::Decoder::new_with_options(cursor, decoder_options);

    let mut reader = decoder.read_info()?;
    let mut buf = allocate::aligned_vec_with_capacity::<f32>(reader.output_buffer_size() / std::mem::size_of::<f32>());
    // SAFETY: Convert the uninitialized buffer into a mutable slice for writing
    let buf_slice: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr().cast::<u8>(), reader.output_buffer_size()) };
    let frame_info = reader.next_frame(buf_slice)?;
    unsafe {
        buf.set_len(frame_info.buffer_size() / std::mem::size_of::<f32>());
    }

    let info = reader.info();
    Ok((
        buf,
        RasterSize::with_rows_cols(Rows(info.height as i32), Columns(info.width as i32)),
        info.color_type,
    ))
}

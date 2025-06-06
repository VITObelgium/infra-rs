use crate::Result;

use std::io::Cursor;

/// Decodes PNG data from a &[u8] slice.
///
/// # Arguments
/// - `data`: A byte slice containing the raw PNG bytes.
///
/// # Returns
/// - `Ok((Vec<u8>, (u32, u32)))`: The decoded image data as a vector of bytes and its dimensions (width, height).
/// - `Err(DecodingError)`: An error if the decoding fails.
pub fn decode_png(data: &[u8]) -> Result<(Vec<u8>, (u32, u32), png::ColorType)> {
    let mut decoder_options = png::DecodeOptions::default();
    decoder_options.set_ignore_checksums(true);
    decoder_options.set_ignore_text_chunk(true);
    decoder_options.set_ignore_iccp_chunk(true);

    let cursor = Cursor::new(data);
    let decoder = png::Decoder::new_with_options(cursor, decoder_options);

    let mut reader = decoder.read_info()?;
    //let mut buf = vec![0; reader.output_buffer_size()];
    let mut buf = Vec::with_capacity(reader.output_buffer_size());
    // SAFETY: Convert the uninitialized buffer into a mutable slice for writing
    let buf_slice: &mut [u8] = unsafe { std::slice::from_raw_parts_mut(buf.as_mut_ptr(), reader.output_buffer_size()) };
    let frame_info = reader.next_frame(buf_slice)?;
    unsafe {
        buf.set_len(frame_info.buffer_size());
    }

    let info = reader.info();
    Ok((buf, (info.width, info.height), info.color_type))
}

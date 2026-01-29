//! LERC (Limited Error Raster Compression) decoder in pure Rust
//!
//! This crate provides a native Rust implementation of the LERC2 decompression algorithm.
//! LERC is developed by Esri for efficient compression of raster data with controlled precision loss.
//!
//! # Example
//! ```no_run
//! use lerc_decoder::{decode, LercInfo};
//!
//! let lerc_blob: &[u8] = &[/* ... lerc encoded data ... */];
//! let info = lerc_decoder::get_blob_info(lerc_blob).unwrap();
//! let decoded = lerc_decoder::decode(lerc_blob).unwrap();
//! ```

pub mod bit_mask;
pub mod bit_stuffer;
pub mod error;
pub mod fpl;
pub mod huffman;
pub mod lerc2;
pub mod rle;

pub use error::{LercError, Result};
pub use lerc2::{DataType, HeaderInfo, Lerc2Decoder};

/// Information about a LERC blob
#[derive(Debug, Clone)]
pub struct LercInfo {
    /// LERC version number (1-6 for Lerc2.1 to Lerc2.6)
    pub version: i32,
    /// Number of values per pixel (depth)
    pub n_depth: i32,
    /// Number of columns
    pub n_cols: i32,
    /// Number of rows
    pub n_rows: i32,
    /// Number of valid pixels
    pub num_valid_pixel: i32,
    /// Number of bands
    pub n_bands: i32,
    /// Total blob size in bytes
    pub blob_size: i32,
    /// Number of masks (0, 1, or n_bands)
    pub n_masks: i32,
    /// Data type
    pub data_type: DataType,
    /// Minimum pixel value
    pub z_min: f64,
    /// Maximum pixel value
    pub z_max: f64,
    /// Maximum error used for encoding
    pub max_z_error: f64,
}

/// Decoded LERC data
#[derive(Debug)]
pub struct DecodedData {
    /// The decoded pixel data
    pub data: DecodedPixels,
    /// Validity mask (true = valid pixel)
    pub mask: Option<Vec<bool>>,
    /// Information about the decoded blob
    pub info: LercInfo,
}

/// Decoded pixel data for various data types
#[derive(Debug, PartialEq)]
pub enum DecodedPixels {
    I8(Vec<i8>),
    U8(Vec<u8>),
    I16(Vec<i16>),
    U16(Vec<u16>),
    I32(Vec<i32>),
    U32(Vec<u32>),
    F32(Vec<f32>),
    F64(Vec<f64>),
}

impl DecodedPixels {
    /// Get the number of elements
    pub fn len(&self) -> usize {
        match self {
            DecodedPixels::I8(v) => v.len(),
            DecodedPixels::U8(v) => v.len(),
            DecodedPixels::I16(v) => v.len(),
            DecodedPixels::U16(v) => v.len(),
            DecodedPixels::I32(v) => v.len(),
            DecodedPixels::U32(v) => v.len(),
            DecodedPixels::F32(v) => v.len(),
            DecodedPixels::F64(v) => v.len(),
        }
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Get information about a LERC blob without fully decoding it
pub fn get_blob_info(data: &[u8]) -> Result<LercInfo> {
    let mut pos = 0;
    let mut total_blob_size = 0;
    let mut n_bands = 0;
    let mut first_info: Option<HeaderInfo> = None;
    let mut first_has_mask = false;

    while pos < data.len() {
        let remaining = &data[pos..];

        // Try to parse a header - if it fails, we've reached the end of valid blobs
        let (header, has_mask) = match Lerc2Decoder::get_header_info(remaining) {
            Ok(result) => result,
            Err(_) => break, // No more valid blobs
        };

        if first_info.is_none() {
            first_info = Some(header.clone());
            first_has_mask = has_mask;
        }

        total_blob_size += header.blob_size as usize;
        n_bands += 1;

        // Move to next blob
        pos += header.blob_size as usize;

        // For version >= 6, use n_blobs_more to know if there are more bands
        // For older versions, just keep trying to parse more blobs until we run out of data
        if header.version >= 6 && header.n_blobs_more == 0 {
            break;
        }
    }

    let header = first_info.ok_or(LercError::InvalidData("Empty LERC blob".into()))?;

    Ok(LercInfo {
        version: header.version,
        n_depth: header.n_depth,
        n_cols: header.n_cols,
        n_rows: header.n_rows,
        num_valid_pixel: header.num_valid_pixel,
        n_bands,
        blob_size: total_blob_size as i32,
        n_masks: if first_has_mask { 1 } else { 0 },
        data_type: header.dt,
        z_min: header.z_min,
        z_max: header.z_max,
        max_z_error: header.max_z_error,
    })
}

/// Decode a LERC blob
pub fn decode(data: &[u8]) -> Result<DecodedData> {
    let info = get_blob_info(data)?;
    let mut decoder = Lerc2Decoder::new();

    let total_pixels = info.n_rows as usize * info.n_cols as usize * info.n_depth as usize * info.n_bands as usize;

    match info.data_type {
        DataType::Char => {
            let mut output = vec![0i8; total_pixels];
            decode_bands(&mut decoder, data, &mut output, &info)?;
            let mask = decoder.get_mask_as_bool_vec();
            Ok(DecodedData {
                data: DecodedPixels::I8(output),
                mask,
                info,
            })
        }
        DataType::Byte => {
            let mut output = vec![0u8; total_pixels];
            decode_bands(&mut decoder, data, &mut output, &info)?;
            let mask = decoder.get_mask_as_bool_vec();
            Ok(DecodedData {
                data: DecodedPixels::U8(output),
                mask,
                info,
            })
        }
        DataType::Short => {
            let mut output = vec![0i16; total_pixels];
            decode_bands(&mut decoder, data, &mut output, &info)?;
            let mask = decoder.get_mask_as_bool_vec();
            Ok(DecodedData {
                data: DecodedPixels::I16(output),
                mask,
                info,
            })
        }
        DataType::UShort => {
            let mut output = vec![0u16; total_pixels];
            decode_bands(&mut decoder, data, &mut output, &info)?;
            let mask = decoder.get_mask_as_bool_vec();
            Ok(DecodedData {
                data: DecodedPixels::U16(output),
                mask,
                info,
            })
        }
        DataType::Int => {
            let mut output = vec![0i32; total_pixels];
            decode_bands(&mut decoder, data, &mut output, &info)?;
            let mask = decoder.get_mask_as_bool_vec();
            Ok(DecodedData {
                data: DecodedPixels::I32(output),
                mask,
                info,
            })
        }
        DataType::UInt => {
            let mut output = vec![0u32; total_pixels];
            decode_bands(&mut decoder, data, &mut output, &info)?;
            let mask = decoder.get_mask_as_bool_vec();
            Ok(DecodedData {
                data: DecodedPixels::U32(output),
                mask,
                info,
            })
        }
        DataType::Float => {
            let mut output = vec![0f32; total_pixels];
            decode_bands(&mut decoder, data, &mut output, &info)?;
            let mask = decoder.get_mask_as_bool_vec();
            Ok(DecodedData {
                data: DecodedPixels::F32(output),
                mask,
                info,
            })
        }
        DataType::Double => {
            let mut output = vec![0f64; total_pixels];
            decode_bands(&mut decoder, data, &mut output, &info)?;
            let mask = decoder.get_mask_as_bool_vec();
            Ok(DecodedData {
                data: DecodedPixels::F64(output),
                mask,
                info,
            })
        }
        DataType::Undefined => Err(LercError::UnsupportedDataType),
    }
}

fn decode_bands<T: lerc2::LercDataType + Default + Clone>(
    decoder: &mut Lerc2Decoder,
    data: &[u8],
    output: &mut [T],
    info: &LercInfo,
) -> Result<()> {
    let band_size = info.n_rows as usize * info.n_cols as usize * info.n_depth as usize;
    let mut offset = 0usize;
    let mut data_offset = 0usize;

    for _band_idx in 0..info.n_bands {
        let remaining_data = &data[data_offset..];
        let mut bytes_remaining = remaining_data.len();

        decoder.decode(remaining_data, &mut bytes_remaining, &mut output[offset..offset + band_size])?;

        // The decode function sets bytes_remaining to reflect remaining data after this blob
        // So bytes_consumed = original_len - bytes_remaining
        let _bytes_consumed = remaining_data.len() - bytes_remaining;

        // For multi-band, advance by the blob_size which is set in the header
        let blob_size = decoder.header_info().blob_size as usize;
        data_offset += blob_size;
        offset += band_size;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_data_type_size() {
        assert_eq!(std::mem::size_of::<i8>(), 1);
        assert_eq!(std::mem::size_of::<u8>(), 1);
        assert_eq!(std::mem::size_of::<i16>(), 2);
        assert_eq!(std::mem::size_of::<u16>(), 2);
        assert_eq!(std::mem::size_of::<i32>(), 4);
        assert_eq!(std::mem::size_of::<u32>(), 4);
        assert_eq!(std::mem::size_of::<f32>(), 4);
        assert_eq!(std::mem::size_of::<f64>(), 8);
    }
}

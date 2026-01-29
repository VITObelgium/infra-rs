//! Float-point lossless compression (FPL) module
//!
//! This module implements LERC2 v6 float-point lossless compression decoding.
//! The FPL algorithm compresses floating-point data by:
//! 1. Applying a bit transform to move sign/exponent bits
//! 2. Applying a predictor (none, delta1, or rows/cols)
//! 3. Splitting into byte planes and applying per-plane delta encoding
//! 4. Huffman compressing each byte plane

use crate::error::{LercError, Result};
use crate::huffman::Huffman;

/// Maximum delta level for byte plane encoding
pub const MAX_DELTA: u8 = 5;

/// Predictor types used in FPL compression
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PredictorType {
    None = 0,
    Delta1 = 1,
    RowsCols = 2,
    Unknown = 255,
}

impl PredictorType {
    /// Create predictor type from code byte
    pub fn from_code(code: u8) -> Self {
        match code {
            0 => PredictorType::None,
            1 => PredictorType::Delta1,
            2 => PredictorType::RowsCols,
            _ => PredictorType::Unknown,
        }
    }

    /// Get the integer delta value for this predictor
    pub fn get_int_delta(&self) -> i32 {
        match self {
            PredictorType::None => 0,
            PredictorType::Delta1 => 1,
            PredictorType::RowsCols => 2,
            PredictorType::Unknown => 0,
        }
    }
}

/// Decode FPL-compressed float data
///
/// # Arguments
/// * `data` - Input byte buffer
/// * `pos` - Current position (will be updated)
/// * `is_double` - True for f64, false for f32
/// * `width` - Image width
/// * `height` - Image height
/// * `depth` - Image depth (values per pixel)
///
/// # Returns
/// Decoded bytes that can be reinterpreted as f32 or f64
pub fn decode_fpl(
    data: &[u8],
    pos: &mut usize,
    is_double: bool,
    width: i32,
    height: i32,
    depth: i32,
) -> Result<Vec<u8>> {
    if depth == 1 {
        decode_fpl_slice(data, pos, is_double, width as usize, height as usize)
    } else {
        decode_fpl_slice(
            data,
            pos,
            is_double,
            depth as usize,
            (width * height) as usize,
        )
    }
}

/// Decode a single FPL slice
fn decode_fpl_slice(
    data: &[u8],
    pos: &mut usize,
    is_double: bool,
    width: usize,
    height: usize,
) -> Result<Vec<u8>> {
    let unit_size = if is_double { 8 } else { 4 };
    let expected_size = width * height;

    // Read predictor code
    if *pos >= data.len() {
        return Err(LercError::UnexpectedEof);
    }
    let pred_code = data[*pos];
    *pos += 1;

    if pred_code > 2 {
        return Err(LercError::FplError(format!(
            "Invalid predictor code: {}",
            pred_code
        )));
    }

    let predictor = PredictorType::from_code(pred_code);

    // Decode each byte plane
    let mut byte_planes: Vec<(usize, Vec<u8>)> = Vec::with_capacity(unit_size);

    for _ in 0..unit_size {
        // Read byte_index
        if *pos + 6 > data.len() {
            return Err(LercError::UnexpectedEof);
        }

        let byte_index = data[*pos] as usize;
        *pos += 1;

        if byte_index >= unit_size {
            return Err(LercError::FplError(format!(
                "Invalid byte index: {}",
                byte_index
            )));
        }

        let best_level = data[*pos];
        *pos += 1;

        if best_level > MAX_DELTA {
            return Err(LercError::FplError(format!(
                "Invalid delta level: {}",
                best_level
            )));
        }

        let compressed_size =
            u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]])
                as usize;
        *pos += 4;

        if *pos + compressed_size > data.len() {
            return Err(LercError::UnexpectedEof);
        }

        // Decompress the byte plane using Huffman
        let compressed_data = &data[*pos..*pos + compressed_size];
        *pos += compressed_size;

        let mut decompressed = decode_fpl_huffman(compressed_data, expected_size)?;

        // Apply restore sequence (undo delta encoding)
        restore_sequence(&mut decompressed, best_level as i32);

        byte_planes.push((byte_index, decompressed));
    }

    // Reconstruct the data from byte planes
    let mut output = vec![0u8; expected_size * unit_size];

    for i in 0..expected_size {
        for (byte_index, plane_data) in &byte_planes {
            output[i * unit_size + *byte_index] = plane_data[i];
        }
    }

    // Apply predictor restoration and undo float transform
    if is_double {
        let mut values: Vec<u64> = output
            .chunks_exact(8)
            .map(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()))
            .collect();

        match predictor {
            PredictorType::RowsCols => {
                restore_cross_double(&mut values, width, height);
            }
            PredictorType::Delta1 | PredictorType::None => {
                restore_block_sequence_double(
                    predictor.get_int_delta(),
                    &mut values,
                    width,
                    height,
                );
            }
            PredictorType::Unknown => {
                return Err(LercError::FplError("Unknown predictor type".into()));
            }
        }

        // No float transform for double (it's handled differently)
        output = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    } else {
        let mut values: Vec<u32> = output
            .chunks_exact(4)
            .map(|chunk| u32::from_le_bytes(chunk.try_into().unwrap()))
            .collect();

        match predictor {
            PredictorType::RowsCols => {
                restore_cross_float(&mut values, width, height);
                // Undo float bit transform
                undo_float_transform(&mut values);
            }
            PredictorType::Delta1 | PredictorType::None => {
                restore_block_sequence_float(predictor.get_int_delta(), &mut values, width, height);
                // Undo float bit transform
                undo_float_transform(&mut values);
            }
            PredictorType::Unknown => {
                return Err(LercError::FplError("Unknown predictor type".into()));
            }
        }

        output = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    }

    Ok(output)
}

/// Decode FPL Huffman-compressed data
///
/// FPL uses a slightly different Huffman format than the main LERC Huffman
fn decode_fpl_huffman(data: &[u8], expected_size: usize) -> Result<Vec<u8>> {
    if data.is_empty() {
        return Err(LercError::UnexpectedEof);
    }

    let first_byte = data[0];

    match first_byte {
        // HUFFMAN_NORMAL
        0 => {
            let mut pos = 1usize;
            let mut huffman = Huffman::new();

            // Read code table - FPL uses version 5 format
            huffman.read_code_table(data, &mut pos, 5)?;
            huffman.build_tree_from_codes()?;

            let mut output = vec![0u8; expected_size];
            let mut bit_pos = 0i32;

            for i in 0..expected_size {
                let val = huffman.decode_one_value(data, &mut pos, &mut bit_pos)?;
                output[i] = val as u8;
            }

            Ok(output)
        }
        // HUFFMAN_RLE - all same value
        1 => {
            if data.len() < 6 {
                return Err(LercError::UnexpectedEof);
            }
            let value = data[1];
            let count = u32::from_le_bytes([data[2], data[3], data[4], data[5]]) as usize;

            if count != expected_size {
                return Err(LercError::FplError(format!(
                    "RLE count mismatch: {} vs {}",
                    count, expected_size
                )));
            }

            Ok(vec![value; expected_size])
        }
        // HUFFMAN_NO_ENCODING - uncompressed
        2 => {
            if data.len() < 1 + expected_size {
                return Err(LercError::UnexpectedEof);
            }
            Ok(data[1..1 + expected_size].to_vec())
        }
        // HUFFMAN_PACKBITS - RLE variant
        3 => decode_packbits(&data[1..], expected_size),
        _ => Err(LercError::FplError(format!(
            "Unknown FPL Huffman encoding: {}",
            first_byte
        ))),
    }
}

/// Decode PackBits RLE compression
/// This matches the C++ implementation in fpl_EsriHuffman.cpp
fn decode_packbits(data: &[u8], expected_size: usize) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(expected_size);
    let mut i = 0;

    while i < data.len() && output.len() < expected_size {
        let mut b = data[i] as i32;

        if b <= 127 {
            // Literal run: copy b+1 bytes
            while b >= 0 {
                i += 1;
                if i >= data.len() {
                    return Err(LercError::UnexpectedEof);
                }
                output.push(data[i]);
                b -= 1;
            }
            i += 1;
        } else {
            // Repeat run: repeat next byte (b - 127) times
            i += 1;
            if i >= data.len() {
                return Err(LercError::UnexpectedEof);
            }
            let value = data[i];
            while b >= 127 {
                output.push(value);
                b -= 1;
            }
            i += 1;
        }
    }

    if output.len() != expected_size {
        return Err(LercError::FplError(format!(
            "PackBits size mismatch: {} vs {}",
            output.len(),
            expected_size
        )));
    }

    Ok(output)
}

/// Restore byte sequence with delta decoding
pub fn restore_sequence(data: &mut [u8], level: i32) {
    if level <= 0 || data.is_empty() {
        return;
    }

    for l in (1..=level).rev() {
        for i in (l as usize)..data.len() {
            data[i] = data[i].wrapping_add(data[i - 1]);
        }
    }
}

// Float bit manipulation constants
const FLT_MANT_MASK: u32 = 0x007FFFFF;
const FLT_9BIT_MASK: u32 = 0xFF800000;

const DBL_MANT_MASK: u64 = 0x000FFFFFFFFFFFFF;
const DBL_12BIT_MASK: u64 = 0xFFF0000000000000;

/// Undo float bit reordering transform
pub fn undo_float_transform(data: &mut [u32]) {
    for val in data.iter_mut() {
        *val = undo_move_bits_to_front(*val);
    }
}

/// Undo the bit reordering for a single float value
fn undo_move_bits_to_front(a: u32) -> u32 {
    let mut ret = a & FLT_MANT_MASK;

    let ae = ((a & FLT_9BIT_MASK) >> 24) & 0xFF;
    let a_sign = (a >> 23) & 0x01;

    ret |= ae << 23;
    ret |= a_sign << 31;

    ret
}

/// Add two float bit patterns (for predictor restoration)
fn add_float(a: u32, b: u32) -> u32 {
    let mut ret = (a.wrapping_add(b)) & FLT_MANT_MASK;

    let ae = ((a & FLT_9BIT_MASK) >> 23) & 0x1FF;
    let be = ((b & FLT_9BIT_MASK) >> 23) & 0x1FF;

    ret |= (ae.wrapping_add(be) & 0x1FF) << 23;

    ret
}

/// Add two double bit patterns (for predictor restoration)
fn add_double(a: u64, b: u64) -> u64 {
    let am = a & DBL_MANT_MASK;
    let bm = b & DBL_MANT_MASK;

    let mut ret = am.wrapping_add(bm) & DBL_MANT_MASK;

    let ae = ((a & DBL_12BIT_MASK) >> 52) & 0xFFF;
    let be = ((b & DBL_12BIT_MASK) >> 52) & 0xFFF;

    ret |= (ae.wrapping_add(be) & 0xFFF) << 52;

    ret
}

/// Restore block sequence for float data (PREDICTOR_NONE or PREDICTOR_DELTA1)
fn restore_block_sequence_float(delta: i32, data: &mut [u32], cols: usize, rows: usize) {
    if delta == 2 {
        for row in 0..rows {
            let row_start = row * cols;
            for i in 2..cols {
                data[row_start + i] = add_float(data[row_start + i], data[row_start + i - 1]);
            }
        }
    }

    if delta >= 1 {
        for row in 0..rows {
            let row_start = row * cols;
            for i in 1..cols {
                data[row_start + i] = add_float(data[row_start + i], data[row_start + i - 1]);
            }
        }
    }
}

/// Restore block sequence for double data (PREDICTOR_NONE or PREDICTOR_DELTA1)
fn restore_block_sequence_double(delta: i32, data: &mut [u64], cols: usize, rows: usize) {
    if delta == 2 {
        for row in 0..rows {
            let row_start = row * cols;
            for i in 2..cols {
                data[row_start + i] = add_double(data[row_start + i], data[row_start + i - 1]);
            }
        }
    }

    if delta >= 1 {
        for row in 0..rows {
            let row_start = row * cols;
            for i in 1..cols {
                data[row_start + i] = add_double(data[row_start + i], data[row_start + i - 1]);
            }
        }
    }
}

/// Restore cross derivative for float data (PREDICTOR_ROWS_COLS)
/// delta is always 2 for ROWS_COLS predictor
fn restore_cross_float(data: &mut [u32], cols: usize, rows: usize) {
    // First restore columns (vertical)
    for col in 0..cols {
        for row in 1..rows {
            let idx = row * cols + col;
            let prev_idx = (row - 1) * cols + col;
            data[idx] = add_float(data[idx], data[prev_idx]);
        }
    }

    // Then restore rows (horizontal)
    for row in 0..rows {
        let row_start = row * cols;
        for i in 1..cols {
            data[row_start + i] = add_float(data[row_start + i], data[row_start + i - 1]);
        }
    }
}

/// Restore cross derivative for double data (PREDICTOR_ROWS_COLS)
/// delta is always 2 for ROWS_COLS predictor
fn restore_cross_double(data: &mut [u64], cols: usize, rows: usize) {
    // First restore columns (vertical)
    for col in 0..cols {
        for row in 1..rows {
            let idx = row * cols + col;
            let prev_idx = (row - 1) * cols + col;
            data[idx] = add_double(data[idx], data[prev_idx]);
        }
    }

    // Then restore rows (horizontal)
    for row in 0..rows {
        let row_start = row * cols;
        for i in 1..cols {
            data[row_start + i] = add_double(data[row_start + i], data[row_start + i - 1]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_predictor_type_from_code() {
        assert_eq!(PredictorType::from_code(0), PredictorType::None);
        assert_eq!(PredictorType::from_code(1), PredictorType::Delta1);
        assert_eq!(PredictorType::from_code(2), PredictorType::RowsCols);
        assert_eq!(PredictorType::from_code(3), PredictorType::Unknown);
    }

    #[test]
    fn test_restore_sequence() {
        let mut data = vec![1, 2, 3, 4, 5];
        restore_sequence(&mut data, 1);
        assert_eq!(data, vec![1, 3, 6, 10, 15]);
    }

    #[test]
    fn test_restore_sequence_delta_0() {
        let mut data = vec![1, 2, 3, 4, 5];
        let original = data.clone();
        restore_sequence(&mut data, 0);
        assert_eq!(data, original);
    }

    #[test]
    fn test_float_transform_roundtrip() {
        // Test that the transform is reversible for typical float bit patterns
        let original: u32 = 0x40490FDB; // approximately pi as f32

        // Apply forward transform (move bits to front)
        let transformed = {
            let mut ret = original & FLT_MANT_MASK;
            let ae = ((original >> 23) & 0xFF) as u32;
            let a_sign = ((original >> 31) & 0x01) as u32;
            ret |= ae << 24;
            ret |= a_sign << 23;
            ret
        };

        // Apply reverse transform
        let recovered = undo_move_bits_to_front(transformed);

        assert_eq!(recovered, original);
    }

    #[test]
    fn test_decode_packbits_literal() {
        // Header 0x02 means copy next 3 bytes (b+1 where b=2)
        let data = [0x02, 0xAA, 0xBB, 0xCC];
        let result = decode_packbits(&data, 3).unwrap();
        assert_eq!(result, vec![0xAA, 0xBB, 0xCC]);
    }

    #[test]
    fn test_decode_packbits_repeat() {
        // Header 129 (0x81) means repeat next byte while b >= 127
        // So for b=129: iterations are b=129,128,127 = 3 times
        let data = [0x81, 0x42];
        let result = decode_packbits(&data, 3).unwrap();
        assert_eq!(result, vec![0x42, 0x42, 0x42]);
    }
}

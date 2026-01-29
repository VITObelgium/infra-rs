//! Run-Length Encoding (RLE) decompression for LERC
//!
//! The RLE format uses signed 16-bit counts:
//! - Positive count: read that many literal bytes
//! - Negative count: repeat the next byte that many times (absolute value)
//! - -32768: end of stream marker

use crate::error::{LercError, Result};

/// RLE decoder for LERC bit masks
pub struct Rle;

impl Rle {
    /// Read a 16-bit count from the byte stream (little-endian)
    #[inline]
    fn read_count(data: &[u8], pos: &mut usize) -> Result<i16> {
        if *pos + 2 > data.len() {
            return Err(LercError::UnexpectedEof);
        }

        let count = i16::from_le_bytes([data[*pos], data[*pos + 1]]);
        *pos += 2;
        Ok(count)
    }

    /// Decompress RLE-encoded data into a pre-allocated buffer
    ///
    /// # Arguments
    /// * `data` - The RLE compressed data
    /// * `output` - Pre-allocated output buffer
    ///
    /// # Returns
    /// `Ok(())` if decompression succeeded, error otherwise
    pub fn decompress(data: &[u8], output: &mut [u8]) -> Result<()> {
        if data.len() < 2 {
            return Err(LercError::RleError("Input too small".into()));
        }

        let mut src_pos = 0;
        let mut dst_pos = 0;
        let output_size = output.len();

        loop {
            let count = Self::read_count(data, &mut src_pos)?;

            // End of stream marker
            if count == -32768 {
                break;
            }

            if count > 0 {
                // Literal run: copy count bytes
                let run_len = count as usize;
                if src_pos + run_len > data.len() {
                    return Err(LercError::RleError("Insufficient input data".into()));
                }
                if dst_pos + run_len > output_size {
                    return Err(LercError::RleError("Output buffer overflow".into()));
                }

                output[dst_pos..dst_pos + run_len].copy_from_slice(&data[src_pos..src_pos + run_len]);
                src_pos += run_len;
                dst_pos += run_len;
            } else {
                // Repeat run: repeat the next byte |count| times
                let run_len = (-count) as usize;
                if src_pos >= data.len() {
                    return Err(LercError::RleError("Insufficient input data".into()));
                }
                if dst_pos + run_len > output_size {
                    return Err(LercError::RleError("Output buffer overflow".into()));
                }

                let byte = data[src_pos];
                src_pos += 1;

                for i in 0..run_len {
                    output[dst_pos + i] = byte;
                }
                dst_pos += run_len;
            }
        }

        Ok(())
    }

    /// Decompress RLE-encoded data, allocating the output buffer
    ///
    /// # Arguments
    /// * `data` - The RLE compressed data
    /// * `expected_size` - Expected output size
    ///
    /// # Returns
    /// The decompressed data
    pub fn decompress_alloc(data: &[u8], expected_size: usize) -> Result<Vec<u8>> {
        let mut output = vec![0u8; expected_size];
        Self::decompress(data, &mut output)?;
        Ok(output)
    }

    /// Calculate the decompressed size without actually decompressing
    ///
    /// # Arguments
    /// * `data` - The RLE compressed data
    ///
    /// # Returns
    /// The decompressed size in bytes
    pub fn compute_decompressed_size(data: &[u8]) -> Result<usize> {
        if data.len() < 2 {
            return Err(LercError::RleError("Input too small".into()));
        }

        let mut src_pos = 0;
        let mut total_size = 0usize;

        loop {
            let count = Self::read_count(data, &mut src_pos)?;

            // End of stream marker
            if count == -32768 {
                break;
            }

            if count > 0 {
                // Literal run
                let run_len = count as usize;
                total_size += run_len;

                // Skip the literal bytes
                if src_pos + run_len > data.len() {
                    return Err(LercError::RleError("Insufficient input data".into()));
                }
                src_pos += run_len;
            } else {
                // Repeat run
                let run_len = (-count) as usize;
                total_size += run_len;

                // Skip the repeated byte
                if src_pos >= data.len() {
                    return Err(LercError::RleError("Insufficient input data".into()));
                }
                src_pos += 1;
            }
        }

        Ok(total_size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decompress_literal() {
        // 3 literal bytes: 0xAA, 0xBB, 0xCC, then end marker
        let data = [
            0x03, 0x00, // count = 3 (literal)
            0xAA, 0xBB, 0xCC, // the bytes
            0x00, 0x80, // -32768 (end marker)
        ];

        let mut output = vec![0u8; 3];
        Rle::decompress(&data, &mut output).unwrap();
        assert_eq!(output, vec![0xAA, 0xBB, 0xCC]);
    }

    #[test]
    fn test_decompress_repeat() {
        // Repeat 0xFF 5 times, then end marker
        let data = [
            0xFB, 0xFF, // count = -5 (repeat)
            0xFF,       // the byte to repeat
            0x00, 0x80, // -32768 (end marker)
        ];

        let mut output = vec![0u8; 5];
        Rle::decompress(&data, &mut output).unwrap();
        assert_eq!(output, vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn test_decompress_mixed() {
        // 2 literal bytes, then repeat 3 times, then end
        let data = [
            0x02, 0x00, // count = 2 (literal)
            0x11, 0x22, // literal bytes
            0xFD, 0xFF, // count = -3 (repeat)
            0x33,       // byte to repeat
            0x00, 0x80, // end marker
        ];

        let mut output = vec![0u8; 5];
        Rle::decompress(&data, &mut output).unwrap();
        assert_eq!(output, vec![0x11, 0x22, 0x33, 0x33, 0x33]);
    }

    #[test]
    fn test_compute_size() {
        let data = [
            0x02, 0x00, // count = 2 (literal)
            0x11, 0x22, // literal bytes
            0xFD, 0xFF, // count = -3 (repeat)
            0x33,       // byte to repeat
            0x00, 0x80, // end marker
        ];

        let size = Rle::compute_decompressed_size(&data).unwrap();
        assert_eq!(size, 5);
    }

    #[test]
    fn test_decompress_alloc() {
        let data = [
            0x02, 0x00,
            0xAA, 0xBB,
            0x00, 0x80,
        ];

        let output = Rle::decompress_alloc(&data, 2).unwrap();
        assert_eq!(output, vec![0xAA, 0xBB]);
    }

    #[test]
    fn test_empty_stream() {
        // Just end marker
        let data = [0x00, 0x80];
        let output = Rle::decompress_alloc(&data, 0).unwrap();
        assert!(output.is_empty());
    }

    #[test]
    fn test_insufficient_input() {
        let data = [0x03, 0x00, 0xAA]; // Claims 3 bytes but only 1
        let result = Rle::decompress_alloc(&data, 3);
        assert!(result.is_err());
    }

    #[test]
    fn test_output_overflow() {
        let data = [
            0x05, 0x00, // 5 literal bytes
            0x01, 0x02, 0x03, 0x04, 0x05,
            0x00, 0x80,
        ];

        let mut output = vec![0u8; 3]; // Too small
        let result = Rle::decompress(&data, &mut output);
        assert!(result.is_err());
    }
}

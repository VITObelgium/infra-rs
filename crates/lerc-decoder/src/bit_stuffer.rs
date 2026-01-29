//! Bit stuffer for decoding packed unsigned integers
//!
//! This module handles unpacking of bit-stuffed unsigned integer arrays
//! as used in LERC compression. Supports both simple mode and LUT mode.
//!
//! Performance optimizations:
//! - Inline annotations on hot paths
//! - Direct memory copying for u32 buffer initialization
//! - Unsafe bounds-unchecked access in validated hot loops
//! - Buffer reuse to minimize allocations

use crate::error::{LercError, Result};

/// Bit stuffer decoder for packed unsigned integers
pub struct BitStuffer2 {
    tmp_lut_vec: Vec<u32>,
    tmp_bit_stuff_vec: Vec<u32>,
    /// Reusable output buffer to avoid allocations in hot path
    output_buffer: Vec<u32>,
}

impl BitStuffer2 {
    /// Create a new BitStuffer2
    pub fn new() -> Self {
        BitStuffer2 {
            tmp_lut_vec: Vec::new(),
            tmp_bit_stuff_vec: Vec::new(),
            output_buffer: Vec::new(),
        }
    }

    /// Decode an unsigned integer of 1, 2, or 4 bytes
    #[inline(always)]
    fn decode_uint(data: &[u8], pos: &mut usize, num_bytes: usize) -> Result<u32> {
        if *pos + num_bytes > data.len() {
            return Err(LercError::UnexpectedEof);
        }

        let value = match num_bytes {
            1 => data[*pos] as u32,
            2 => u16::from_le_bytes([data[*pos], data[*pos + 1]]) as u32,
            4 => u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]),
            _ => return Err(LercError::BitStufferError("Invalid num_bytes".into())),
        };

        *pos += num_bytes;
        Ok(value)
    }

    /// Calculate how many tail bytes are not needed in the last uint
    #[inline(always)]
    fn num_tail_bytes_not_needed(num_elem: u32, num_bits: i32) -> u32 {
        let num_bits_tail = ((num_elem as u64 * num_bits as u64) & 31) as i32;
        let num_bytes_tail = (num_bits_tail + 7) >> 3;
        if num_bytes_tail > 0 { 4 - num_bytes_tail as u32 } else { 0 }
    }

    /// Decode bit-stuffed data (LERC2 v3+) into reusable buffer
    #[inline(always)]
    fn bit_unstuff(&mut self, data: &[u8], pos: &mut usize, num_elements: u32, num_bits: i32) -> Result<()> {
        if num_elements == 0 || num_bits >= 32 {
            return Err(LercError::BitStufferError("Invalid parameters".into()));
        }

        let num_uints = (num_elements as u64 * num_bits as u64).div_ceil(32) as usize;
        let num_bytes = num_uints * 4;
        let num_bytes_used = num_bytes - Self::num_tail_bytes_not_needed(num_elements, num_bits) as usize;

        if *pos + num_bytes_used > data.len() {
            return Err(LercError::UnexpectedEof);
        }

        // Resize temporary buffer
        self.tmp_bit_stuff_vec.clear();
        self.tmp_bit_stuff_vec.resize(num_uints, 0);

        // Copy bytes into u32 buffer (little-endian) - optimized version
        // Copy full u32s directly where possible
        let full_uints = num_bytes_used / 4;
        let src_slice = &data[*pos..];

        for i in 0..full_uints {
            let byte_offset = i * 4;
            // SAFETY: We checked pos + num_bytes_used <= data.len() and byte_offset + 3 < num_bytes_used
            self.tmp_bit_stuff_vec[i] = u32::from_le_bytes([
                src_slice[byte_offset],
                src_slice[byte_offset + 1],
                src_slice[byte_offset + 2],
                src_slice[byte_offset + 3],
            ]);
        }

        // Handle remaining bytes for the last partial u32
        let remaining_bytes = num_bytes_used - full_uints * 4;
        if remaining_bytes > 0 {
            let byte_offset = full_uints * 4;
            let mut val = 0u32;
            for j in 0..remaining_bytes {
                val |= (src_slice[byte_offset + j] as u32) << (j * 8);
            }
            self.tmp_bit_stuff_vec[full_uints] = val;
        }

        // Resize output buffer (reuses capacity if possible)
        let num_elements_usize = num_elements as usize;
        self.output_buffer.clear();
        self.output_buffer.resize(num_elements_usize, 0);

        // Do the unstuffing - use unsafe for hot loop
        let mut src_idx = 0usize;
        let mut bit_pos = 0i32;
        let nb = 32 - num_bits;

        // SAFETY: We resized output_buffer to num_elements_usize elements,
        // and tmp_bit_stuff_vec to num_uints elements which is enough for all bits
        unsafe {
            for i in 0..num_elements_usize {
                if nb - bit_pos >= 0 {
                    *self.output_buffer.get_unchecked_mut(i) = (*self.tmp_bit_stuff_vec.get_unchecked(src_idx) << (nb - bit_pos)) >> nb;
                    bit_pos += num_bits;
                    if bit_pos == 32 {
                        src_idx += 1;
                        bit_pos = 0;
                    }
                } else {
                    *self.output_buffer.get_unchecked_mut(i) = *self.tmp_bit_stuff_vec.get_unchecked(src_idx) >> bit_pos;
                    src_idx += 1;
                    *self.output_buffer.get_unchecked_mut(i) |=
                        (*self.tmp_bit_stuff_vec.get_unchecked(src_idx) << (64 - num_bits - bit_pos)) >> nb;
                    bit_pos -= nb;
                }
            }
        }

        *pos += num_bytes_used;
        Ok(())
    }

    /// Decode bit-stuffed data (LERC2 v1-v2, big-endian style) into reusable buffer
    #[inline(always)]
    fn bit_unstuff_before_v3(&mut self, data: &[u8], pos: &mut usize, num_elements: u32, num_bits: i32) -> Result<()> {
        if num_elements == 0 || num_bits >= 32 {
            return Err(LercError::BitStufferError("Invalid parameters".into()));
        }

        let num_uints = (num_elements as u64 * num_bits as u64).div_ceil(32) as usize;
        let ntbnn = Self::num_tail_bytes_not_needed(num_elements, num_bits) as usize;
        let num_bytes_to_copy = (num_elements as usize * num_bits as usize).div_ceil(8);

        if *pos + num_bytes_to_copy > data.len() {
            return Err(LercError::UnexpectedEof);
        }

        self.tmp_bit_stuff_vec.resize(num_uints, 0);
        self.tmp_bit_stuff_vec[num_uints - 1] = 0;

        // Copy bytes directly
        let src_bytes = &data[*pos..*pos + num_bytes_to_copy];

        // Convert bytes to u32s (big-endian interpretation for this older format)
        for i in 0..num_uints {
            let mut val = 0u32;
            for j in 0..4 {
                let byte_idx = i * 4 + j;
                if byte_idx < num_bytes_to_copy {
                    val |= (src_bytes[byte_idx] as u32) << (24 - j * 8);
                }
            }
            self.tmp_bit_stuff_vec[i] = val;
        }

        // Shift the last uint
        let p_last = &mut self.tmp_bit_stuff_vec[num_uints - 1];
        for _ in 0..ntbnn {
            *p_last <<= 8;
        }

        // Resize output buffer (reuses capacity if possible)
        let num_elements_usize = num_elements as usize;
        self.output_buffer.clear();
        self.output_buffer.resize(num_elements_usize, 0);

        // Unstuff
        let mut src_idx = 0usize;
        let mut bit_pos = 0i32;

        for i in 0..num_elements_usize {
            if 32 - bit_pos >= num_bits {
                let n = self.tmp_bit_stuff_vec[src_idx] << bit_pos;
                self.output_buffer[i] = n >> (32 - num_bits);
                bit_pos += num_bits;

                if bit_pos == 32 {
                    bit_pos = 0;
                    src_idx += 1;
                }
            } else {
                let n = self.tmp_bit_stuff_vec[src_idx] << bit_pos;
                src_idx += 1;
                self.output_buffer[i] = n >> (32 - num_bits);
                bit_pos -= 32 - num_bits;
                self.output_buffer[i] |= self.tmp_bit_stuff_vec[src_idx] >> (32 - bit_pos);
            }
        }

        *pos += num_bytes_to_copy;
        Ok(())
    }

    /// Decode bit-stuffed unsigned integer array
    ///
    /// # Arguments
    /// * `data` - Input byte slice
    /// * `pos` - Current position (will be updated)
    /// * `max_element_count` - Maximum allowed element count
    /// * `lerc2_version` - LERC2 version for selecting decode method
    ///
    /// # Returns
    /// Vector of decoded unsigned integers
    #[inline]
    pub fn decode(&mut self, data: &[u8], pos: &mut usize, max_element_count: usize, lerc2_version: i32) -> Result<Vec<u32>> {
        if *pos >= data.len() {
            return Err(LercError::UnexpectedEof);
        }

        let num_bits_byte = data[*pos];
        *pos += 1;

        let bits67 = (num_bits_byte >> 6) as i32;
        let nb = if bits67 == 0 { 4 } else { 3 - bits67 } as usize;

        let do_lut = (num_bits_byte & (1 << 5)) != 0;
        let num_bits = (num_bits_byte & 31) as i32;

        let num_elements = Self::decode_uint(data, pos, nb)?;
        if num_elements as usize > max_element_count {
            return Err(LercError::BitStufferError("Element count exceeds maximum".into()));
        }

        if !do_lut {
            // Simple mode
            if num_bits > 0 {
                if lerc2_version >= 3 {
                    self.bit_unstuff(data, pos, num_elements, num_bits)?;
                } else {
                    self.bit_unstuff_before_v3(data, pos, num_elements, num_bits)?;
                }
                // Move output buffer out to return (swap with empty vec to avoid clone)
                let mut result = Vec::new();
                std::mem::swap(&mut result, &mut self.output_buffer);
                Ok(result)
            } else {
                // num_bits == 0 means all values are 0
                Ok(vec![0u32; num_elements as usize])
            }
        } else {
            // LUT mode
            if num_bits == 0 {
                return Err(LercError::BitStufferError("Invalid LUT encoding".into()));
            }

            if *pos >= data.len() {
                return Err(LercError::UnexpectedEof);
            }

            let n_lut_byte = data[*pos];
            *pos += 1;

            let n_lut = (n_lut_byte as i32) - 1;
            if n_lut < 1 {
                return Err(LercError::BitStufferError("Invalid LUT size".into()));
            }

            // Unstuff the LUT (without the 0)
            if lerc2_version >= 3 {
                self.bit_unstuff(data, pos, n_lut as u32, num_bits)?;
            } else {
                self.bit_unstuff_before_v3(data, pos, n_lut as u32, num_bits)?;
            }

            // Build LUT with 0 at the front - take ownership of output_buffer
            let mut lut_values = Vec::new();
            std::mem::swap(&mut lut_values, &mut self.output_buffer);

            self.tmp_lut_vec.clear();
            self.tmp_lut_vec.push(0);
            self.tmp_lut_vec.extend_from_slice(&lut_values);

            // Calculate bits needed for LUT indices
            let mut n_bits_lut = 0;
            let mut tmp = n_lut;
            while tmp > 0 {
                n_bits_lut += 1;
                tmp >>= 1;
            }

            if n_bits_lut == 0 {
                return Err(LercError::BitStufferError("Invalid LUT bits".into()));
            }

            // Unstuff the indices
            if lerc2_version >= 3 {
                self.bit_unstuff(data, pos, num_elements, n_bits_lut)?;
            } else {
                self.bit_unstuff_before_v3(data, pos, num_elements, n_bits_lut)?;
            }

            // Take ownership of output_buffer for result
            let mut indices = Vec::new();
            std::mem::swap(&mut indices, &mut self.output_buffer);

            // Replace indices with values
            if lerc2_version >= 3 {
                for val in &mut indices {
                    *val = self.tmp_lut_vec[*val as usize];
                }
            } else {
                for val in &mut indices {
                    if *val as usize >= self.tmp_lut_vec.len() {
                        return Err(LercError::BitStufferError("LUT index out of bounds".into()));
                    }
                    *val = self.tmp_lut_vec[*val as usize];
                }
            }

            Ok(indices)
        }
    }
}

impl Default for BitStuffer2 {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_uint_1byte() {
        let data = [0x42u8];
        let mut pos = 0;
        let val = BitStuffer2::decode_uint(&data, &mut pos, 1).unwrap();
        assert_eq!(val, 0x42);
        assert_eq!(pos, 1);
    }

    #[test]
    fn test_decode_uint_2byte() {
        let data = [0x34, 0x12];
        let mut pos = 0;
        let val = BitStuffer2::decode_uint(&data, &mut pos, 2).unwrap();
        assert_eq!(val, 0x1234);
        assert_eq!(pos, 2);
    }

    #[test]
    fn test_decode_uint_4byte() {
        let data = [0x78, 0x56, 0x34, 0x12];
        let mut pos = 0;
        let val = BitStuffer2::decode_uint(&data, &mut pos, 4).unwrap();
        assert_eq!(val, 0x12345678);
        assert_eq!(pos, 4);
    }

    #[test]
    fn test_num_tail_bytes_not_needed() {
        // 4 elements * 8 bits = 32 bits = exactly 4 bytes, no tail
        assert_eq!(BitStuffer2::num_tail_bytes_not_needed(4, 8), 0);

        // 3 elements * 8 bits = 24 bits = 3 bytes, 1 byte not needed
        assert_eq!(BitStuffer2::num_tail_bytes_not_needed(3, 8), 1);

        // 1 element * 8 bits = 8 bits = 1 byte, 3 bytes not needed
        assert_eq!(BitStuffer2::num_tail_bytes_not_needed(1, 8), 3);
    }

    #[test]
    fn test_simple_decode_zeros() {
        // Header: 0 bits, 1-byte count encoding, 5 elements
        // bits67 = 2 (1-byte), do_lut = 0, num_bits = 0
        let data = [
            0b10_0_00000, // bits67=2, no LUT, 0 bits
            0x05,         // 5 elements (1-byte encoding)
        ];

        let mut stuffer = BitStuffer2::new();
        let mut pos = 0;
        let result = stuffer.decode(&data, &mut pos, 100, 3).unwrap();

        assert_eq!(result.len(), 5);
        assert!(result.iter().all(|&x| x == 0));
    }
}

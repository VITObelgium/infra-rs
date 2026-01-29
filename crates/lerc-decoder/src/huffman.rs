//! Huffman coding for LERC decompression
//!
//! This module implements Huffman decoding as used in LERC2 for
//! compressing 8-bit data types (Byte, Char).

use crate::bit_stuffer::BitStuffer2;
use crate::error::{LercError, Result};

/// Maximum number of bits for the decode lookup table
const MAX_NUM_BITS_LUT: i32 = 12;

/// Maximum histogram size (2^15)
const MAX_HISTO_SIZE: usize = 1 << 15;

/// A Huffman decoder node for building the decode tree
#[derive(Clone)]
struct Node {
    value: i16,
    child0: Option<Box<Node>>,
    child1: Option<Box<Node>>,
}

impl Node {
    #[allow(dead_code)]
    fn new_leaf(value: i16) -> Self {
        Node {
            value,
            child0: None,
            child1: None,
        }
    }

    fn new_internal() -> Self {
        Node {
            value: -1,
            child0: None,
            child1: None,
        }
    }
}

/// Huffman decoder for LERC
pub struct Huffman {
    /// Code table: (code_length, code_value) for each symbol
    code_table: Vec<(u16, u32)>,
    /// Decode lookup table for fast decoding of short codes
    decode_lut: Vec<(i16, i16)>, // (length, value)
    /// Number of bits used by the LUT
    num_bits_lut: i32,
    /// Number of leading zero bits to skip before entering the tree
    num_bits_to_skip_in_tree: i32,
    /// Root of the Huffman tree for codes longer than LUT
    root: Option<Box<Node>>,
}

impl Huffman {
    /// Create a new Huffman decoder
    pub fn new() -> Self {
        Huffman {
            code_table: Vec::new(),
            decode_lut: Vec::new(),
            num_bits_lut: 0,
            num_bits_to_skip_in_tree: 0,
            root: None,
        }
    }

    /// Get index with wrap-around for handling code ranges that wrap
    #[inline]
    fn get_index_wrap_around(i: i32, size: i32) -> i32 {
        if i < size { i } else { i - size }
    }

    /// Read the Huffman code table from the data stream
    pub fn read_code_table(&mut self, data: &[u8], pos: &mut usize, lerc2_version: i32) -> Result<()> {
        // Read header: version, size, i0, i1
        if *pos + 16 > data.len() {
            return Err(LercError::UnexpectedEof);
        }

        let version = i32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
        *pos += 4;

        if version < 2 {
            return Err(LercError::HuffmanError("Unsupported Huffman version".into()));
        }

        let size = i32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
        *pos += 4;

        let i0 = i32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
        *pos += 4;

        let i1 = i32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
        *pos += 4;

        if i0 >= i1 || i0 < 0 || size < 0 || size > MAX_HISTO_SIZE as i32 {
            return Err(LercError::HuffmanError("Invalid code table parameters".into()));
        }

        if Self::get_index_wrap_around(i0, size) >= size
            || Self::get_index_wrap_around(i1 - 1, size) >= size
        {
            return Err(LercError::HuffmanError("Invalid code range".into()));
        }

        // Read code lengths using bit stuffer
        let num_lengths = (i1 - i0) as usize;
        let mut bit_stuffer = BitStuffer2::new();
        let code_lengths = bit_stuffer.decode(data, pos, num_lengths, lerc2_version)?;

        // Initialize code table
        self.code_table = vec![(0u16, 0u32); size as usize];

        // Fill code lengths
        for i in i0..i1 {
            let k = Self::get_index_wrap_around(i, size) as usize;
            self.code_table[k].0 = code_lengths[(i - i0) as usize] as u16;
        }

        // Read the actual codes
        self.bit_unstuff_codes(data, pos, i0, i1)?;

        Ok(())
    }

    /// Unstuff the Huffman codes from the byte stream
    fn bit_unstuff_codes(&mut self, data: &[u8], pos: &mut usize, i0: i32, i1: i32) -> Result<()> {
        let size = self.code_table.len() as i32;
        let mut bit_pos = 0i32;
        let ptr0 = *pos;

        for i in i0..i1 {
            let k = Self::get_index_wrap_around(i, size) as usize;
            let len = self.code_table[k].0 as i32;

            if len > 0 {
                if *pos + 4 > data.len() {
                    return Err(LercError::UnexpectedEof);
                }

                // Read current uint32
                let mut temp = u32::from_le_bytes([
                    data[*pos],
                    data[*pos + 1],
                    data[*pos + 2],
                    data[*pos + 3],
                ]);

                let code = (temp << bit_pos) >> (32 - len);

                if 32 - bit_pos >= len {
                    bit_pos += len;
                    if bit_pos == 32 {
                        bit_pos = 0;
                        *pos += 4;
                    }
                } else {
                    bit_pos += len - 32;
                    *pos += 4;

                    if *pos + 4 > data.len() {
                        return Err(LercError::UnexpectedEof);
                    }

                    temp = u32::from_le_bytes([
                        data[*pos],
                        data[*pos + 1],
                        data[*pos + 2],
                        data[*pos + 3],
                    ]);

                    self.code_table[k].1 = code | (temp >> (32 - bit_pos));
                    continue;
                }

                self.code_table[k].1 = code;
            }
        }

        // Advance position by the bytes consumed
        let len = (*pos - ptr0) + if bit_pos > 0 { 4 } else { 0 };
        *pos = ptr0 + len;

        Ok(())
    }

    /// Build the decode tree from the code table
    pub fn build_tree_from_codes(&mut self) -> Result<i32> {
        let (i0, i1, max_len) = self.get_range()?;

        let size = self.code_table.len() as i32;
        let mut min_num_zero_bits = 32i32;

        let need_tree = max_len > MAX_NUM_BITS_LUT;
        self.num_bits_lut = std::cmp::min(max_len, MAX_NUM_BITS_LUT);

        let size_lut = 1usize << self.num_bits_lut;

        // Initialize decode LUT
        self.decode_lut = vec![(-1i16, -1i16); size_lut];

        for i in i0..i1 {
            let k = Self::get_index_wrap_around(i, size) as usize;
            let len = self.code_table[k].0 as i32;

            if len == 0 {
                continue;
            }

            let code = self.code_table[k].1;

            if len <= self.num_bits_lut {
                // Fits in LUT
                let shifted_code = code << (self.num_bits_lut - len);
                let num_entries = 1u32 << (self.num_bits_lut - len);
                let entry = (len as i16, k as i16);

                for j in 0..num_entries {
                    self.decode_lut[(shifted_code | j) as usize] = entry;
                }
            } else {
                // Count leading zero bits for tree navigation
                let mut shift = 1;
                let mut tmp_code = code;
                while tmp_code > 1 {
                    tmp_code >>= 1;
                    shift += 1;
                }
                min_num_zero_bits = std::cmp::min(min_num_zero_bits, len - shift);
            }
        }

        self.num_bits_to_skip_in_tree = if need_tree { min_num_zero_bits } else { 0 };

        if !need_tree {
            return Ok(self.num_bits_lut);
        }

        // Build the tree for codes longer than LUT
        self.root = Some(Box::new(Node::new_internal()));

        for i in i0..i1 {
            let k = Self::get_index_wrap_around(i, size) as usize;
            let len = self.code_table[k].0 as i32;

            if len > 0 && len > self.num_bits_lut {
                let code = self.code_table[k].1;
                let mut node = self.root.as_mut().unwrap();
                let mut j = len - self.num_bits_to_skip_in_tree;

                while j > 0 {
                    j -= 1;
                    let bit = (code >> j) & 1;

                    if bit == 1 {
                        if node.child1.is_none() {
                            node.child1 = Some(Box::new(Node::new_internal()));
                        }
                        node = node.child1.as_mut().unwrap();
                    } else {
                        if node.child0.is_none() {
                            node.child0 = Some(Box::new(Node::new_internal()));
                        }
                        node = node.child0.as_mut().unwrap();
                    }

                    if j == 0 {
                        node.value = k as i16;
                    }
                }
            }
        }

        Ok(self.num_bits_lut)
    }

    /// Get the range of non-zero codes and max code length
    fn get_range(&self) -> Result<(i32, i32, i32)> {
        if self.code_table.is_empty() || self.code_table.len() >= MAX_HISTO_SIZE {
            return Err(LercError::HuffmanError("Invalid code table".into()));
        }

        let size = self.code_table.len() as i32;

        // Find first and last non-zero entries
        let mut i = 0;
        while i < size && self.code_table[i as usize].0 == 0 {
            i += 1;
        }
        let i0_simple = i;

        let mut i = size - 1;
        while i >= 0 && self.code_table[i as usize].0 == 0 {
            i -= 1;
        }
        let i1_simple = i + 1;

        if i1_simple <= i0_simple {
            return Err(LercError::HuffmanError("Empty code table".into()));
        }

        // Find largest stretch of zeros for wrap-around optimization
        let mut segm = (0i32, 0i32);
        let mut j = 0;
        while j < size {
            while j < size && self.code_table[j as usize].0 > 0 {
                j += 1;
            }
            let k0 = j;
            while j < size && self.code_table[j as usize].0 == 0 {
                j += 1;
            }
            let k1 = j;

            if k1 - k0 > segm.1 {
                segm = (k0, k1 - k0);
            }
        }

        let (i0, i1) = if size - segm.1 < i1_simple - i0_simple {
            (segm.0 + segm.1, segm.0 + size)
        } else {
            (i0_simple, i1_simple)
        };

        if i1 <= i0 {
            return Err(LercError::HuffmanError("Invalid code range".into()));
        }

        // Find max code length
        let mut max_len = 0;
        for i in i0..i1 {
            let k = Self::get_index_wrap_around(i, size) as usize;
            let len = self.code_table[k].0 as i32;
            max_len = std::cmp::max(max_len, len);
        }

        if max_len <= 0 || max_len > 32 {
            return Err(LercError::HuffmanError("Invalid max code length".into()));
        }

        Ok((i0, i1, max_len))
    }

    /// Decode a single Huffman-coded value
    ///
    /// # Arguments
    /// * `data` - Input byte buffer
    /// * `pos` - Current byte position (will be updated)
    /// * `bit_pos` - Current bit position within byte (will be updated)
    ///
    /// # Returns
    /// The decoded symbol value
    pub fn decode_one_value(
        &self,
        data: &[u8],
        pos: &mut usize,
        bit_pos: &mut i32,
    ) -> Result<i32> {
        if *bit_pos < 0 || *bit_pos >= 32 || *pos + 4 > data.len() {
            return Err(LercError::UnexpectedEof);
        }

        // Read current uint32
        let temp = u32::from_le_bytes([
            data[*pos],
            data[*pos + 1],
            data[*pos + 2],
            data[*pos + 3],
        ]);

        let val_tmp = ((temp << *bit_pos) >> (32 - self.num_bits_lut)) as usize;

        // Handle case where we need bits from next uint32
        let val_tmp = if 32 - *bit_pos < self.num_bits_lut {
            if *pos + 8 > data.len() {
                return Err(LercError::UnexpectedEof);
            }
            let temp2 = u32::from_le_bytes([
                data[*pos + 4],
                data[*pos + 5],
                data[*pos + 6],
                data[*pos + 7],
            ]);
            val_tmp | ((temp2 >> (64 - *bit_pos - self.num_bits_lut)) as usize)
        } else {
            val_tmp
        };

        // Try LUT first
        let (len, value) = self.decode_lut[val_tmp];
        if len >= 0 {
            *bit_pos += len as i32;
            if *bit_pos >= 32 {
                *bit_pos -= 32;
                *pos += 4;
            }
            return Ok(value as i32);
        }

        // Fall back to tree traversal
        if self.root.is_none() {
            return Err(LercError::HuffmanError("No decode tree".into()));
        }

        // Skip leading zero bits
        *bit_pos += self.num_bits_to_skip_in_tree;
        if *bit_pos >= 32 {
            *bit_pos -= 32;
            *pos += 4;
        }

        let mut node = self.root.as_ref().unwrap();
        let mut value = -1i32;

        while value < 0 {
            if *pos + 4 > data.len() {
                return Err(LercError::UnexpectedEof);
            }

            let temp = u32::from_le_bytes([
                data[*pos],
                data[*pos + 1],
                data[*pos + 2],
                data[*pos + 3],
            ]);

            let bit = ((temp << *bit_pos) >> 31) as i32;
            *bit_pos += 1;
            if *bit_pos == 32 {
                *bit_pos = 0;
                *pos += 4;
            }

            node = if bit != 0 {
                match &node.child1 {
                    Some(child) => child,
                    None => return Err(LercError::HuffmanError("Invalid tree".into())),
                }
            } else {
                match &node.child0 {
                    Some(child) => child,
                    None => return Err(LercError::HuffmanError("Invalid tree".into())),
                }
            };

            if node.value >= 0 {
                value = node.value as i32;
            }
        }

        Ok(value)
    }

    /// Clear the decoder state
    pub fn clear(&mut self) {
        self.code_table.clear();
        self.decode_lut.clear();
        self.root = None;
        self.num_bits_lut = 0;
        self.num_bits_to_skip_in_tree = 0;
    }
}

impl Default for Huffman {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_index_wrap_around() {
        assert_eq!(Huffman::get_index_wrap_around(5, 10), 5);
        assert_eq!(Huffman::get_index_wrap_around(10, 10), 0);
        assert_eq!(Huffman::get_index_wrap_around(15, 10), 5);
    }

    #[test]
    fn test_new_huffman() {
        let h = Huffman::new();
        assert!(h.code_table.is_empty());
        assert!(h.decode_lut.is_empty());
        assert!(h.root.is_none());
    }
}

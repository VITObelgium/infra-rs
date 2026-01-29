//! Huffman coding for LERC decompression
//!
//! This module implements Huffman decoding as used in LERC2 for
//! compressing 8-bit data types (Byte, Char).
//!
//! Performance optimizations:
//! - Flat array-based tree (no Box pointer chasing)
//! - 64-bit bit buffer for efficient bit reading
//! - Lookup table for short codes (≤12 bits)
//! - Unsafe bounds-unchecked access in hot paths

use crate::bit_stuffer::BitStuffer2;
use crate::error::{LercError, Result};

/// Maximum number of bits for the decode lookup table
const MAX_NUM_BITS_LUT: i32 = 12;

/// Maximum histogram size (2^15)
const MAX_HISTO_SIZE: usize = 1 << 15;

/// Invalid node index sentinel value
const INVALID_NODE: u32 = u32::MAX;

/// A flat tree node using indices instead of Box pointers
#[derive(Clone, Copy)]
struct FlatNode {
    /// Decoded value (-1 for internal nodes, >= 0 for leaves)
    value: i16,
    /// Index of child0 in the flat tree array (INVALID_NODE if none)
    child0: u32,
    /// Index of child1 in the flat tree array (INVALID_NODE if none)
    child1: u32,
}

impl FlatNode {
    #[inline(always)]
    const fn new_internal() -> Self {
        FlatNode {
            value: -1,
            child0: INVALID_NODE,
            child1: INVALID_NODE,
        }
    }

    #[inline(always)]
    const fn is_leaf(&self) -> bool {
        self.value >= 0
    }
}

/// Huffman decoder for LERC with optimized flat tree and bit buffer
pub struct Huffman {
    /// Code table: (code_length, code_value) for each symbol
    code_table: Vec<(u16, u32)>,
    /// Decode lookup table for fast decoding of short codes
    /// Entry format: (length, value) - if length >= 0, it's valid
    decode_lut: Vec<(i16, i16)>,
    /// Number of bits used by the LUT
    num_bits_lut: i32,
    /// Number of leading zero bits to skip before entering the tree
    num_bits_to_skip_in_tree: i32,
    /// Flat array representation of the Huffman tree
    flat_tree: Vec<FlatNode>,
    /// Whether a tree is needed (codes longer than LUT)
    need_tree: bool,
}

impl Huffman {
    /// Create a new Huffman decoder
    pub fn new() -> Self {
        Huffman {
            code_table: Vec::new(),
            decode_lut: Vec::new(),
            num_bits_lut: 0,
            num_bits_to_skip_in_tree: 0,
            flat_tree: Vec::new(),
            need_tree: false,
        }
    }

    /// Get index with wrap-around for handling code ranges that wrap
    #[inline(always)]
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

        if Self::get_index_wrap_around(i0, size) >= size || Self::get_index_wrap_around(i1 - 1, size) >= size {
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
                let mut temp = u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);

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

                    temp = u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);

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

    /// Build the decode tree from the code table (flat array version)
    pub fn build_tree_from_codes(&mut self) -> Result<i32> {
        let (i0, i1, max_len) = self.get_range()?;

        let size = self.code_table.len() as i32;
        let mut min_num_zero_bits = 32i32;

        self.need_tree = max_len > MAX_NUM_BITS_LUT;
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

        self.num_bits_to_skip_in_tree = if self.need_tree { min_num_zero_bits } else { 0 };

        if !self.need_tree {
            self.flat_tree.clear();
            return Ok(self.num_bits_lut);
        }

        // Build the flat tree for codes longer than LUT
        // Pre-allocate with a reasonable size estimate
        self.flat_tree.clear();
        self.flat_tree.push(FlatNode::new_internal()); // Root at index 0

        for i in i0..i1 {
            let k = Self::get_index_wrap_around(i, size) as usize;
            let len = self.code_table[k].0 as i32;

            if len > 0 && len > self.num_bits_lut {
                let code = self.code_table[k].1;
                let mut node_idx = 0u32; // Start at root
                let mut j = len - self.num_bits_to_skip_in_tree;

                while j > 0 {
                    j -= 1;
                    let bit = (code >> j) & 1;

                    let next_idx = if bit == 1 {
                        let child_idx = self.flat_tree[node_idx as usize].child1;
                        if child_idx == INVALID_NODE {
                            let new_idx = self.flat_tree.len() as u32;
                            self.flat_tree.push(FlatNode::new_internal());
                            self.flat_tree[node_idx as usize].child1 = new_idx;
                            new_idx
                        } else {
                            child_idx
                        }
                    } else {
                        let child_idx = self.flat_tree[node_idx as usize].child0;
                        if child_idx == INVALID_NODE {
                            let new_idx = self.flat_tree.len() as u32;
                            self.flat_tree.push(FlatNode::new_internal());
                            self.flat_tree[node_idx as usize].child0 = new_idx;
                            new_idx
                        } else {
                            child_idx
                        }
                    };

                    node_idx = next_idx;

                    if j == 0 {
                        self.flat_tree[node_idx as usize].value = k as i16;
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

    /// Read a u32 from the data buffer at the given position (little-endian)
    #[inline(always)]
    unsafe fn read_u32_unchecked(data: &[u8], pos: usize) -> u32 {
        // SAFETY: Caller must ensure pos + 3 < data.len()
        unsafe {
            u32::from_le_bytes([
                *data.get_unchecked(pos),
                *data.get_unchecked(pos + 1),
                *data.get_unchecked(pos + 2),
                *data.get_unchecked(pos + 3),
            ])
        }
    }

    /// Read a u64 from the data buffer at the given position (for bit window)
    #[inline(always)]
    unsafe fn read_u64_window_unchecked(data: &[u8], pos: usize) -> u64 {
        // SAFETY: Caller must ensure pos + 7 < data.len()
        // Read two u32s and combine them into a 64-bit window
        // First u32 is the high bits, second is the low bits
        unsafe {
            let hi = Self::read_u32_unchecked(data, pos) as u64;
            let lo = Self::read_u32_unchecked(data, pos + 4) as u64;
            (hi << 32) | lo
        }
    }

    /// Decode a single Huffman-coded value (optimized version)
    ///
    /// # Arguments
    /// * `data` - Input byte buffer
    /// * `pos` - Current byte position (will be updated)
    /// * `bit_pos` - Current bit position within byte (will be updated)
    ///
    /// # Returns
    /// The decoded symbol value
    #[inline(always)]
    pub fn decode_one_value(&self, data: &[u8], pos: &mut usize, bit_pos: &mut i32) -> Result<i32> {
        // Bounds check once at the start - need at least 8 bytes for the 64-bit window
        if *pos + 8 > data.len() {
            return self.decode_one_value_safe(data, pos, bit_pos);
        }

        // SAFETY: We just checked that pos + 8 <= data.len()
        unsafe { self.decode_one_value_fast(data, pos, bit_pos) }
    }

    /// Fast path for decode_one_value when we know we have enough bytes
    #[inline(always)]
    unsafe fn decode_one_value_fast(&self, data: &[u8], pos: &mut usize, bit_pos: &mut i32) -> Result<i32> {
        // SAFETY: Caller must ensure pos + 8 <= data.len()
        unsafe {
            // Read 64-bit window starting at current position
            let window = Self::read_u64_window_unchecked(data, *pos);

            // Extract bits for LUT lookup (top num_bits_lut bits after shifting by bit_pos)
            let val_tmp = ((window << *bit_pos) >> (64 - self.num_bits_lut)) as usize;

            // Try LUT first (most common case)
            let (len, value) = *self.decode_lut.get_unchecked(val_tmp);

            if len >= 0 {
                // LUT hit - update position and return
                *bit_pos += len as i32;
                if *bit_pos >= 32 {
                    *bit_pos -= 32;
                    *pos += 4;
                }
                return Ok(value as i32);
            }

            // Fall back to tree traversal for longer codes
            if !self.need_tree || self.flat_tree.is_empty() {
                return Err(LercError::HuffmanError("No decode tree".into()));
            }

            // Skip leading zero bits
            *bit_pos += self.num_bits_to_skip_in_tree;
            if *bit_pos >= 32 {
                *bit_pos -= 32;
                *pos += 4;
            }

            // Traverse the flat tree
            let mut node_idx = 0u32;

            loop {
                // Check bounds for tree traversal (may need to read more data)
                if *pos + 8 > data.len() {
                    return self.decode_tree_safe(data, pos, bit_pos, node_idx);
                }

                let window = Self::read_u64_window_unchecked(data, *pos);
                let bit = ((window << *bit_pos) >> 63) as u32;

                *bit_pos += 1;
                if *bit_pos == 32 {
                    *bit_pos = 0;
                    *pos += 4;
                }

                let node = self.flat_tree.get_unchecked(node_idx as usize);
                node_idx = if bit != 0 { node.child1 } else { node.child0 };

                if node_idx == INVALID_NODE {
                    return Err(LercError::HuffmanError("Invalid tree".into()));
                }

                let node = self.flat_tree.get_unchecked(node_idx as usize);
                if node.is_leaf() {
                    return Ok(node.value as i32);
                }
            }
        }
    }

    /// Safe (bounds-checked) version for when we're near the end of data
    #[inline(never)]
    fn decode_one_value_safe(&self, data: &[u8], pos: &mut usize, bit_pos: &mut i32) -> Result<i32> {
        if *bit_pos < 0 || *bit_pos >= 32 || *pos + 4 > data.len() {
            return Err(LercError::UnexpectedEof);
        }

        // Read current uint32
        let temp = u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);

        let val_tmp = ((temp << *bit_pos) >> (32 - self.num_bits_lut)) as usize;

        // Handle case where we need bits from next uint32
        let val_tmp = if 32 - *bit_pos < self.num_bits_lut {
            if *pos + 8 > data.len() {
                return Err(LercError::UnexpectedEof);
            }
            let temp2 = u32::from_le_bytes([data[*pos + 4], data[*pos + 5], data[*pos + 6], data[*pos + 7]]);
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
        if !self.need_tree || self.flat_tree.is_empty() {
            return Err(LercError::HuffmanError("No decode tree".into()));
        }

        // Skip leading zero bits
        *bit_pos += self.num_bits_to_skip_in_tree;
        if *bit_pos >= 32 {
            *bit_pos -= 32;
            *pos += 4;
        }

        self.decode_tree_safe(data, pos, bit_pos, 0)
    }

    /// Safe tree traversal for when we're near end of data
    #[inline(never)]
    fn decode_tree_safe(&self, data: &[u8], pos: &mut usize, bit_pos: &mut i32, start_node: u32) -> Result<i32> {
        let mut node_idx = start_node;

        loop {
            if *pos + 4 > data.len() {
                return Err(LercError::UnexpectedEof);
            }

            let temp = u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);

            let bit = ((temp << *bit_pos) >> 31) as i32;
            *bit_pos += 1;
            if *bit_pos == 32 {
                *bit_pos = 0;
                *pos += 4;
            }

            let node = &self.flat_tree[node_idx as usize];
            node_idx = if bit != 0 { node.child1 } else { node.child0 };

            if node_idx == INVALID_NODE {
                return Err(LercError::HuffmanError("Invalid tree".into()));
            }

            let node = &self.flat_tree[node_idx as usize];
            if node.is_leaf() {
                return Ok(node.value as i32);
            }
        }
    }

    /// Clear the decoder state
    pub fn clear(&mut self) {
        self.code_table.clear();
        self.decode_lut.clear();
        self.flat_tree.clear();
        self.num_bits_lut = 0;
        self.num_bits_to_skip_in_tree = 0;
        self.need_tree = false;
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
        assert!(h.flat_tree.is_empty());
    }

    #[test]
    fn test_flat_node() {
        let internal = FlatNode::new_internal();
        assert!(!internal.is_leaf());
        assert_eq!(internal.value, -1);
        assert_eq!(internal.child0, INVALID_NODE);
        assert_eq!(internal.child1, INVALID_NODE);

        let mut leaf = FlatNode::new_internal();
        leaf.value = 42;
        assert!(leaf.is_leaf());
    }
}

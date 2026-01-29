//! Bit mask for tracking valid/invalid pixels
//!
//! The bit mask stores one bit per pixel to indicate validity.
//! Bit 7 of byte 0 corresponds to pixel 0, bit 6 to pixel 1, etc.

use crate::error::{LercError, Result};

/// BitMask for tracking valid/invalid pixels in a raster
#[derive(Debug, Clone)]
pub struct BitMask {
    bits: Vec<u8>,
    n_cols: i32,
    n_rows: i32,
}

impl BitMask {
    /// Create a new empty BitMask
    pub fn new() -> Self {
        BitMask {
            bits: Vec::new(),
            n_cols: 0,
            n_rows: 0,
        }
    }

    /// Create a BitMask with the specified dimensions
    pub fn with_size(n_cols: i32, n_rows: i32) -> Result<Self> {
        if n_cols <= 0 || n_rows <= 0 {
            return Err(LercError::InvalidData("Invalid mask dimensions".into()));
        }

        let size = Self::compute_size(n_cols, n_rows);
        Ok(BitMask {
            bits: vec![0; size],
            n_cols,
            n_rows,
        })
    }

    /// Set the size of the mask, resizing internal storage
    pub fn set_size(&mut self, n_cols: i32, n_rows: i32) -> Result<()> {
        if n_cols <= 0 || n_rows <= 0 {
            return Err(LercError::InvalidData("Invalid mask dimensions".into()));
        }

        let size = Self::compute_size(n_cols, n_rows);
        self.bits.resize(size, 0);
        self.n_cols = n_cols;
        self.n_rows = n_rows;
        Ok(())
    }

    /// Compute the byte size needed for the given dimensions
    fn compute_size(n_cols: i32, n_rows: i32) -> usize {
        ((n_cols as usize * n_rows as usize) + 7) >> 3
    }

    /// Get the size in bytes
    pub fn size(&self) -> usize {
        Self::compute_size(self.n_cols, self.n_rows)
    }

    /// Get the width
    pub fn width(&self) -> i32 {
        self.n_cols
    }

    /// Get the height
    pub fn height(&self) -> i32 {
        self.n_rows
    }

    /// Get the bit pattern for position k (which bit within a byte)
    #[inline]
    fn bit(k: i32) -> u8 {
        (1 << 7) >> (k & 7)
    }

    /// Check if pixel at linear index k is valid
    #[inline]
    pub fn is_valid(&self, k: i32) -> bool {
        let byte_idx = (k >> 3) as usize;
        if byte_idx >= self.bits.len() {
            return false;
        }
        (self.bits[byte_idx] & Self::bit(k)) != 0
    }

    /// Check if pixel at (row, col) is valid
    #[inline]
    pub fn is_valid_at(&self, row: i32, col: i32) -> bool {
        self.is_valid(row * self.n_cols + col)
    }

    /// Set pixel at linear index k as valid
    #[inline]
    pub fn set_valid(&mut self, k: i32) {
        let byte_idx = (k >> 3) as usize;
        if byte_idx < self.bits.len() {
            self.bits[byte_idx] |= Self::bit(k);
        }
    }

    /// Set pixel at (row, col) as valid
    #[inline]
    pub fn set_valid_at(&mut self, row: i32, col: i32) {
        self.set_valid(row * self.n_cols + col);
    }

    /// Set pixel at linear index k as invalid
    #[inline]
    pub fn set_invalid(&mut self, k: i32) {
        let byte_idx = (k >> 3) as usize;
        if byte_idx < self.bits.len() {
            self.bits[byte_idx] &= !Self::bit(k);
        }
    }

    /// Set pixel at (row, col) as invalid
    #[inline]
    pub fn set_invalid_at(&mut self, row: i32, col: i32) {
        self.set_invalid(row * self.n_cols + col);
    }

    /// Set all pixels as valid
    pub fn set_all_valid(&mut self) {
        for byte in &mut self.bits {
            *byte = 0xFF;
        }
    }

    /// Set all pixels as invalid
    pub fn set_all_invalid(&mut self) {
        for byte in &mut self.bits {
            *byte = 0x00;
        }
    }

    /// Count the number of valid bits
    pub fn count_valid_bits(&self) -> i32 {
        let total_pixels = self.n_cols * self.n_rows;
        let mut count = 0i32;

        for k in 0..total_pixels {
            if self.is_valid(k) {
                count += 1;
            }
        }

        count
    }

    /// Get direct access to the bits
    pub fn bits(&self) -> &[u8] {
        &self.bits
    }

    /// Get mutable access to the bits
    pub fn bits_mut(&mut self) -> &mut [u8] {
        &mut self.bits
    }

    /// Copy bits from a slice
    pub fn copy_from_slice(&mut self, src: &[u8]) -> Result<()> {
        let len = self.bits.len();
        if src.len() < len {
            return Err(LercError::BufferTooSmall);
        }
        self.bits.copy_from_slice(&src[..len]);
        Ok(())
    }

    /// Convert to a vector of booleans (true = valid)
    pub fn to_bool_vec(&self) -> Vec<bool> {
        let total_pixels = (self.n_cols * self.n_rows) as usize;
        let mut result = Vec::with_capacity(total_pixels);

        for k in 0..total_pixels {
            result.push(self.is_valid(k as i32));
        }

        result
    }

    /// Clear the mask
    pub fn clear(&mut self) {
        self.bits.clear();
        self.n_cols = 0;
        self.n_rows = 0;
    }
}

impl Default for BitMask {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bit_mask_creation() {
        let mask = BitMask::with_size(10, 10).unwrap();
        assert_eq!(mask.width(), 10);
        assert_eq!(mask.height(), 10);
        assert_eq!(mask.size(), 13); // (100 + 7) / 8 = 13
    }

    #[test]
    fn test_bit_mask_valid_invalid() {
        let mut mask = BitMask::with_size(8, 1).unwrap();
        assert_eq!(mask.size(), 1);

        // Initially all invalid
        for k in 0..8 {
            assert!(!mask.is_valid(k));
        }

        // Set some valid
        mask.set_valid(0);
        mask.set_valid(3);
        mask.set_valid(7);

        assert!(mask.is_valid(0));
        assert!(!mask.is_valid(1));
        assert!(!mask.is_valid(2));
        assert!(mask.is_valid(3));
        assert!(!mask.is_valid(4));
        assert!(!mask.is_valid(5));
        assert!(!mask.is_valid(6));
        assert!(mask.is_valid(7));

        assert_eq!(mask.count_valid_bits(), 3);

        // Set one invalid again
        mask.set_invalid(3);
        assert!(!mask.is_valid(3));
        assert_eq!(mask.count_valid_bits(), 2);
    }

    #[test]
    fn test_set_all_valid() {
        let mut mask = BitMask::with_size(16, 1).unwrap();
        mask.set_all_valid();

        for k in 0..16 {
            assert!(mask.is_valid(k));
        }
        assert_eq!(mask.count_valid_bits(), 16);
    }

    #[test]
    fn test_set_all_invalid() {
        let mut mask = BitMask::with_size(16, 1).unwrap();
        mask.set_all_valid();
        mask.set_all_invalid();

        for k in 0..16 {
            assert!(!mask.is_valid(k));
        }
        assert_eq!(mask.count_valid_bits(), 0);
    }

    #[test]
    fn test_2d_access() {
        let mut mask = BitMask::with_size(4, 4).unwrap();

        mask.set_valid_at(1, 2); // row 1, col 2 => k = 1*4 + 2 = 6
        assert!(mask.is_valid_at(1, 2));
        assert!(mask.is_valid(6));
        assert!(!mask.is_valid_at(0, 0));
    }

    #[test]
    fn test_to_bool_vec() {
        let mut mask = BitMask::with_size(4, 1).unwrap();
        mask.set_valid(1);
        mask.set_valid(3);

        let bools = mask.to_bool_vec();
        assert_eq!(bools, vec![false, true, false, true]);
    }
}

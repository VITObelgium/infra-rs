//! LERC2 decoder implementation
//!
//! This module implements the core LERC2 decompression algorithm.

use crate::bit_mask::BitMask;
use crate::bit_stuffer::BitStuffer2;
use crate::error::{LercError, Result};
use crate::huffman::Huffman;
use crate::rle::Rle;

/// File key for LERC2 format identification
const FILE_KEY: &[u8] = b"Lerc2 ";

/// Current maximum supported LERC2 version
const CURRENT_VERSION: i32 = 6;

/// Data types supported by LERC2
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(i32)]
pub enum DataType {
    Char = 0,
    Byte = 1,
    Short = 2,
    UShort = 3,
    Int = 4,
    UInt = 5,
    Float = 6,
    Double = 7,
    Undefined = 8,
}

impl DataType {
    /// Create DataType from integer value
    pub fn from_i32(val: i32) -> Option<Self> {
        match val {
            0 => Some(DataType::Char),
            1 => Some(DataType::Byte),
            2 => Some(DataType::Short),
            3 => Some(DataType::UShort),
            4 => Some(DataType::Int),
            5 => Some(DataType::UInt),
            6 => Some(DataType::Float),
            7 => Some(DataType::Double),
            _ => None,
        }
    }

    /// Get the size in bytes for this data type
    pub fn size(&self) -> usize {
        match self {
            DataType::Char | DataType::Byte => 1,
            DataType::Short | DataType::UShort => 2,
            DataType::Int | DataType::UInt | DataType::Float => 4,
            DataType::Double => 8,
            DataType::Undefined => 0,
        }
    }
}

/// Image encoding mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImageEncodeMode {
    Tiling = 0,
    DeltaHuffman = 1,
    Huffman = 2,
    DeltaDeltaHuffman = 3,
}

impl ImageEncodeMode {
    fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(ImageEncodeMode::Tiling),
            1 => Some(ImageEncodeMode::DeltaHuffman),
            2 => Some(ImageEncodeMode::Huffman),
            3 => Some(ImageEncodeMode::DeltaDeltaHuffman),
            _ => None,
        }
    }
}

/// LERC2 header information
#[derive(Debug, Clone)]
pub struct HeaderInfo {
    pub version: i32,
    pub checksum: u32,
    pub n_rows: i32,
    pub n_cols: i32,
    pub n_depth: i32,
    pub num_valid_pixel: i32,
    pub micro_block_size: i32,
    pub blob_size: i32,
    pub n_blobs_more: i32,
    pub dt: DataType,
    pub max_z_error: f64,
    pub z_min: f64,
    pub z_max: f64,
    pub b_pass_no_data_values: u8,
    pub b_is_int: u8,
    pub no_data_val: f64,
    pub no_data_val_orig: f64,
}

impl HeaderInfo {
    /// Create a new zeroed HeaderInfo
    pub fn new() -> Self {
        HeaderInfo {
            version: 0,
            checksum: 0,
            n_rows: 0,
            n_cols: 0,
            n_depth: 1,
            num_valid_pixel: 0,
            micro_block_size: 0,
            blob_size: 0,
            n_blobs_more: 0,
            dt: DataType::Undefined,
            max_z_error: 0.0,
            z_min: 0.0,
            z_max: 0.0,
            b_pass_no_data_values: 0,
            b_is_int: 0,
            no_data_val: 0.0,
            no_data_val_orig: 0.0,
        }
    }

    /// Check if Huffman coding should be tried for integer types
    pub fn try_huffman_int(&self) -> bool {
        self.version >= 2 && (self.dt == DataType::Byte || self.dt == DataType::Char) && self.max_z_error == 0.5
    }

    /// Check if Huffman coding should be tried for float types
    pub fn try_huffman_flt(&self) -> bool {
        self.version >= 6 && (self.dt == DataType::Float || self.dt == DataType::Double) && self.max_z_error == 0.0
    }
}

impl Default for HeaderInfo {
    fn default() -> Self {
        Self::new()
    }
}

/// Trait for types that can be decoded from LERC
pub trait LercDataType: Copy + Clone + Default + std::fmt::Debug {
    fn from_f64(val: f64) -> Self;
    fn to_f64(self) -> f64;
    fn data_type() -> DataType;
    /// Wrapping addition for integer types, regular addition for floats
    fn wrapping_add(self, other: Self) -> Self;
}

impl LercDataType for i8 {
    fn from_f64(val: f64) -> Self {
        val as i8
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn data_type() -> DataType {
        DataType::Char
    }
    fn wrapping_add(self, other: Self) -> Self {
        self.wrapping_add(other)
    }
}

impl LercDataType for u8 {
    fn from_f64(val: f64) -> Self {
        val as u8
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn data_type() -> DataType {
        DataType::Byte
    }
    fn wrapping_add(self, other: Self) -> Self {
        self.wrapping_add(other)
    }
}

impl LercDataType for i16 {
    fn from_f64(val: f64) -> Self {
        val as i16
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn data_type() -> DataType {
        DataType::Short
    }
    fn wrapping_add(self, other: Self) -> Self {
        self.wrapping_add(other)
    }
}

impl LercDataType for u16 {
    fn from_f64(val: f64) -> Self {
        val as u16
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn data_type() -> DataType {
        DataType::UShort
    }
    fn wrapping_add(self, other: Self) -> Self {
        self.wrapping_add(other)
    }
}

impl LercDataType for i32 {
    fn from_f64(val: f64) -> Self {
        val as i32
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn data_type() -> DataType {
        DataType::Int
    }
    fn wrapping_add(self, other: Self) -> Self {
        self.wrapping_add(other)
    }
}

impl LercDataType for u32 {
    fn from_f64(val: f64) -> Self {
        val as u32
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn data_type() -> DataType {
        DataType::UInt
    }
    fn wrapping_add(self, other: Self) -> Self {
        self.wrapping_add(other)
    }
}

impl LercDataType for f32 {
    fn from_f64(val: f64) -> Self {
        val as f32
    }
    fn to_f64(self) -> f64 {
        self as f64
    }
    fn data_type() -> DataType {
        DataType::Float
    }
    fn wrapping_add(self, other: Self) -> Self {
        self + other
    }
}

impl LercDataType for f64 {
    fn from_f64(val: f64) -> Self {
        val
    }
    fn to_f64(self) -> f64 {
        self
    }
    fn data_type() -> DataType {
        DataType::Double
    }
    fn wrapping_add(self, other: Self) -> Self {
        self + other
    }
}

/// LERC2 decoder
pub struct Lerc2Decoder {
    header_info: HeaderInfo,
    bit_mask: BitMask,
    bit_stuffer: BitStuffer2,
    z_min_vec: Vec<f64>,
    z_max_vec: Vec<f64>,
    image_encode_mode: ImageEncodeMode,
}

impl Lerc2Decoder {
    /// Create a new LERC2 decoder
    pub fn new() -> Self {
        Lerc2Decoder {
            header_info: HeaderInfo::new(),
            bit_mask: BitMask::new(),
            bit_stuffer: BitStuffer2::new(),
            z_min_vec: Vec::new(),
            z_max_vec: Vec::new(),
            image_encode_mode: ImageEncodeMode::Tiling,
        }
    }

    /// Get header information from a LERC2 blob
    pub fn get_header_info(data: &[u8]) -> Result<(HeaderInfo, bool)> {
        let mut pos = 0;
        let mut bytes_remaining = data.len();
        let (header, has_mask) = Self::read_header(data, &mut pos, &mut bytes_remaining)?;
        Ok((header, has_mask))
    }

    /// Get the current header info
    pub fn header_info(&self) -> &HeaderInfo {
        &self.header_info
    }

    /// Get the validity mask as a boolean vector
    pub fn get_mask_as_bool_vec(&self) -> Option<Vec<bool>> {
        if self.header_info.num_valid_pixel == self.header_info.n_rows * self.header_info.n_cols {
            None // All valid
        } else {
            Some(self.bit_mask.to_bool_vec())
        }
    }

    /// Read and parse the LERC2 header
    fn read_header(data: &[u8], pos: &mut usize, bytes_remaining: &mut usize) -> Result<(HeaderInfo, bool)> {
        let key_len = FILE_KEY.len();

        if *bytes_remaining < key_len {
            return Err(LercError::InvalidHeader("Data too small".into()));
        }

        // Check file key
        if &data[*pos..*pos + key_len] != FILE_KEY {
            return Err(LercError::InvalidHeader("Invalid file key".into()));
        }
        *pos += key_len;
        *bytes_remaining -= key_len;

        // Read version
        if *bytes_remaining < 4 {
            return Err(LercError::UnexpectedEof);
        }
        let version = i32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
        *pos += 4;
        *bytes_remaining -= 4;

        if version < 0 || version > CURRENT_VERSION {
            return Err(LercError::UnsupportedVersion(version));
        }

        let mut hd = HeaderInfo::new();
        hd.version = version;

        // Read checksum (version >= 3)
        if version >= 3 {
            if *bytes_remaining < 4 {
                return Err(LercError::UnexpectedEof);
            }
            hd.checksum = u32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
            *pos += 4;
            *bytes_remaining -= 4;
        }

        // Calculate number of integers, bytes, and doubles to read
        let n_ints = 6 + if version >= 4 { 1 } else { 0 } + if version >= 6 { 1 } else { 0 };
        let n_bytes_extra = if version >= 6 { 4 } else { 0 };
        let n_dbls = 3 + if version >= 6 { 2 } else { 0 };

        // Read integers
        let int_len = n_ints * 4;
        if *bytes_remaining < int_len {
            return Err(LercError::UnexpectedEof);
        }

        let mut int_vec = Vec::with_capacity(n_ints);
        for i in 0..n_ints {
            let offset = *pos + i * 4;
            int_vec.push(i32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]));
        }
        *pos += int_len;
        *bytes_remaining -= int_len;

        // Read extra bytes (version >= 6)
        let mut byte_vec = vec![0u8; n_bytes_extra];
        if version >= 6 {
            if *bytes_remaining < n_bytes_extra {
                return Err(LercError::UnexpectedEof);
            }
            byte_vec.copy_from_slice(&data[*pos..*pos + n_bytes_extra]);
            *pos += n_bytes_extra;
            *bytes_remaining -= n_bytes_extra;
        }

        // Read doubles
        let dbl_len = n_dbls * 8;
        if *bytes_remaining < dbl_len {
            return Err(LercError::UnexpectedEof);
        }

        let mut dbl_vec = Vec::with_capacity(n_dbls);
        for i in 0..n_dbls {
            let offset = *pos + i * 8;
            dbl_vec.push(f64::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]));
        }
        *pos += dbl_len;
        *bytes_remaining -= dbl_len;

        // Parse integers
        let mut i = 0;
        hd.n_rows = int_vec[i];
        i += 1;
        hd.n_cols = int_vec[i];
        i += 1;
        if version >= 4 {
            hd.n_depth = int_vec[i];
            i += 1;
        }
        hd.num_valid_pixel = int_vec[i];
        i += 1;
        hd.micro_block_size = int_vec[i];
        i += 1;
        hd.blob_size = int_vec[i];
        i += 1;
        let dt = int_vec[i];
        i += 1;

        hd.dt = DataType::from_i32(dt).ok_or_else(|| LercError::InvalidHeader(format!("Invalid data type: {}", dt)))?;

        if version >= 6 {
            hd.n_blobs_more = int_vec[i];
        }

        // Parse extra bytes
        if version >= 6 {
            hd.b_pass_no_data_values = byte_vec[0];
            hd.b_is_int = byte_vec[1];
        }

        // Parse doubles
        i = 0;
        hd.max_z_error = dbl_vec[i];
        i += 1;
        hd.z_min = dbl_vec[i];
        i += 1;
        hd.z_max = dbl_vec[i];
        i += 1;
        if version >= 6 {
            hd.no_data_val = dbl_vec[i];
            i += 1;
            hd.no_data_val_orig = dbl_vec[i];
        }

        // Validate header
        if hd.n_rows <= 0 || hd.n_cols <= 0 || hd.n_depth <= 0 || hd.num_valid_pixel < 0 || hd.micro_block_size <= 0 || hd.blob_size <= 0 {
            return Err(LercError::InvalidHeader("Invalid header values".into()));
        }

        if hd.n_rows > i32::MAX / hd.n_cols {
            return Err(LercError::InvalidHeader("Dimensions overflow".into()));
        }

        if hd.num_valid_pixel > hd.n_rows * hd.n_cols {
            return Err(LercError::InvalidHeader("Invalid num_valid_pixel".into()));
        }

        // Determine if there's a mask
        let has_mask = hd.num_valid_pixel > 0 && hd.num_valid_pixel < hd.n_rows * hd.n_cols;

        Ok((hd, has_mask))
    }

    /// Read the bit mask from the data stream
    fn read_mask(&mut self, data: &[u8], pos: &mut usize, bytes_remaining: &mut usize) -> Result<()> {
        let num_valid = self.header_info.num_valid_pixel;
        let w = self.header_info.n_cols;
        let h = self.header_info.n_rows;

        if *bytes_remaining < 4 {
            return Err(LercError::UnexpectedEof);
        }

        let num_bytes_mask = i32::from_le_bytes([data[*pos], data[*pos + 1], data[*pos + 2], data[*pos + 3]]);
        *pos += 4;
        *bytes_remaining -= 4;

        // Validate
        if (num_valid == 0 || num_valid == w * h) && num_bytes_mask != 0 {
            return Err(LercError::InvalidMask("Unexpected mask data".into()));
        }

        self.bit_mask.set_size(w, h)?;

        if num_valid == 0 {
            self.bit_mask.set_all_invalid();
        } else if num_valid == w * h {
            self.bit_mask.set_all_valid();
        } else if num_bytes_mask > 0 {
            // Decompress RLE mask
            if *bytes_remaining < num_bytes_mask as usize {
                return Err(LercError::UnexpectedEof);
            }

            let rle_data = &data[*pos..*pos + num_bytes_mask as usize];
            Rle::decompress(rle_data, self.bit_mask.bits_mut())?;

            *pos += num_bytes_mask as usize;
            *bytes_remaining -= num_bytes_mask as usize;
        }

        Ok(())
    }

    /// Compute Fletcher32 checksum
    fn compute_checksum_fletcher32(data: &[u8]) -> u32 {
        let mut sum1 = 0xffffu32;
        let mut sum2 = 0xffffu32;
        let mut words = data.len() / 2;
        let mut i = 0;

        while words > 0 {
            let tlen = std::cmp::min(words, 359);
            words -= tlen;

            for _ in 0..tlen {
                // C++: sum1 += (*pByte++ << 8);
                // C++: sum2 += sum1 += *pByte++;
                // This means: add high byte << 8 to sum1, then add low byte to sum1, then add sum1 to sum2
                sum1 = sum1.wrapping_add((data[i] as u32) << 8);
                i += 1;
                sum1 = sum1.wrapping_add(data[i] as u32);
                sum2 = sum2.wrapping_add(sum1);
                i += 1;
            }

            sum1 = (sum1 & 0xffff) + (sum1 >> 16);
            sum2 = (sum2 & 0xffff) + (sum2 >> 16);
        }

        // Handle odd byte: sum2 += sum1 += (*pByte << 8);
        if data.len() & 1 != 0 {
            sum1 = sum1.wrapping_add((data[i] as u32) << 8);
            sum2 = sum2.wrapping_add(sum1);
        }

        // Second reduction step
        sum1 = (sum1 & 0xffff) + (sum1 >> 16);
        sum2 = (sum2 & 0xffff) + (sum2 >> 16);

        (sum2 << 16) | sum1
    }

    /// Decode LERC2 data
    pub fn decode<T: LercDataType>(&mut self, data: &[u8], bytes_remaining: &mut usize, output: &mut [T]) -> Result<()> {
        let _blob_start = data.len() - *bytes_remaining;
        let mut pos = 0;

        // Read header
        let (header, _has_mask) = Self::read_header(data, &mut pos, bytes_remaining)?;
        self.header_info = header;

        if data.len() < self.header_info.blob_size as usize {
            return Err(LercError::BufferTooSmall);
        }

        // Verify checksum (version >= 3)
        if self.header_info.version >= 3 {
            let n_bytes = FILE_KEY.len() + 4 + 4; // key + version + checksum
            if self.header_info.blob_size < n_bytes as i32 {
                return Err(LercError::InvalidData("Blob size too small".into()));
            }

            let checksum_data = &data[n_bytes..self.header_info.blob_size as usize];
            let checksum = Self::compute_checksum_fletcher32(checksum_data);

            if checksum != self.header_info.checksum {
                return Err(LercError::ChecksumMismatch);
            }
        }

        // Read mask
        self.read_mask(data, &mut pos, bytes_remaining)?;

        // Initialize output to zero
        let total_size = self.header_info.n_cols as usize * self.header_info.n_rows as usize * self.header_info.n_depth as usize;

        if output.len() < total_size {
            return Err(LercError::BufferTooSmall);
        }

        for val in output.iter_mut().take(total_size) {
            *val = T::default();
        }

        // Handle empty image
        if self.header_info.num_valid_pixel == 0 {
            *bytes_remaining = data.len() - self.header_info.blob_size as usize;
            return Ok(());
        }

        // Handle constant image
        if self.header_info.z_min == self.header_info.z_max {
            self.fill_const_image(output)?;
            *bytes_remaining = data.len() - self.header_info.blob_size as usize;
            return Ok(());
        }

        // Read min/max ranges (version >= 4)
        if self.header_info.version >= 4 {
            self.read_min_max_ranges::<T>(data, &mut pos, bytes_remaining)?;

            // Check if all bands are constant
            if self.check_min_max_equal() {
                self.fill_const_image(output)?;
                *bytes_remaining = data.len() - self.header_info.blob_size as usize;
                return Ok(());
            }
        }

        // Read data encoding flag
        if *bytes_remaining < 1 {
            return Err(LercError::UnexpectedEof);
        }

        let read_data_one_sweep = data[pos] != 0;
        pos += 1;
        *bytes_remaining -= 1;

        if !read_data_one_sweep {
            // Check for Huffman encoding
            if self.header_info.try_huffman_int() || self.header_info.try_huffman_flt() {
                if *bytes_remaining < 1 {
                    return Err(LercError::UnexpectedEof);
                }

                let flag = data[pos];
                pos += 1;
                *bytes_remaining -= 1;

                if flag > 3 || (flag > 2 && self.header_info.version < 6) || (flag > 1 && self.header_info.version < 4) {
                    return Err(LercError::InvalidData("Invalid encoding flag".into()));
                }

                self.image_encode_mode =
                    ImageEncodeMode::from_u8(flag).ok_or_else(|| LercError::InvalidData("Invalid image encode mode".into()))?;

                if self.image_encode_mode != ImageEncodeMode::Tiling {
                    if self.header_info.try_huffman_int() {
                        if self.image_encode_mode == ImageEncodeMode::DeltaHuffman
                            || (self.header_info.version >= 4 && self.image_encode_mode == ImageEncodeMode::Huffman)
                        {
                            return self.decode_huffman(data, &mut pos, bytes_remaining, output);
                        } else {
                            return Err(LercError::InvalidData("Invalid Huffman mode".into()));
                        }
                    } else if self.header_info.try_huffman_flt() && self.image_encode_mode == ImageEncodeMode::DeltaDeltaHuffman {
                        // Float point lossless compression
                        return self.decode_fpl(data, &mut pos, bytes_remaining, output);
                    } else {
                        return Err(LercError::InvalidData("Invalid encoding mode".into()));
                    }
                }
            }

            // Tile-based decoding
            self.read_tiles(data, &mut pos, bytes_remaining, output)?;
        } else {
            // Read all data in one sweep (uncompressed)
            self.read_data_one_sweep(data, &mut pos, bytes_remaining, output)?;
        }

        *bytes_remaining = data.len() - self.header_info.blob_size as usize;
        Ok(())
    }

    /// Read min/max ranges per depth
    fn read_min_max_ranges<T: LercDataType>(&mut self, data: &[u8], pos: &mut usize, bytes_remaining: &mut usize) -> Result<()> {
        let n_depth = self.header_info.n_depth as usize;
        let type_size = T::data_type().size();
        let len = n_depth * type_size;

        if *bytes_remaining < len * 2 {
            return Err(LercError::UnexpectedEof);
        }

        self.z_min_vec.resize(n_depth, 0.0);
        self.z_max_vec.resize(n_depth, 0.0);

        // Read min values
        for i in 0..n_depth {
            let val = self.read_value_as_f64::<T>(data, *pos + i * type_size);
            self.z_min_vec[i] = val;
        }
        *pos += len;
        *bytes_remaining -= len;

        // Read max values
        for i in 0..n_depth {
            let val = self.read_value_as_f64::<T>(data, *pos + i * type_size);
            self.z_max_vec[i] = val;
        }
        *pos += len;
        *bytes_remaining -= len;

        Ok(())
    }

    /// Read a value from the data stream as f64
    fn read_value_as_f64<T: LercDataType>(&self, data: &[u8], pos: usize) -> f64 {
        match T::data_type() {
            DataType::Char => data[pos] as i8 as f64,
            DataType::Byte => data[pos] as f64,
            DataType::Short => i16::from_le_bytes([data[pos], data[pos + 1]]) as f64,
            DataType::UShort => u16::from_le_bytes([data[pos], data[pos + 1]]) as f64,
            DataType::Int => i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as f64,
            DataType::UInt => u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as f64,
            DataType::Float => f32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as f64,
            DataType::Double => f64::from_le_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]),
            DataType::Undefined => 0.0,
        }
    }

    /// Check if all min/max ranges are equal (constant image)
    fn check_min_max_equal(&self) -> bool {
        for i in 0..self.z_min_vec.len() {
            if self.z_min_vec[i] != self.z_max_vec[i] {
                return false;
            }
        }
        true
    }

    /// Fill output with constant values
    fn fill_const_image<T: LercDataType>(&self, output: &mut [T]) -> Result<()> {
        let n_cols = self.header_info.n_cols;
        let n_rows = self.header_info.n_rows;
        let n_depth = self.header_info.n_depth as usize;
        let z0 = T::from_f64(self.header_info.z_min);

        if n_depth == 1 {
            for i in 0..n_rows {
                for j in 0..n_cols {
                    let k = i * n_cols + j;
                    if self.bit_mask.is_valid(k) {
                        output[k as usize] = z0;
                    }
                }
            }
        } else {
            let z_buf: Vec<T> = if self.header_info.z_min != self.header_info.z_max {
                if self.z_min_vec.len() != n_depth {
                    return Err(LercError::InvalidData("Min vector size mismatch".into()));
                }
                self.z_min_vec.iter().map(|&v| T::from_f64(v)).collect()
            } else {
                vec![z0; n_depth]
            };

            for i in 0..n_rows {
                for j in 0..n_cols {
                    let k = i * n_cols + j;
                    let m = (k as usize) * n_depth;
                    if self.bit_mask.is_valid(k) {
                        output[m..m + n_depth].copy_from_slice(&z_buf);
                    }
                }
            }
        }

        Ok(())
    }

    /// Read uncompressed data in one sweep
    fn read_data_one_sweep<T: LercDataType>(
        &self,
        data: &[u8],
        pos: &mut usize,
        bytes_remaining: &mut usize,
        output: &mut [T],
    ) -> Result<()> {
        let n_depth = self.header_info.n_depth as usize;
        let type_size = T::data_type().size();
        let len = n_depth * type_size;

        let n_valid_pix = self.bit_mask.count_valid_bits() as usize;

        if *bytes_remaining < n_valid_pix * len {
            return Err(LercError::UnexpectedEof);
        }

        let n_cols = self.header_info.n_cols;
        let n_rows = self.header_info.n_rows;

        let mut src_pos = *pos;

        for i in 0..n_rows {
            for j in 0..n_cols {
                let k = i * n_cols + j;
                let m0 = (k as usize) * n_depth;

                if self.bit_mask.is_valid(k) {
                    for d in 0..n_depth {
                        output[m0 + d] = self.read_typed_value::<T>(data, src_pos + d * type_size);
                    }
                    src_pos += len;
                }
            }
        }

        let bytes_read = n_valid_pix * len;
        *pos += bytes_read;
        *bytes_remaining -= bytes_read;

        Ok(())
    }

    /// Read a typed value from the data stream
    fn read_typed_value<T: LercDataType>(&self, data: &[u8], pos: usize) -> T {
        T::from_f64(self.read_value_as_f64::<T>(data, pos))
    }

    /// Read and decode tiles
    fn read_tiles<T: LercDataType>(&mut self, data: &[u8], pos: &mut usize, bytes_remaining: &mut usize, output: &mut [T]) -> Result<()> {
        let mb_size = self.header_info.micro_block_size;
        let n_depth = self.header_info.n_depth;
        let n_rows = self.header_info.n_rows;
        let n_cols = self.header_info.n_cols;

        if mb_size > 32 {
            return Err(LercError::InvalidData("Micro block size too large".into()));
        }

        let num_tiles_vert = (n_rows + mb_size - 1) / mb_size;
        let num_tiles_hori = (n_cols + mb_size - 1) / mb_size;

        let mut buffer_vec = Vec::new();

        for i_tile in 0..num_tiles_vert {
            let tile_h = if i_tile == num_tiles_vert - 1 {
                n_rows - i_tile * mb_size
            } else {
                mb_size
            };
            let i0 = i_tile * mb_size;

            for j_tile in 0..num_tiles_hori {
                let tile_w = if j_tile == num_tiles_hori - 1 {
                    n_cols - j_tile * mb_size
                } else {
                    mb_size
                };
                let j0 = j_tile * mb_size;

                for i_depth in 0..n_depth {
                    self.read_tile(
                        data,
                        pos,
                        bytes_remaining,
                        output,
                        i0,
                        i0 + tile_h,
                        j0,
                        j0 + tile_w,
                        i_depth,
                        &mut buffer_vec,
                    )?;
                }
            }
        }

        Ok(())
    }

    /// Read and decode a single tile
    fn read_tile<T: LercDataType>(
        &mut self,
        data: &[u8],
        pos: &mut usize,
        bytes_remaining: &mut usize,
        output: &mut [T],
        i0: i32,
        i1: i32,
        j0: i32,
        j1: i32,
        i_depth: i32,
        buffer_vec: &mut Vec<u32>,
    ) -> Result<()> {
        if *bytes_remaining < 1 {
            return Err(LercError::UnexpectedEof);
        }

        let n_cols = self.header_info.n_cols;
        let n_depth = self.header_info.n_depth as usize;

        let compr_flag = data[*pos];
        *pos += 1;
        *bytes_remaining -= 1;

        let b_diff_enc = if self.header_info.version >= 5 {
            (compr_flag & 4) != 0
        } else {
            false
        };

        let pattern = if self.header_info.version >= 5 { 14 } else { 15 };

        // Integrity check
        if ((compr_flag >> 2) & pattern as u8) != (((j0 >> 3) & pattern) as u8) {
            return Err(LercError::InvalidData("Tile integrity check failed".into()));
        }

        if b_diff_enc && i_depth == 0 {
            return Err(LercError::InvalidData("Invalid diff encoding".into()));
        }

        let bits67 = compr_flag >> 6;
        let compr_flag = compr_flag & 3;

        if compr_flag == 2 {
            // Entire tile is constant 0
            for i in i0..i1 {
                let mut k = i * n_cols + j0;
                let mut m = (k as usize) * n_depth + i_depth as usize;

                for _j in j0..j1 {
                    if self.bit_mask.is_valid(k) {
                        output[m] = if b_diff_enc { output[m - 1] } else { T::from_f64(0.0) };
                    }
                    k += 1;
                    m += n_depth;
                }
            }
            return Ok(());
        }

        if compr_flag == 0 {
            // Uncompressed binary
            if b_diff_enc {
                return Err(LercError::InvalidData("Binary mode with diff encoding".into()));
            }

            let type_size = T::data_type().size();

            for i in i0..i1 {
                let mut k = i * n_cols + j0;
                let mut m = (k as usize) * n_depth + i_depth as usize;

                for _j in j0..j1 {
                    if self.bit_mask.is_valid(k) {
                        if *bytes_remaining < type_size {
                            return Err(LercError::UnexpectedEof);
                        }
                        output[m] = self.read_typed_value::<T>(data, *pos);
                        *pos += type_size;
                        *bytes_remaining -= type_size;
                    }
                    k += 1;
                    m += n_depth;
                }
            }
            return Ok(());
        }

        // compr_flag == 1 or 3: Read z's as bit-stuffed integers
        let dt_used = Self::get_data_type_used(
            if b_diff_enc && self.header_info.dt < DataType::Float {
                DataType::Int
            } else {
                self.header_info.dt
            },
            bits67,
        );

        let type_size = dt_used.size();
        if *bytes_remaining < type_size {
            return Err(LercError::UnexpectedEof);
        }

        let offset = self.read_variable_data_type(data, *pos, dt_used);
        *pos += type_size;
        *bytes_remaining -= type_size;

        let z_max = if self.header_info.version >= 4 && self.header_info.n_depth > 1 {
            self.z_max_vec[i_depth as usize]
        } else {
            self.header_info.z_max
        };

        if compr_flag == 3 {
            // Entire tile is constant (offset value)
            for i in i0..i1 {
                let mut k = i * n_cols + j0;
                let mut m = (k as usize) * n_depth + i_depth as usize;

                if !b_diff_enc {
                    let val = T::from_f64(offset);
                    for _j in j0..j1 {
                        if self.bit_mask.is_valid(k) {
                            output[m] = val;
                        }
                        k += 1;
                        m += n_depth;
                    }
                } else {
                    for _j in j0..j1 {
                        if self.bit_mask.is_valid(k) {
                            let z = offset + output[m - 1].to_f64();
                            output[m] = T::from_f64(z.min(z_max));
                        }
                        k += 1;
                        m += n_depth;
                    }
                }
            }
            return Ok(());
        }

        // Bit-stuffed data
        let max_element_count = ((i1 - i0) * (j1 - j0)) as usize;
        *buffer_vec = self.bit_stuffer.decode(data, pos, max_element_count, self.header_info.version)?;

        let inv_scale = 2.0 * self.header_info.max_z_error;

        if buffer_vec.len() == max_element_count {
            // All valid
            let mut src_idx = 0;

            for i in i0..i1 {
                let k = i * n_cols + j0;
                let mut m = (k as usize) * n_depth + i_depth as usize;

                if !b_diff_enc {
                    for _j in j0..j1 {
                        let z = offset + (buffer_vec[src_idx] as f64) * inv_scale;
                        output[m] = T::from_f64(z.min(z_max));
                        src_idx += 1;
                        m += n_depth;
                    }
                } else {
                    for _j in j0..j1 {
                        let z = offset + (buffer_vec[src_idx] as f64) * inv_scale + output[m - 1].to_f64();
                        output[m] = T::from_f64(z.min(z_max));
                        src_idx += 1;
                        m += n_depth;
                    }
                }
            }
        } else {
            // Not all valid
            let mut src_idx = 0;

            for i in i0..i1 {
                let mut k = i * n_cols + j0;
                let mut m = (k as usize) * n_depth + i_depth as usize;

                if !b_diff_enc {
                    for _j in j0..j1 {
                        if self.bit_mask.is_valid(k) {
                            if src_idx >= buffer_vec.len() {
                                return Err(LercError::InvalidData("Buffer underrun".into()));
                            }
                            let z = offset + (buffer_vec[src_idx] as f64) * inv_scale;
                            output[m] = T::from_f64(z.min(z_max));
                            src_idx += 1;
                        }
                        k += 1;
                        m += n_depth;
                    }
                } else {
                    for _j in j0..j1 {
                        if self.bit_mask.is_valid(k) {
                            if src_idx >= buffer_vec.len() {
                                return Err(LercError::InvalidData("Buffer underrun".into()));
                            }
                            let z = offset + (buffer_vec[src_idx] as f64) * inv_scale + output[m - 1].to_f64();
                            output[m] = T::from_f64(z.min(z_max));
                            src_idx += 1;
                        }
                        k += 1;
                        m += n_depth;
                    }
                }
            }
        }

        Ok(())
    }

    /// Get the data type used based on reduction code
    /// This matches the C++ GetDataTypeUsed function logic
    fn get_data_type_used(dt: DataType, reduced_type_code: u8) -> DataType {
        let tc = reduced_type_code as i32;
        match dt {
            DataType::Short | DataType::Int => {
                // dt - tc
                let new_dt = (dt as i32) - tc;
                DataType::from_i32(new_dt).unwrap_or(dt)
            }
            DataType::UShort | DataType::UInt => {
                // dt - 2 * tc
                let new_dt = (dt as i32) - 2 * tc;
                DataType::from_i32(new_dt).unwrap_or(dt)
            }
            DataType::Float => {
                // tc == 0 ? dt : (tc == 1 ? DT_Short : DT_Byte)
                if tc == 0 {
                    dt
                } else if tc == 1 {
                    DataType::Short
                } else {
                    DataType::Byte
                }
            }
            DataType::Double => {
                // tc == 0 ? dt : ValidateDataType(dt - 2 * tc + 1)
                if tc == 0 {
                    dt
                } else {
                    let new_dt = (dt as i32) - 2 * tc + 1;
                    DataType::from_i32(new_dt).unwrap_or(dt)
                }
            }
            _ => dt,
        }
    }

    /// Read a variable-sized value from the data stream
    fn read_variable_data_type(&self, data: &[u8], pos: usize, dt: DataType) -> f64 {
        match dt {
            DataType::Char => data[pos] as i8 as f64,
            DataType::Byte => data[pos] as f64,
            DataType::Short => i16::from_le_bytes([data[pos], data[pos + 1]]) as f64,
            DataType::UShort => u16::from_le_bytes([data[pos], data[pos + 1]]) as f64,
            DataType::Int => i32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as f64,
            DataType::UInt => u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as f64,
            DataType::Float => f32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as f64,
            DataType::Double => f64::from_le_bytes([
                data[pos],
                data[pos + 1],
                data[pos + 2],
                data[pos + 3],
                data[pos + 4],
                data[pos + 5],
                data[pos + 6],
                data[pos + 7],
            ]),
            DataType::Undefined => 0.0,
        }
    }

    /// Decode Huffman-encoded data
    fn decode_fpl<T: LercDataType>(&mut self, data: &[u8], pos: &mut usize, _bytes_remaining: &mut usize, output: &mut [T]) -> Result<()> {
        use crate::fpl::decode_fpl;

        let is_double = self.header_info.dt == DataType::Double;
        let width = self.header_info.n_cols;
        let height = self.header_info.n_rows;
        let depth = self.header_info.n_depth;

        let decoded_bytes = decode_fpl(data, pos, is_double, width, height, depth)?;

        // Convert bytes to output type
        let type_size = T::data_type().size();
        let n_values = output.len();

        if decoded_bytes.len() != n_values * type_size {
            return Err(LercError::FplError(format!(
                "FPL size mismatch: {} vs {}",
                decoded_bytes.len(),
                n_values * type_size
            )));
        }

        // Reinterpret bytes as the target type
        for (i, chunk) in decoded_bytes.chunks_exact(type_size).enumerate() {
            if i < output.len() {
                // Read as f64 then convert
                let val = match type_size {
                    4 => {
                        let bits = u32::from_le_bytes(chunk.try_into().unwrap());
                        f32::from_bits(bits) as f64
                    }
                    8 => {
                        let bits = u64::from_le_bytes(chunk.try_into().unwrap());
                        f64::from_bits(bits)
                    }
                    _ => 0.0,
                };
                output[i] = T::from_f64(val);
            }
        }

        Ok(())
    }

    fn decode_huffman<T: LercDataType>(
        &mut self,
        data: &[u8],
        pos: &mut usize,
        _bytes_remaining: &mut usize,
        output: &mut [T],
    ) -> Result<()> {
        let mut huffman = Huffman::new();
        huffman.read_code_table(data, pos, self.header_info.version)?;
        huffman.build_tree_from_codes()?;

        let offset = if self.header_info.dt == DataType::Char { 128 } else { 0 };
        let height = self.header_info.n_rows;
        let width = self.header_info.n_cols;
        let n_depth = self.header_info.n_depth as usize;

        let mut bit_pos = 0i32;

        if self.header_info.num_valid_pixel == width * height {
            // All valid
            if self.image_encode_mode == ImageEncodeMode::DeltaHuffman {
                for i_depth in 0..n_depth {
                    let mut prev_val = T::from_f64(0.0);

                    for i in 0..height {
                        for j in 0..width {
                            let m = ((i * width + j) as usize) * n_depth + i_depth;

                            let val = huffman.decode_one_value(data, pos, &mut bit_pos)?;
                            let delta = T::from_f64((val - offset) as f64);

                            // Use wrapping addition to match C++ integer overflow behavior
                            let result = if j > 0 {
                                delta.wrapping_add(prev_val)
                            } else if i > 0 {
                                delta.wrapping_add(output[m - (width as usize) * n_depth])
                            } else {
                                delta.wrapping_add(prev_val)
                            };

                            output[m] = result;
                            prev_val = result;
                        }
                    }
                }
            } else if self.image_encode_mode == ImageEncodeMode::Huffman {
                for i in 0..height {
                    for j in 0..width {
                        let m0 = ((i * width + j) as usize) * n_depth;

                        for m in 0..n_depth {
                            let val = huffman.decode_one_value(data, pos, &mut bit_pos)?;
                            output[m0 + m] = T::from_f64((val - offset) as f64);
                        }
                    }
                }
            } else {
                return Err(LercError::HuffmanError("Invalid encode mode".into()));
            }
        } else {
            // Not all valid
            if self.image_encode_mode == ImageEncodeMode::DeltaHuffman {
                for i_depth in 0..n_depth {
                    let mut prev_val = T::from_f64(0.0);

                    for i in 0..height {
                        for j in 0..width {
                            let k = i * width + j;
                            let m = (k as usize) * n_depth + i_depth;

                            if self.bit_mask.is_valid(k) {
                                let val = huffman.decode_one_value(data, pos, &mut bit_pos)?;
                                let delta = T::from_f64((val - offset) as f64);

                                // Use wrapping addition to match C++ integer overflow behavior
                                let result = if j > 0 && self.bit_mask.is_valid(k - 1) {
                                    delta.wrapping_add(prev_val)
                                } else if i > 0 && self.bit_mask.is_valid(k - width) {
                                    delta.wrapping_add(output[m - (width as usize) * n_depth])
                                } else {
                                    delta.wrapping_add(prev_val)
                                };

                                output[m] = result;
                                prev_val = result;
                            }
                        }
                    }
                }
            } else if self.image_encode_mode == ImageEncodeMode::Huffman {
                for i in 0..height {
                    for j in 0..width {
                        let k = i * width + j;
                        let m0 = (k as usize) * n_depth;

                        if self.bit_mask.is_valid(k) {
                            for m in 0..n_depth {
                                let val = huffman.decode_one_value(data, pos, &mut bit_pos)?;
                                output[m0 + m] = T::from_f64((val - offset) as f64);
                            }
                        }
                    }
                }
            } else {
                return Err(LercError::HuffmanError("Invalid encode mode".into()));
            }
        }

        // Advance position by consumed bytes
        let num_uints = if bit_pos > 0 { 1 } else { 0 } + 1;
        *pos += num_uints * 4;

        Ok(())
    }
}

impl Default for Lerc2Decoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_from_i32() {
        assert_eq!(DataType::from_i32(0), Some(DataType::Char));
        assert_eq!(DataType::from_i32(1), Some(DataType::Byte));
        assert_eq!(DataType::from_i32(7), Some(DataType::Double));
        assert_eq!(DataType::from_i32(8), None);
        assert_eq!(DataType::from_i32(-1), None);
    }

    #[test]
    fn test_data_type_size() {
        assert_eq!(DataType::Char.size(), 1);
        assert_eq!(DataType::Byte.size(), 1);
        assert_eq!(DataType::Short.size(), 2);
        assert_eq!(DataType::UShort.size(), 2);
        assert_eq!(DataType::Int.size(), 4);
        assert_eq!(DataType::UInt.size(), 4);
        assert_eq!(DataType::Float.size(), 4);
        assert_eq!(DataType::Double.size(), 8);
    }

    #[test]
    fn test_header_info_default() {
        let hd = HeaderInfo::new();
        assert_eq!(hd.version, 0);
        assert_eq!(hd.n_depth, 1);
        assert_eq!(hd.dt, DataType::Undefined);
    }
}

// TIFF header offsets where ghost data begins
const CLASSIC_TIFF_GHOST_DATA_OFFSET: usize = 8;
const BIGTIFF_GHOST_DATA_OFFSET: usize = 16;
// Size of the GDAL metadata header line "GDAL_STRUCTURAL_METADATA_SIZE=XXXXXX bytes\n"
const GDAL_METADATA_HEADER_LINE_SIZE: usize = 43;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CogBlockOrder {
    RowMajor,
    ColumnMajor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CogLayout {
    IfdsBeforeData,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockLeader {
    SizeAsUint4,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockTrailer {
    Last4BytesRepeated,
}

#[derive(Debug, Clone, Default)]
pub struct GdalGhostData {
    pub known_incompatible_edition: bool,
    pub layout: Option<CogLayout>,
    pub block_order: Option<CogBlockOrder>,
    pub block_leader: Option<BlockLeader>,
    pub block_trailer: Option<BlockTrailer>,
}

impl GdalGhostData {
    pub fn from_tiff_header_buffer(header: &[u8]) -> Option<GdalGhostData> {
        let is_big_tiff = match header[2] {
            0x2a => false, // Classic TIFF magic number
            0x2b => true,  // BigTIFF magic number
            _ => return None,
        };

        let offset = if is_big_tiff {
            BIGTIFF_GHOST_DATA_OFFSET
        } else {
            CLASSIC_TIFF_GHOST_DATA_OFFSET
        };
        debug_assert!(
            header.len() >= offset + GDAL_METADATA_HEADER_LINE_SIZE,
            "Provided header is too small to contain GDAL metadata"
        );

        // GDAL_STRUCTURAL_METADATA_SIZE=XXXXXX bytes\n
        if let Ok(first_line) = std::str::from_utf8(&header[offset..offset + GDAL_METADATA_HEADER_LINE_SIZE]) {
            // The header size is at bytes 30..36 (6 bytes)
            let header_size_str = &first_line[30..36];
            let header_size: usize = match header_size_str.trim().parse() {
                Ok(size) => size,
                Err(_) => return None, // Return None if header size is malformed
            };

            // Check bounds to prevent buffer overflow
            let metadata_start = offset + GDAL_METADATA_HEADER_LINE_SIZE;
            let metadata_end = metadata_start + header_size;

            if metadata_end > header.len() {
                // Size is larger than available buffer, return None for malformed data
                return None;
            }

            let header_str = String::from_utf8_lossy(&header[metadata_start..metadata_end]);
            parse_ghost_metadata(&header_str)
        } else {
            None
        }
    }

    pub fn is_cog(&self) -> bool {
        self.layout == Some(CogLayout::IfdsBeforeData)
            && !self.known_incompatible_edition
            && self.block_order == Some(CogBlockOrder::RowMajor)
            && self.block_leader == Some(BlockLeader::SizeAsUint4)
            && self.block_trailer == Some(BlockTrailer::Last4BytesRepeated)
    }
}

fn parse_ghost_metadata(header_str: &str) -> Option<GdalGhostData> {
    let mut ghost_data = GdalGhostData::default();

    header_str
        .split('\n')
        .filter_map(|s| -> Option<(&str, &str)> {
            s.find('=').map(|pos| {
                let (key, value) = s.split_at(pos);
                (key, &value[1..])
            })
        })
        .for_each(|(key, value)| match key {
            "KNOWN_INCOMPATIBLE_EDITION" => {
                ghost_data.known_incompatible_edition = value.trim() != "NO";
            }
            "LAYOUT" => {
                ghost_data.layout = match value.trim() {
                    "IFDS_BEFORE_DATA" => Some(CogLayout::IfdsBeforeData),
                    _ => None,
                };
            }
            "BLOCK_ORDER" => {
                ghost_data.block_order = match value.trim() {
                    "ROW_MAJOR" => Some(CogBlockOrder::RowMajor),
                    "COLUMN_MAJOR" => Some(CogBlockOrder::ColumnMajor),
                    _ => None,
                };
            }
            "BLOCK_LEADER" => {
                ghost_data.block_leader = match value.trim() {
                    "SIZE_AS_UINT4" => Some(BlockLeader::SizeAsUint4),
                    _ => None,
                };
            }
            "BLOCK_TRAILER" => {
                ghost_data.block_trailer = match value.trim() {
                    "LAST_4_BYTES_REPEATED" => Some(BlockTrailer::Last4BytesRepeated),
                    _ => None,
                };
            }
            _ => {}
        });

    Some(ghost_data)
}

#[cfg(test)]
mod tests {
    //! Unit tests for GDAL ghost data parsing from TIFF headers.
    //!
    //! These tests verify the `from_tiff_header_buffer` function which parses
    //! COG metadata from TIFF file headers.
    //!
    //! The ghost data format is documented at:
    //! <https://gdal.org/en/stable/drivers/raster/cog.html#header-ghost-area>
    //!
    //! Example ghost data format:
    //! ```
    //! GDAL_STRUCTURAL_METADATA_SIZE=000174 bytes
    //! LAYOUT=IFDS_BEFORE_DATA
    //! BLOCK_ORDER=ROW_MAJOR
    //! BLOCK_LEADER=SIZE_AS_UINT4
    //! BLOCK_TRAILER=LAST_4_BYTES_REPEATED
    //! KNOWN_INCOMPATIBLE_EDITION=NO
    //! MASK_INTERLEAVED_WITH_IMAGERY=YES
    //! ```

    use super::*;

    fn create_classic_tiff_header() -> Vec<u8> {
        let mut header = vec![0u8; 1024];
        header[0] = 0x49; // Little endian marker
        header[1] = 0x49;
        header[2] = 0x2a; // Classic TIFF magic number
        header[3] = 0x00;
        header
    }

    fn create_bigtiff_header() -> Vec<u8> {
        let mut header = vec![0u8; 1024];
        header[0] = 0x49; // Little endian marker
        header[1] = 0x49;
        header[2] = 0x2b; // BigTIFF magic number
        header[3] = 0x00;
        header
    }

    fn add_ghost_data(header: &mut [u8], offset: usize, metadata: &str) {
        let metadata_bytes = metadata.as_bytes();
        let size_line = format!("GDAL_STRUCTURAL_METADATA_SIZE={:06} bytes\n", metadata_bytes.len());

        // Add the size line
        header[offset..offset + size_line.len()].copy_from_slice(size_line.as_bytes());

        // Add the metadata
        let start = offset + GDAL_METADATA_HEADER_LINE_SIZE;
        header[start..start + metadata_bytes.len()].copy_from_slice(metadata_bytes);
    }

    #[test]
    fn test_from_tiff_header_buffer_classic_tiff_valid_cog() {
        let mut header = create_classic_tiff_header();
        let metadata = "LAYOUT=IFDS_BEFORE_DATA\nBLOCK_ORDER=ROW_MAJOR\nBLOCK_LEADER=SIZE_AS_UINT4\nBLOCK_TRAILER=LAST_4_BYTES_REPEATED\nKNOWN_INCOMPATIBLE_EDITION=NO\n";
        add_ghost_data(&mut header, CLASSIC_TIFF_GHOST_DATA_OFFSET, metadata);

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_some());

        let ghost_data = result.unwrap();
        assert_eq!(ghost_data.layout, Some(CogLayout::IfdsBeforeData));
        assert_eq!(ghost_data.block_order, Some(CogBlockOrder::RowMajor));
        assert_eq!(ghost_data.block_leader, Some(BlockLeader::SizeAsUint4));
        assert_eq!(ghost_data.block_trailer, Some(BlockTrailer::Last4BytesRepeated));
        assert!(!ghost_data.known_incompatible_edition);
        assert!(ghost_data.is_cog());
    }

    #[test]
    fn test_from_tiff_header_buffer_bigtiff_valid_cog() {
        let mut header = create_bigtiff_header();
        let metadata = "LAYOUT=IFDS_BEFORE_DATA\nBLOCK_ORDER=ROW_MAJOR\nBLOCK_LEADER=SIZE_AS_UINT4\nBLOCK_TRAILER=LAST_4_BYTES_REPEATED\nKNOWN_INCOMPATIBLE_EDITION=NO\n";
        add_ghost_data(&mut header, BIGTIFF_GHOST_DATA_OFFSET, metadata);

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_some());

        let ghost_data = result.unwrap();
        assert_eq!(ghost_data.layout, Some(CogLayout::IfdsBeforeData));
        assert_eq!(ghost_data.block_order, Some(CogBlockOrder::RowMajor));
        assert_eq!(ghost_data.block_leader, Some(BlockLeader::SizeAsUint4));
        assert_eq!(ghost_data.block_trailer, Some(BlockTrailer::Last4BytesRepeated));
        assert!(!ghost_data.known_incompatible_edition);
        assert!(ghost_data.is_cog());
    }

    #[test]
    fn test_from_tiff_header_buffer_with_mask_interleaved() {
        let mut header = create_classic_tiff_header();
        let metadata = "LAYOUT=IFDS_BEFORE_DATA\nBLOCK_ORDER=ROW_MAJOR\nBLOCK_LEADER=SIZE_AS_UINT4\nBLOCK_TRAILER=LAST_4_BYTES_REPEATED\nKNOWN_INCOMPATIBLE_EDITION=NO\nMASK_INTERLEAVED_WITH_IMAGERY=YES\n";
        add_ghost_data(&mut header, CLASSIC_TIFF_GHOST_DATA_OFFSET, metadata);

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_some());

        let ghost_data = result.unwrap();
        assert!(ghost_data.is_cog());
    }

    #[test]
    fn test_from_tiff_header_buffer_incompatible_edition() {
        let mut header = create_classic_tiff_header();
        let metadata = "LAYOUT=IFDS_BEFORE_DATA\nBLOCK_ORDER=ROW_MAJOR\nBLOCK_LEADER=SIZE_AS_UINT4\nBLOCK_TRAILER=LAST_4_BYTES_REPEATED\nKNOWN_INCOMPATIBLE_EDITION=YES\n";
        add_ghost_data(&mut header, CLASSIC_TIFF_GHOST_DATA_OFFSET, metadata);

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_some());

        let ghost_data = result.unwrap();
        assert!(ghost_data.known_incompatible_edition);
        assert!(!ghost_data.is_cog()); // Should not be considered a valid COG
    }

    #[test]
    fn test_from_tiff_header_buffer_column_major() {
        let mut header = create_classic_tiff_header();
        let metadata = "LAYOUT=IFDS_BEFORE_DATA\nBLOCK_ORDER=COLUMN_MAJOR\nBLOCK_LEADER=SIZE_AS_UINT4\nBLOCK_TRAILER=LAST_4_BYTES_REPEATED\nKNOWN_INCOMPATIBLE_EDITION=NO\n";
        add_ghost_data(&mut header, CLASSIC_TIFF_GHOST_DATA_OFFSET, metadata);

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_some());

        let ghost_data = result.unwrap();
        assert_eq!(ghost_data.block_order, Some(CogBlockOrder::ColumnMajor));
        assert!(!ghost_data.is_cog()); // Should not be considered a valid COG due to column major
    }

    #[test]
    fn test_from_tiff_header_buffer_invalid_magic_number() {
        let mut header = vec![0u8; 1024];
        header[0] = 0x49;
        header[1] = 0x49;
        header[2] = 0x28; // Invalid magic number
        header[3] = 0x00;

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_none());
    }

    #[test]
    fn test_from_tiff_header_buffer_minimal_ghost_data() {
        let mut header = create_classic_tiff_header();
        let metadata = "KNOWN_INCOMPATIBLE_EDITION=NO\n";
        add_ghost_data(&mut header, CLASSIC_TIFF_GHOST_DATA_OFFSET, metadata);

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_some());

        let ghost_data = result.unwrap();
        assert!(!ghost_data.known_incompatible_edition);
        assert_eq!(ghost_data.layout, None);
        assert_eq!(ghost_data.block_order, None);
        assert_eq!(ghost_data.block_leader, None);
        assert_eq!(ghost_data.block_trailer, None);
        assert!(!ghost_data.is_cog()); // Not a COG without required fields
    }

    #[test]
    fn test_from_tiff_header_buffer_gdal_example_format() {
        // This test uses the exact format from the GDAL documentation
        let mut header = create_classic_tiff_header();
        let metadata = "LAYOUT=IFDS_BEFORE_DATA\nBLOCK_ORDER=ROW_MAJOR\nBLOCK_LEADER=SIZE_AS_UINT4\nBLOCK_TRAILER=LAST_4_BYTES_REPEATED\nKNOWN_INCOMPATIBLE_EDITION=NO\nMASK_INTERLEAVED_WITH_IMAGERY=YES\n";

        // Manually construct the exact ghost data format from GDAL docs
        let ghost_data = format!("GDAL_STRUCTURAL_METADATA_SIZE={:06} bytes\n{}", metadata.len(), metadata);
        let ghost_bytes = ghost_data.as_bytes();

        // Place it at offset for classic TIFF
        header[CLASSIC_TIFF_GHOST_DATA_OFFSET..CLASSIC_TIFF_GHOST_DATA_OFFSET + ghost_bytes.len()].copy_from_slice(ghost_bytes);

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_some());

        let parsed_ghost_data = result.unwrap();
        assert_eq!(parsed_ghost_data.layout, Some(CogLayout::IfdsBeforeData));
        assert_eq!(parsed_ghost_data.block_order, Some(CogBlockOrder::RowMajor));
        assert_eq!(parsed_ghost_data.block_leader, Some(BlockLeader::SizeAsUint4));
        assert_eq!(parsed_ghost_data.block_trailer, Some(BlockTrailer::Last4BytesRepeated));
        assert!(!parsed_ghost_data.known_incompatible_edition);
        assert!(parsed_ghost_data.is_cog());
    }

    #[test]
    fn test_from_tiff_header_buffer_empty_values() {
        let mut header = create_classic_tiff_header();
        let metadata = "LAYOUT=\nBLOCK_ORDER=INVALID\nKNOWN_INCOMPATIBLE_EDITION=NO\n";
        add_ghost_data(&mut header, CLASSIC_TIFF_GHOST_DATA_OFFSET, metadata);

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_some());

        let ghost_data = result.unwrap();
        assert_eq!(ghost_data.layout, None); // Empty value should result in None
        assert_eq!(ghost_data.block_order, None); // Invalid value should result in None
        assert!(!ghost_data.known_incompatible_edition);
    }

    #[test]
    fn test_from_tiff_header_buffer_header_too_small() {
        let header = vec![0u8; 20]; // Too small to contain ghost data
        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_none());
    }

    #[test]
    fn test_from_tiff_header_buffer_exact_gdal_documentation_example() {
        // Test with the exact 172-byte example from GDAL documentation
        let mut header = create_classic_tiff_header();

        // This is the exact metadata size from the GDAL docs example (172 bytes)
        let metadata = "LAYOUT=IFDS_BEFORE_DATA\nBLOCK_ORDER=ROW_MAJOR\nBLOCK_LEADER=SIZE_AS_UINT4\nBLOCK_TRAILER=LAST_4_BYTES_REPEATED\nKNOWN_INCOMPATIBLE_EDITION=NO\nMASK_INTERLEAVED_WITH_IMAGERY=YES\n ";
        assert_eq!(metadata.len(), 174); // Verify it matches the actual calculated size

        add_ghost_data(&mut header, CLASSIC_TIFF_GHOST_DATA_OFFSET, metadata);

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_some());

        let ghost_data = result.unwrap();
        assert_eq!(ghost_data.layout, Some(CogLayout::IfdsBeforeData));
        assert_eq!(ghost_data.block_order, Some(CogBlockOrder::RowMajor));
        assert_eq!(ghost_data.block_leader, Some(BlockLeader::SizeAsUint4));
        assert_eq!(ghost_data.block_trailer, Some(BlockTrailer::Last4BytesRepeated));
        assert!(!ghost_data.known_incompatible_edition);
        assert!(ghost_data.is_cog());
    }

    #[test]
    fn test_from_tiff_header_buffer_with_spaces_and_special_chars() {
        let mut header = create_classic_tiff_header();
        let metadata = "LAYOUT=IFDS_BEFORE_DATA\nBLOCK_ORDER=ROW_MAJOR\nBLOCK_LEADER=SIZE_AS_UINT4\nBLOCK_TRAILER=LAST_4_BYTES_REPEATED\nKNOWN_INCOMPATIBLE_EDITION=NO\n ";
        add_ghost_data(&mut header, CLASSIC_TIFF_GHOST_DATA_OFFSET, metadata);

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_some());

        let ghost_data = result.unwrap();
        assert!(ghost_data.is_cog());
    }

    #[test]
    fn test_from_tiff_header_buffer_malformed_size_field() {
        let mut header = create_classic_tiff_header();

        // Create malformed ghost data with invalid size
        let malformed_size_line = "GDAL_STRUCTURAL_METADATA_SIZE=INVALID bytes\n";
        let metadata = "LAYOUT=IFDS_BEFORE_DATA\n";

        header[CLASSIC_TIFF_GHOST_DATA_OFFSET..CLASSIC_TIFF_GHOST_DATA_OFFSET + malformed_size_line.len()]
            .copy_from_slice(malformed_size_line.as_bytes());
        let start = CLASSIC_TIFF_GHOST_DATA_OFFSET + GDAL_METADATA_HEADER_LINE_SIZE;
        header[start..start + metadata.len()].copy_from_slice(metadata.as_bytes());

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_none()); // Should return None due to malformed size field
    }

    #[test]
    fn test_from_tiff_header_buffer_non_utf8_metadata() {
        let mut header = create_classic_tiff_header();

        // Create proper size line but with invalid UTF-8 in metadata portion
        let size_line = "GDAL_STRUCTURAL_METADATA_SIZE=000010 bytes\n";
        header[CLASSIC_TIFF_GHOST_DATA_OFFSET..CLASSIC_TIFF_GHOST_DATA_OFFSET + size_line.len()].copy_from_slice(size_line.as_bytes());

        // Add invalid UTF-8 bytes in the metadata section
        let start = CLASSIC_TIFF_GHOST_DATA_OFFSET + GDAL_METADATA_HEADER_LINE_SIZE;
        header[start] = 0xFF; // Invalid UTF-8 byte
        header[start + 1] = 0xFE;

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_some()); // Should still parse due to from_utf8_lossy usage in metadata section

        let ghost_data = result.unwrap();
        // The metadata parsing will succeed but with lossy conversion
        assert_eq!(ghost_data.layout, None); // No valid layout parsed from corrupted data
    }

    #[test]
    fn test_from_tiff_header_buffer_boundary_conditions() {
        // Test with exactly minimum required size (offset + header line size)
        let mut header = create_classic_tiff_header();
        header.resize(CLASSIC_TIFF_GHOST_DATA_OFFSET + GDAL_METADATA_HEADER_LINE_SIZE, 0); // offset + header line size (minimum for first line)

        let metadata = "";
        add_ghost_data(&mut header, CLASSIC_TIFF_GHOST_DATA_OFFSET, metadata);

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_some());

        let ghost_data = result.unwrap();
        assert_eq!(ghost_data.layout, None);
        assert!(!ghost_data.is_cog());
    }

    #[test]
    fn test_from_tiff_header_buffer_multiple_equals_signs() {
        // Test handling of key-value pairs with multiple equals signs
        let mut header = create_classic_tiff_header();
        let metadata = "LAYOUT=IFDS_BEFORE_DATA\nSTRANGE_KEY=value=with=equals\nKNOWN_INCOMPATIBLE_EDITION=NO\n";
        add_ghost_data(&mut header, CLASSIC_TIFF_GHOST_DATA_OFFSET, metadata);

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_some());

        let ghost_data = result.unwrap();
        assert_eq!(ghost_data.layout, Some(CogLayout::IfdsBeforeData));
        assert!(!ghost_data.known_incompatible_edition);
    }

    #[test]
    fn test_from_tiff_header_buffer_case_sensitivity() {
        // Verify that the parsing is case-sensitive
        let mut header = create_classic_tiff_header();
        let metadata = "layout=IFDS_BEFORE_DATA\nblock_order=ROW_MAJOR\nKNOWN_INCOMPATIBLE_EDITION=no\n";
        add_ghost_data(&mut header, CLASSIC_TIFF_GHOST_DATA_OFFSET, metadata);

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_some());

        let ghost_data = result.unwrap();
        // Should not parse due to case sensitivity
        assert_eq!(ghost_data.layout, None);
        assert_eq!(ghost_data.block_order, None);
        assert!(ghost_data.known_incompatible_edition); // "no" != "NO"
    }

    #[test]
    fn test_from_tiff_header_buffer_zero_size_metadata() {
        // Test with zero-length metadata section
        let mut header = create_classic_tiff_header();
        let size_line = "GDAL_STRUCTURAL_METADATA_SIZE=000000 bytes\n";
        header[CLASSIC_TIFF_GHOST_DATA_OFFSET..CLASSIC_TIFF_GHOST_DATA_OFFSET + size_line.len()].copy_from_slice(size_line.as_bytes());

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_some());

        let ghost_data = result.unwrap();
        assert_eq!(ghost_data.layout, None);
        assert!(!ghost_data.is_cog());
    }

    #[test]
    fn test_from_tiff_header_buffer_negative_size() {
        // Test with negative size value (should fail to parse)
        let mut header = create_classic_tiff_header();
        let malformed_size_line = "GDAL_STRUCTURAL_METADATA_SIZE=-00001 bytes\n";

        header[CLASSIC_TIFF_GHOST_DATA_OFFSET..CLASSIC_TIFF_GHOST_DATA_OFFSET + malformed_size_line.len()]
            .copy_from_slice(malformed_size_line.as_bytes());

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_none()); // Should return None due to negative size
    }

    #[test]
    fn test_from_tiff_header_buffer_overflow_size() {
        // Test with size value that would cause overflow - should handle gracefully
        let mut header = create_classic_tiff_header();
        let malformed_size_line = "GDAL_STRUCTURAL_METADATA_SIZE=999999 bytes\n";
        let metadata = "LAYOUT=IFDS_BEFORE_DATA\n"; // Add some actual metadata

        header[CLASSIC_TIFF_GHOST_DATA_OFFSET..CLASSIC_TIFF_GHOST_DATA_OFFSET + malformed_size_line.len()]
            .copy_from_slice(malformed_size_line.as_bytes());

        // Add some metadata after the header line
        let start = CLASSIC_TIFF_GHOST_DATA_OFFSET + GDAL_METADATA_HEADER_LINE_SIZE;
        let end = (start + metadata.len()).min(header.len());
        header[start..end].copy_from_slice(&metadata.as_bytes()[..end - start]);

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_none()); // Should return None when size exceeds buffer
    }

    #[test]
    fn test_from_tiff_header_buffer_empty_size_field() {
        // Test with completely empty size field
        let mut header = create_classic_tiff_header();
        let malformed_size_line = "GDAL_STRUCTURAL_METADATA_SIZE=       bytes\n";

        header[CLASSIC_TIFF_GHOST_DATA_OFFSET..CLASSIC_TIFF_GHOST_DATA_OFFSET + malformed_size_line.len()]
            .copy_from_slice(malformed_size_line.as_bytes());

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_none()); // Should return None due to empty size field
    }

    #[test]
    fn test_from_tiff_header_buffer_non_numeric_size() {
        // Test with alphabetic characters in size field
        let mut header = create_classic_tiff_header();
        let malformed_size_line = "GDAL_STRUCTURAL_METADATA_SIZE=ABCDEF bytes\n";

        header[CLASSIC_TIFF_GHOST_DATA_OFFSET..CLASSIC_TIFF_GHOST_DATA_OFFSET + malformed_size_line.len()]
            .copy_from_slice(malformed_size_line.as_bytes());

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_none()); // Should return None due to non-numeric size
    }

    #[test]
    fn test_from_tiff_header_buffer_size_exceeds_buffer_exact() {
        // Test with exact size that exceeds buffer bounds
        let mut header = create_classic_tiff_header();
        header.resize(100, 0); // Small buffer to force size exceed condition

        // Set size to exceed buffer - buffer is 100 bytes, offset is 8, header line is 43
        // So available space for metadata is 100 - 8 - 43 = 49 bytes
        // But we declare size as 100 bytes
        let malformed_size_line = "GDAL_STRUCTURAL_METADATA_SIZE=000100 bytes\n";

        header[CLASSIC_TIFF_GHOST_DATA_OFFSET..CLASSIC_TIFF_GHOST_DATA_OFFSET + malformed_size_line.len()]
            .copy_from_slice(malformed_size_line.as_bytes());

        let result = GdalGhostData::from_tiff_header_buffer(&header);
        assert!(result.is_none()); // Should return None when declared size exceeds available buffer
    }
}

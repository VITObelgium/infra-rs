//! Comparison tests that verify lerc-decoder produces identical results
//! to the reference C LERC library by comparing against pre-generated test data.
//!
//! Test data is generated using the C library (see tools/generate_test_data.c)
//! and stored in the test_data/ directory with:
//! - .lerc2 files: LERC-encoded blobs
//! - .raw files: Expected decoded data (C library output)
//! - .meta files: Metadata (dimensions, data type, etc.)
//! - .mask files: Validity masks (optional)
//!
//! ## Fixed Issues
//!
//! The following test cases were previously failing but have been fixed:
//! - `i8_range_64x64`: Fixed signed char (i8) wrapping arithmetic
//! - `u8_smooth_256x256`: Fixed Huffman decoding with wrapping arithmetic
//! - `u8_mask_64x64`: Fixed mask handling with wrapping arithmetic
//!
//! The fix was to use wrapping addition for integer types in Huffman decoding
//! to match C++ integer overflow behavior.

use std::fs;
use std::path::PathBuf;

/// Metadata for a test case
#[derive(Debug)]
struct TestMeta {
    #[allow(dead_code)]
    data_type: u32,
    n_cols: usize,
    n_rows: usize,
    n_depth: usize,
    n_bands: usize,
    max_z_err: f64,
    has_mask: bool,
}

fn get_test_data_dir() -> PathBuf {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("test_data");
    path
}

fn parse_meta_file(content: &str) -> TestMeta {
    let mut data_type = 0u32;
    let mut n_cols = 0usize;
    let mut n_rows = 0usize;
    let mut n_depth = 1usize;
    let mut n_bands = 1usize;
    let mut max_z_err = 0.0f64;
    let mut has_mask = false;

    for line in content.lines() {
        let parts: Vec<&str> = line.splitn(2, '=').collect();
        if parts.len() != 2 {
            continue;
        }
        let key = parts[0].trim();
        let value = parts[1].trim();

        match key {
            "data_type" => data_type = value.parse().unwrap_or(0),
            "n_cols" => n_cols = value.parse().unwrap_or(0),
            "n_rows" => n_rows = value.parse().unwrap_or(0),
            "n_depth" => n_depth = value.parse().unwrap_or(1),
            "n_bands" => n_bands = value.parse().unwrap_or(1),
            "max_z_err" => max_z_err = value.parse().unwrap_or(0.0),
            "has_mask" => has_mask = value == "1",
            _ => {}
        }
    }

    TestMeta {
        data_type,
        n_cols,
        n_rows,
        n_depth,
        n_bands,
        max_z_err,
        has_mask,
    }
}

fn load_test_case(name: &str) -> Option<(Vec<u8>, Vec<u8>, TestMeta, Option<Vec<u8>>)> {
    let dir = get_test_data_dir();

    let lerc_path = dir.join(format!("{}.lerc2", name));
    let raw_path = dir.join(format!("{}.raw", name));
    let meta_path = dir.join(format!("{}.meta", name));

    if !lerc_path.exists() || !raw_path.exists() || !meta_path.exists() {
        eprintln!("Test data not found for '{}', skipping", name);
        return None;
    }

    let lerc_blob = fs::read(&lerc_path).ok()?;
    let raw_data = fs::read(&raw_path).ok()?;
    let meta_content = fs::read_to_string(&meta_path).ok()?;
    let meta = parse_meta_file(&meta_content);

    let mask = if meta.has_mask {
        let mask_path = dir.join(format!("{}.mask", name));
        fs::read(&mask_path).ok()
    } else {
        None
    };

    Some((lerc_blob, raw_data, meta, mask))
}

/// Compare decoded integer data with original
fn compare_integers<T: Copy + PartialEq + std::fmt::Debug>(decoded: &[T], expected: &[T], mask: Option<&[bool]>) {
    assert_eq!(decoded.len(), expected.len(), "Length mismatch");

    for i in 0..decoded.len() {
        // Skip invalid pixels
        if let Some(m) = mask {
            if i < m.len() && !m[i] {
                continue;
            }
        }

        assert_eq!(
            decoded[i], expected[i],
            "Value mismatch at index {}: {:?} vs {:?}",
            i, decoded[i], expected[i]
        );
    }
}

/// Compare decoded float data exactly (expected is C-decoded data, should be identical)
fn compare_f32(decoded: &[f32], expected: &[f32], _max_err: f64, mask: Option<&[bool]>) {
    assert_eq!(decoded.len(), expected.len(), "Length mismatch");

    for i in 0..decoded.len() {
        if let Some(m) = mask {
            if i < m.len() && !m[i] {
                continue;
            }
        }

        // Compare bit-for-bit since expected is C-decoded output
        // Both decoders should produce identical results
        assert!(
            decoded[i].to_bits() == expected[i].to_bits() || (decoded[i].is_nan() && expected[i].is_nan()),
            "Value mismatch at index {}: {} (bits: {:08x}) vs {} (bits: {:08x})",
            i,
            decoded[i],
            decoded[i].to_bits(),
            expected[i],
            expected[i].to_bits()
        );
    }
}

/// Compare decoded f64 data exactly (expected is C-decoded data, should be identical)
fn compare_f64(decoded: &[f64], expected: &[f64], _max_err: f64, mask: Option<&[bool]>) {
    assert_eq!(decoded.len(), expected.len(), "Length mismatch");

    for i in 0..decoded.len() {
        if let Some(m) = mask {
            if i < m.len() && !m[i] {
                continue;
            }
        }

        // Compare bit-for-bit since expected is C-decoded output
        assert!(
            decoded[i].to_bits() == expected[i].to_bits() || (decoded[i].is_nan() && expected[i].is_nan()),
            "Value mismatch at index {}: {} (bits: {:016x}) vs {} (bits: {:016x})",
            i,
            decoded[i],
            decoded[i].to_bits(),
            expected[i],
            expected[i].to_bits()
        );
    }
}

/// Convert raw bytes to typed slice
fn bytes_to_slice<T: Copy>(bytes: &[u8]) -> Vec<T> {
    let elem_size = std::mem::size_of::<T>();
    let count = bytes.len() / elem_size;
    let mut result = Vec::with_capacity(count);

    for i in 0..count {
        let offset = i * elem_size;
        let slice = &bytes[offset..offset + elem_size];
        // Safe because we're reading from aligned bytes
        let value = unsafe { std::ptr::read_unaligned(slice.as_ptr() as *const T) };
        result.push(value);
    }

    result
}

/// Previously failing test cases (now fixed)
const KNOWN_FAILING_TESTS: &[&str] = &[
    // All decoder bugs have been fixed!
];

/// Run a test case by name
fn run_test_case(name: &str) {
    let Some((lerc_blob, raw_data, meta, mask_bytes)) = load_test_case(name) else {
        return; // Test data not available
    };

    // Convert mask bytes to bool vec
    let bool_mask: Option<Vec<bool>> = mask_bytes.as_ref().map(|m| m.iter().map(|&b| b != 0).collect());

    // Decode with our Rust implementation
    let decoded = lerc_decoder::decode(&lerc_blob).unwrap_or_else(|e| panic!("Failed to decode {}: {}", name, e));

    // Verify dimensions match
    assert_eq!(decoded.info.n_cols as usize, meta.n_cols, "n_cols mismatch for {}", name);
    assert_eq!(decoded.info.n_rows as usize, meta.n_rows, "n_rows mismatch for {}", name);
    assert_eq!(decoded.info.n_depth as usize, meta.n_depth, "n_depth mismatch for {}", name);
    assert_eq!(decoded.info.n_bands as usize, meta.n_bands, "n_bands mismatch for {}", name);

    // Compare decoded data with original
    match decoded.data {
        lerc_decoder::DecodedPixels::I8(ref pixels) => {
            let expected: Vec<i8> = bytes_to_slice(&raw_data);
            compare_integers(pixels, &expected, bool_mask.as_deref());
        }
        lerc_decoder::DecodedPixels::U8(ref pixels) => {
            let expected: Vec<u8> = raw_data.clone();
            compare_integers(pixels, &expected, bool_mask.as_deref());
        }
        lerc_decoder::DecodedPixels::I16(ref pixels) => {
            let expected: Vec<i16> = bytes_to_slice(&raw_data);
            compare_integers(pixels, &expected, bool_mask.as_deref());
        }
        lerc_decoder::DecodedPixels::U16(ref pixels) => {
            let expected: Vec<u16> = bytes_to_slice(&raw_data);
            compare_integers(pixels, &expected, bool_mask.as_deref());
        }
        lerc_decoder::DecodedPixels::I32(ref pixels) => {
            let expected: Vec<i32> = bytes_to_slice(&raw_data);
            compare_integers(pixels, &expected, bool_mask.as_deref());
        }
        lerc_decoder::DecodedPixels::U32(ref pixels) => {
            let expected: Vec<u32> = bytes_to_slice(&raw_data);
            compare_integers(pixels, &expected, bool_mask.as_deref());
        }
        lerc_decoder::DecodedPixels::F32(ref pixels) => {
            let expected: Vec<f32> = bytes_to_slice(&raw_data);
            compare_f32(pixels, &expected, meta.max_z_err, bool_mask.as_deref());
        }
        lerc_decoder::DecodedPixels::F64(ref pixels) => {
            let expected: Vec<f64> = bytes_to_slice(&raw_data);
            compare_f64(pixels, &expected, meta.max_z_err, bool_mask.as_deref());
        }
    }

    println!("✓ Test passed: {}", name);
}

// ============================================================================
// Basic data type tests
// ============================================================================

#[test]
fn test_u8_constant() {
    run_test_case("u8_constant_64x64");
}

#[test]
fn test_u8_gradient() {
    run_test_case("u8_gradient_128x128");
}

#[test]
fn test_u8_random() {
    run_test_case("u8_random_100x100");
}

#[test]
fn test_i8_range() {
    run_test_case("i8_range_64x64");
}

#[test]
fn test_u16_pattern() {
    run_test_case("u16_pattern_80x80");
}

#[test]
fn test_i16_pattern() {
    run_test_case("i16_pattern_80x80");
}

#[test]
fn test_u32_pattern() {
    run_test_case("u32_pattern_64x64");
}

#[test]
fn test_i32_pattern() {
    run_test_case("i32_pattern_64x64");
}

#[test]
fn test_f32_lossless() {
    run_test_case("f32_lossless_64x64");
}

#[test]
fn test_f32_lossy() {
    run_test_case("f32_lossy_100x100");
}

#[test]
fn test_f64_lossless() {
    run_test_case("f64_lossless_50x50");
}

#[test]
fn test_f64_lossy() {
    run_test_case("f64_lossy_80x80");
}

// ============================================================================
// Mask tests
// ============================================================================

#[test]
fn test_u8_with_mask() {
    run_test_case("u8_mask_64x64");
}

#[test]
fn test_f32_with_mask() {
    run_test_case("f32_mask_100x100");
}

#[test]
fn test_mostly_invalid_mask() {
    run_test_case("u16_mostly_invalid_32x32");
}

// ============================================================================
// Multi-band tests
// ============================================================================

#[test]
fn test_rgb_multiband() {
    run_test_case("u8_rgb_64x64");
}

#[test]
fn test_multiband_f32() {
    run_test_case("f32_4band_50x50");
}

// ============================================================================
// Edge case tests
// ============================================================================

#[test]
fn test_single_pixel() {
    run_test_case("f32_single_pixel");
}

#[test]
fn test_single_row() {
    run_test_case("u8_single_row_256");
}

#[test]
fn test_single_column() {
    run_test_case("u8_single_col_256");
}

#[test]
fn test_constant_pi() {
    run_test_case("f64_constant_pi_128x128");
}

#[test]
fn test_all_zeros() {
    run_test_case("i32_zeros_64x64");
}

#[test]
fn test_minmax_u8() {
    run_test_case("u8_minmax_16x16");
}

#[test]
fn test_minmax_i16() {
    run_test_case("i16_minmax_16x16");
}

// ============================================================================
// Non-standard dimension tests
// ============================================================================

#[test]
fn test_odd_dimensions() {
    run_test_case("u16_odd_73x91");
}

#[test]
fn test_prime_dimensions() {
    run_test_case("f32_prime_67x71");
}

// ============================================================================
// Large image tests
// ============================================================================

#[test]
fn test_large_u8() {
    run_test_case("u8_large_512x512");
}

#[test]
fn test_large_f32_terrain() {
    run_test_case("f32_terrain_400x400");
}

// ============================================================================
// Depth (interleaved) tests
// ============================================================================

#[test]
fn test_depth_rgb() {
    run_test_case("u8_depth3_64x64");
}

// ============================================================================
// Compression mode tests
// ============================================================================

#[test]
fn test_huffman_trigger() {
    run_test_case("u8_smooth_256x256");
}

#[test]
fn test_rle_mask() {
    run_test_case("u8_rle_stripes_200x200");
}

// ============================================================================
// Float edge case tests
// ============================================================================

#[test]
fn test_f32_small_values() {
    run_test_case("f32_small_32x32");
}

#[test]
fn test_f32_large_values() {
    run_test_case("f32_large_32x32");
}

#[test]
fn test_f32_negative_values() {
    run_test_case("f32_negative_64x64");
}

// ============================================================================
// Version-specific tests
// ============================================================================

#[test]
fn test_version_2() {
    run_test_case("f32_version2_64x64");
}

#[test]
fn test_version_3() {
    run_test_case("f32_version3_64x64");
}

#[test]
fn test_version_4() {
    run_test_case("f32_version4_64x64");
}

#[test]
fn test_version_5() {
    run_test_case("f32_version5_64x64");
}

#[test]
fn test_version_6() {
    run_test_case("f32_version6_64x64");
}

// ============================================================================
// Reference LERC2 files from C++ testData directory
// ============================================================================

#[test]
fn test_ref_bluemarble() {
    run_test_case("ref_bluemarble_256x256x3_u8");
}

#[test]
fn test_ref_california() {
    run_test_case("ref_california_400x400_f32");
}

// ============================================================================
// Blob info tests
// ============================================================================

#[test]
fn test_blob_info_accuracy() {
    let dir = get_test_data_dir();
    let test_cases = [
        ("u8_gradient_128x128", 128, 128, 1, 1),
        ("f32_4band_50x50", 50, 50, 1, 4),
        ("u8_depth3_64x64", 64, 64, 3, 1),
    ];

    for (name, expected_cols, expected_rows, expected_depth, expected_bands) in test_cases {
        let lerc_path = dir.join(format!("{}.lerc2", name));
        if !lerc_path.exists() {
            eprintln!("Skipping blob info test for {}: file not found", name);
            continue;
        }

        let blob = fs::read(&lerc_path).expect("Failed to read blob");
        let info = lerc_decoder::get_blob_info(&blob).expect("Failed to get blob info");

        assert_eq!(info.n_cols, expected_cols, "n_cols mismatch for {}", name);
        assert_eq!(info.n_rows, expected_rows, "n_rows mismatch for {}", name);
        assert_eq!(info.n_depth, expected_depth, "n_depth mismatch for {}", name);
        assert_eq!(info.n_bands, expected_bands, "n_bands mismatch for {}", name);

        println!("✓ Blob info test passed: {}", name);
    }
}

// ============================================================================
// Error handling tests
// ============================================================================

#[test]
fn test_invalid_data_handling() {
    // Empty data
    let result = lerc_decoder::get_blob_info(&[]);
    assert!(result.is_err(), "Empty data should fail");

    // Random garbage
    let garbage = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    let result = lerc_decoder::get_blob_info(&garbage);
    assert!(result.is_err(), "Random data should fail");

    // Truncated header
    let truncated = b"Lerc2 ";
    let result = lerc_decoder::get_blob_info(truncated);
    assert!(result.is_err(), "Truncated header should fail");

    // Invalid magic number
    let bad_magic = b"NotLerc data here";
    let result = lerc_decoder::get_blob_info(bad_magic);
    assert!(result.is_err(), "Invalid magic should fail");
}

// ============================================================================
// Comprehensive test runner
// ============================================================================

#[test]
fn run_all_test_cases() {
    let test_cases = [
        // Basic types
        "u8_constant_64x64",
        "u8_gradient_128x128",
        "u8_random_100x100",
        "i8_range_64x64",
        "u16_pattern_80x80",
        "i16_pattern_80x80",
        "u32_pattern_64x64",
        "i32_pattern_64x64",
        "f32_lossless_64x64",
        "f32_lossy_100x100",
        "f64_lossless_50x50",
        "f64_lossy_80x80",
        // Masks
        "u8_mask_64x64",
        "f32_mask_100x100",
        "u16_mostly_invalid_32x32",
        // Multi-band
        "u8_rgb_64x64",
        "f32_4band_50x50",
        // Edge cases
        "f32_single_pixel",
        "u8_single_row_256",
        "u8_single_col_256",
        "f64_constant_pi_128x128",
        "i32_zeros_64x64",
        "u8_minmax_16x16",
        "i16_minmax_16x16",
        // Dimensions
        "u16_odd_73x91",
        "f32_prime_67x71",
        // Large
        "u8_large_512x512",
        "f32_terrain_400x400",
        // Depth
        "u8_depth3_64x64",
        // Compression modes
        "u8_smooth_256x256",
        "u8_rle_stripes_200x200",
        // Float edge cases
        "f32_small_32x32",
        "f32_large_32x32",
        "f32_negative_64x64",
        // Versions
        "f32_version2_64x64",
        "f32_version3_64x64",
        "f32_version4_64x64",
        "f32_version5_64x64",
        "f32_version6_64x64",
        // Reference files from C++ testData directory
        "ref_bluemarble_256x256x3_u8",
        "ref_california_400x400_f32",
    ];

    let dir = get_test_data_dir();
    if !dir.exists() {
        eprintln!("Test data directory not found. Run 'tools/generate_test_data' first.");
        eprintln!("Expected path: {:?}", dir);
        return;
    }

    let mut passed = 0;
    let mut skipped = 0;
    let mut failed = 0;
    let mut known_failures = 0;

    for name in test_cases {
        let lerc_path = dir.join(format!("{}.lerc2", name));
        if !lerc_path.exists() {
            eprintln!("⊘ Skipping {}: test data not found", name);
            skipped += 1;
            continue;
        }

        // Check if this is a known failing test
        let is_known_failure = KNOWN_FAILING_TESTS.contains(&name);

        match std::panic::catch_unwind(|| run_test_case(name)) {
            Ok(_) => {
                if is_known_failure {
                    println!("✓ Test passed (unexpectedly!): {}", name);
                }
                passed += 1;
            }
            Err(_) => {
                if is_known_failure {
                    eprintln!("⚠ Known failure: {} (decoder bug)", name);
                    known_failures += 1;
                } else {
                    eprintln!("✗ Test failed: {}", name);
                    failed += 1;
                }
            }
        }
    }

    println!("\n========================================");
    println!(
        "Test Summary: {} passed, {} skipped, {} known failures, {} unexpected failures",
        passed, skipped, known_failures, failed
    );
    println!("========================================");

    if failed > 0 {
        panic!("{} unexpected test failures", failed);
    }
}

//! Roundtrip tests: encode with `lerc` crate (C++ bindings), decode with `lerc-decoder`
//!
//! These tests verify that our pure Rust decoder correctly decodes data encoded
//! by the official C++ LERC library.

use lerc::encode;
use rand::rngs::StdRng;
use rand::{Rng as _, SeedableRng};

/// Helper to create a seeded RNG for reproducible tests
fn seeded_rng() -> StdRng {
    StdRng::seed_from_u64(12345)
}

/// Helper function to encode data with the lerc crate
fn lerc_encode<T: lerc::LercDataType>(data: &[T], width: usize, height: usize, depth: usize, n_bands: usize, max_z_error: f64) -> Vec<u8> {
    encode(data, None, width, height, depth, n_bands, 0, max_z_error).expect("encode failed")
}

// ============================================================================
// u8 roundtrip tests
// ============================================================================

#[test]
fn roundtrip_u8_random() {
    let mut rng = seeded_rng();
    let width = 64;
    let height = 64;
    let data: Vec<u8> = (0..width * height).map(|_| rng.random()).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::U8(data));
}

#[test]
fn roundtrip_u8_constant() {
    let width = 32;
    let height = 32;
    let data: Vec<u8> = vec![42; width * height];

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::U8(data));
}

#[test]
fn roundtrip_u8_gradient() {
    let width = 256;
    let height = 256;
    let data: Vec<u8> = (0..width * height).map(|i| (i % 256) as u8).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::U8(data));
}

#[test]
fn roundtrip_u8_multiband() {
    let mut rng = seeded_rng();
    let width = 32;
    let height = 32;
    let bands = 3;
    let data: Vec<u8> = (0..width * height * bands).map(|_| rng.random()).collect();

    let encoded = lerc_encode(&data, width, height, 1, bands, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::U8(data));
    assert_eq!(decoded.info.n_bands, bands as i32);
}

#[test]
fn roundtrip_u8_large() {
    let mut rng = seeded_rng();
    let width = 512;
    let height = 512;
    let data: Vec<u8> = (0..width * height).map(|_| rng.random()).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::U8(data));
}

// ============================================================================
// i8 roundtrip tests
// ============================================================================

#[test]
fn roundtrip_i8_random() {
    let mut rng = seeded_rng();
    let width = 64;
    let height = 64;
    let data: Vec<i8> = (0..width * height).map(|_| rng.random()).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::I8(data));
}

#[test]
fn roundtrip_i8_full_range() {
    let width = 32;
    let height = 32;
    let data: Vec<i8> = (0..width * height).map(|i| (i % 256) as i8).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::I8(data));
}

// ============================================================================
// u16 roundtrip tests
// ============================================================================

#[test]
fn roundtrip_u16_random() {
    let mut rng = seeded_rng();
    let width = 64;
    let height = 64;
    let data: Vec<u16> = (0..width * height).map(|_| rng.random()).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::U16(data));
}

#[test]
fn roundtrip_u16_gradient() {
    let width = 256;
    let height = 256;
    let data: Vec<u16> = (0..width * height).map(|i| i as u16).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::U16(data));
}

// ============================================================================
// i16 roundtrip tests
// ============================================================================

#[test]
fn roundtrip_i16_random() {
    let mut rng = seeded_rng();
    let width = 64;
    let height = 64;
    let data: Vec<i16> = (0..width * height).map(|_| rng.random()).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::I16(data));
}

#[test]
fn roundtrip_i16_negative() {
    let width = 64;
    let height = 64;
    let data: Vec<i16> = (0..width * height).map(|i| -((i % 1000) as i16)).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::I16(data));
}

// ============================================================================
// u32 roundtrip tests
// ============================================================================

#[test]
fn roundtrip_u32_random() {
    let mut rng = seeded_rng();
    let width = 64;
    let height = 64;
    let data: Vec<u32> = (0..width * height).map(|_| rng.random()).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::U32(data));
}

#[test]
fn roundtrip_u32_large_values() {
    let width = 32;
    let height = 32;
    let data: Vec<u32> = (0..width * height).map(|i| (i as u32) * 1_000_000 + 500_000).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::U32(data));
}

// ============================================================================
// i32 roundtrip tests
// ============================================================================

#[test]
fn roundtrip_i32_random() {
    let mut rng = seeded_rng();
    let width = 64;
    let height = 64;
    let data: Vec<i32> = (0..width * height).map(|_| rng.random()).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::I32(data));
}

#[test]
fn roundtrip_i32_mixed_sign() {
    let width = 64;
    let height = 64;
    let data: Vec<i32> = (0..width * height)
        .map(|i| if i % 2 == 0 { i as i32 } else { -(i as i32) })
        .collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::I32(data));
}

// ============================================================================
// f32 roundtrip tests
// ============================================================================

#[test]
fn roundtrip_f32_random() {
    let mut rng = seeded_rng();
    let width = 64;
    let height = 64;
    let data: Vec<f32> = (0..width * height).map(|_| rng.random_range(-1000.0..1000.0)).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::F32(data));
}

#[test]
fn roundtrip_f32_terrain_like() {
    let width = 128;
    let height = 128;
    let data: Vec<f32> = (0..width * height)
        .map(|i| {
            let x = (i % width) as f32;
            let y = (i / width) as f32;
            100.0 + (x * 0.1).sin() * 50.0 + (y * 0.1).cos() * 30.0
        })
        .collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::F32(data));
}

#[test]
fn roundtrip_f32_small_values() {
    let width = 32;
    let height = 32;
    let data: Vec<f32> = (0..width * height).map(|i| (i as f32) * 0.0001).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::F32(data));
}

#[test]
fn roundtrip_f32_large_values() {
    let width = 32;
    let height = 32;
    let data: Vec<f32> = (0..width * height).map(|i| (i as f32) * 100000.0).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::F32(data));
}

#[test]
fn roundtrip_f32_negative() {
    let mut rng = seeded_rng();
    let width = 64;
    let height = 64;
    let data: Vec<f32> = (0..width * height).map(|_| rng.random_range(-10000.0..-1.0)).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::F32(data));
}

#[test]
fn roundtrip_f32_multiband() {
    let mut rng = seeded_rng();
    let width = 32;
    let height = 32;
    let bands = 4;
    let data: Vec<f32> = (0..width * height * bands).map(|_| rng.random_range(0.0..255.0)).collect();

    let encoded = lerc_encode(&data, width, height, 1, bands, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::F32(data));
    assert_eq!(decoded.info.n_bands, bands as i32);
}

// ============================================================================
// f64 roundtrip tests
// ============================================================================

#[test]
fn roundtrip_f64_random() {
    let mut rng = seeded_rng();
    let width = 64;
    let height = 64;
    let data: Vec<f64> = (0..width * height).map(|_| rng.random_range(-1000.0..1000.0)).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::F64(data));
}

#[test]
fn roundtrip_f64_high_precision() {
    let width = 32;
    let height = 32;
    let data: Vec<f64> = (0..width * height)
        .map(|i| std::f64::consts::PI * (i as f64) + 0.123456789012345)
        .collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::F64(data));
}

// ============================================================================
// Edge cases and special dimensions
// ============================================================================

#[test]
fn roundtrip_single_pixel() {
    let data: Vec<f32> = vec![42.5];

    let encoded = lerc_encode(&data, 1, 1, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::F32(data));
}

#[test]
fn roundtrip_single_row() {
    let mut rng = seeded_rng();
    let width = 100;
    let data: Vec<f32> = (0..width).map(|_| rng.random_range(0.0..100.0)).collect();

    let encoded = lerc_encode(&data, width, 1, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::F32(data));
}

#[test]
fn roundtrip_single_column() {
    let mut rng = seeded_rng();
    let height = 100;
    let data: Vec<f32> = (0..height).map(|_| rng.random_range(0.0..100.0)).collect();

    let encoded = lerc_encode(&data, 1, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::F32(data));
}

#[test]
fn roundtrip_odd_dimensions() {
    let mut rng = seeded_rng();
    let width = 37;
    let height = 53;
    let data: Vec<f32> = (0..width * height).map(|_| rng.random_range(0.0..100.0)).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::F32(data));
}

#[test]
fn roundtrip_prime_dimensions() {
    let mut rng = seeded_rng();
    let width = 127;
    let height = 131;
    let data: Vec<u16> = (0..width * height).map(|_| rng.random()).collect();

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::U16(data));
}

#[test]
fn roundtrip_all_zeros() {
    let width = 64;
    let height = 64;
    let data: Vec<f32> = vec![0.0; width * height];

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::F32(data));
}

#[test]
fn roundtrip_all_same_value() {
    let width = 64;
    let height = 64;
    let data: Vec<f32> = vec![std::f32::consts::PI; width * height];

    let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::F32(data));
}

// ============================================================================
// Stress tests with various random seeds
// ============================================================================

#[test]
fn roundtrip_multiple_seeds_u8() {
    for seed in 0..10u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let width = 64;
        let height = 64;
        let data: Vec<u8> = (0..width * height).map(|_| rng.random()).collect();

        let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
        let decoded = lerc_decoder::decode(&encoded).unwrap_or_else(|e| panic!("decode failed for seed {}: {:?}", seed, e));

        assert_eq!(decoded.data, lerc_decoder::DecodedPixels::U8(data), "mismatch for seed {}", seed);
    }
}

#[test]
fn roundtrip_multiple_seeds_f32() {
    for seed in 0..10u64 {
        let mut rng = StdRng::seed_from_u64(seed);
        let width = 64;
        let height = 64;
        let data: Vec<f32> = (0..width * height).map(|_| rng.random_range(-1000.0..1000.0)).collect();

        let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
        let decoded = lerc_decoder::decode(&encoded).unwrap_or_else(|e| panic!("decode failed for seed {}: {:?}", seed, e));

        assert_eq!(decoded.data, lerc_decoder::DecodedPixels::F32(data), "mismatch for seed {}", seed);
    }
}

#[test]
fn roundtrip_various_sizes() {
    let sizes = [(8, 8), (16, 16), (32, 32), (64, 64), (100, 100), (128, 128), (200, 150), (256, 256)];

    for (width, height) in sizes {
        let mut rng = seeded_rng();
        let data: Vec<f32> = (0..width * height).map(|_| rng.random_range(0.0..1000.0)).collect();

        let encoded = lerc_encode(&data, width, height, 1, 1, 0.0);
        let decoded = lerc_decoder::decode(&encoded).unwrap_or_else(|e| panic!("decode failed for {}x{}: {:?}", width, height, e));

        assert_eq!(
            decoded.data,
            lerc_decoder::DecodedPixels::F32(data),
            "mismatch for {}x{}",
            width,
            height
        );
    }
}

// ============================================================================
// Depth tests (3D data)
// ============================================================================

#[test]
fn roundtrip_with_depth() {
    let mut rng = seeded_rng();
    let width = 32;
    let height = 32;
    let depth = 4;
    let data: Vec<f32> = (0..width * height * depth).map(|_| rng.random_range(0.0..100.0)).collect();

    let encoded = lerc_encode(&data, width, height, depth, 1, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::F32(data));
    assert_eq!(decoded.info.n_depth, depth as i32);
}

#[test]
fn roundtrip_depth_and_bands() {
    let mut rng = seeded_rng();
    let width = 16;
    let height = 16;
    let depth = 2;
    let bands = 3;
    let data: Vec<u8> = (0..width * height * depth * bands).map(|_| rng.random()).collect();

    let encoded = lerc_encode(&data, width, height, depth, bands, 0.0);
    let decoded = lerc_decoder::decode(&encoded).expect("decode failed");

    assert_eq!(decoded.data, lerc_decoder::DecodedPixels::U8(data));
    assert_eq!(decoded.info.n_depth, depth as i32);
    assert_eq!(decoded.info.n_bands, bands as i32);
}

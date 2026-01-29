//! Performance tests that compare lerc-decoder (Rust) against the C++ implementation
//!
//! These tests ensure that the Rust decoder performance is within an acceptable range
//! of the reference C++ implementation (via the `lerc` crate).
//!
//! The tests will fail if the Rust implementation is more than 20% slower than C++
//! in release mode.
//!
//! Run with: cargo test --release -p lerc-decoder --test performance_tests -- --ignored --nocapture

use lerc::{decode as lerc_decode, encode as lerc_encode};
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Number of iterations for each benchmark
const BENCHMARK_ITERATIONS: usize = 200;

/// Number of warmup iterations before timing
const WARMUP_ITERATIONS: usize = 50;

/// Maximum acceptable performance degradation (20%)
const MAX_SLOWDOWN_FACTOR: f64 = 1.20;

/// Result of a benchmark run
#[derive(Debug, Clone)]
struct BenchmarkResult {
    name: String,
    rust_time: Duration,
    cpp_time: Duration,
    iterations: usize,
    data_size: usize,
}

impl BenchmarkResult {
    fn rust_avg_ms(&self) -> f64 {
        self.rust_time.as_secs_f64() * 1000.0 / self.iterations as f64
    }

    fn cpp_avg_ms(&self) -> f64 {
        self.cpp_time.as_secs_f64() * 1000.0 / self.iterations as f64
    }

    fn slowdown_factor(&self) -> f64 {
        self.rust_avg_ms() / self.cpp_avg_ms()
    }

    fn throughput_rust_mbps(&self) -> f64 {
        let avg_time_s = self.rust_time.as_secs_f64() / self.iterations as f64;
        (self.data_size as f64 / 1_048_576.0) / avg_time_s
    }

    fn throughput_cpp_mbps(&self) -> f64 {
        let avg_time_s = self.cpp_time.as_secs_f64() / self.iterations as f64;
        (self.data_size as f64 / 1_048_576.0) / avg_time_s
    }

    fn print_report(&self) {
        println!("\n╔═══════════════════════════════════════════════════════════════╗");
        println!("║ Performance Report: {:<42} ║", self.name);
        println!("╠═══════════════════════════════════════════════════════════════╣");
        println!(
            "║ Data size:        {:>8} KB                               ║",
            self.data_size / 1024
        );
        println!("║ Iterations:       {:>8}                                  ║", self.iterations);
        println!("╠═══════════════════════════════════════════════════════════════╣");
        println!(
            "║ Rust decoder:     {:>8.3} ms/decode ({:>7.2} MB/s)       ║",
            self.rust_avg_ms(),
            self.throughput_rust_mbps()
        );
        println!(
            "║ C++ decoder:      {:>8.3} ms/decode ({:>7.2} MB/s)       ║",
            self.cpp_avg_ms(),
            self.throughput_cpp_mbps()
        );
        println!("╠═══════════════════════════════════════════════════════════════╣");

        let slowdown = self.slowdown_factor();
        let slowdown_pct = (slowdown - 1.0) * 100.0;

        if slowdown <= MAX_SLOWDOWN_FACTOR {
            println!("║ Slowdown:         {:>7.2}% ✓ PASS                         ║", slowdown_pct);
        } else {
            println!("║ Slowdown:         {:>7.2}% ✗ FAIL (max: 20%)             ║", slowdown_pct);
        }
        println!("╚═══════════════════════════════════════════════════════════════╝\n");
    }

    fn assert_acceptable_performance(&self) {
        let slowdown = self.slowdown_factor();
        assert!(
            slowdown <= MAX_SLOWDOWN_FACTOR,
            "Rust decoder is {:.1}% slower than C++ (max acceptable: 20%)\n\
             Rust: {:.3} ms, C++: {:.3} ms",
            (slowdown - 1.0) * 100.0,
            self.rust_avg_ms(),
            self.cpp_avg_ms()
        );
    }
}

/// Get the path to test data directory
fn test_data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data")
}

/// Benchmark the Rust decoder
fn benchmark_rust_decoder(data: &[u8], iterations: usize) -> Duration {
    // Warm-up with more iterations for stable CPU state
    for _ in 0..WARMUP_ITERATIONS {
        let _ = lerc_decoder::decode(data).expect("Rust decode failed");
    }

    // Actual benchmark
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = lerc_decoder::decode(data).expect("Rust decode failed");
    }
    start.elapsed()
}

/// Benchmark the C++ decoder (via lerc crate)
fn benchmark_cpp_decoder<T: lerc::LercDataType>(data: &[u8], width: usize, height: usize, n_bands: usize, iterations: usize) -> Duration {
    // Warm-up with more iterations for stable CPU state
    for _ in 0..WARMUP_ITERATIONS {
        let _ = lerc_decode::<T>(data, width, height, 1, n_bands, 0).expect("C++ decode failed");
    }

    // Actual benchmark
    let start = Instant::now();
    for _ in 0..iterations {
        let _ = lerc_decode::<T>(data, width, height, 1, n_bands, 0).expect("C++ decode failed");
    }
    start.elapsed()
}

/// Run benchmark by generating test data with C++ encoder
fn run_benchmark_generated<T: lerc::LercDataType + 'static>(
    name: &str,
    data: &[T],
    width: usize,
    height: usize,
    n_bands: usize,
    max_z_error: f64,
) -> BenchmarkResult {
    // Encode with C++ implementation
    let encoded = lerc_encode(data, None, width, height, 1, n_bands, 0, max_z_error).expect("Encode failed");

    println!("Benchmarking: {} ({} KB encoded)", name, encoded.len() / 1024);

    let rust_time = benchmark_rust_decoder(&encoded, BENCHMARK_ITERATIONS);
    let cpp_time = benchmark_cpp_decoder::<T>(&encoded, width, height, n_bands, BENCHMARK_ITERATIONS);

    BenchmarkResult {
        name: name.to_string(),
        rust_time,
        cpp_time,
        iterations: BENCHMARK_ITERATIONS,
        data_size: encoded.len(),
    }
}

/// Run benchmark on pre-existing test file
fn run_benchmark_file(name: &str, file_path: &str) -> BenchmarkResult {
    let path = test_data_dir().join(file_path);
    let data = fs::read(&path).expect(&format!("Failed to read test file: {:?}", path));

    println!("Benchmarking: {} ({} KB)", name, data.len() / 1024);

    let rust_time = benchmark_rust_decoder(&data, BENCHMARK_ITERATIONS);

    BenchmarkResult {
        name: name.to_string(),
        rust_time,
        cpp_time: Duration::ZERO,
        iterations: BENCHMARK_ITERATIONS,
        data_size: data.len(),
    }
}

#[test]
#[ignore]
fn test_performance_u8_small() {
    let width = 256;
    let height = 256;
    let n_bands = 1;
    let data: Vec<u8> = (0..width * height).map(|i| (i % 256) as u8).collect();

    let result = run_benchmark_generated::<u8>("U8 Small (256x256x1)", &data, width, height, n_bands, 0.0);

    result.print_report();

    if cfg!(not(debug_assertions)) {
        result.assert_acceptable_performance();
    } else {
        println!("⚠️  Debug mode detected - skipping performance assertion");
        println!("   Run with --release to validate performance requirements");
    }
}

#[test]
#[ignore]
fn test_performance_u8_multiband() {
    let width = 256;
    let height = 256;
    let n_bands = 3;
    let data: Vec<u8> = (0..width * height * n_bands).map(|i| (i % 256) as u8).collect();

    let result = run_benchmark_generated::<u8>("U8 Multi-band (256x256x3)", &data, width, height, n_bands, 0.0);

    result.print_report();

    if cfg!(not(debug_assertions)) {
        result.assert_acceptable_performance();
    } else {
        println!("⚠️  Debug mode detected - skipping performance assertion");
        println!("   Run with --release to validate performance requirements");
    }
}

#[test]
#[ignore]
fn test_performance_f32_medium() {
    let width = 400;
    let height = 400;
    let n_bands = 1;
    let data: Vec<f32> = (0..width * height)
        .map(|i| {
            let x = (i % width) as f32;
            let y = (i / width) as f32;
            100.0 + (x * 0.1).sin() * 50.0 + (y * 0.1).cos() * 30.0
        })
        .collect();

    let result = run_benchmark_generated::<f32>("F32 Medium (400x400x1)", &data, width, height, n_bands, 0.0);

    result.print_report();

    if cfg!(not(debug_assertions)) {
        result.assert_acceptable_performance();
    } else {
        println!("⚠️  Debug mode detected - skipping performance assertion");
        println!("   Run with --release to validate performance requirements");
    }
}

#[test]
#[ignore]
fn test_performance_f32_large() {
    let width = 1024;
    let height = 1024;
    let n_bands = 1;
    let data: Vec<f32> = (0..width * height).map(|i| (i as f32) * 0.001).collect();

    let result = run_benchmark_generated::<f32>("F32 Large (1024x1024x1)", &data, width, height, n_bands, 0.0);

    result.print_report();

    if cfg!(not(debug_assertions)) {
        result.assert_acceptable_performance();
    } else {
        println!("⚠️  Debug mode detected - skipping performance assertion");
        println!("   Run with --release to validate performance requirements");
    }
}

#[test]
#[ignore]
fn test_performance_bluemarble_file() {
    let result = run_benchmark_file("Blue Marble File (256x256x3)", "bluemarble_256_256_3_byte.lerc2");

    println!("\n╔═══════════════════════════════════════════════════════════════╗");
    println!("║ Performance Report: {:<42} ║", result.name);
    println!("╠═══════════════════════════════════════════════════════════════╣");
    println!(
        "║ Data size:        {:>8} KB                               ║",
        result.data_size / 1024
    );
    println!("║ Iterations:       {:>8}                                  ║", result.iterations);
    println!("╠═══════════════════════════════════════════════════════════════╣");
    println!(
        "║ Rust decoder:     {:>8.3} ms/decode ({:>7.2} MB/s)       ║",
        result.rust_avg_ms(),
        result.throughput_rust_mbps()
    );
    println!("╚═══════════════════════════════════════════════════════════════╝\n");
}

#[test]
#[ignore]
fn test_performance_california_file() {
    let result = run_benchmark_file("California File (400x400x1)", "california_400_400_1_float.lerc2");

    println!("\n╔═══════════════════════════════════════════════════════════════╗");
    println!("║ Performance Report: {:<42} ║", result.name);
    println!("╠═══════════════════════════════════════════════════════════════╣");
    println!(
        "║ Data size:        {:>8} KB                               ║",
        result.data_size / 1024
    );
    println!("║ Iterations:       {:>8}                                  ║", result.iterations);
    println!("╠═══════════════════════════════════════════════════════════════╣");
    println!(
        "║ Rust decoder:     {:>8.3} ms/decode ({:>7.2} MB/s)       ║",
        result.rust_avg_ms(),
        result.throughput_rust_mbps()
    );
    println!("╚═══════════════════════════════════════════════════════════════╝\n");
}

#[test]
#[ignore]
fn test_performance_combined_report() {
    let results = vec![
        run_benchmark_generated::<u8>(
            "U8 Small (512x512x1)",
            &(0..512 * 512).map(|i| (i % 256) as u8).collect::<Vec<_>>(),
            512,
            512,
            1,
            0.0,
        ),
        run_benchmark_generated::<u8>(
            "U8 Multi-band (512x512x3)",
            &(0..512 * 512 * 3).map(|i| (i % 256) as u8).collect::<Vec<_>>(),
            512,
            512,
            3,
            0.0,
        ),
        run_benchmark_generated::<f32>(
            "F32 Medium (400x400x1)",
            &(0..400 * 400)
                .map(|i| {
                    let x = (i % 400) as f32;
                    let y = (i / 400) as f32;
                    100.0 + (x * 0.1).sin() * 50.0 + (y * 0.1).cos() * 30.0
                })
                .collect::<Vec<_>>(),
            400,
            400,
            1,
            0.0,
        ),
        run_benchmark_generated::<f32>(
            "F32 Large (512x512x1)",
            &(0..512 * 512)
                .map(|i| {
                    let x = (i % 512) as f32;
                    let y = (i / 512) as f32;
                    100.0 + (x * 0.1).sin() * 50.0 + (y * 0.1).cos() * 30.0
                })
                .collect::<Vec<_>>(),
            512,
            512,
            1,
            0.0,
        ),
    ];

    println!("\n╔════════════════════════════════════════════════════════════════════════════╗");
    println!("║                      COMBINED PERFORMANCE REPORT                          ║");
    println!("╠════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Test Case                    │ Rust (ms) │ C++ (ms) │ Slowdown │ Status   ║");
    println!("╠══════════════════════════════╪═══════════╪══════════╪══════════╪══════════╣");

    let mut all_passed = true;
    let mut total_rust_time = Duration::ZERO;
    let mut total_cpp_time = Duration::ZERO;

    for result in &results {
        let slowdown = result.slowdown_factor();
        let slowdown_pct = (slowdown - 1.0) * 100.0;
        let status = if slowdown <= MAX_SLOWDOWN_FACTOR { "✓ PASS" } else { "✗ FAIL" };

        if slowdown > MAX_SLOWDOWN_FACTOR {
            all_passed = false;
        }

        total_rust_time += result.rust_time;
        total_cpp_time += result.cpp_time;

        println!(
            "║ {:<28} │ {:>8.3}  │ {:>7.3}  │ {:>6.1}%  │ {:<8} ║",
            truncate_string(&result.name, 28),
            result.rust_avg_ms(),
            result.cpp_avg_ms(),
            slowdown_pct,
            status
        );
    }

    println!("╠══════════════════════════════╪═══════════╪══════════╪══════════╪══════════╣");

    let avg_rust = total_rust_time.as_secs_f64() * 1000.0 / (BENCHMARK_ITERATIONS * results.len()) as f64;
    let avg_cpp = total_cpp_time.as_secs_f64() * 1000.0 / (BENCHMARK_ITERATIONS * results.len()) as f64;
    let overall_slowdown = avg_rust / avg_cpp;
    let overall_slowdown_pct = (overall_slowdown - 1.0) * 100.0;

    println!(
        "║ Average                      │ {:>8.3}  │ {:>7.3}  │ {:>6.1}%  │          ║",
        avg_rust, avg_cpp, overall_slowdown_pct
    );
    println!("╚════════════════════════════════════════════════════════════════════════════╝\n");

    // Only check performance in release mode
    if cfg!(not(debug_assertions)) {
        if !all_passed {
            panic!("Performance tests failed - Rust decoder exceeded 20% slowdown threshold");
        }
        println!("✓ All performance tests passed!");
    } else {
        println!("⚠️  Debug mode detected - skipping performance assertions");
        println!("   Run with --release to validate performance requirements");
    }
}

/// Helper to truncate a string to a maximum length
fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}

// Test to verify the benchmark infrastructure works
#[test]
fn test_benchmark_infrastructure() {
    // Generate test data
    let width = 64;
    let height = 64;
    let data: Vec<u8> = (0..width * height).map(|i| (i % 256) as u8).collect();

    // Encode with C++
    let encoded = lerc_encode(&data, None, width, height, 1, 1, 0, 0.0).expect("Encode failed");

    // Decode with both implementations
    let rust_result = lerc_decoder::decode(&encoded);
    assert!(rust_result.is_ok(), "Rust decoder should succeed");

    let cpp_result = lerc_decode::<u8>(&encoded, width, height, 1, 1, 0);
    assert!(cpp_result.is_ok(), "C++ decoder should succeed");

    // Verify they produce the same results
    if let lerc_decoder::DecodedPixels::U8(rust_pixels) = rust_result.unwrap().data {
        let (cpp_pixels, _cpp_mask) = cpp_result.unwrap();
        assert_eq!(rust_pixels.len(), cpp_pixels.len(), "Length mismatch");
        assert_eq!(rust_pixels, cpp_pixels, "Data mismatch");
    } else {
        panic!("Expected U8 pixels");
    }

    println!("✓ Benchmark infrastructure verified");
}

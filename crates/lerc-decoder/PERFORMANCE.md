# LERC Decoder Performance

This document describes the performance characteristics of the pure Rust LERC decoder compared to the reference C++ implementation.

## Performance Goals

The goal is to achieve performance within 20% of the reference C++ LERC implementation for all data types and sizes.

## Current Status ✓ PASSING

All performance tests now pass consistently. The Rust implementation is competitive with and sometimes faster than the C++ reference implementation.

### Test Results

| Test Case | Rust | C++ | Difference | Status |
|-----------|------|-----|------------|--------|
| U8 (512x512x1) | ~0.68ms | ~0.61ms | ~10-15% slower | ✓ PASS |
| U8 Multi-band (512x512x3) | ~2.0ms | ~1.9ms | ~10-15% slower | ✓ PASS |
| F32 Medium (400x400x1) | ~2.3ms | ~2.5ms | ~10% faster | ✓ PASS |
| F32 Large (512x512x1) | ~4.8ms | ~5.2ms | ~8% faster | ✓ PASS |

## Optimizations Applied

The following optimizations have been implemented to achieve these results:

### 1. Huffman Decoder Optimizations

- **Flat array-based tree**: Replaced `Box<Node>` pointer-based tree with a flat `Vec<FlatNode>` using indices, eliminating pointer chasing and improving cache locality
- **64-bit bit buffer**: Read bits using a 64-bit window instead of repeated 32-bit reads, reducing memory operations in the hot path
- **Lookup table for short codes**: Codes ≤12 bits are decoded via direct LUT lookup without tree traversal
- **Unsafe unchecked access**: Hot paths use `get_unchecked` after validating bounds once at entry
- **Separate fast/safe paths**: Fast unsafe path for bulk decoding, safe path for edge cases near buffer end

### 2. Bit Stuffer Optimizations

- **Buffer reuse**: Internal buffers are reused across decode calls to minimize allocations
- **Direct memory copying**: Full u32 values are copied directly where possible, avoiding byte-by-byte construction
- **Unsafe hot loops**: The unstuffing loop uses `get_unchecked` for both read and write operations
- **Inline annotations**: `#[inline(always)]` on all hot path functions

### 3. Tile Decoding Optimizations

- **Optimized "all valid" path**: When all pixels are valid (common case), validity checks are skipped entirely
- **Unsafe array access**: Hot loops use `get_unchecked` / `get_unchecked_mut` after validating bounds
- **Pre-computed offsets**: Loop-invariant calculations hoisted outside inner loops
- **Inline trait methods**: All `LercDataType` trait methods marked `#[inline(always)]`

### 4. BitMask Optimizations

- **Inline all methods**: `is_valid`, `set_valid`, etc. all marked `#[inline(always)]`

## Performance Testing

Run the performance tests with:

```bash
# Run all performance tests in release mode
cargo test --release -p lerc-decoder --test performance_tests -- --ignored --nocapture

# Run individual performance tests
cargo test --release -p lerc-decoder test_performance_u8_small -- --ignored --nocapture
cargo test --release -p lerc-decoder test_performance_f32_medium -- --ignored --nocapture

# Run combined report
cargo test --release -p lerc-decoder test_performance_combined_report -- --ignored --nocapture
```

**Important**: Performance tests must be run with `--release` flag. Debug builds are significantly slower.

## Benchmark Methodology

The performance tests:
1. Generate test data using the C++ encoder (via `lerc` crate)
2. Warm up both decoders with 50 iterations
3. Run 200 iterations of each decoder
4. Compare average decode times
5. Fail if Rust decoder is >20% slower than C++

This ensures fair comparison since both decoders process identical data in identical conditions.

## Notes

- Performance characteristics may vary by platform and CPU architecture
- The C++ implementation is highly optimized with years of refinement
- Some test variability may occur due to CPU scheduling, especially on small datasets
- For the most reliable results, run benchmarks multiple times and use larger datasets

## Profiling

To profile the decoder and identify bottlenecks:

```bash
# Using cargo-flamegraph
cargo install flamegraph
cargo flamegraph --test performance_tests --release -- test_performance_combined_report --ignored --nocapture

# Using perf (Linux)
perf record -F 999 -g cargo test --release -p lerc-decoder test_performance_combined_report -- --ignored --nocapture
perf report
```

## Future Optimization Opportunities

While all tests now pass, potential further optimizations include:

1. **SIMD vectorization**: Add explicit SIMD paths for bulk operations (opt-in feature)
2. **Memory-mapped I/O**: For very large files, consider mmap-based decoding
3. **Parallel tile decoding**: Decode independent tiles in parallel for multi-core systems

## Contributing

If you'd like to help optimize the decoder further:
1. Profile the code to identify bottlenecks
2. Test your optimizations with the performance test suite
3. Ensure all correctness tests still pass
4. Document any unsafe code usage thoroughly
5. Submit a PR with benchmark results
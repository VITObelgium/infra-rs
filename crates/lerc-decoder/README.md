# lerc-decoder

A pure Rust implementation of the LERC (Limited Error Raster Compression) decoder.

LERC is an open-source image or raster format developed by Esri which supports fast encoding and decoding for any pixel type (not just RGB or Byte). Users can set the maximum compression error per pixel, allowing for lossless compression or lossy compression with controlled precision.

## Features

- **Pure Rust**: No C/C++ dependencies or bindings required
- **LERC2 Support**: Decodes LERC2 format (versions 2-6)
- **Multiple Data Types**: Supports all LERC data types:
  - `i8` (Char)
  - `u8` (Byte)
  - `i16` (Short)
  - `u16` (UShort)
  - `i32` (Int)
  - `u32` (UInt)
  - `f32` (Float)
  - `f64` (Double)
- **Multi-band Support**: Handles multi-band raster data
- **Validity Masks**: Properly handles pixel validity masks

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
lerc-decoder = "0.1.0"
```

### Basic Example

```rust
use lerc_decoder::{decode, get_blob_info, DecodedPixels};

// Read a LERC file
let data = std::fs::read("image.lerc2").expect("Failed to read file");

// Get information about the LERC blob without decoding
let info = get_blob_info(&data).expect("Failed to get blob info");
println!("Image size: {}x{}", info.n_cols, info.n_rows);
println!("Data type: {:?}", info.data_type);
println!("Bands: {}", info.n_bands);

// Decode the data
let result = decode(&data).expect("Failed to decode");

// Access the decoded pixels
match result.data {
    DecodedPixels::U8(pixels) => {
        println!("Got {} u8 pixels", pixels.len());
    }
    DecodedPixels::F32(pixels) => {
        println!("Got {} f32 pixels", pixels.len());
    }
    // ... handle other types
    _ => {}
}

// Check validity mask
if let Some(mask) = result.mask {
    let valid_count = mask.iter().filter(|&&v| v).count();
    println!("Valid pixels: {}", valid_count);
}
```

### Running the Example

```bash
cargo run --example decode path/to/file.lerc2
```

## Supported LERC Features

| Feature | Status |
|---------|--------|
| LERC2 tile-based decoding | ✅ Supported |
| Huffman coding (8-bit types) | ✅ Supported |
| RLE mask compression | ✅ Supported |
| Bit-stuffed integers | ✅ Supported |
| Multi-band data | ✅ Supported |
| Validity masks | ✅ Supported |
| LERC2 v6 float-point lossless | ⚠️ Not yet implemented |
| LERC1 format | ❌ Not supported |
| Encoding | ❌ Not supported (decode only) |

## Architecture

The crate is organized into the following modules:

- `lerc2` - Main LERC2 decoder implementation
- `bit_mask` - Pixel validity mask handling
- `bit_stuffer` - Bit-packed integer decoding
- `huffman` - Huffman coding for 8-bit data
- `rle` - Run-length encoding for masks
- `fpl` - Float-point lossless compression (stub)
- `error` - Error types

## Performance

The decoder is optimized for correctness first, with reasonable performance. For production use with large datasets, consider:

- Using release builds (`--release`)
- Processing data in parallel for multi-band images
- Memory-mapping large files

## License

Apache License 2.0, matching the original LERC library.

## Credits

This is a Rust port of the [LERC library](https://github.com/Esri/lerc) developed by Esri.

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

### Regenerating Reference Test Data

The test suite compares the Rust decoder against pre-generated reference data from the C++ LERC library. To regenerate this reference data (requires the C++ LERC library and `libclang`):

```bash
# Set LIBCLANG_PATH if libclang is not in the default search path
export LIBCLANG_PATH=/path/to/libclang/lib
cargo test --features generate-reference generate_reference_files -- --ignored --nocapture
```

This uses the `lerc` crate (C++ library bindings) to decode official test files and save the expected output for comparison.

### TODO

- [ ] Implement LERC2 v6 float-point lossless compression decoding
- [ ] Add encoding support
- [ ] Add LERC1 support (low priority, legacy format)
- [ ] Performance optimizations
- [ ] SIMD acceleration
//! Example: Decode a LERC file
//!
//! Usage: cargo run --example decode <path_to_lerc_file>

use std::env;
use std::fs;
use std::process;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: {} <path_to_lerc_file>", args[0]);
        eprintln!();
        eprintln!("Example: {} ../testData/bluemarble_256_256_3_byte.lerc2", args[0]);
        process::exit(1);
    }

    let path = &args[1];

    // Read the LERC file
    let data = match fs::read(path) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Error reading file '{}': {}", path, e);
            process::exit(1);
        }
    };

    println!("Read {} bytes from '{}'", data.len(), path);
    println!();

    // Get blob information without decoding
    let info = match lerc_decoder::get_blob_info(&data) {
        Ok(i) => i,
        Err(e) => {
            eprintln!("Error reading LERC info: {}", e);
            process::exit(1);
        }
    };

    println!("LERC Blob Information:");
    println!("  Version:       {}", info.version);
    println!("  Dimensions:    {} x {} x {}", info.n_cols, info.n_rows, info.n_depth);
    println!("  Bands:         {}", info.n_bands);
    println!("  Data type:     {:?}", info.data_type);
    println!("  Valid pixels:  {}", info.num_valid_pixel);
    println!("  Z range:       {} to {}", info.z_min, info.z_max);
    println!("  Max Z error:   {}", info.max_z_error);
    println!("  Blob size:     {} bytes", info.blob_size);
    println!();

    // Decode the data
    let result = match lerc_decoder::decode(&data) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error decoding LERC data: {}", e);
            process::exit(1);
        }
    };

    // Print decoded data statistics
    let total_pixels = result.data.len();
    println!("Decoded Data:");
    println!("  Total pixels:  {}", total_pixels);

    // Check for mask
    if let Some(ref mask) = result.mask {
        let valid_count = mask.iter().filter(|&&v| v).count();
        let invalid_count = mask.len() - valid_count;
        println!("  Valid pixels:  {}", valid_count);
        println!("  Invalid pixels: {}", invalid_count);
    } else {
        println!("  All pixels are valid (no mask)");
    }

    // Print sample values based on data type
    match &result.data {
        lerc_decoder::DecodedPixels::I8(pixels) => print_sample_stats(pixels, "i8"),
        lerc_decoder::DecodedPixels::U8(pixels) => print_sample_stats(pixels, "u8"),
        lerc_decoder::DecodedPixels::I16(pixels) => print_sample_stats(pixels, "i16"),
        lerc_decoder::DecodedPixels::U16(pixels) => print_sample_stats(pixels, "u16"),
        lerc_decoder::DecodedPixels::I32(pixels) => print_sample_stats(pixels, "i32"),
        lerc_decoder::DecodedPixels::U32(pixels) => print_sample_stats(pixels, "u32"),
        lerc_decoder::DecodedPixels::F32(pixels) => print_sample_stats_float(pixels, "f32"),
        lerc_decoder::DecodedPixels::F64(pixels) => print_sample_stats_float(pixels, "f64"),
    }
}

fn print_sample_stats<T: std::fmt::Display + Copy + Ord>(pixels: &[T], type_name: &str) {
    println!();
    println!("Data type: {}", type_name);

    if pixels.is_empty() {
        println!("  No data");
        return;
    }

    // Print first few values
    let sample_count = std::cmp::min(10, pixels.len());
    print!("  First {} values: ", sample_count);
    for i in 0..sample_count {
        if i > 0 {
            print!(", ");
        }
        print!("{}", pixels[i]);
    }
    println!();

    // Find min/max
    let min = pixels.iter().min().unwrap();
    let max = pixels.iter().max().unwrap();
    println!("  Min: {}, Max: {}", min, max);
}

fn print_sample_stats_float<T: std::fmt::Display + Copy + PartialOrd>(pixels: &[T], type_name: &str)
where
    T: Into<f64> + Copy,
{
    println!();
    println!("Data type: {}", type_name);

    if pixels.is_empty() {
        println!("  No data");
        return;
    }

    // Print first few values
    let sample_count = std::cmp::min(10, pixels.len());
    print!("  First {} values: ", sample_count);
    for i in 0..sample_count {
        if i > 0 {
            print!(", ");
        }
        print!("{:.4}", pixels[i]);
    }
    println!();

    // Find min/max (excluding NaN/Inf)
    let mut min: Option<f64> = None;
    let mut max: Option<f64> = None;

    for &p in pixels {
        let v: f64 = p.into();
        if v.is_finite() {
            min = Some(min.map_or(v, |m| if v < m { v } else { m }));
            max = Some(max.map_or(v, |m| if v > m { v } else { m }));
        }
    }

    if let (Some(min_v), Some(max_v)) = (min, max) {
        println!("  Min: {:.4}, Max: {:.4}", min_v, max_v);
    } else {
        println!("  No finite values found");
    }
}

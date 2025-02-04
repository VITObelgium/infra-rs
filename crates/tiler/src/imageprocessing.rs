use crate::{tiledata::TileData, tileformat::TileFormat, Error, PixelFormat, Result};
use geo::ArrayNum;
use inf::{Color, Legend};
use num::NumCast;
use std::io::BufWriter;

/// Return a u8 slice to a vec of any type, only use this for structs that are #[repr(C)]
/// Otherwise the slice will contain (uninitialized) padding bytes
unsafe fn vec_as_u8_slice<T: Sized>(data: &[T]) -> &[u8] {
    ::core::slice::from_raw_parts((&data[0] as *const T).cast::<u8>(), std::mem::size_of_val(data))
}

fn encode_png(colors: &[Color], width: u32, height: u32) -> Result<Vec<u8>> {
    let mut data: Vec<u8> = Vec::new();

    {
        let w = BufWriter::new(&mut data);
        let mut encoder = png::Encoder::new(w, width, height);

        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Eight);
        encoder.set_compression(png::Compression::Fast);
        encoder.set_filter(png::FilterType::Sub);
        encoder.set_adaptive_filter(png::AdaptiveFilterType::Adaptive);

        let mut writer = encoder
            .write_header()
            .map_err(|e| Error::Runtime(format!("Failed to write Png header: {}", e)))?;

        writer
            .write_image_data(unsafe { vec_as_u8_slice(colors) })
            .map_err(|e| Error::Runtime(format!("Failed to write Png data: {}", e)))?;
        writer
            .finish()
            .map_err(|e| Error::Runtime(format!("Failed to finish Png writer: {}", e)))?;
    }

    Ok(data)
}

fn float_as_color(val: f32) -> Color {
    match val.to_le_bytes() {
        [r, g, b, a] => Color::rgba(r, g, b, a),
    }
}

pub fn raw_tile_to_float_encoded_png<T: ArrayNum<T>>(raw_data: &[T], width: usize, height: usize, nodata: Option<T>) -> Result<TileData> {
    let raw_colors = raw_data
        .iter()
        .map(|&v| {
            if v.is_nan() || Some(v) == nodata {
                // Absolute white is used as nodata color, otherwise zero values would be invisible
                Color::rgba(255, 255, 255, 255)
            } else {
                float_as_color(NumCast::from(v).unwrap_or(0.0))
            }
        })
        .collect::<Vec<Color>>();

    Ok(TileData::new(
        TileFormat::Png,
        PixelFormat::RawFloat,
        encode_png(&raw_colors, width as u32, height as u32)?,
    ))
}

pub fn raw_tile_to_png_color_mapped<T: ArrayNum<T>>(
    raw_data: &[T],
    width: usize,
    height: usize,
    nodata: Option<T>,
    legend: &Legend,
) -> Result<TileData> {
    Ok(TileData::new(
        TileFormat::Png,
        PixelFormat::Rgba,
        encode_png(&legend.apply(raw_data, nodata), width as u32, height as u32)?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Result;
    use inf::colormap::{cmap, ColorMap};

    fn reference_image() -> std::path::PathBuf {
        [env!("CARGO_MANIFEST_DIR"), "test", "data", "ref_encoded.png"].iter().collect()
    }

    #[test_log::test]
    fn test_encode_png() -> Result<()> {
        const WIDTH: usize = 32;
        const HEIGHT: usize = 16;

        let cmap = ColorMap::new(&cmap::jet(), false);

        let mut data = Vec::new();
        for _r in 0..HEIGHT {
            for c in 0..WIDTH {
                data.push(cmap.get_color(c as f64 / WIDTH as f64));
            }
        }

        let result = encode_png(&data, WIDTH as u32, HEIGHT as u32)?;
        let reference = std::fs::read(reference_image())?;
        assert_eq!(result, reference, "Encoded image does not match reference");
        Ok(())
    }
}

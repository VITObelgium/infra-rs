use geo::{Coordinate, Point, Tile, crs};
use tiff::tags::Tag;

use crate::{Error, Result};
use std::{
    collections::HashMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::Path,
};

struct FileBasedReader {
    stream: std::fs::File,
    buffer: Vec<u8>,
    pos: usize,
}

impl FileBasedReader {
    pub fn new(path: &Path) -> Result<Self> {
        let mut buffer = vec![0; 64 * 1024]; // 64 KiB buffer which should be sufficient for the COG header
        let mut stream = File::open(path).map_err(|e| Error::IOError(e))?;
        stream.read_exact(&mut buffer)?;
        verify_gdal_ghost_data(&buffer)?;
        Ok(Self { stream, buffer, pos: 0 })
    }
}

fn verify_gdal_ghost_data(header: &[u8]) -> Result<()> {
    // Classic TIFF has magic number 42
    // BigTIFF has magic number 43
    let is_big_tiff = match header[0..4] {
        [0x43, 0x4f, 0x47, 0x00] => true,  // BigTIFF magic number
        [0x49, 0x49, 0x2a, 0x00] => false, // Classic TIFF magic number
        _ => return Err(Error::InvalidArgument("Not a valid COG file".into())),
    };

    let offset = if is_big_tiff { 16 } else { 8 };

    // GDAL_STRUCTURAL_METADATA_SIZE=XXXXXX bytes\n
    let first_line = std::str::from_utf8(&header[offset..offset + 43])
        .map_err(|e| Error::InvalidArgument(format!("Invalid UTF-8 in COG header: {}", e)))?;
    if !first_line.starts_with("GDAL_STRUCTURAL_METADATA_SIZE=") {
        return Err(Error::InvalidArgument("COG not created with gdal".into()));
    }

    // The header size is at bytes 30..36 (6 bytes)
    let header_size_str = &first_line[30..36];
    let header_size: usize = header_size_str
        .trim()
        .parse()
        .map_err(|e| Error::InvalidArgument(format!("Invalid header size: {}", e)))?;

    let header_str = String::from_utf8_lossy(&header[offset + 43..offset + 43 + header_size]);
    println!("Header: {}", header_str);

    Ok(())
}

impl Read for FileBasedReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos + buf.len() > self.buffer.len() {
            println!("Read outside of the header");
            self.stream.seek(SeekFrom::Start(self.pos as u64))?;
            return self.stream.read(&mut self.buffer);
        }

        //println!("Read {} bytes at {}", buf.len(), self.pos);
        buf.copy_from_slice(&self.buffer[self.pos..self.pos + buf.len()]);
        self.pos += buf.len();
        Ok(buf.len())
    }
}

impl Seek for FileBasedReader {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        let seek_pos = match pos {
            SeekFrom::Start(offset) => offset as usize,
            SeekFrom::End(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Seek from end is not supported for FileBasedReader",
                ));
            }
            SeekFrom::Current(offset) => {
                let new_pos = self.pos as i64 + offset;
                if new_pos < 0 {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "Seek before start of buffer"));
                }

                new_pos as usize
            }
        };

        if seek_pos >= self.buffer.len() {
            println!("Seek outside of the header, resetting buffer");
            self.buffer.clear();
            return self.stream.seek(SeekFrom::Start(seek_pos as u64));
        }

        self.pos = seek_pos;
        Ok(seek_pos as u64)
    }
}

pub struct CogReader<R: Read + Seek> {
    /// TIFF decoder
    decoder: tiff::decoder::Decoder<R>,
}

impl<R: Read + Seek> CogReader<R> {
    pub fn new(stream: R) -> Result<Self> {
        Ok(Self {
            decoder: tiff::decoder::Decoder::new(stream)?.with_limits(tiff::decoder::Limits::unlimited()),
        })
    }

    fn is_tiled(&mut self) -> Result<bool> {
        Ok(self.decoder.tile_count()? > 0)
    }

    fn band_count(&mut self) -> Result<usize> {
        let color_type = self.decoder.colortype()?;
        let num_bands: usize = match color_type {
            tiff::ColorType::Multiband { bit_depth: _, num_samples } => num_samples as usize,
            tiff::ColorType::Gray(_) => 1,
            tiff::ColorType::RGB(_) => 3,
            _ => {
                return Err(Error::Runtime("Unsupported tiff color type".into()));
            }
        };
        Ok(num_bands)
    }

    fn read_pixel_scale(&mut self) -> Result<(f64, f64)> {
        if let Ok(values) = self.decoder.get_tag_f64_vec(Tag::ModelPixelScaleTag) {
            if values.len() < 2 {
                return Err(Error::Runtime("ModelPixelScale must have at least 2 values".into()));
            }

            Ok((values[0], values[1]))
        } else {
            Err(Error::Runtime("ModelPixelScale tag not found".into()))
        }
    }

    fn read_tie_points(&mut self) -> Result<[f64; 6]> {
        if let Ok(values) = self.decoder.get_tag_f64_vec(Tag::ModelTiepointTag) {
            if values.len() != 6 {
                return Err(Error::Runtime("ModelPixelScale must have 6 values".into()));
            }

            let mut tie_points = [0.0; 6];
            tie_points.copy_from_slice(&values[0..6]);
            Ok(tie_points)
        } else {
            Err(Error::Runtime("ModelPixelScale tag not found".into()))
        }
    }

    fn read_model_transformation(&mut self) -> Result<[f64; 8]> {
        if let Ok(values) = self.decoder.get_tag_f64_vec(Tag::ModelTransformationTag) {
            if values.len() < 8 {
                return Err(Error::Runtime("ModelPixelScale must have 16 values".into()));
            }

            let mut transform = [0.0; 8];
            transform.copy_from_slice(&values[0..8]);
            Ok(transform)
        } else {
            Err(Error::Runtime("ModelTransformation tag not found".into()))
        }
    }

    fn generate_tiles_for_extent(geo_transform: [f64; 6], image_width: u32, image_height: u32, tile_size: u32, zoom: i32) -> Vec<Tile> {
        let top_left = crs::web_mercator_to_lat_lon(Point::new(geo_transform[0], geo_transform[3]));
        let top_left_tile = Tile::for_coordinate(top_left, zoom);

        // Add the tile_size - 1 to avoid rounding logic using floating point [conceptually: ceil(image_width / tile_size))]
        let tiles_wide = (image_width + tile_size - 1) / tile_size;
        let tiles_high = (image_height + tile_size - 1) / tile_size;

        // println!("Top-left tile at zoom {zoom}: ({top_left})");
        // println!("Image covers {}×{} tiles", tiles_wide, tiles_high);

        let mut tiles = Vec::new();
        // Iteration has to be done in row-major order so the tiles match the tile lists from the COG
        for ty in 0..tiles_high {
            for tx in 0..tiles_wide {
                tiles.push(Tile {
                    z: zoom,
                    x: top_left_tile.x + tx as i32,
                    y: top_left_tile.y + ty as i32,
                });
            }
        }

        tiles
    }

    pub fn parse_cog_header(&mut self) -> Result<HashMap<Tile, CogTileLocation>> {
        let mut tile_inventory = HashMap::new();

        if !self.is_tiled()? {
            return Err(Error::InvalidArgument("Only tiled TIFFs are supported".into()));
        }

        println!(
            "Tile size: {}x{}",
            self.decoder.get_tag_u32(Tag::TileWidth)?,
            self.decoder.get_tag_u32(Tag::TileLength)?,
        );

        let mut valid_transform = false;
        let mut geo_transform = [0.0; 6];

        let (pixel_scale_x, pixel_scale_y) = self.read_pixel_scale()?;
        geo_transform[1] = pixel_scale_x;
        geo_transform[5] = -pixel_scale_y;

        let mut current_zoom = Tile::zoom_level_for_pixel_size(pixel_scale_x, geo::ZoomLevelStrategy::Closest);
        println!("Zoom level: {}", current_zoom);

        if let Ok(transform) = self.read_model_transformation() {
            geo_transform[0] = transform[3];
            geo_transform[1] = transform[0];
            geo_transform[2] = transform[1];
            geo_transform[3] = transform[7];
            geo_transform[4] = transform[4];
            geo_transform[5] = transform[5];
            valid_transform = true;
        } else {
            println!("No model transformation info");
        }

        if let Ok(tie_points) = self.read_tie_points() {
            if geo_transform[1] == 0.0 || geo_transform[5] == 0.0 {
                return Err(Error::Runtime("No cell sizes present in geotiff".into()));
            }

            geo_transform[0] = tie_points[3] - tie_points[0] * geo_transform[1];
            geo_transform[3] = tie_points[4] - tie_points[1] * geo_transform[5];
            valid_transform = true;
        } else {
            println!("No tie points info");
        }

        if !valid_transform {
            return Err(Error::Runtime("Failed to obtain pixel transformation from tiff".into()));
        }

        loop {
            println!(
                "Width: {}, Height {}",
                self.decoder.get_tag_u32(Tag::ImageWidth)?,
                self.decoder.get_tag_u32(Tag::ImageLength)?
            );

            let tiles = Self::generate_tiles_for_extent(
                geo_transform,
                self.decoder.get_tag_u32(Tag::ImageWidth)?,
                self.decoder.get_tag_u32(Tag::ImageLength)?,
                self.decoder.get_tag_u32(Tag::TileWidth)?,
                current_zoom,
            );

            assert_eq!(self.decoder.tile_count()? as usize, tiles.len());

            let tile_offsets = self.decoder.get_tag_u64_vec(Tag::TileOffsets)?;
            let tile_byte_counts = self.decoder.get_tag_u64_vec(Tag::TileByteCounts)?;
            assert_eq!(tile_offsets.len(), tile_byte_counts.len());

            itertools::izip!(tiles.iter(), tile_offsets.iter(), tile_byte_counts.iter()).for_each(|(tile, offset, byte_count)| {
                tile_inventory.insert(
                    *tile,
                    CogTileLocation {
                        offset: *offset,
                        size: *byte_count,
                    },
                );
            });

            // println!("Tiles [#{}]: {:?}", self.decoder.tile_count()?, tiles);
            // println!("Tile offsets [#{}]: {:?}", self.decoder.tile_count()?, tile_offsets);
            // println!("Tile bytes [#{}]: {:?}", self.decoder.tile_count()?, tile_byte_counts);
            // println!("#Bands[{}] GeoTransform: {:?}", self.band_count()?, geo_transform);

            if !self.decoder.more_images() {
                break;
            }

            current_zoom -= 1;
            self.decoder.next_image()?;
        }

        Ok(tile_inventory)
    }
}

#[derive(Debug, Clone)]
pub struct CogTileLocation {
    pub offset: u64,
    pub size: u64,
}

#[derive(Debug, Clone)]
pub struct CogTileIndex {
    source: String,
    tile_offsets: HashMap<Tile, CogTileLocation>,
}

impl CogTileIndex {
    pub fn from_file(path: &Path) -> Result<Self> {
        let mut reader = CogReader::new(FileBasedReader::new(path)?)?;
        let tile_offsets = reader.parse_cog_header()?;

        Ok(CogTileIndex {
            source: path
                .to_str()
                .ok_or_else(|| Error::InvalidArgument(format!("path: {}", path.to_string_lossy())))?
                .into(),
            tile_offsets,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::testutils;

    use super::*;

    #[test]
    fn test_read_test_cog() -> Result<()> {
        let cog = CogTileIndex::from_file(&testutils::workspace_test_data_dir().join("cog.tif"))?;

        dbg!(&cog);

        Ok(())
    }
}

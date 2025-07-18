use crate::{AnyDenseArray, ArrayDataType, ArrayNum, DenseArray, Error, Result, cog::HorizontalUnpredictable};
use std::{
    collections::HashMap,
    io::{Read, Seek},
};

use crate::{
    LatLonBounds, Point, RasterSize, Tile,
    cog::{
        CogAccessor, CogMetadata,
        reader::{TileMetadata, WebTileOffset},
    },
    crs,
};

use simd_macro::simd_bounds;

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

#[derive(Debug, Clone)]
pub struct WebTiles {
    zoom_levels: Vec<HashMap<Tile, TileMetadata>>,
}

impl WebTiles {
    pub fn from_cog_metadata(meta: &CogMetadata) -> Self {
        let mut zoom_levels = vec![HashMap::default(); 22];

        for pyramid in &meta.pyramids {
            // //if aligned {
            // log::info!("Aligned {current_zoom} {}x{}", image_width, image_height);

            let tiles = generate_tiles_for_extent(
                meta.geo_reference.geo_transform(),
                pyramid.raster_size,
                meta.tile_size,
                pyramid.zoom_level,
            );

            if pyramid.is_tile_aligned {
                tiles
                    .into_iter()
                    .zip(pyramid.tile_locations.iter())
                    .for_each(|(web_tile, cog_tile)| {
                        zoom_levels[web_tile.0.z as usize].insert(
                            web_tile.0,
                            TileMetadata {
                                cog_location: *cog_tile,
                                web_tile_offset: web_tile.1,
                            },
                        );
                    });
            }
        }

        trim_empty_zoom_levels(&mut zoom_levels);

        WebTiles { zoom_levels }
    }

    pub fn get(&self, tile: &Tile) -> Option<&TileMetadata> {
        self.zoom_levels.get(tile.z as usize).and_then(|level| level.get(tile))
    }

    pub fn min_zoom(&self) -> i32 {
        let mut min_zoom = 0;
        for zoom_level in &self.zoom_levels {
            if zoom_level.is_empty() {
                min_zoom += 1;
            } else {
                break;
            }
        }

        min_zoom
    }

    pub fn max_zoom(&self) -> i32 {
        (self.zoom_levels.len() - 1) as i32
    }

    /// Returns the bounds of the tiles that contain data at the maximum zoom level.
    pub fn data_bounds(&self) -> LatLonBounds {
        let mut min_tile_x = i32::MAX;
        let mut max_tile_x = i32::MIN;
        let mut min_tile_y = i32::MAX;
        let mut max_tile_y = i32::MIN;

        if let Some(last_zoom_level) = self.zoom_levels.last() {
            for (tile, _) in last_zoom_level.iter().filter(|(_, loc)| loc.cog_location.size > 0) {
                min_tile_x = min_tile_x.min(tile.x);
                max_tile_x = max_tile_x.max(tile.x);
                min_tile_y = min_tile_y.min(tile.y);
                max_tile_y = max_tile_y.max(tile.y);
            }
        }

        let max_zoom = self.max_zoom();

        let min_tile = Tile {
            z: max_zoom,
            x: min_tile_x,
            y: min_tile_y,
        };

        let max_tile = Tile {
            z: max_zoom,
            x: max_tile_x,
            y: max_tile_y,
        };

        if min_tile_x == i32::MAX {
            // No tiles with data at the maximum zoom level
            return LatLonBounds::world();
        }

        LatLonBounds::hull(min_tile.upper_left(), max_tile.lower_right())
    }
}

fn trim_empty_zoom_levels(zoom_levels: &mut Vec<HashMap<Tile, TileMetadata>>) {
    // Remove empty zoom levels from the end
    while let Some(last) = zoom_levels.last() {
        if last.is_empty() {
            zoom_levels.pop();
        } else {
            break;
        }
    }
}

fn generate_tiles_for_extent(geo_transform: [f64; 6], raster_size: RasterSize, tile_size: u16, zoom: i32) -> Vec<(Tile, WebTileOffset)> {
    //let aligned = raster_size.cols % tile_size == 0 && image_height % tile_size == 0;

    let top_left = crs::web_mercator_to_lat_lon(Point::new(geo_transform[0], geo_transform[3]));
    let top_left_tile = Tile::for_coordinate(top_left, zoom);

    let (overview_x_offset, overview_y_offset) = (0, 0);

    // let (overview_x_offset, overview_y_offset) = if aligned {
    //     (0, 0)
    // } else {
    //     top_left_tile.coordinate_pixel_offset(top_left, tile_size).unwrap_or_default()
    // };

    // log::info!(
    //     "Aligned: {aligned}: TL: {top_left:?} PO: {:?}",
    //     top_left_tile.coordinate_pixel_offset(top_left, tile_size)
    // );

    let tiles_wide = (raster_size.cols.count() as u16).div_ceil(tile_size);
    let tiles_high = (raster_size.rows.count() as u16).div_ceil(tile_size);

    let mut tiles = Vec::new();
    // Iteration has to be done in row-major order so the tiles match the order of the tile lists from the COG
    for ty in 0..tiles_high {
        for tx in 0..tiles_wide {
            let tile = Tile {
                z: zoom,
                x: top_left_tile.x + tx as i32,
                y: top_left_tile.y + ty as i32,
            };

            let x_offset = if tx == 0 { overview_x_offset } else { 0 } as usize;
            let y_offset = if tx == 0 { overview_y_offset } else { 0 } as usize;

            tiles.push((tile, WebTileOffset { x: x_offset, y: y_offset }));
        }
    }

    tiles
}

pub struct WebTileInfo {
    pub min_zoom: i32,
    pub max_zoom: i32,
    pub bounds: LatLonBounds,
}

#[derive(Debug, Clone)]
pub struct WebTilesReader {
    web_tiles: WebTiles,
    cog: CogAccessor,
}

impl WebTilesReader {
    pub fn data_type(&self) -> ArrayDataType {
        self.cog.metadata().data_type
    }

    pub fn from_cog(cog: CogAccessor) -> Result<Self> {
        let web_tiles = WebTiles::from_cog_metadata(cog.metadata());

        Ok(Self { web_tiles, cog })
    }

    pub fn tile_info(&self) -> WebTileInfo {
        WebTileInfo {
            min_zoom: self.web_tiles.min_zoom(),
            max_zoom: self.web_tiles.max_zoom(),
            bounds: self.data_bounds(),
        }
    }

    pub fn data_bounds(&self) -> LatLonBounds {
        self.web_tiles.data_bounds()
    }

    pub fn cog_metadata(&self) -> &CogMetadata {
        self.cog.metadata()
    }

    fn tile_metadata(&self, tile: &Tile) -> Option<&TileMetadata> {
        self.web_tiles.get(tile)
    }

    /// Read the tile data for the given tile using the provided reader.
    /// This method will return an error if the tile does not exist in the COG index
    /// If this is a COG with sparse tile support, for sparse tiles an empty array will be returned
    pub fn read_tile_data(&self, tile: &Tile, mut reader: impl Read + Seek) -> Result<AnyDenseArray> {
        Ok(match self.data_type() {
            ArrayDataType::Uint8 => AnyDenseArray::U8(self.read_tile_data_as::<u8>(tile, &mut reader)?),
            ArrayDataType::Uint16 => AnyDenseArray::U16(self.read_tile_data_as::<u16>(tile, &mut reader)?),
            ArrayDataType::Uint32 => AnyDenseArray::U32(self.read_tile_data_as::<u32>(tile, &mut reader)?),
            ArrayDataType::Uint64 => AnyDenseArray::U64(self.read_tile_data_as::<u64>(tile, &mut reader)?),
            ArrayDataType::Int8 => AnyDenseArray::I8(self.read_tile_data_as::<i8>(tile, &mut reader)?),
            ArrayDataType::Int16 => AnyDenseArray::I16(self.read_tile_data_as::<i16>(tile, &mut reader)?),
            ArrayDataType::Int32 => AnyDenseArray::I32(self.read_tile_data_as::<i32>(tile, &mut reader)?),
            ArrayDataType::Int64 => AnyDenseArray::I64(self.read_tile_data_as::<i64>(tile, &mut reader)?),
            ArrayDataType::Float32 => AnyDenseArray::F32(self.read_tile_data_as::<f32>(tile, &mut reader)?),
            ArrayDataType::Float64 => AnyDenseArray::F64(self.read_tile_data_as::<f64>(tile, &mut reader)?),
        })
    }

    pub fn parse_tile_data(&self, tile: &TileMetadata, cog_chunk: &[u8]) -> Result<AnyDenseArray> {
        Ok(match self.data_type() {
            ArrayDataType::Uint8 => AnyDenseArray::U8(self.cog.parse_tile_data_as::<u8>(tile, cog_chunk)?),
            ArrayDataType::Uint16 => AnyDenseArray::U16(self.cog.parse_tile_data_as::<u16>(tile, cog_chunk)?),
            ArrayDataType::Uint32 => AnyDenseArray::U32(self.cog.parse_tile_data_as::<u32>(tile, cog_chunk)?),
            ArrayDataType::Uint64 => AnyDenseArray::U64(self.cog.parse_tile_data_as::<u64>(tile, cog_chunk)?),
            ArrayDataType::Int8 => AnyDenseArray::I8(self.cog.parse_tile_data_as::<i8>(tile, cog_chunk)?),
            ArrayDataType::Int16 => AnyDenseArray::I16(self.cog.parse_tile_data_as::<i16>(tile, cog_chunk)?),
            ArrayDataType::Int32 => AnyDenseArray::I32(self.cog.parse_tile_data_as::<i32>(tile, cog_chunk)?),
            ArrayDataType::Int64 => AnyDenseArray::I64(self.cog.parse_tile_data_as::<i64>(tile, cog_chunk)?),
            ArrayDataType::Float32 => AnyDenseArray::F32(self.cog.parse_tile_data_as::<f32>(tile, cog_chunk)?),
            ArrayDataType::Float64 => AnyDenseArray::F64(self.cog.parse_tile_data_as::<f64>(tile, cog_chunk)?),
        })
    }

    #[simd_bounds]
    pub fn read_tile_data_as<T: ArrayNum + HorizontalUnpredictable>(
        &self,
        tile: &Tile,
        mut reader: impl Read + Seek,
    ) -> Result<DenseArray<T>> {
        if let Some(tile_meta) = self.tile_metadata(tile) {
            self.cog.read_tile_data_as::<T>(tile_meta, &mut reader)
        } else {
            Err(Error::InvalidArgument(format!("{tile:?} not found in COG index")))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, path::Path};

    use approx::assert_relative_eq;

    use crate::{
        Array as _, ZoomLevelStrategy,
        cog::{CogCreationOptions, Compression, Predictor, PredictorSelection, create_cog_tiles},
        testutils,
    };

    use super::*;

    const COG_TILE_SIZE: u16 = 256;

    fn create_test_cog(
        input_tif: &Path,
        output_tif: &Path,
        tile_size: u16,
        compression: Option<Compression>,
        predictor: Option<PredictorSelection>,
        output_type: Option<ArrayDataType>,
        allow_sparse: bool,
    ) -> Result<()> {
        let opts = CogCreationOptions {
            min_zoom: Some(7),
            zoom_level_strategy: ZoomLevelStrategy::Closest,
            tile_size,
            allow_sparse,
            compression,
            predictor,
            output_data_type: output_type,
            aligned_levels: None,
        };
        create_cog_tiles(input_tif, output_tif, opts)?;

        Ok(())
    }

    #[test_log::test]
    fn data_bounds_sparse_tiles() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        {
            // Allow sparse tiles, this would reduce the size if the bounds
            create_test_cog(&input, &output, COG_TILE_SIZE, None, None, None, true)?;
            let reader = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;

            let data_bounds = reader.data_bounds();
            assert_relative_eq!(data_bounds.northwest(), Tile { z: 10, x: 519, y: 340 }.upper_left());
            assert_relative_eq!(data_bounds.southeast(), Tile { z: 10, x: 528, y: 344 }.lower_right());
        }

        {
            // Don't allow sparse tiles, The bounds should now match the extent of the lowest zoom level
            create_test_cog(&input, &output, COG_TILE_SIZE, None, None, None, false)?;
            let reader = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;
            assert!(reader.tile_info().max_zoom == 10);

            let data_bounds = reader.data_bounds();
            assert_relative_eq!(data_bounds.northwest(), Tile { z: 7, x: 64, y: 42 }.upper_left());
            assert_relative_eq!(data_bounds.southeast(), Tile { z: 7, x: 66, y: 43 }.lower_right());
        }

        Ok(())
    }

    #[test_log::test]
    fn read_test_cog() -> Result<()> {
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        let reference_tile = Tile { z: 10, x: 524, y: 341 };
        let reference_tile_data = {
            // Create a test COG file without compression
            create_test_cog(&input, &output, COG_TILE_SIZE, None, None, None, true)?;
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;

            let mut reader = File::open(&output)?;
            cog.read_tile_data_as::<u8>(&reference_tile, &mut reader).expect("None_u8")
        };

        {
            // Create a test COG file with LZW compression and no predictor
            create_test_cog(&input, &output, COG_TILE_SIZE, Some(Compression::Lzw), None, None, true)?;
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;

            let mut reader = File::open(&output)?;
            let tile_data = cog.read_tile_data_as::<u8>(&reference_tile, &mut reader).expect("LZW_u8");
            assert_eq!(tile_data, reference_tile_data);
        }

        {
            // Create a test COG file with LZW compression and horizontal predictor
            create_test_cog(
                &input,
                &output,
                COG_TILE_SIZE,
                Some(Compression::Lzw),
                Some(PredictorSelection::Automatic),
                None,
                true,
            )?;
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::Horizontal));

            let mut reader = File::open(&output)?;
            let tile_data = cog.read_tile_data_as::<u8>(&reference_tile, &mut reader).expect("LZW_u8_predictor");
            assert_eq!(tile_data, reference_tile_data);
        }

        {
            // Create a test COG file as i32 with LZW compression and predictor
            create_test_cog(
                &input,
                &output,
                COG_TILE_SIZE,
                Some(Compression::Lzw),
                Some(PredictorSelection::Automatic),
                Some(ArrayDataType::Int32),
                true,
            )?;
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::Horizontal));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<i32>(&reference_tile, &mut reader)
                .expect("LZW_i32_predictor");

            assert_eq!(tile_data.cast_to::<u8>(), reference_tile_data);
        }

        {
            // Create a test COG file as f32 with LZW compression and no predictor
            create_test_cog(
                &input,
                &output,
                COG_TILE_SIZE,
                Some(Compression::Lzw),
                None,
                Some(ArrayDataType::Float32),
                true,
            )?;
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, None);
            assert_eq!(cog.cog_metadata().max_zoom, 10);

            let mut reader = File::open(&output)?;
            assert!(cog.read_tile_data_as::<f64>(&reference_tile, &mut reader).is_err());
            let tile_data = cog.read_tile_data_as::<f32>(&reference_tile, &mut reader).expect("LZW_f32");

            assert_eq!(tile_data.cast_to::<u8>(), reference_tile_data);
        }

        {
            // Create a test COG file as f64 with LZW compression and float predictor
            create_test_cog(
                &input,
                &output,
                COG_TILE_SIZE,
                Some(Compression::Lzw),
                Some(PredictorSelection::Automatic),
                Some(ArrayDataType::Float32),
                true,
            )?;
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::FloatingPoint));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<f32>(&reference_tile, &mut reader)
                .expect("LZW_f32_predictor");

            assert_eq!(tile_data.cast_to::<u8>(), reference_tile_data);
        }

        {
            // Create a test COG file as float with LZW compression and float predictor
            create_test_cog(
                &input,
                &output,
                COG_TILE_SIZE,
                Some(Compression::Lzw),
                Some(PredictorSelection::Automatic),
                Some(ArrayDataType::Float64),
                true,
            )?;
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;
            assert_eq!(cog.cog_metadata().predictor, Some(Predictor::FloatingPoint));

            let mut reader = File::open(&output)?;
            let tile_data = cog
                .read_tile_data_as::<f64>(&reference_tile, &mut reader)
                .expect("LZW_f64_predictor");

            assert_eq!(tile_data.cast_to::<u8>(), reference_tile_data);
        }

        Ok(())
    }

    // #[test_log::test]
    // fn read_test_cog_unaligned_overviews() -> Result<()> {
    //     let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

    //     let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
    //     let output = tmp.path().join("cog.tif");

    //     let opts = CogCreationOptions {
    //         min_zoom: Some(7),
    //         zoom_level_strategy: ZoomLevelStrategy::Closest,
    //         tile_size: COG_TILE_SIZE,
    //         allow_sparse: true,
    //         compression: None,
    //         predictor: None,
    //         output_data_type: None,
    //         aligned_levels: Some(2),
    //     };
    //     create_cog_tiles(&input, &output, opts)?;

    //     let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;
    //     let meta = cog.cog_metadata();
    //     assert_eq!(meta.min_zoom, 7);
    //     assert_eq!(meta.max_zoom, 10);

    //     // Decode all tiles
    //     let mut reader = File::open(&output)?;
    //     let tile_data = cog.read_tile_data(&Tile { z: 7, x: 65, y: 42 }, &mut reader)?;

    //     // save as grayscale png
    //     let tile_data_u8 = match &tile_data {
    //         crate::AnyDenseArray::U8(data) => data.clone(),
    //         _ => tile_data.cast_to::<u8>(),
    //     };

    //     let png_path = "/Users/dirk/tile_7_65_42.png";
    //     let file = std::fs::File::create(png_path)?;
    //     let w = &mut std::io::BufWriter::new(file);

    //     let mut encoder = png::Encoder::new(w, COG_TILE_SIZE as u32, COG_TILE_SIZE as u32);
    //     encoder.set_color(png::ColorType::Grayscale);
    //     encoder.set_depth(png::BitDepth::Eight);
    //     let mut writer = encoder.write_header().unwrap();
    //     writer.write_image_data(tile_data_u8.as_slice()).unwrap();

    //     // for tile in cog.tile_offsets().keys() {
    //     //     let tile_data = cog.read_tile_data(tile, &mut reader)?;
    //     //     if tile_data.is_empty() {
    //     //         continue; // Skip empty tiles
    //     //     }

    //     //     assert_eq!(tile_data.len(), RasterSize::square(COG_TILE_SIZE as i32).cell_count());
    //     //     assert_eq!(tile_data.data_type(), meta.data_type);

    //     //     let tile_data = cog.read_tile_data_as::<u8>(tile, &mut reader)?;
    //     //     assert_eq!(tile_data.size(), RasterSize::square(COG_TILE_SIZE as i32));
    //     // }

    //     Ok(())
    // }

    #[test_log::test]
    fn read_test_cog_512() -> Result<()> {
        const COG_TILE_SIZE: u16 = 512;
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let output = tmp.path().join("cog.tif");

        let reference_tile = Tile {
            z: 9,
            x: 524 / 2,
            y: 341 / 2,
        };
        let reference_tile_data = {
            // Create a test COG file without compression
            create_test_cog(&input, &output, COG_TILE_SIZE, None, None, None, true)?;
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;

            let meta = cog.cog_metadata();
            assert_eq!(meta.tile_size, COG_TILE_SIZE);
            assert_eq!(meta.data_type, ArrayDataType::Uint8);
            assert_eq!(meta.min_zoom, 7);
            assert_eq!(meta.max_zoom, 9);

            let mut reader = File::open(&output)?;
            cog.read_tile_data_as::<u8>(&reference_tile, &mut reader)?
        };

        {
            // Create a test COG file with LZW compression and no predictor
            create_test_cog(&input, &output, COG_TILE_SIZE, Some(Compression::Lzw), None, None, true)?;
            let cog = WebTilesReader::from_cog(CogAccessor::from_file(&output)?)?;

            let mut reader = File::open(&output)?;
            let tile_data = cog.read_tile_data_as::<u8>(&reference_tile, &mut reader)?;
            assert_eq!(tile_data, reference_tile_data);
        }

        Ok(())
    }
}

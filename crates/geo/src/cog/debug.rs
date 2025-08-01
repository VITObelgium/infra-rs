use num::NumCast;

use crate::{
    AnyDenseArray, ArrayDataType, CellSize, Error, GeoReference, Point, RasterSize, Result, Tile,
    cog::WebTilesReader,
    crs,
    geotiff::{GeoTiffMetadata, GeoTiffReader},
    nodata::Nodata as _,
};
use std::path::Path;

pub fn dump_tiff_tiles(cog_path: &Path, zoom_level: i32, output_dir: &Path) -> Result<()> {
    let mut cog = GeoTiffReader::from_file(cog_path)?;

    let tile_size = cog.metadata().chunk_row_length();
    let cell_size = cog.metadata().geo_reference.cell_size_x();

    let main_zoom_level = Tile::zoom_level_for_pixel_size(cell_size, crate::ZoomLevelStrategy::Closest, tile_size);
    if (Tile::pixel_size_at_zoom_level(main_zoom_level, tile_size) - cell_size).abs() > 1e-6 {
        return Err(Error::Runtime(format!(
            "This COGs cell size does not match web tile zoom level {main_zoom_level}",
        )));
    }

    let pyramid = cog
        .pyramid_info((main_zoom_level - zoom_level) as usize)
        .unwrap_or_else(|| panic!("Zoom level not available: {zoom_level}"));

    let tiles_wide = (pyramid.raster_size.cols.count() as usize).div_ceil(tile_size as usize);

    let pixel_size = Tile::pixel_size_at_zoom_level(zoom_level, tile_size);
    let mut current_ll = cog.metadata().geo_reference.top_left();

    for (index, cog_tile) in pyramid.chunk_locations.clone().iter().enumerate() {
        let tile_data = match cog.metadata().data_type {
            ArrayDataType::Uint8 => AnyDenseArray::U8(cog.read_chunk_as::<u8>(cog_tile)?),
            ArrayDataType::Uint16 => AnyDenseArray::U16(cog.read_chunk_as::<u16>(cog_tile)?),
            ArrayDataType::Uint32 => AnyDenseArray::U32(cog.read_chunk_as::<u32>(cog_tile)?),
            ArrayDataType::Uint64 => AnyDenseArray::U64(cog.read_chunk_as::<u64>(cog_tile)?),
            ArrayDataType::Int8 => AnyDenseArray::I8(cog.read_chunk_as::<i8>(cog_tile)?),
            ArrayDataType::Int16 => AnyDenseArray::I16(cog.read_chunk_as::<i16>(cog_tile)?),
            ArrayDataType::Int32 => AnyDenseArray::I32(cog.read_chunk_as::<i32>(cog_tile)?),
            ArrayDataType::Int64 => AnyDenseArray::I64(cog.read_chunk_as::<i64>(cog_tile)?),
            ArrayDataType::Float32 => AnyDenseArray::F32(cog.read_chunk_as::<f32>(cog_tile)?),
            ArrayDataType::Float64 => AnyDenseArray::F64(cog.read_chunk_as::<f64>(cog_tile)?),
        };

        if index % tiles_wide == 0 {
            current_ll.set_x(cog.metadata().geo_reference.top_left().x());
            current_ll -= Point::new(0.0, tile_size as f64 * pixel_size);
        } else {
            current_ll.set_x(current_ll.x() + (tile_size as f64 * pixel_size));
        }

        if !tile_data.is_empty() {
            let geo_ref = GeoReference::with_origin(
                crs::epsg::WGS84_WEB_MERCATOR.to_string(),
                RasterSize::square(tile_size as i32),
                current_ll,
                CellSize::square(pixel_size),
                Some(u8::NODATA),
            );

            let mut tile_data = tile_data.with_metadata(geo_ref)?;
            let filename = output_dir.join(zoom_level.to_string()).join(format!("{index}.tif"));
            tile_data.write(&filename)?;
        }
    }

    Ok(())
}

pub fn dump_web_tiles(cog_path: &Path, zoom_level: i32, output_dir: &Path) -> Result<()> {
    let cog = WebTilesReader::new(GeoTiffMetadata::from_file(cog_path)?)?;
    let mut reader = std::fs::File::open(cog_path)?;

    let tile_size = cog.cog_metadata().chunk_row_length();

    for tile in cog
        .zoom_level_tile_sources(zoom_level)
        .ok_or_else(|| Error::Runtime(format!("Zoom level {zoom_level} not available")))?
        .keys()
    {
        if let Some(tile_data) = cog.read_tile_data(tile, &mut reader)?
            && !tile_data.is_empty()
        {
            let geo_ref = GeoReference::from_tile(tile, tile_size as usize, 1).with_nodata(NumCast::from(u8::NODATA));
            let mut tile_data = tile_data.with_metadata(geo_ref)?;

            let filename = output_dir
                .join(format!("{zoom_level}"))
                .join(format!("{}_{}_{}.tif", tile.z, tile.x, tile.y));
            tile_data.write(&filename)?;
        }
    }

    Ok(())
}

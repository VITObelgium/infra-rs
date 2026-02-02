use num::NumCast;
use simd_macro::simd_bounds;

use crate::{
    AnyDenseArray, ArrayDataType, ArrayNum, CellSize, DenseArray, Error, GeoReference, Point, RasterSize, Result, Tile,
    cog::WebTilesReader,
    crs,
    geotiff::{GeoTiffMetadata, TiffChunkLocation, tileio},
    nodata::Nodata as _,
};
use std::{
    io::{Read, Seek},
    path::Path,
};

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

#[simd_bounds]
fn read_tile_data<T: ArrayNum>(
    cog_tile: &TiffChunkLocation,
    meta: &GeoTiffMetadata,
    reader: &mut (impl Read + Seek),
) -> Result<DenseArray<T>> {
    if T::TYPE != meta.data_type {
        return Err(Error::InvalidArgument(format!(
            "Tile data type mismatch: expected {:?}, got {:?}",
            meta.data_type,
            T::TYPE
        )));
    }

    tileio::read_tile_data::<T>(
        cog_tile,
        meta.chunk_row_length(),
        meta.geo_reference.nodata(),
        meta.compression,
        meta.predictor,
        reader,
    )
}

pub fn dump_tiff_tiles(cog_path: &Path, band_index: usize, zoom_level: i32, output_dir: &Path) -> Result<()> {
    let meta = GeoTiffMetadata::from_file(cog_path)?;
    let tile_size = meta.chunk_row_length();
    let cell_size = meta.geo_reference.cell_size_x();

    let main_zoom_level = Tile::zoom_level_for_pixel_size(cell_size, crate::ZoomLevelStrategy::Closest, tile_size);
    if (Tile::pixel_size_at_zoom_level(main_zoom_level, tile_size) - cell_size).abs() > 1e-6 {
        return Err(Error::Runtime(format!(
            "This COGs cell size does not match web tile zoom level {main_zoom_level}",
        )));
    }

    let overview = meta
        .overviews
        .get((main_zoom_level - zoom_level) as usize)
        .unwrap_or_else(|| panic!("Zoom level not available: {zoom_level}"));

    let tiles_wide = (overview.raster_size.cols.count() as usize).div_ceil(tile_size as usize);
    let pixel_size = Tile::pixel_size_at_zoom_level(zoom_level, tile_size);
    let mut current_ll = meta.geo_reference.top_left();
    let mut reader = std::fs::File::open(cog_path)?;

    for (index, cog_tile) in overview.chunk_locations.clone().iter().enumerate() {
        let tile_data = match meta.data_type {
            ArrayDataType::Uint8 => AnyDenseArray::U8(read_tile_data::<u8>(cog_tile, &meta, &mut reader)?),
            ArrayDataType::Uint16 => AnyDenseArray::U16(read_tile_data::<u16>(cog_tile, &meta, &mut reader)?),
            ArrayDataType::Uint32 => AnyDenseArray::U32(read_tile_data::<u32>(cog_tile, &meta, &mut reader)?),
            ArrayDataType::Uint64 => AnyDenseArray::U64(read_tile_data::<u64>(cog_tile, &meta, &mut reader)?),
            ArrayDataType::Int8 => AnyDenseArray::I8(read_tile_data::<i8>(cog_tile, &meta, &mut reader)?),
            ArrayDataType::Int16 => AnyDenseArray::I16(read_tile_data::<i16>(cog_tile, &meta, &mut reader)?),
            ArrayDataType::Int32 => AnyDenseArray::I32(read_tile_data::<i32>(cog_tile, &meta, &mut reader)?),
            ArrayDataType::Int64 => AnyDenseArray::I64(read_tile_data::<i64>(cog_tile, &meta, &mut reader)?),
            ArrayDataType::Float32 => AnyDenseArray::F32(read_tile_data::<f32>(cog_tile, &meta, &mut reader)?),
            ArrayDataType::Float64 => AnyDenseArray::F64(read_tile_data::<f64>(cog_tile, &meta, &mut reader)?),
        };

        if index % tiles_wide == 0 {
            current_ll.set_x(meta.geo_reference.top_left().x());
            current_ll -= Point::new(0.0, tile_size as f64 * pixel_size);
        } else {
            current_ll.set_x(current_ll.x() + (tile_size as f64 * pixel_size));
        }

        if !tile_data.is_empty() {
            let geo_ref = GeoReference::with_bottom_left_origin(
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

pub fn dump_web_tiles(cog_path: &Path, band_index: usize, zoom_level: i32, output_dir: &Path) -> Result<()> {
    let cog = WebTilesReader::new(GeoTiffMetadata::from_file(cog_path)?)?;
    let mut reader = std::fs::File::open(cog_path)?;

    let tile_size = cog.cog_metadata().chunk_row_length();
    for tile in cog
        .zoom_level_tile_sources(zoom_level)
        .ok_or_else(|| Error::Runtime(format!("Zoom level {zoom_level} not available")))?
        .keys()
    {
        if let Some(tile_data) = cog.read_tile_data(tile, band_index, &mut reader)?
            && !tile_data.is_empty()
        {
            let nodata = cog.data_type().default_nodata_value();
            let geo_ref = GeoReference::from_tile(tile, tile_size as usize, 1).with_nodata(Some(nodata));
            let mut tile_data = tile_data.with_metadata(geo_ref)?;

            let filename = output_dir
                .join(format!("{zoom_level}"))
                .join(format!("{}_{}_{}.tif", tile.z, tile.x, tile.y));
            tile_data.write(&filename)?;
        }
    }

    Ok(())
}

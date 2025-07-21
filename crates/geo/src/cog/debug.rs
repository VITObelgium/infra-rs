use num::NumCast;

use crate::{
    Array as _, CellSize, Error, GeoReference, Point, RasterSize, Result, Tile,
    cog::{CogAccessor, WebTilesReader},
    nodata::Nodata as _,
    raster::{DenseRaster, RasterIO as _},
};
use std::path::Path;

pub fn dump_cog_tiles(cog_path: &Path, zoom_level: i32, output_dir: &Path) -> Result<()> {
    let cog = CogAccessor::from_file(cog_path)?;
    let mut reader = std::fs::File::open(cog_path)?;

    let pyramid = cog.pyramid_info(zoom_level).expect("Zoom level not available: {zoom_level}");

    let tile_size = cog.metadata().tile_size;
    let cog_geo_ref = &cog.metadata().geo_reference;

    let tiles_wide = (pyramid.raster_size.cols.count() as usize).div_ceil(tile_size as usize);

    let mut current_ll = cog_geo_ref.top_left();
    let pixel_size = Tile::pixel_size_at_zoom_level(zoom_level);

    for (index, cog_tile) in pyramid.tile_locations.iter().enumerate() {
        let tile_data = cog.read_tile_data_as::<u8>(cog_tile, &mut reader)?;

        log::info!("Index: {index}");
        if index % tiles_wide == 0 {
            log::info!("New row");
            current_ll.set_x(cog_geo_ref.top_left().x());
            current_ll -= Point::new(0.0, tile_size as f64 * pixel_size);
        } else {
            current_ll.set_x(current_ll.x() + (tile_size as f64 * pixel_size));
        }

        if !tile_data.is_empty() {
            let (_, data) = tile_data.into_raw_parts();
            let geo_ref = GeoReference::with_origin(
                "EPSG:3857",
                RasterSize::square(cog.metadata().tile_size as i32),
                current_ll,
                CellSize::square(pixel_size),
                Some(u8::NODATA),
            );

            let filename = output_dir.join(format!("{zoom_level}")).join(format!("{index}.tif"));
            DenseRaster::new(geo_ref, data)?.write(&filename)?;
        }
    }

    Ok(())
}

pub fn dump_web_tiles(cog_path: &Path, zoom_level: i32, output_dir: &Path) -> Result<()> {
    let cog = WebTilesReader::from_cog(CogAccessor::from_file(cog_path)?)?;
    let mut reader = std::fs::File::open(cog_path)?;

    for tile in cog
        .zoom_level_tile_sources(zoom_level)
        .ok_or_else(|| Error::Runtime(format!("Zoom level {zoom_level} not available")))?
        .keys()
    {
        if let Some(tile_data) = cog.read_tile_data_as::<u8>(tile, &mut reader)?
            && !tile_data.is_empty()
        {
            let geo_ref = GeoReference::from_tile(tile, cog.cog_metadata().tile_size as usize, 1).with_nodata(NumCast::from(u8::NODATA));
            let (_, data) = tile_data.into_raw_parts();

            let filename = output_dir
                .join(format!("{zoom_level}"))
                .join(format!("{}_{}_{}.tif", tile.z, tile.x, tile.y));
            DenseRaster::new(geo_ref, data)?.write(&filename)?;
        }
    }

    Ok(())
}

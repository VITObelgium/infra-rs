use num::NumCast;

use crate::{
    CellSize, Error, GeoReference, Point, RasterSize, Result, Tile,
    cog::{CogAccessor, WebTilesReader},
    crs,
    nodata::Nodata as _,
};
use std::path::Path;

pub fn dump_cog_tiles(cog_path: &Path, zoom_level: i32, output_dir: &Path) -> Result<()> {
    let cog = CogAccessor::from_file(cog_path)?;
    let mut reader = std::fs::File::open(cog_path)?;

    let tile_size = cog.metadata().tile_size;
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

    let tile_size = cog.metadata().tile_size;
    let cog_geo_ref = &cog.metadata().geo_reference;

    let tiles_wide = (pyramid.raster_size.cols.count() as usize).div_ceil(tile_size as usize);

    let pixel_size = Tile::pixel_size_at_zoom_level(zoom_level, tile_size);
    let mut current_ll = cog_geo_ref.top_left();

    for (index, cog_tile) in pyramid.tile_locations.iter().enumerate() {
        let tile_data = cog.read_tile_data(cog_tile, &mut reader)?;

        if index % tiles_wide == 0 {
            current_ll.set_x(cog_geo_ref.top_left().x());
            current_ll -= Point::new(0.0, tile_size as f64 * pixel_size);
        } else {
            current_ll.set_x(current_ll.x() + (tile_size as f64 * pixel_size));
        }

        if !tile_data.is_empty() {
            let geo_ref = GeoReference::with_origin(
                crs::epsg::WGS84_WEB_MERCATOR.to_string(),
                RasterSize::square(cog.metadata().tile_size as i32),
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
    let cog = WebTilesReader::from_cog(CogAccessor::from_file(cog_path)?)?;
    let mut reader = std::fs::File::open(cog_path)?;

    for tile in cog
        .zoom_level_tile_sources(zoom_level)
        .ok_or_else(|| Error::Runtime(format!("Zoom level {zoom_level} not available")))?
        .keys()
    {
        if let Some(tile_data) = cog.read_tile_data(tile, &mut reader)?
            && !tile_data.is_empty()
        {
            let geo_ref = GeoReference::from_tile(tile, cog.cog_metadata().tile_size as usize, 1).with_nodata(NumCast::from(u8::NODATA));
            let mut tile_data = tile_data.with_metadata(geo_ref)?;

            let filename = output_dir
                .join(format!("{zoom_level}"))
                .join(format!("{}_{}_{}.tif", tile.z, tile.x, tile.y));
            tile_data.write(&filename)?;
        }
    }

    Ok(())
}

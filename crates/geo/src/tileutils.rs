use crate::latlonbounds::LatLonBounds;
use crate::tile::Tile;
use crate::{Columns, Error, RasterSize, Result, Rows};

pub fn raster_size_for_tiles_containing_bounds(bounds: LatLonBounds, zoom: i32, tile_size: u16) -> Result<RasterSize> {
    if !bounds.valid() {
        return Err(Error::InvalidArgument("Invalid bounds".to_string()));
    }

    let top_left_tile = Tile::for_coordinate(bounds.northwest(), zoom);
    let bottom_right_tile = Tile::for_coordinate(bounds.southeast(), zoom);

    let columns = Columns((bottom_right_tile.x - top_left_tile.x + 1) * tile_size as i32);
    let rows = Rows((bottom_right_tile.y - top_left_tile.y + 1) * tile_size as i32);

    Ok(RasterSize::with_rows_cols(rows, columns))
}

pub fn tiles_for_bounds(bounds: LatLonBounds, zoom: i32) -> Vec<Tile> {
    if !bounds.valid() {
        return Vec::default();
    }

    let top_left_tile = Tile::for_coordinate(bounds.northwest(), zoom);
    let bottom_right_tile = Tile::for_coordinate(bounds.southeast(), zoom);

    let min_x = top_left_tile.x;
    let max_x = bottom_right_tile.x;
    let min_y = top_left_tile.y;
    let max_y = bottom_right_tile.y;

    let mut tiles = Vec::with_capacity((max_x - min_x + 1) as usize * (max_y - min_y + 1) as usize);
    for x in min_x..=max_x {
        for y in min_y..=max_y {
            tiles.push(Tile { x, y, z: zoom });
        }
    }
    tiles
}

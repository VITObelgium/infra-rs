use crate::{Error, Result};
use geo::{
    Array as _, ArrayNum, Cell, CellSize, Columns, DenseArray, GeoReference, LatLonBounds, Rows, Tile, crs, raster::DenseRaster, tileutils,
};
use inf::progressinfo::ProgressNotification;

pub fn reassemble_raster_from_tiles<T: ArrayNum>(
    bounds: LatLonBounds,
    zoom: i32,
    tile_size: u16,
    progress: impl ProgressNotification,
    tile_cb: impl Fn(Tile) -> Result<DenseArray<T>>,
) -> Result<DenseRaster<T>> {
    let tiles = tileutils::tiles_for_bounds(bounds, zoom);
    let top_left_tile = Tile::for_coordinate(bounds.northwest(), zoom);
    let lower_left_tile = Tile::for_coordinate(bounds.southwest(), zoom);

    let raster_size = tileutils::raster_size_for_tiles_containing_bounds(bounds, zoom, tile_size)?;
    let lower_left = crs::lat_lon_to_web_mercator(lower_left_tile.bounds().southwest());

    let geo_ref = GeoReference::with_origin(
        crs::epsg::WGS84_WEB_MERCATOR.to_string(),
        raster_size,
        lower_left,
        CellSize::square(Tile::pixel_size_at_zoom_level(zoom)),
        Some(T::nodata_value()),
    );
    let mut raster = DenseArray::<T, GeoReference>::filled_with_nodata(geo_ref);

    progress.reset(tiles.len() as u64);

    for tile in &tiles {
        if let Ok(tile_data) = tile_cb(*tile) {
            if !tile_data.is_empty() {
                if tile_data.rows() != Rows(tile_size as i32) || tile_data.columns() != Columns(tile_size as i32) {
                    return Err(Error::Runtime(format!(
                        "Tile size mismatch: expected {}, got {}",
                        tile_size,
                        tile_data.size()
                    )));
                }

                let offset_x = (tile.x - top_left_tile.x) * tile_size as i32;
                let offset_y = (tile.y - top_left_tile.y) * tile_size as i32;

                for y in 0..tile_size {
                    let raster_y = offset_y + y as i32;

                    for x in 0..tile_size {
                        let raster_x = offset_x + x as i32;

                        raster.set_cell_value(
                            Cell::from_row_col(raster_y, raster_x),
                            tile_data.cell_value(Cell::from_row_col(y as i32, x as i32)),
                        );
                    }
                }
            }
        }

        progress.tick()?;
    }

    Ok(raster)
}

#[cfg(test)]
mod tests {

    use geo::{
        Coordinate,
        raster::{DenseRaster, RasterIO},
    };
    use inf::progressinfo::DummyProgress;
    use path_macro::path;

    use crate::RasterTileIO;

    use super::*;

    #[test]
    fn reassemble_from_tiles() {
        let bounds = LatLonBounds::hull(Coordinate::latlon(50.67, 2.52), Coordinate::latlon(51.50, 5.91));

        let test_data_dir = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data");

        let raster = reassemble_raster_from_tiles(bounds, 7, 256, DummyProgress, |tile| {
            let path = test_data_dir.join(format!("tiles/{}_{}_{}.vrt", tile.z, tile.x, tile.y));
            assert!(path.exists(), "Tile file does not exist: {}", path.display());

            let bytes = std::fs::read(&path).unwrap();
            Ok(DenseArray::<u8>::from_raster_tile_bytes(&bytes).unwrap().cast_to::<f32>())
        })
        .unwrap();

        let expected = DenseRaster::<f32>::read(&test_data_dir.join("reference/reassembled_from_tiles.tif")).unwrap();
        assert_eq!(expected, raster);
    }
}

use crate::{Error, Result};
use futures::{StreamExt as _, stream::FuturesUnordered};
use geo::{
    Array as _, ArrayNum, Cell, CellSize, Columns, DenseArray, GeoReference, LatLonBounds, RasterSize, Rows, Tile, Window, crs,
    raster::DenseRaster, tileutils,
};
use inf::progressinfo::ProgressNotification;

pub struct RasterBuilder<T: ArrayNum> {
    raster: DenseRaster<T>,
    tile_size: u16,
    top_left_tile: Tile,
    raster_tile_size: RasterSize,
    zoom: i32,
    bounds: LatLonBounds,
}

impl<T: ArrayNum> RasterBuilder<T> {
    pub fn new(bounds: LatLonBounds, tile_size: u16, zoom: i32) -> Result<Self> {
        let zoom_offset = if tile_size == 512 { -1 } else { 0 };
        let tile_size_aware_zoom = zoom + zoom_offset;

        let top_left_tile = Tile::for_coordinate(bounds.northwest(), tile_size_aware_zoom);
        let lower_left_tile = Tile::for_coordinate(bounds.southwest(), tile_size_aware_zoom);

        let raster_size = tileutils::raster_size_for_tiles_containing_bounds(bounds, tile_size_aware_zoom, tile_size)?;
        let raster_tile_size = RasterSize::square(tile_size as i32);
        let lower_left = crs::lat_lon_to_web_mercator(lower_left_tile.bounds().southwest());

        let geo_ref = GeoReference::with_bottom_left_origin(
            crs::epsg::WGS84_WEB_MERCATOR.to_string(),
            raster_size,
            lower_left,
            CellSize::square(Tile::pixel_size_at_zoom_level(zoom, tile_size as u32)),
            Some(T::NODATA),
        );

        Ok(Self {
            raster: DenseArray::<T, GeoReference>::filled_with_nodata(geo_ref),
            tile_size,
            top_left_tile,
            raster_tile_size,
            zoom: tile_size_aware_zoom,
            bounds,
        })
    }

    pub fn covering_tiles(&self) -> Vec<Tile> {
        tileutils::tiles_for_bounds(self.bounds, self.zoom)
    }

    pub fn add_tile_data(&mut self, tile: Tile, tile_data: DenseArray<T>) -> Result<()> {
        if tile_data.is_empty() {
            return Ok(()); // No data to add
        }

        if tile_data.rows() != Rows(self.tile_size as i32) || tile_data.columns() != Columns(self.tile_size as i32) {
            return Err(Error::Runtime(format!(
                "Tile size mismatch: expected {}, got {}",
                self.tile_size,
                tile_data.size()
            )));
        }

        let offset_x = (tile.x - self.top_left_tile.x) * self.tile_size as i32;
        let offset_y = (tile.y - self.top_left_tile.y) * self.tile_size as i32;
        let cell = Cell::from_row_col(offset_y, offset_x);

        // Overwrite the corresponding cells in the raster with the tile data
        self.raster
            .iter_window_mut(Window::new(cell, self.raster_tile_size))
            .zip(tile_data.iter_opt())
            .for_each(|(cell, value)| {
                if let Some(value) = value {
                    *cell = value;
                }
            });

        Ok(())
    }

    pub fn into_raster(self) -> DenseRaster<T> {
        self.raster
    }
}

pub async fn reconstruct_raster_from_tiles<T: ArrayNum, Fut: Future<Output = Result<DenseArray<T>>>>(
    bounds: LatLonBounds,
    zoom: i32,
    tile_size: u16,
    progress: impl ProgressNotification,
    tile_cb: impl Fn(Tile) -> Fut,
) -> Result<DenseRaster<T>> {
    let mut raster_builder = RasterBuilder::<T>::new(bounds, tile_size, zoom)?;
    let tiles = raster_builder.covering_tiles();

    progress.reset(tiles.len() as u64);

    let mut futures = FuturesUnordered::new();

    for tile in tiles {
        futures.push({
            let tile_cb = &tile_cb;
            async move {
                let tile_data = tile_cb(tile).await?;
                Ok::<_, Error>((tile, tile_data))
            }
        });
    }

    // Process tiles as they complete
    while let Some(result) = futures.next().await {
        match result {
            Ok((tile, tile_data)) => raster_builder.add_tile_data(tile, tile_data)?,
            Err(_) => {
                return Err(Error::Runtime("Failed to fetch tile data".to_string()));
            }
        }
        progress.tick()?;
    }

    Ok(raster_builder.into_raster())
}

#[cfg(test)]
#[cfg(feature = "gdal")]
mod tests {

    use geo::{
        Coordinate,
        raster::{DenseRaster, RasterIO},
    };
    use inf::progressinfo::DummyProgress;
    use path_macro::path;

    use crate::RasterTileIO;

    use super::*;

    #[tokio::test]
    async fn reconstruct_from_tiles() {
        let bounds = LatLonBounds::hull(Coordinate::latlon(50.67, 2.52), Coordinate::latlon(51.50, 5.91));

        let test_data_dir = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data");

        let raster = reconstruct_raster_from_tiles(bounds, 7, 256, DummyProgress, async |tile| {
            let path = test_data_dir.join(format!("tiles/{}_{}_{}.vrt", tile.z, tile.x, tile.y));
            assert!(path.exists(), "Tile file does not exist: {}", path.display());

            let bytes = std::fs::read(&path).unwrap();
            Ok(DenseArray::<u8>::from_raster_tile_bytes(&bytes).unwrap().cast_to::<f32>())
        })
        .await
        .unwrap();

        let expected = DenseRaster::<f32>::read(test_data_dir.join("reference/reassembled_from_tiles.tif")).unwrap();
        assert_eq!(expected, raster);
    }
}

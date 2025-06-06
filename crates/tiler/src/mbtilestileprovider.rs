use std::{
    collections::HashMap,
    ops::Range,
    path::{Path, PathBuf},
    str::FromStr,
};

use geo::ArrayDataType;
use geo::{Coordinate, LatLonBounds, Tile, crs};
use mbtilesdb::MbtilesDb;

use crate::{
    Error, PixelFormat, Result, TileProvider,
    layermetadata::{LayerId, LayerMetadata, LayerSourceType},
    rasterprocessing::raster_pixel,
    tiledata::TileData,
    tileformat::TileFormat,
    tileprovider::{ColorMappedTileRequest, TileRequest, unique_layer_id},
};

pub struct MbtilesTileProvider {
    db_path: PathBuf,
    meta: LayerMetadata,
}

fn parse_bounds(bounds: &str) -> Result<[f64; 4]> {
    if !bounds.is_empty() {
        let bounds_array: Vec<f64> = bounds.split(',').filter_map(|s| s.parse::<f64>().ok()).collect();
        let mut bounds = [0.0; 4];
        if bounds_array.len() == 4 {
            bounds.copy_from_slice(&bounds_array);
            return Ok(bounds);
        }
    }

    Err(Error::Runtime(format!("Invalid mbtiles bounds: {}", bounds)))
}

impl MbtilesTileProvider {
    pub fn new(db_path: &Path) -> Result<Self> {
        let db = mbtilesdb::MbtilesDb::new(db_path)?;
        let mut meta_map = db.get_metadata()?;

        let mut meta = LayerMetadata {
            id: unique_layer_id(),
            path: PathBuf::from(db_path),
            min_zoom: meta_map.remove("minzoom").unwrap_or_default().parse().unwrap_or(0),
            max_zoom: meta_map.remove("maxzoom").unwrap_or_default().parse().unwrap_or(20),
            tile_format: TileFormat::from(meta_map.remove("format").unwrap_or_default().as_str()),
            name: meta_map.remove("name").unwrap_or(meta_map.remove("basename").unwrap_or_default()),
            description: meta_map.remove("description").unwrap_or_default(),
            epsg: Some(crs::epsg::WGS84_WEB_MERCATOR),
            bounds: parse_bounds(meta_map.remove("bounds").unwrap_or_default().as_str())?,
            url: "".to_string(),
            source_is_web_mercator: true,
            supports_dpi_ratio: false,
            nodata: None,
            min_value: meta_map.remove("min_value").and_then(|s| f64::from_str(&s).ok()).unwrap_or(0.0),
            max_value: meta_map.remove("max_value").and_then(|s| f64::from_str(&s).ok()).unwrap_or(0.0),
            data_type: ArrayDataType::Float32,
            source_format: LayerSourceType::Mbtiles,
            scheme: meta_map.remove("scheme").unwrap_or("tms".to_string()),
            additional_data: HashMap::new(),
            band_nr: None,
        };

        meta.additional_data = meta_map;

        log::info!(
            "[TILE] Serving {} [{}] ({})",
            meta.name,
            meta.tile_format,
            db_path.file_name().unwrap_or_default().to_string_lossy()
        );

        Ok(MbtilesTileProvider {
            db_path: PathBuf::from(db_path),
            meta,
        })
    }

    pub fn tile(meta: &LayerMetadata, tile: Tile) -> Result<TileData> {
        let mut db = MbtilesDb::new(&meta.path)?;
        Ok(TileData::new(
            meta.tile_format,
            PixelFormat::Rgba,
            db.get_tile_data(&tile)?.unwrap_or_default(),
        ))
    }

    pub fn value_range_for_extent(_meta: &LayerMetadata, _extent: LatLonBounds, _zoom: Option<i32>) -> Result<std::ops::Range<f64>> {
        Err(Error::Runtime("Extent value range not supported for vector tiles".to_string()))
    }

    pub fn raster_pixel(_meta: &LayerMetadata, _coord: Coordinate) -> Result<Option<f32>> {
        Err(Error::Runtime("Raster pixel not supported for vector tiles".to_string()))
    }
}

impl TileProvider for MbtilesTileProvider {
    fn extent_value_range(&self, _layer_id: LayerId, _extent: LatLonBounds, _zoom: Option<i32>) -> Result<Range<f64>> {
        Err(Error::Runtime("Extent value range not supported for mbtiles".to_string()))
    }

    fn get_raster_value(&self, _layer_id: LayerId, coord: Coordinate, _dpi_ratio: u8) -> Result<Option<f32>> {
        raster_pixel(
            &self.db_path,
            1,
            Coordinate::from(crs::lat_lon_to_web_mercator(coord)),
            Some(self.meta.name.as_str()),
        )
    }

    fn get_tile(&self, _layer_id: LayerId, req: &TileRequest) -> Result<TileData> {
        log::debug!("Get tile {}/{}/{}", req.tile.z(), req.tile.x(), req.tile.y());

        if req.tile_format != TileFormat::Png {
            return Err(Error::Runtime("Only png format is supported for mbtiles".to_string()));
        }

        if req.tile_size != Tile::TILE_SIZE {
            return Err(Error::Runtime("Only 256px tile size is supported for mbtiles".to_string()));
        }

        let mut db = mbtilesdb::MbtilesDb::new(&self.db_path)?;
        Ok(TileData::new(
            self.meta.tile_format,
            PixelFormat::Rgba,
            db.get_tile_data(&req.tile)?.unwrap_or_default(),
        ))
    }

    fn get_tile_color_mapped(&self, layer_id: LayerId, req: &ColorMappedTileRequest) -> Result<TileData> {
        let tile_req = TileRequest {
            tile: req.tile,
            dpi_ratio: req.dpi_ratio,
            tile_size: req.tile_size,
            tile_format: TileFormat::Png,
        };

        self.get_tile(layer_id, &tile_req)
    }

    fn layers(&self) -> Vec<LayerMetadata> {
        vec![self.meta.clone()]
    }

    fn layer(&self, _id: LayerId) -> Result<LayerMetadata> {
        Ok(self.meta.clone())
    }
}

mod mbtilesdb {
    use geo::Tile;
    use std::{collections::HashMap, path::Path};

    use crate::{Error, Result};

    pub struct MbtilesDb {
        conn: sqlite::Connection,
        tile_query: sqlite::Statement,
    }

    impl MbtilesDb {
        pub fn new(db_path: &Path) -> Result<Self> {
            let conn = sqlite::Connection::new(db_path, sqlite::AccessMode::ReadOnly)?;
            let tile_query = conn.prepare_statement(
                "SELECT tile_data
                FROM tiles 
                WHERE zoom_level = ?1 AND tile_column = ?2 AND tile_row = ?3;",
            )?;

            Ok(MbtilesDb { conn, tile_query })
        }

        pub fn get_metadata(&self) -> Result<HashMap<String, String>> {
            let stmt = self.conn.prepare_statement("SELECT * FROM metadata;")?;

            let meta = stmt
                .into_iter()
                .map(|row| {
                    let key = String::from(row.column_string(0).ok_or(Error::Runtime("Metadata key error".to_string()))?);
                    let value = String::from(row.column_string(1).ok_or(Error::Runtime("Metadata value error".to_string()))?);
                    Ok((key, value))
                })
                .filter_map(Result::ok)
                .collect();

            Ok(meta)
        }

        pub fn get_tile_data(&mut self, tile: &Tile) -> Result<Option<Vec<u8>>> {
            self.tile_query.reset()?;

            self.tile_query.bind(1, tile.z())?;
            self.tile_query.bind(2, tile.x())?;
            self.tile_query.bind(3, tile.y())?;

            if let Some(row) = self.tile_query.next() {
                let blob = row.column_blob(0).ok_or(Error::Runtime("Tile blob error".to_string()))?;
                return Ok(Some(Vec::from(blob)));
            }

            // Tile not in database
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::{layermetadata::LayerId, tileprovider::TileProvider};

    fn test_tiles() -> std::path::PathBuf {
        [env!("CARGO_MANIFEST_DIR"), "test", "data", "gem_limburg.mbtiles"].iter().collect()
    }

    #[test]
    fn test_mbtiles_tile_provider() {
        let provider = MbtilesTileProvider::new(test_tiles().as_path()).unwrap();
        assert_eq!(provider.layers().len(), 1);

        let layer = provider.layer(LayerId::from(1)).unwrap();
        assert_eq!(layer.name, "gem_limburg");
        assert_eq!(layer.min_zoom, 8);
        assert_eq!(layer.max_zoom, 11);
        assert_eq!(layer.tile_format, TileFormat::Png);
        assert_eq!(
            layer.bounds,
            [
                4.793_288_340_591_671,
                50.677_197_732_274_95,
                5.948_226_084_732_296,
                51.378_950_006_005_1
            ]
        );
    }
}

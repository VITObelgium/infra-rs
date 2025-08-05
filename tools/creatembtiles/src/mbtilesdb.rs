use std::path::Path;

use geo::Tile;

pub type Result<T> = tiler::Result<T>;

pub struct MbtilesDb {
    conn: sqlite::Connection,
    tile_query: sqlite::Statement,
}

impl MbtilesDb {
    pub fn new(db_path: &Path) -> Result<Self> {
        let conn = sqlite::Connection::new(db_path, sqlite::AccessMode::Create)?;
        conn.execute_sql_statements(include_str!("mbtiles-schema.sql"))?;

        let tile_query = conn.prepare_statement("INSERT INTO tiles values (?1, ?2, ?3, ?4)")?;

        Ok(MbtilesDb { conn, tile_query })
    }

    pub fn start_transaction(&mut self) -> Result<()> {
        self.conn.execute("BEGIN")?;
        Ok(())
    }

    pub fn commit_transaction(&mut self) -> Result<()> {
        self.conn.execute("COMMIT")?;
        Ok(())
    }

    pub fn insert_metadata(&mut self, metadata: &[(String, String)]) -> Result<()> {
        let query = self.conn.prepare_statement("INSERT INTO metadata values (?1, ?2)")?;
        for (key, value) in metadata {
            query.bind_text(1, key)?;
            query.bind_text(2, value)?;
            query.step();
            query.reset()?;
        }

        Ok(())
    }

    pub fn insert_tile_data(&mut self, tile: &Tile, tile_data: Vec<u8>) -> Result<()> {
        self.tile_query.reset()?;

        self.tile_query.bind(1, tile.z())?;
        self.tile_query.bind(2, tile.x())?;
        self.tile_query.bind(3, tile.y())?;
        self.tile_query.bind_blob(4, &tile_data)?;

        self.tile_query.step();

        Ok(())
    }
}

// #[cfg(test)]
// mod tests {

//     use super::*;
//     use crate::{layermetadata::LayerId, tileprovider::TileProvider};

//     fn test_tiles() -> std::path::PathBuf {
//         [env!("CARGO_MANIFEST_DIR"), "test", "data", "gem_limburg.mbtiles"]
//             .iter()
//             .collect()
//     }

//     #[test]
//     fn test_mbtiles_tile_provider() {
//         let provider = MbtilesTileProvider::new(test_tiles().as_path()).unwrap();
//         assert_eq!(provider.layers().len(), 1);

//         let layer = provider.layer(LayerId::from(1)).unwrap();
//         assert_eq!(layer.name, "gem_limburg");
//         assert_eq!(layer.min_zoom, 8);
//         assert_eq!(layer.max_zoom, 11);
//         assert_eq!(layer.tile_format, TileFormat::Png);
//         assert_eq!(
//             layer.bounds,
//             [
//                 4.793_288_340_591_671,
//                 50.677_197_732_274_95,
//                 5.948_226_084_732_296,
//                 51.378_950_006_005_1
//             ]
//         );
//     }
// }

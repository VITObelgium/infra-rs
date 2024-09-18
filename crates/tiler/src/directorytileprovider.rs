use geo::raster::io::RasterFormat;
use geo::{Coordinate, LatLonBounds};

use crate::layermetadata::{LayerId, LayerMetadata, LayerSourceType};
use crate::mbtilestileprovider::MbtilesTileProvider;
use crate::tiledata::TileData;
use crate::tileprovider::{ColorMappedTileRequest, TileRequest};
use crate::tileproviderfactory::{create_single_file_tile_provider, TileProviderOptions};
use crate::warpingtileprovider::WarpingTileProvider;
use crate::{Error, Result, TileProvider};
use std::collections::HashMap;
use std::ops::Range;

/// Tile provider for a directory
/// The provider will scan the directory (non-recursively) for supported raster files and provide them as layers
#[derive(Clone)]
pub struct DirectoryTileProvider {
    layers: HashMap<LayerId, LayerMetadata>,
}

impl DirectoryTileProvider {
    pub fn new(input_path: &std::path::Path) -> Result<Self> {
        Ok(DirectoryTileProvider {
            layers: DirectoryTileProvider::build_metadata_list(input_path)?,
        })
    }

    fn layer_data(&self, id: LayerId) -> Result<&LayerMetadata> {
        self.layers
            .get(&id)
            .ok_or(Error::InvalidArgument(format!("Invalid layer id: {}", id)))
    }

    fn build_metadata_list(input_dir: &std::path::Path) -> Result<HashMap<LayerId, LayerMetadata>> {
        let mut layers = HashMap::new();

        for file_entry in std::fs::read_dir(input_dir)?.flatten() {
            if !file_entry.file_type()?.is_file()
                || RasterFormat::guess_from_path(&file_entry.path()) == RasterFormat::Unknown
            {
                continue;
            }

            match create_single_file_tile_provider(&file_entry.path(), TileProviderOptions { calculate_stats: true }) {
                Ok(provider) => {
                    let file_layers = provider.layers();
                    let layer_count = file_layers.len();
                    if layer_count == 0 {
                        log::warn!("No layer found in file: {}", file_entry.path().to_string_lossy());
                    } else {
                        for layer in file_layers.into_iter() {
                            layers.insert(layer.id, layer);
                        }

                        log::info!(
                            "Serving {}, layer count: {}",
                            &file_entry.path().to_string_lossy(),
                            layer_count
                        );
                    }
                }
                Err(e) => {
                    log::warn!("Error serving {}: {}", &file_entry.path().to_string_lossy(), e);
                }
            }
        }

        Ok(layers)
    }
}

impl TileProvider for DirectoryTileProvider {
    fn layers(&self) -> Vec<LayerMetadata> {
        self.layers.values().cloned().collect()
    }

    fn layer(&self, id: LayerId) -> Result<LayerMetadata> {
        Ok(self.layer_data(id)?.clone())
    }

    fn extent_value_range(&self, id: LayerId, extent: LatLonBounds, zoom: Option<i32>) -> Result<Range<f64>> {
        let layer = self.layer_data(id)?;
        match layer.source_format {
            LayerSourceType::GeoTiff
            | LayerSourceType::GeoPackage
            | LayerSourceType::ArcAscii
            | LayerSourceType::Netcdf => WarpingTileProvider::value_range_for_extent(layer, extent, zoom),
            LayerSourceType::Mbtiles => MbtilesTileProvider::value_range_for_extent(layer, extent, zoom),
            LayerSourceType::Unknown => Err(Error::Runtime("Unsupported source format".to_string())),
        }
    }

    fn get_raster_value(&self, id: LayerId, coord: Coordinate) -> Result<Option<f32>> {
        let layer = self.layer_data(id)?;
        match layer.source_format {
            LayerSourceType::GeoTiff
            | LayerSourceType::GeoPackage
            | LayerSourceType::ArcAscii
            | LayerSourceType::Netcdf => WarpingTileProvider::raster_pixel(layer, coord),
            LayerSourceType::Mbtiles => MbtilesTileProvider::raster_pixel(layer, coord),
            LayerSourceType::Unknown => Err(Error::Runtime("Unsupported source format".to_string())),
        }
    }

    fn get_tile(&self, id: LayerId, tile_req: &TileRequest) -> Result<TileData> {
        let layer = self.layer_data(id)?;
        match layer.source_format {
            LayerSourceType::GeoTiff
            | LayerSourceType::GeoPackage
            | LayerSourceType::ArcAscii
            | LayerSourceType::Netcdf => WarpingTileProvider::tile(layer, tile_req),
            LayerSourceType::Mbtiles => MbtilesTileProvider::tile(layer, tile_req.tile),
            LayerSourceType::Unknown => Err(Error::Runtime("Unsupported source format".to_string())),
        }
    }

    fn get_tile_color_mapped(&self, id: LayerId, tile_req: &ColorMappedTileRequest) -> Result<TileData> {
        let layer = self.layer_data(id)?;
        match layer.source_format {
            LayerSourceType::GeoTiff
            | LayerSourceType::GeoPackage
            | LayerSourceType::ArcAscii
            | LayerSourceType::Netcdf => WarpingTileProvider::color_mapped_tile(layer, tile_req),
            LayerSourceType::Mbtiles => MbtilesTileProvider::tile(layer, tile_req.tile),
            LayerSourceType::Unknown => Err(Error::Runtime("Unsupported source format".to_string())),
        }
    }
}

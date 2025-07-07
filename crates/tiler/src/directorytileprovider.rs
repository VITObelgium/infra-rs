use geo::DenseArray;
use geo::raster::io::RasterFormat;
use geo::{Coordinate, LatLonBounds};
use raster_tile::RasterTileCastIO;

use crate::cogtileprovider::CogTileProvider;
use crate::layermetadata::{LayerId, LayerMetadata, LayerSourceType};
use crate::mbtilestileprovider::MbtilesTileProvider;
use crate::tiledata::TileData;
use crate::tileprovider::{ColorMappedTileRequest, TileRequest};
use crate::tileproviderfactory::{TileProviderOptions, create_single_file_tile_provider};
use crate::warpingtileprovider::WarpingTileProvider;
use crate::{Error, Result, TileProvider, tilediff};
use std::collections::HashMap;
use std::ops::Range;

/// Tile provider for a directory
/// The provider will scan the directory (non-recursively) for supported raster files and provide them as layers
#[derive(Clone)]
pub struct DirectoryTileProvider {
    layers: HashMap<LayerId, LayerMetadata>,
}

impl DirectoryTileProvider {
    pub fn new(input_path: &std::path::Path, opts: TileProviderOptions) -> Result<Self> {
        Ok(DirectoryTileProvider {
            layers: DirectoryTileProvider::build_metadata_list(input_path, &opts)?,
        })
    }

    fn layer_data(&self, id: LayerId) -> Result<&LayerMetadata> {
        self.layers
            .get(&id)
            .ok_or(Error::InvalidArgument(format!("Invalid layer id: {id}")))
    }

    fn build_metadata_list(input_dir: &std::path::Path, opts: &TileProviderOptions) -> Result<HashMap<LayerId, LayerMetadata>> {
        let mut layers = HashMap::new();

        for file_entry in std::fs::read_dir(input_dir)?.flatten() {
            if !file_entry.file_type()?.is_file() || RasterFormat::guess_from_path(&file_entry.path()) == RasterFormat::Unknown {
                continue;
            }

            match create_single_file_tile_provider(&file_entry.path(), opts) {
                Ok(provider) => {
                    let file_layers = provider.layers();
                    let layer_count = file_layers.len();
                    if layer_count == 0 {
                        log::warn!("No layer found in file: {}", file_entry.path().to_string_lossy());
                    } else {
                        for layer in file_layers {
                            layers.insert(layer.id, layer);
                        }

                        log::info!("Serving {}, layer count: {}", &file_entry.path().to_string_lossy(), layer_count);
                    }
                }
                Err(e) => {
                    log::warn!("Error serving {}: {}", &file_entry.path().to_string_lossy(), e);
                }
            }
        }

        Ok(layers)
    }

    pub fn extent_value_range_for_layer(layer: &LayerMetadata, extent: LatLonBounds, zoom: Option<i32>) -> Result<Range<f64>> {
        match layer.source_format {
            LayerSourceType::CloudOptimizedGeoTiff
            | LayerSourceType::GeoTiff
            | LayerSourceType::GeoPackage
            | LayerSourceType::ArcAscii
            | LayerSourceType::Netcdf => WarpingTileProvider::value_range_for_extent(layer, extent, zoom),
            LayerSourceType::Mbtiles => MbtilesTileProvider::value_range_for_extent(layer, extent, zoom),
            LayerSourceType::Unknown => Err(Error::Runtime("Unsupported source format".to_string())),
        }
    }

    pub fn get_raster_value_for_layer(layer: &LayerMetadata, coord: Coordinate, dpi_ratio: u8) -> Result<Option<f32>> {
        match layer.source_format {
            LayerSourceType::CloudOptimizedGeoTiff
            | LayerSourceType::GeoTiff
            | LayerSourceType::GeoPackage
            | LayerSourceType::ArcAscii
            | LayerSourceType::Netcdf => WarpingTileProvider::raster_pixel(layer, coord, dpi_ratio),
            LayerSourceType::Mbtiles => MbtilesTileProvider::raster_pixel(layer, coord),
            LayerSourceType::Unknown => Err(Error::Runtime("Unsupported source format".to_string())),
        }
    }

    pub fn get_tile_for_layer(layer: &LayerMetadata, tile_req: &TileRequest) -> Result<TileData> {
        match layer.source_format {
            LayerSourceType::CloudOptimizedGeoTiff => CogTileProvider::tile(layer, tile_req),
            LayerSourceType::GeoTiff | LayerSourceType::GeoPackage | LayerSourceType::ArcAscii | LayerSourceType::Netcdf => {
                WarpingTileProvider::tile(layer, tile_req)
            }
            LayerSourceType::Mbtiles => MbtilesTileProvider::tile(layer, tile_req.tile),
            LayerSourceType::Unknown => Err(Error::Runtime("Unsupported source format".to_string())),
        }
    }

    pub fn get_tile_color_mapped_for_layer(layer: &LayerMetadata, tile_req: &ColorMappedTileRequest) -> Result<TileData> {
        match layer.source_format {
            LayerSourceType::CloudOptimizedGeoTiff => CogTileProvider::tile_color_mapped(layer, tile_req),
            LayerSourceType::GeoTiff | LayerSourceType::GeoPackage | LayerSourceType::ArcAscii | LayerSourceType::Netcdf => {
                WarpingTileProvider::color_mapped_tile(layer, tile_req)
            }
            LayerSourceType::Mbtiles => MbtilesTileProvider::tile(layer, tile_req.tile),
            LayerSourceType::Unknown => Err(Error::Runtime("Unsupported source format".to_string())),
        }
    }

    pub fn diff_tile(layer1: &LayerMetadata, layer2: &LayerMetadata, tile_req: &TileRequest) -> Result<TileData> {
        let tile1 = DenseArray::<f32>::from_raster_tile_bytes_with_cast(&Self::get_tile_for_layer(layer1, tile_req)?.data)?;
        let tile2 = DenseArray::<f32>::from_raster_tile_bytes_with_cast(&Self::get_tile_for_layer(layer2, tile_req)?.data)?;

        tilediff::diff_tiles(&tile1, &tile2, layer1.tile_format)
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
        Self::extent_value_range_for_layer(self.layer_data(id)?, extent, zoom)
    }

    fn get_raster_value(&self, id: LayerId, coord: Coordinate, dpi_ratio: u8) -> Result<Option<f32>> {
        Self::get_raster_value_for_layer(self.layer_data(id)?, coord, dpi_ratio)
    }

    fn get_tile(&self, id: LayerId, tile_req: &TileRequest) -> Result<TileData> {
        Self::get_tile_for_layer(self.layer_data(id)?, tile_req)
    }

    fn get_tile_color_mapped(&self, id: LayerId, tile_req: &ColorMappedTileRequest) -> Result<TileData> {
        Self::get_tile_color_mapped_for_layer(self.layer_data(id)?, tile_req)
    }
}

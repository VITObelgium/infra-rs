use geo::{Coordinate, LatLonBounds};

use crate::layermetadata::{LayerId, LayerMetadata, LayerSourceType};
use crate::mbtilestileprovider::MbtilesTileProvider;
use crate::tiledata::TileData;
use crate::tileprovider::{ColorMappedTileRequest, TileRequest};
use crate::tileproviderfactory::{create_single_file_tile_provider, TileProviderOptions};
use crate::warpingtileprovider::WarpingTileProvider;
use crate::{DirectoryTileProvider, Error, Result, TileProvider};
use std::collections::HashMap;
use std::ops::Range;

/// Tile provider that can be configured dynamically with raster files
/// Starts empty and can be populated with raster files at runtime
#[derive(Clone)]
pub struct DynamicTileProvider {
    layers: HashMap<LayerId, LayerMetadata>,
    opts: TileProviderOptions,
}

impl DynamicTileProvider {
    pub fn new(opts: TileProviderOptions) -> Self {
        DynamicTileProvider {
            layers: HashMap::default(),
            opts,
        }
    }

    fn layer_data(&self, id: LayerId) -> Result<&LayerMetadata> {
        self.layers
            .get(&id)
            .ok_or(Error::InvalidArgument(format!("Invalid layer id: {}", id)))
    }

    pub fn reset(&mut self) {
        self.layers.clear();
    }

    pub fn add_dir(&mut self, input: &std::path::Path) -> Result<Vec<LayerMetadata>> {
        let dir_provider = DirectoryTileProvider::new(input, self.opts.clone())?;
        let dir_layers = dir_provider.layers();
        dir_provider.layers().iter().for_each(|layer| {
            self.layers.insert(layer.id, layer.clone());
        });

        Ok(dir_layers)
    }

    pub fn add_file(&mut self, input: &std::path::Path) -> Result<Vec<LayerMetadata>> {
        if !input.exists() {
            return Err(Error::InvalidArgument(format!("File not found: {}", input.display())));
        }

        if !input.is_file() {
            return Err(Error::InvalidArgument(format!(
                "Raster path is not a file: {}",
                input.display()
            )));
        }

        let provider = create_single_file_tile_provider(input, &self.opts)?;
        let layers = provider.layers();
        provider.layers().iter().for_each(|layer| {
            self.layers.insert(layer.id, layer.clone());
        });

        Ok(layers)
    }

    pub fn extent_value_range_for_layer(
        layer: &LayerMetadata,
        extent: LatLonBounds,
        zoom: Option<i32>,
    ) -> Result<Range<f64>> {
        match layer.source_format {
            LayerSourceType::GeoTiff
            | LayerSourceType::GeoPackage
            | LayerSourceType::ArcAscii
            | LayerSourceType::Netcdf => WarpingTileProvider::value_range_for_extent(layer, extent, zoom),
            LayerSourceType::Mbtiles => MbtilesTileProvider::value_range_for_extent(layer, extent, zoom),
            LayerSourceType::Unknown => Err(Error::Runtime("Unsupported source format".to_string())),
        }
    }

    pub fn get_raster_value_for_layer(layer: &LayerMetadata, coord: Coordinate, dpi_ratio: u8) -> Result<Option<f32>> {
        match layer.source_format {
            LayerSourceType::GeoTiff
            | LayerSourceType::GeoPackage
            | LayerSourceType::ArcAscii
            | LayerSourceType::Netcdf => WarpingTileProvider::raster_pixel(layer, coord, dpi_ratio),
            LayerSourceType::Mbtiles => MbtilesTileProvider::raster_pixel(layer, coord),
            LayerSourceType::Unknown => Err(Error::Runtime("Unsupported source format".to_string())),
        }
    }

    pub fn get_tile_for_layer(layer: &LayerMetadata, tile_req: &TileRequest) -> Result<TileData> {
        match layer.source_format {
            LayerSourceType::GeoTiff
            | LayerSourceType::GeoPackage
            | LayerSourceType::ArcAscii
            | LayerSourceType::Netcdf => WarpingTileProvider::tile(layer, tile_req),
            LayerSourceType::Mbtiles => MbtilesTileProvider::tile(layer, tile_req.tile),
            LayerSourceType::Unknown => Err(Error::Runtime("Unsupported source format".to_string())),
        }
    }

    pub fn get_tile_color_mapped_for_layer(
        layer: &LayerMetadata,
        tile_req: &ColorMappedTileRequest,
    ) -> Result<TileData> {
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

impl Default for DynamicTileProvider {
    fn default() -> Self {
        Self::new(TileProviderOptions::default())
    }
}

impl TileProvider for DynamicTileProvider {
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

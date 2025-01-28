use std::{ops::Range, path::Path, sync::Arc};

use gdal::raster::GdalType;
use geo::{georaster::io::RasterFormat, Coordinate, LatLonBounds, Tile};
use raster::{Cell, RasterDataType, RasterNum};

use crate::{
    layermetadata::{LayerId, LayerMetadata},
    tiledata::TileData,
    tileformat::TileFormat,
    tileio,
    tileprovider::{self, ColorMappedTileRequest, TileProvider, TileRequest},
    tileproviderfactory::TileProviderOptions,
    Error, PixelFormat, Result,
};

#[derive(Clone)]
struct DiffLayerData {
    layer1: LayerMetadata,
    layer2: LayerMetadata,
}

#[derive(Default)]
pub struct DiffTileProvider {
    raster_layer1: Option<LayerMetadata>,
    raster_layer2: Option<LayerMetadata>,
    diff_layer: Option<LayerMetadata>,
}

fn diff_tiles<T: RasterNum<T> + GdalType>(
    layer_meta1: &LayerMetadata,
    layer_meta2: &LayerMetadata,
    tile: Tile,
) -> Result<TileData> {
    let (tile1_data, tile1_nodata) = tileio::read_tile_data::<T>(layer_meta1, 1, tile, 1)?;
    let (tile2_data, tile2_nodata) = tileio::read_tile_data::<T>(layer_meta2, 1, tile, 1)?;

    if tile1_data.len() != tile2_data.len() {
        return Err(Error::InvalidArgument("Tile data length mismatch".to_string()));
    }

    if tile1_data.is_empty() {
        return Ok(TileData::default());
    }

    let mut tile = mvt::Tile::new(Tile::TILE_SIZE as u32);

    for row in 0..Tile::TILE_SIZE {
        for col in 0..Tile::TILE_SIZE {
            let idx = (row * Tile::TILE_SIZE + col) as usize;
            let lhs = *unsafe { tile1_data.get_unchecked(idx) };
            let rhs = *unsafe { tile2_data.get_unchecked(idx) };

            match (lhs.is_nodata(), rhs.is_nodata()) {
                (true, true) => continue,
                (true, false) => continue,
                (false, true) => continue,
                (false, false) => {
                    let cell = Cell::from_row_col(row as i32, col as i32);
                    let r = cell.row as f32;
                    let c = cell.col as f32;

                    let cell_geom = mvt::GeomEncoder::new(mvt::GeomType::Polygon)
                        .point(c, r)?
                        .point(c + 1.0, r)?
                        .point(c + 1.0, r + 1.0)?
                        .point(c, r + 1.0)?
                        .point(c, r)?
                        .encode()?;

                    let lhs = lhs.to_f64().unwrap();
                    let rhs = rhs.to_f64().unwrap();

                    let diff = (lhs - rhs).abs();

                    let layer = tile.create_layer(&idx.to_string());
                    let mut feature = layer.into_feature(cell_geom);
                    feature.set_id(idx as u64);
                    feature.add_tag_double("diff", diff);
                    feature.add_tag_double("v1", lhs);
                    feature.add_tag_double("v2", rhs);
                    tile.add_layer(feature.into_layer())?;
                }
            }
        }
    }

    Ok(TileData::new(
        TileFormat::Protobuf,
        PixelFormat::Unknown,
        tile.to_bytes()?,
    ))
}

impl DiffTileProvider {
    pub fn new(path1: &std::path::Path, path2: &std::path::Path, opts: &TileProviderOptions) -> Result<Self> {
        let mut provider = DiffTileProvider::default();
        provider.set_files(path1, path2, opts)?;
        Ok(provider)
    }

    pub fn set_raster_1(&mut self, path: &Path, opts: &TileProviderOptions) -> Result<LayerMetadata> {
        let meta = tileio::create_metadata_for_file(path, opts, TileFormat::Png)?;

        if meta.is_empty() {
            return Err(Error::InvalidArgument("No layers found for diffing".to_string()));
        }

        self.raster_layer1 = Some(meta[0].clone());
        self.create_diff_layer_if_needed()?;

        Ok(meta[0].clone())
    }

    pub fn set_raster_2(&mut self, path: &Path, opts: &TileProviderOptions) -> Result<LayerMetadata> {
        let meta = tileio::create_metadata_for_file(path, opts, TileFormat::Png)?;

        if meta.is_empty() {
            return Err(Error::InvalidArgument("No layers found for diffing".to_string()));
        }

        self.raster_layer2 = Some(meta[0].clone());
        self.create_diff_layer_if_needed()?;

        Ok(meta[0].clone())
    }

    pub fn diff_layer(&self) -> Option<LayerMetadata> {
        self.diff_layer.clone()
    }

    pub fn set_files(
        &mut self,
        path1: &Path,
        path2: &Path,
        opts: &TileProviderOptions,
    ) -> Result<(LayerMetadata, LayerMetadata)> {
        Ok((self.set_raster_1(path1, opts)?, self.set_raster_2(path2, opts)?))
    }

    fn create_diff_layer_if_needed(&mut self) -> Result<()> {
        if let (Some(l1), Some(l2)) = (&self.raster_layer1, &self.raster_layer2) {
            let mut diff_layer = l1.clone();
            diff_layer.id = tileprovider::unique_layer_id();
            diff_layer.name = format!("{} - {} diff", l1.name, l2.name);
            diff_layer.tile_format = TileFormat::Protobuf;

            diff_layer.provider_data = Some(Arc::new(Box::new(DiffLayerData {
                layer1: l1.clone(),
                layer2: l2.clone(),
            })));

            self.diff_layer = Some(diff_layer);
        }

        Ok(())
    }

    pub fn supports_raster_type(raster_type: RasterFormat) -> bool {
        matches!(
            raster_type,
            RasterFormat::GeoTiff | RasterFormat::Vrt | RasterFormat::Netcdf
        )
    }

    fn extract_provider_data(layer_meta: &LayerMetadata) -> Result<DiffLayerData> {
        let prov_data = layer_meta
            .provider_data
            .as_ref()
            .ok_or_else(|| Error::InvalidArgument("Missing provider data".into()))?
            .downcast_ref::<DiffLayerData>()
            .ok_or_else(|| Error::InvalidArgument("Invalid provider data".into()))?;
        Ok((*prov_data).clone())
    }

    pub fn tile(layer_meta: &LayerMetadata, tile_req: &TileRequest) -> Result<TileData> {
        let tile = &tile_req.tile;
        if tile.z() < layer_meta.min_zoom || tile.z() > layer_meta.max_zoom {
            return Ok(TileData::default());
        }

        let tile_data = match layer_meta.tile_format {
            TileFormat::Png => {
                let band_nr = layer_meta.band_nr.unwrap_or(1);
                match layer_meta.data_type {
                    RasterDataType::Int8 => tileio::read_tile_as_png::<i8>(layer_meta, band_nr, tile_req)?,
                    RasterDataType::Int16 => tileio::read_tile_as_png::<i16>(layer_meta, band_nr, tile_req)?,
                    RasterDataType::Int32 => tileio::read_tile_as_png::<i32>(layer_meta, band_nr, tile_req)?,
                    RasterDataType::Int64 => tileio::read_tile_as_png::<i64>(layer_meta, band_nr, tile_req)?,
                    RasterDataType::Uint8 => tileio::read_tile_as_png::<u8>(layer_meta, band_nr, tile_req)?,
                    RasterDataType::Uint16 => tileio::read_tile_as_png::<u16>(layer_meta, band_nr, tile_req)?,
                    RasterDataType::Uint32 => tileio::read_tile_as_png::<u32>(layer_meta, band_nr, tile_req)?,
                    RasterDataType::Uint64 => tileio::read_tile_as_png::<u64>(layer_meta, band_nr, tile_req)?,
                    RasterDataType::Float32 => tileio::read_tile_as_png::<f32>(layer_meta, band_nr, tile_req)?,
                    RasterDataType::Float64 => tileio::read_tile_as_png::<f64>(layer_meta, band_nr, tile_req)?,
                }
            }
            TileFormat::Protobuf => {
                let layer_data = Self::extract_provider_data(layer_meta)?;
                match layer_meta.data_type {
                    RasterDataType::Int8 => diff_tiles::<i8>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    RasterDataType::Int16 => diff_tiles::<i16>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    RasterDataType::Int32 => diff_tiles::<i32>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    RasterDataType::Int64 => diff_tiles::<i64>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    RasterDataType::Uint8 => diff_tiles::<u8>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    RasterDataType::Uint16 => diff_tiles::<u16>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    RasterDataType::Uint32 => diff_tiles::<u32>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    RasterDataType::Uint64 => diff_tiles::<u64>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    RasterDataType::Float32 => {
                        diff_tiles::<f32>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?
                    }
                    RasterDataType::Float64 => {
                        diff_tiles::<f64>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?
                    }
                }
            }
            _ => return Err(Error::Runtime("Unsupported tile format".into())),
        };

        Ok(tile_data)
    }

    pub fn color_mapped_tile(layer_meta: &LayerMetadata, tile_req: &ColorMappedTileRequest) -> Result<TileData> {
        let tile = &tile_req.tile;
        if tile.z() < layer_meta.min_zoom || tile.z() > layer_meta.max_zoom {
            return Ok(TileData::default());
        }

        let tile_data = match layer_meta.tile_format {
            TileFormat::Png => {
                let band = layer_meta.band_nr.unwrap_or(1);
                match layer_meta.data_type {
                    RasterDataType::Uint8 => tileio::read_color_mapped_tile_as_png::<u8>(layer_meta, band, tile_req)?,
                    RasterDataType::Uint16 => tileio::read_color_mapped_tile_as_png::<u16>(layer_meta, band, tile_req)?,
                    RasterDataType::Uint32 => tileio::read_color_mapped_tile_as_png::<u32>(layer_meta, band, tile_req)?,
                    RasterDataType::Uint64 => tileio::read_color_mapped_tile_as_png::<u64>(layer_meta, band, tile_req)?,
                    RasterDataType::Int8 => tileio::read_color_mapped_tile_as_png::<i8>(layer_meta, band, tile_req)?,
                    RasterDataType::Int16 => tileio::read_color_mapped_tile_as_png::<i16>(layer_meta, band, tile_req)?,
                    RasterDataType::Int32 => tileio::read_color_mapped_tile_as_png::<i32>(layer_meta, band, tile_req)?,
                    RasterDataType::Int64 => tileio::read_color_mapped_tile_as_png::<i64>(layer_meta, band, tile_req)?,
                    RasterDataType::Float32 => {
                        tileio::read_color_mapped_tile_as_png::<f32>(layer_meta, band, tile_req)?
                    }
                    RasterDataType::Float64 => {
                        tileio::read_color_mapped_tile_as_png::<f64>(layer_meta, band, tile_req)?
                    }
                }
            }
            TileFormat::Protobuf => {
                return Err(Error::InvalidArgument("Diff tiles cannot be colormapped".into()));
            }
            _ => return Err(Error::Runtime("Unsupported tile format".into())),
        };

        Ok(tile_data)
    }

    pub fn raster_pixel(_layer_meta: &LayerMetadata, _coord: Coordinate) -> Result<Option<f32>> {
        Err(Error::Runtime("Raster pixels not supported for the diff tiler".into()))
    }

    pub fn value_range_for_extent(
        _layer_meta: &LayerMetadata,
        _extent: LatLonBounds,
        _zoom: Option<i32>,
    ) -> Result<Range<f64>> {
        Err(Error::Runtime("Value ranges not supported for the diff tiler".into()))
    }

    fn layer_ref(&self, id: LayerId) -> Result<&LayerMetadata> {
        if let Some(l) = self.raster_layer1.as_ref() {
            if l.id == id {
                return Ok(l);
            }
        }

        if let Some(l) = self.raster_layer2.as_ref() {
            if l.id == id {
                return Ok(l);
            }
        }

        if let Some(l) = self.diff_layer.as_ref() {
            if l.id == id {
                return Ok(l);
            }
        }

        Err(Error::InvalidArgument(format!("Invalid layer id: {}", id)))
    }
}

impl TileProvider for DiffTileProvider {
    fn layers(&self) -> Vec<LayerMetadata> {
        let mut layers = Vec::with_capacity(3);
        if let Some(l) = self.raster_layer1.as_ref() {
            layers.push(l.clone());
        }

        if let Some(l) = self.raster_layer2.as_ref() {
            layers.push(l.clone());
        }

        if let Some(l) = self.diff_layer.as_ref() {
            layers.push(l.clone());
        }

        layers
    }

    fn layer(&self, id: LayerId) -> Result<LayerMetadata> {
        Ok(self.layer_ref(id)?.clone())
    }

    fn extent_value_range(&self, id: LayerId, extent: LatLonBounds, zoom: Option<i32>) -> Result<std::ops::Range<f64>> {
        let layer_meta = self.layer_ref(id)?;
        DiffTileProvider::value_range_for_extent(layer_meta, extent, zoom)
    }

    fn get_raster_value(&self, id: LayerId, coord: Coordinate, dpi_ratio: u8) -> Result<Option<f32>> {
        let layer_meta = self.layer_ref(id)?;
        DiffTileProvider::raster_pixel(layer_meta, coord)
    }

    fn get_tile(&self, id: LayerId, tile_req: &TileRequest) -> Result<TileData> {
        let layer_meta = self.layer_ref(id)?;
        DiffTileProvider::tile(layer_meta, tile_req)
    }

    fn get_tile_color_mapped(&self, id: LayerId, tile_req: &ColorMappedTileRequest) -> Result<TileData> {
        let layer_meta = self.layer_ref(id)?;
        DiffTileProvider::color_mapped_tile(layer_meta, tile_req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::RuntimeConfiguration;
    use path_macro::path;

    use crate::{tileproviderfactory::TileProviderOptions, LayerSourceType};

    use super::DiffTileProvider;

    #[ctor::ctor]
    fn init() {
        let mut data_dir = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "target" / "data");
        if !data_dir.exists() {
            // Infra used as a subcrate, try the parent directory
            data_dir = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / ".." / "target" / "data");
            if !data_dir.exists() {
                panic!("Proj.db data directory not found");
            }
        }

        let config = RuntimeConfiguration::builder().proj_db(&data_dir).build();
        config.apply().expect("Failed to configure runtime");
    }

    #[test]
    fn test_diff_tile_provider() {
        let path1 = path!(env!("CARGO_MANIFEST_DIR") / "test" / "data" / "potgeo_lim_bebouwd.tif");
        let path2 = path!(env!("CARGO_MANIFEST_DIR") / "test" / "data" / "residentieel_dakopp_50m_lim.tif");
        assert!(path1.exists());
        assert!(path2.exists());

        let provider = DiffTileProvider::new(&path1, &path2, &TileProviderOptions::default()).unwrap();
        let meta = provider.diff_layer().unwrap();

        assert!(meta.source_format == LayerSourceType::GeoTiff);
        assert!(meta.tile_format == TileFormat::Protobuf);

        let req = TileRequest {
            tile: Tile::for_coordinate(meta.bounds().center(), 10),
            dpi_ratio: 1,
            tile_format: TileFormat::Protobuf,
        };

        dbg!("Tile: {:?}", req.tile);

        let mvt = provider.get_tile(meta.id, &req).unwrap();

        // write mvt to file for debugging
        std::fs::write("/Users/dirk/tile.mvt", mvt.data).unwrap();
    }
}

use std::{ops::Range, path::Path, sync::Arc};

use gdal::{raster::GdalType, vector::LayerAccess};
use geo::{
    georaster::{self, io::RasterFormat},
    CellSize, Coordinate, GeoReference, LatLonBounds, Point, Tile,
};

use raster::{Raster, RasterDataType, RasterNum};

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

#[cfg(feature = "vector-tiles")]
fn diff_tiles_as_vector<T: RasterNum<T> + GdalType>(
    layer_meta1: &LayerMetadata,
    layer_meta2: &LayerMetadata,
    tile: Tile,
) -> Result<TileData> {
    let tile1 = tileio::read_tile_data::<T>(layer_meta1, 1, tile, 1)?;
    let tile2 = tileio::read_tile_data::<T>(layer_meta2, 1, tile, 1)?;

    if tile1.len() != tile2.len() {
        return Err(Error::InvalidArgument("Tile data length mismatch".to_string()));
    }

    if tile1.is_empty() {
        return Ok(TileData::default());
    }

    let diff = tile2 - tile1;

    let geo_ref = GeoReference::with_origin(
        "",
        diff.size(),
        Point::new(0.0, Tile::TILE_SIZE as f64),
        CellSize::square(1.0),
        Option::<f64>::None,
    );

    let vec_ds = georaster::algo::polygonize(&geo_ref, diff.as_ref())?;

    let mut tile = mvt::Tile::new(Tile::TILE_SIZE as u32);

    let mut idx = 0;
    for feature in vec_ds.layer(0)?.features() {
        if let Some(geom) = feature.geometry() {
            if let Ok(geo_types::Geometry::Polygon(geom)) = geom.to_geo() {
                let mut cell_geom = mvt::GeomEncoder::new(mvt::GeomType::Polygon);
                for point in geom.exterior().points() {
                    cell_geom.add_point(point.x(), point.y())?;
                }

                let layer = tile.create_layer(&idx.to_string());
                let mut mvt_feat = layer.into_feature(cell_geom.encode()?);
                mvt_feat.add_tag_double(
                    "diff",
                    feature.field_as_double_by_name("Value")?.expect("Value not found"),
                );
                tile.add_layer(mvt_feat.into_layer())?;
                idx += 1;
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

        use RasterDataType::*;
        let tile_data = match layer_meta.tile_format {
            TileFormat::Png => {
                let band_nr = layer_meta.band_nr.unwrap_or(1);
                match layer_meta.data_type {
                    Int8 => tileio::read_tile_as_png::<i8>(layer_meta, band_nr, tile_req)?,
                    Int16 => tileio::read_tile_as_png::<i16>(layer_meta, band_nr, tile_req)?,
                    Int32 => tileio::read_tile_as_png::<i32>(layer_meta, band_nr, tile_req)?,
                    Int64 => tileio::read_tile_as_png::<i64>(layer_meta, band_nr, tile_req)?,
                    Uint8 => tileio::read_tile_as_png::<u8>(layer_meta, band_nr, tile_req)?,
                    Uint16 => tileio::read_tile_as_png::<u16>(layer_meta, band_nr, tile_req)?,
                    Uint32 => tileio::read_tile_as_png::<u32>(layer_meta, band_nr, tile_req)?,
                    Uint64 => tileio::read_tile_as_png::<u64>(layer_meta, band_nr, tile_req)?,
                    Float32 => tileio::read_tile_as_png::<f32>(layer_meta, band_nr, tile_req)?,
                    Float64 => tileio::read_tile_as_png::<f64>(layer_meta, band_nr, tile_req)?,
                }
            }
            TileFormat::Protobuf => {
                let layer_data = Self::extract_provider_data(layer_meta)?;

                #[cfg(feature = "vector-tiles")]
                match layer_meta.data_type {
                    Int8 => diff_tiles_as_vector::<i8>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    Int16 => diff_tiles_as_vector::<i16>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    Int32 => diff_tiles_as_vector::<i32>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    Int64 => diff_tiles_as_vector::<i64>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    Uint8 => diff_tiles_as_vector::<u8>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    Uint16 => diff_tiles_as_vector::<u16>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    Uint32 => diff_tiles_as_vector::<u32>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    Uint64 => diff_tiles_as_vector::<u64>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    Float32 => diff_tiles_as_vector::<f32>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                    Float64 => diff_tiles_as_vector::<f64>(&layer_data.layer1, &layer_data.layer2, tile_req.tile)?,
                }

                #[cfg(not(feature = "vector-tiles"))]
                return Err(Error::Runtime("Vector tile support is not enabled".into()));
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

        use RasterDataType::*;
        let tile_data = match layer_meta.tile_format {
            TileFormat::Png => {
                let band = layer_meta.band_nr.unwrap_or(1);
                match layer_meta.data_type {
                    Uint8 => tileio::read_color_mapped_tile_as_png::<u8>(layer_meta, band, tile_req)?,
                    Uint16 => tileio::read_color_mapped_tile_as_png::<u16>(layer_meta, band, tile_req)?,
                    Uint32 => tileio::read_color_mapped_tile_as_png::<u32>(layer_meta, band, tile_req)?,
                    Uint64 => tileio::read_color_mapped_tile_as_png::<u64>(layer_meta, band, tile_req)?,
                    Int8 => tileio::read_color_mapped_tile_as_png::<i8>(layer_meta, band, tile_req)?,
                    Int16 => tileio::read_color_mapped_tile_as_png::<i16>(layer_meta, band, tile_req)?,
                    Int32 => tileio::read_color_mapped_tile_as_png::<i32>(layer_meta, band, tile_req)?,
                    Int64 => tileio::read_color_mapped_tile_as_png::<i64>(layer_meta, band, tile_req)?,
                    Float32 => tileio::read_color_mapped_tile_as_png::<f32>(layer_meta, band, tile_req)?,
                    Float64 => tileio::read_color_mapped_tile_as_png::<f64>(layer_meta, band, tile_req)?,
                }
            }
            TileFormat::Protobuf => {
                return Err(Error::InvalidArgument("Diff tiles cannot be colormapped".into()));
            }
            _ => return Err(Error::Runtime("Unsupported tile format".into())),
        };

        Ok(tile_data)
    }

    pub fn raster_pixel(_layer_meta: &LayerMetadata, _coord: Coordinate, _dpi_ratio: u8) -> Result<Option<f32>> {
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
        DiffTileProvider::raster_pixel(layer_meta, coord, dpi_ratio)
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use geo::RuntimeConfiguration;
//     use path_macro::path;

//     use crate::{tileproviderfactory::TileProviderOptions, LayerSourceType};

//     use super::DiffTileProvider;

//     #[ctor::ctor]
//     fn init() {
//         let mut data_dir = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "target" / "data");
//         if !data_dir.exists() {
//             // Infra used as a subcrate, try the parent directory
//             data_dir = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / ".." / "target" / "data");
//             if !data_dir.exists() {
//                 panic!("Proj.db data directory not found");
//             }
//         }

//         let config = RuntimeConfiguration::builder().proj_db(&data_dir).build();
//         config.apply().expect("Failed to configure runtime");
//     }

//     // #[test]
//     // fn test_diff_tile_provider() {
//     //     let path1 = path!(env!("CARGO_MANIFEST_DIR") / "test" / "data" / "potgeo_lim_bebouwd.tif");
//     //     let path2 = path!(env!("CARGO_MANIFEST_DIR") / "test" / "data" / "residentieel_dakopp_50m_lim.tif");
//     //     assert!(path1.exists());
//     //     assert!(path2.exists());

//     //     let provider = DiffTileProvider::new(&path1, &path2, &TileProviderOptions::default()).unwrap();
//     //     let meta = provider.diff_layer().unwrap();

//     //     assert!(meta.source_format == LayerSourceType::GeoTiff);
//     //     assert!(meta.tile_format == TileFormat::Protobuf);

//     //     let req = TileRequest {
//     //         tile: Tile::for_coordinate(meta.bounds().center(), 10),
//     //         dpi_ratio: 1,
//     //         tile_format: TileFormat::Protobuf,
//     //     };

//     //     let mvt = provider.get_tile(meta.id, &req).unwrap();

//     //     // write mvt to file for debugging
//     //     std::fs::write("/Users/dirk/tile.mvt", mvt.data).unwrap();
//     // }
// }

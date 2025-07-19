use std::{
    collections::HashMap,
    f32,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
};

use geo::cog::{CogAccessor, HorizontalUnpredictable, WebTilesReader};
use geo::{Array as _, ArrayNum, Coordinate, DenseArray, LatLonBounds, Tile, crs};
use raster_tile::{CompressionAlgorithm, RasterTileIO};

#[cfg(feature = "simd")]
const LANES: usize = inf::simd::LANES;

use crate::{
    Error, PixelFormat, Result, TileProvider, imageprocessing,
    layermetadata::{LayerId, LayerMetadata, LayerSourceType},
    tiledata::TileData,
    tileformat::TileFormat,
    tileio,
    tileprovider::{ColorMappedTileRequest, TileRequest, unique_layer_id},
    tileproviderfactory::TileProviderOptions,
};

pub struct CogTileProvider {
    meta: LayerMetadata,
}

impl CogTileProvider {
    pub fn new(path: &Path, _opts: &TileProviderOptions) -> Result<Self> {
        let cog = WebTilesReader::from_cog(CogAccessor::from_file(path)?)?;
        let meta = cog.cog_metadata();

        let meta = LayerMetadata {
            id: unique_layer_id(),
            path: PathBuf::from(path),
            min_zoom: meta.min_zoom,
            max_zoom: meta.max_zoom,
            tile_size: Some(meta.tile_size),
            tile_format: TileFormat::RasterTile,
            name: "COG".into(),
            description: String::default(),
            epsg: Some(crs::epsg::WGS84_WEB_MERCATOR),
            bounds: meta.geo_reference.latlonbounds().array(),
            url: "".to_string(),
            source_is_web_mercator: true,
            supports_dpi_ratio: false,
            nodata: meta.geo_reference.nodata(),
            min_value: meta.statistics.as_ref().map_or(f64::NAN, |stat| stat.minimum_value) as f32,
            max_value: meta.statistics.as_ref().map_or(f64::NAN, |stat| stat.maximum_value) as f32,
            data_type: meta.data_type,
            source_format: LayerSourceType::CloudOptimizedGeoTiff,
            scheme: "xyz".into(),
            additional_data: HashMap::new(),
            band_nr: None,
            tileprovider_data: Some(Box::new(Arc::new(cog))),
        };

        log::info!(
            "[TILE] Serving {} [{}] ({})",
            meta.name,
            meta.tile_format,
            path.file_name().unwrap_or_default().to_string_lossy()
        );

        Ok(CogTileProvider { meta })
    }

    pub fn tiff_is_cog(path: &Path) -> bool {
        CogAccessor::is_cog(path)
    }

    #[geo::simd_bounds]
    fn read_tile_data<T: ArrayNum + HorizontalUnpredictable>(meta: &LayerMetadata, tile: &Tile, tile_size: u16) -> Result<DenseArray<T>> {
        if Some(tile_size) != meta.tile_size {
            return Err(Error::InvalidArgument("Invalid COG tile size requested".to_string()));
        }

        let tile = meta
            .tileprovider_data
            .as_ref()
            .and_then(|data| data.downcast_ref::<WebTilesReader>())
            .map(|cog| cog.read_tile_data_as::<T>(tile, std::fs::File::open(&meta.path)?));

        match tile {
            Some(Ok(Some(tile_data))) => Ok(tile_data),
            Some(Err(e)) => Err(Error::Runtime(format!("Failed to read tile data: {e}"))),
            None | Some(Ok(None)) => Ok(DenseArray::empty()),
        }
    }

    #[geo::simd_bounds]
    fn read_vrt_tile<T: ArrayNum + HorizontalUnpredictable>(meta: &LayerMetadata, tile: &Tile, tile_size: u16) -> Result<TileData> {
        let tile_data = Self::read_tile_data::<T>(meta, tile, tile_size)?;
        if tile_data.is_empty() {
            return Ok(TileData::default());
        }

        let raster_tile = tile_data.encode_raster_tile(CompressionAlgorithm::Lz4Block)?;
        Ok(TileData::new(meta.tile_format, PixelFormat::Native, raster_tile))
    }

    #[geo::simd_bounds]
    fn read_tile_data_color_mappped<T: ArrayNum + HorizontalUnpredictable>(
        meta: &LayerMetadata,
        tile_req: &ColorMappedTileRequest,
    ) -> Result<TileData> {
        log::debug!(
            "COG color map tile: {}@{}x {}px {}",
            tile_req.tile,
            tile_req.dpi_ratio,
            tile_req.tile_size,
            meta.data_type
        );

        if tile_req.dpi_ratio != 1 {
            return Err(Error::InvalidArgument("DPI ratio is not supported for COG tiles".to_string()));
        }

        if Some(tile_req.tile_size) != meta.tile_size {
            return Err(Error::InvalidArgument("Invalid COG tile size requested".to_string()));
        }

        let tile_data = Self::read_tile_data::<T>(meta, &tile_req.tile, tile_req.tile_size)?;
        if tile_data.is_empty() {
            return Ok(TileData::default());
        }

        imageprocessing::raw_tile_to_png_color_mapped::<T>(
            tile_data.as_slice(),
            tile_req.tile_size as usize,
            tile_req.tile_size as usize,
            Some(T::NODATA),
            tile_req.legend,
        )
    }

    pub fn tile(meta: &LayerMetadata, tile_req: &TileRequest) -> Result<TileData> {
        if tile_req.tile_format != TileFormat::RasterTile {
            return Err(Error::Runtime("Only raster tile format is supported for COG".to_string()));
        }

        if Some(tile_req.tile_size) != meta.tile_size {
            return Err(Error::InvalidArgument("Invalid COG tile size requested".to_string()));
        }

        if tile_req.dpi_ratio != 1 {
            return Err(Error::InvalidArgument("DPI ratio is not supported for COG tiles".to_string()));
        }

        match meta.data_type {
            geo::ArrayDataType::Int8 => Self::read_vrt_tile::<i8>(meta, &tile_req.tile, tile_req.tile_size),
            geo::ArrayDataType::Uint8 => Self::read_vrt_tile::<u8>(meta, &tile_req.tile, tile_req.tile_size),
            geo::ArrayDataType::Int16 => Self::read_vrt_tile::<i16>(meta, &tile_req.tile, tile_req.tile_size),
            geo::ArrayDataType::Uint16 => Self::read_vrt_tile::<u16>(meta, &tile_req.tile, tile_req.tile_size),
            geo::ArrayDataType::Int32 => Self::read_vrt_tile::<i32>(meta, &tile_req.tile, tile_req.tile_size),
            geo::ArrayDataType::Uint32 => Self::read_vrt_tile::<u32>(meta, &tile_req.tile, tile_req.tile_size),
            geo::ArrayDataType::Int64 => Self::read_vrt_tile::<i64>(meta, &tile_req.tile, tile_req.tile_size),
            geo::ArrayDataType::Uint64 => Self::read_vrt_tile::<u64>(meta, &tile_req.tile, tile_req.tile_size),
            geo::ArrayDataType::Float32 => Self::read_vrt_tile::<f32>(meta, &tile_req.tile, tile_req.tile_size),
            geo::ArrayDataType::Float64 => Self::read_vrt_tile::<f64>(meta, &tile_req.tile, tile_req.tile_size),
        }
    }

    pub fn tile_color_mapped(meta: &LayerMetadata, tile_req: &ColorMappedTileRequest) -> Result<TileData> {
        match meta.data_type {
            geo::ArrayDataType::Int8 => Self::read_tile_data_color_mappped::<i8>(meta, tile_req),
            geo::ArrayDataType::Uint8 => Self::read_tile_data_color_mappped::<u8>(meta, tile_req),
            geo::ArrayDataType::Int16 => Self::read_tile_data_color_mappped::<i16>(meta, tile_req),
            geo::ArrayDataType::Uint16 => Self::read_tile_data_color_mappped::<u16>(meta, tile_req),
            geo::ArrayDataType::Int32 => Self::read_tile_data_color_mappped::<i32>(meta, tile_req),
            geo::ArrayDataType::Uint32 => Self::read_tile_data_color_mappped::<u32>(meta, tile_req),
            geo::ArrayDataType::Int64 => Self::read_tile_data_color_mappped::<i64>(meta, tile_req),
            geo::ArrayDataType::Uint64 => Self::read_tile_data_color_mappped::<u64>(meta, tile_req),
            geo::ArrayDataType::Float32 => Self::read_tile_data_color_mappped::<f32>(meta, tile_req),
            geo::ArrayDataType::Float64 => Self::read_tile_data_color_mappped::<f64>(meta, tile_req),
        }
    }

    // pub fn value_range_for_extent(_meta: &LayerMetadata, _extent: LatLonBounds, _zoom: Option<i32>) -> Result<std::ops::Range<f64>> {
    //     Err(Error::Runtime("Extent value range not supported for vector tiles".to_string()))
    // }

    // pub fn raster_pixel(_meta: &LayerMetadata, _coord: Coordinate) -> Result<Option<f32>> {
    //     Err(Error::Runtime("Raster pixel not supported for vector tiles".to_string()))
    // }
}

impl TileProvider for CogTileProvider {
    fn extent_value_range(&self, _layer_id: LayerId, extent: LatLonBounds, _zoom: Option<i32>) -> Result<Range<f64>> {
        tileio::detect_raster_range(&self.meta.path, 1, extent)
    }

    fn get_raster_value(&self, _layer_id: LayerId, _coord: Coordinate, _dpi_ratio: u8) -> Result<Option<f32>> {
        todo!()
    }

    fn get_tile(&self, layer_id: LayerId, req: &TileRequest) -> Result<TileData> {
        if layer_id != self.meta.id {
            return Err(Error::Runtime("Layer ID does not match the provider's layer ID".to_string()));
        }

        log::debug!("Get tile {}/{}/{}", req.tile.z(), req.tile.x(), req.tile.y());
        Self::tile(&self.meta, req)
    }

    fn get_tile_color_mapped(&self, layer_id: LayerId, req: &ColorMappedTileRequest) -> Result<TileData> {
        if layer_id != self.meta.id {
            return Err(Error::Runtime("Layer ID does not match the provider's layer ID".to_string()));
        }

        Self::tile_color_mapped(&self.meta, req)
    }

    fn layers(&self) -> Vec<LayerMetadata> {
        vec![self.meta.clone()]
    }

    fn layer(&self, _id: LayerId) -> Result<LayerMetadata> {
        Ok(self.meta.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{TileProvider, tileproviderfactory::TileProviderOptions};
    use inf::{
        Legend,
        colormap::{ColorMap, ColorMapDirection, ColorMapPreset},
    };
    use path_macro::path;

    fn test_raster() -> std::path::PathBuf {
        path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data" / "landusebyte.tif")
    }

    fn create_test_cog(input_tif: &Path, output_tif: &Path, tile_size: i32, compress: &str) -> Result<()> {
        let src_ds = geo::raster::io::dataset::open_read_only(input_tif).expect("Failed to open test COG input file");
        let options = vec![
            "-f".to_string(),
            "COG".to_string(),
            "-co".to_string(),
            format!("BLOCKSIZE={tile_size}"),
            "-co".to_string(),
            "TILING_SCHEME=GoogleMapsCompatible".to_string(),
            "-co".to_string(),
            format!("COMPRESS={compress}"),
            "-co".to_string(),
            "ADD_ALPHA=NO".to_string(),
            "-co".to_string(),
            "STATISTICS=YES".to_string(),
            "-co".to_string(),
            "OVERVIEWS=IGNORE_EXISTING".to_string(),
            "-co".to_string(),
            "RESAMPLING=NEAREST".to_string(),
            "-co".to_string(),
            "OVERVIEW_RESAMPLING=NEAREST".to_string(),
            "-co".to_string(),
            "NUM_THREADS=ALL_CPUS".to_string(),
        ];

        geo::raster::algo::warp_to_disk_cli(&src_ds, output_tif, &options, &vec![]).expect("Failed to create test COG file");

        Ok(())
    }

    #[test]
    fn test_cog_tile_decompression() -> Result<()> {
        const COG_TILE_SIZE: i32 = 256;
        let tmp = tempfile::tempdir().expect("Failed to create temporary directory");

        let no_compression_output = tmp.path().join("cog_no_compression.tif");
        create_test_cog(&test_raster(), &no_compression_output, COG_TILE_SIZE, "NONE")?;

        let lzw_compression_output = tmp.path().join("cog_lzw_compression.tif");
        create_test_cog(&test_raster(), &lzw_compression_output, COG_TILE_SIZE, "LZW")?;

        let cog_tiler_uncompressed = CogTileProvider::new(&no_compression_output, &TileProviderOptions::default())?;
        let cog_tiler_lzw = CogTileProvider::new(&lzw_compression_output, &TileProviderOptions::default())?;

        let layerid_uncompressed = cog_tiler_uncompressed.layers().first().unwrap().id;
        let layerid_lzw = cog_tiler_lzw.layers().first().unwrap().id;

        let tile = Tile { z: 10, x: 524, y: 341 };
        let request = TileRequest {
            tile,
            dpi_ratio: 1,
            tile_size: COG_TILE_SIZE as u16,
            tile_format: TileFormat::RasterTile,
        };

        let uncompressed_tile = cog_tiler_uncompressed.get_tile(layerid_uncompressed, &request)?;
        let lzw_tile = cog_tiler_lzw.get_tile(layerid_lzw, &request)?;
        assert_eq!(uncompressed_tile.data, lzw_tile.data);

        let legend = Legend::linear(
            &ColorMap::Preset(ColorMapPreset::Greys, ColorMapDirection::Regular),
            0.0..254.0,
            None,
        )?;

        let request = ColorMappedTileRequest {
            tile,
            dpi_ratio: 1,
            tile_size: COG_TILE_SIZE as u16,
            legend: &legend,
        };

        let uncompressed_tile = cog_tiler_uncompressed.get_tile_color_mapped(layerid_uncompressed, &request)?;
        let lzw_tile = cog_tiler_lzw.get_tile_color_mapped(layerid_lzw, &request)?;

        assert_eq!(uncompressed_tile.data, lzw_tile.data);

        Ok(())
    }
}

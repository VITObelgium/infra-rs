use std::{ops::Range, path::PathBuf};

use gdal::{
    raster::{GdalDataType, GdalType},
    Dataset,
};

use inf::legend::Legend;

use geo::raster::{io::RasterFormat, RasterNum};
use geo::{crs, raster, CellSize, Coordinate, GeoReference, LatLonBounds, RasterSize, SpatialReference, Tile};
use num::Num;

use crate::{
    imageprocessing::raw_tile_to_png,
    layermetadata::{to_raster_data_type, LayerId, LayerMetadata, RasterDataType},
    rasterprocessing::{metadata_bounds_wgs84, raster_pixel, source_type_for_path},
    tiledata::TileData,
    tileformat::TileFormat,
    tileprovider::{self, TileProvider},
    tileproviderfactory::TileProviderOptions,
    Error, Result,
};

fn type_string<T: GdalType>() -> &'static str {
    match <T as GdalType>::datatype() {
        GdalDataType::UInt8 => "Byte",
        GdalDataType::UInt16 => "UInt16",
        GdalDataType::Int16 => "Int16",
        GdalDataType::UInt32 => "UInt32",
        GdalDataType::Int32 => "Int32",
        GdalDataType::Float32 => "Float32",
        GdalDataType::Float64 => "Float64",
        _ => panic!("Invalid type provided"),
    }
}

fn detect_raster_range(raster_path: &std::path::Path, bbox: LatLonBounds) -> Result<Range<f64>> {
    let options: Vec<String> = vec![
        "-stats".to_string(),
        "-ot".to_string(),
        "Float32".to_string(),
        "-of".to_string(),
        "MEM".to_string(),
        "-ovr".to_string(),
        "AUTO".to_string(),
        "-projwin".to_string(),
        bbox.west().to_string(),
        bbox.north().to_string(),
        bbox.east().to_string(),
        bbox.south().to_string(),
        "-projwin_srs".to_string(),
        "EPSG:4326".to_string(),
    ];

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let output_path = PathBuf::from(format!(
        "/vsimem/range_{}_{}.mem",
        raster_path
            .file_stem()
            .ok_or(Error::Runtime("No filename".to_string()))?
            .to_string_lossy(),
        timestamp
    ));

    if let Ok(ds) = raster::algo::translate(&Dataset::open(raster_path)?, output_path.as_path(), &options) {
        if let Ok(Some(stats)) = ds.rasterband(1)?.get_statistics(true, true) {
            log::info!("Value range: [{:.2} <-> {:.2}]", stats.min, stats.max);
            return Ok(Range {
                start: stats.min,
                end: stats.max,
            });
        }
    }

    Err(Error::Runtime(format!(
        "Failed to obtain value range for bbox: {} - {}",
        bbox.northwest(),
        bbox.southeast(),
    )))
}

fn read_raster_tile<T: RasterNum<T> + GdalType>(
    raster_path: &std::path::Path,
    tile: Tile,
    dpi_ratio: u8,
) -> Result<Vec<T>> {
    let bounds = tile.web_mercator_bounds();
    let scaled_size = Tile::TILE_SIZE * dpi_ratio as u16;

    let options: Vec<String> = vec![
        "-ot".to_string(),
        type_string::<T>().to_string(),
        "-of".to_string(),
        "MEM".to_string(),
        "-outsize".to_string(),
        scaled_size.to_string(),
        scaled_size.to_string(),
        "-projwin".to_string(),
        bounds.top_left().x().to_string(),
        bounds.top_left().y().to_string(),
        bounds.bottom_right().x().to_string(),
        bounds.bottom_right().y().to_string(),
    ];

    let output_path = PathBuf::from(format!("/vsimem/{}_{}_{}.mem", tile.x(), tile.y(), tile.z()));
    let mut data = vec![T::zero(); scaled_size as usize * scaled_size as usize];
    let ds = raster::algo::translate_file(raster_path, &output_path, &options)?;
    raster::io::dataset::read_band(&ds, 1, &mut data)?;
    Ok(data)
}

fn read_raster_tile_warped<T: RasterNum<T> + GdalType>(
    raster_path: &std::path::Path,
    tile: Tile,
    dpi_ratio: u8,
) -> Result<Vec<T>> {
    let bounds = tile.web_mercator_bounds();
    let scaled_size = (Tile::TILE_SIZE * dpi_ratio as u16) as usize;

    let projection = SpatialReference::from_epsg(crs::epsg::WGS84_WEB_MERCATOR)?;
    let dest_extent = GeoReference::with_origin(
        projection.to_wkt()?,
        RasterSize {
            rows: scaled_size,
            cols: scaled_size,
        },
        bounds.bottom_left(),
        CellSize::square(bounds.width() / scaled_size as f64),
        Some(T::nodata_value()),
    );

    let src_ds = gdal::Dataset::open(raster_path)?;

    let mut data = vec![T::nodata_value(); scaled_size * scaled_size];
    let mut dest_ds = raster::io::dataset::create_in_memory_with_data::<T>(&dest_extent, data.as_mut_slice())?;

    let options = vec![
        "-ovr".to_string(),
        "AUTO".to_string(),
        "-r".to_string(),
        "near".to_string(),
    ];
    let key_value_options: Vec<(String, String)> = vec![
        ("INIT_DEST".to_string(), "NO_DATA".to_string()),
        ("SKIP_NOSOURCE".to_string(), "YES".to_string()),
        ("NUM_THREADS".to_string(), "ALL_CPUS".to_string()),
    ];

    raster::algo::warp_cli(&src_ds, &mut dest_ds, &options, &key_value_options)?;

    // Avoid returning tiles containing only nodata values
    if data.iter().all(|&val| T::is_nodata(val)) {
        return Ok(vec![]);
    }

    Ok(data)
}

pub struct WarpingTileProvider {
    meta: LayerMetadata,
}

impl WarpingTileProvider {
    pub fn new(path: &std::path::Path, opts: &TileProviderOptions) -> Result<Self> {
        Ok(WarpingTileProvider {
            meta: WarpingTileProvider::create_metadata_for_file(path, opts)?,
        })
    }

    fn create_metadata_for_file(path: &std::path::Path, opts: &TileProviderOptions) -> Result<LayerMetadata> {
        let ds = raster::io::dataset::open_read_only(path)?;

        // We assume to be working with a single raster band
        let meta = raster::io::dataset::read_band_metadata(&ds, 1)?;
        let raster_band = ds.rasterband(1)?;
        let over_view_count = raster_band.overview_count()?;

        let mut srs = SpatialReference::from_proj(meta.projection())?;
        let zoom_level = Tile::zoom_level_for_pixel_size(meta.cell_size_x(), true);

        let mut layer_meta = LayerMetadata {
            id: tileprovider::unique_layer_id(),
            data_type: to_raster_data_type(raster_band.band_type()),
            url: String::new(),
            path: path.to_path_buf(),
            name: path
                .file_stem()
                .ok_or(Error::Runtime("No path stem".to_string()))?
                .to_string_lossy()
                .to_string(),
            max_zoom: zoom_level,
            min_zoom: if over_view_count > 0 {
                zoom_level - over_view_count
            } else {
                6
            },
            nodata: meta.nodata(),
            supports_dpi_ratio: true,
            tile_format: TileFormat::Png,
            source_is_web_mercator: srs.is_projected() && srs.epsg_cs() == Some(crs::epsg::WGS84_WEB_MERCATOR),
            epsg: srs.epsg_cs().unwrap_or(0.into()),
            bounds: metadata_bounds_wgs84(meta)?.array(),
            description: String::new(),
            min_value: f64::NAN,
            max_value: f64::NAN,
            source_format: source_type_for_path(path),
            scheme: "xyz".to_string(),
            additional_data: Default::default(),
        };

        if opts.calculate_stats {
            let allow_approximation = raster_band.x_size() * raster_band.y_size() > 10000000;
            let force = cfg!(not(debug_assertions));

            if let Ok(Some(stats)) = raster_band.get_statistics(force, allow_approximation) {
                layer_meta.min_value = stats.min;
                layer_meta.max_value = stats.max;
            }
        }

        log::debug!("Serving file: {:?}", layer_meta.path);

        Ok(layer_meta)
    }

    pub fn supports_raster_type(raster_type: RasterFormat) -> bool {
        matches!(raster_type, RasterFormat::GeoTiff | RasterFormat::Vrt)
    }

    fn read_tile<T: RasterNum<T> + Num + GdalType>(
        meta: &LayerMetadata,
        tile: Tile,
        dpi_ratio: u8,
        legend: &Legend,
    ) -> Result<TileData> {
        let raw_tile_data: Vec<T>;
        let start = std::time::Instant::now();

        let mut nodata: T = meta.nodata::<T>().unwrap_or(T::nodata_value());

        if !meta.source_is_web_mercator {
            raw_tile_data = read_raster_tile_warped(meta.path.as_path(), tile, dpi_ratio)?;
            nodata = T::nodata_value();
        } else {
            raw_tile_data = read_raster_tile(meta.path.as_path(), tile, dpi_ratio)?;
        }

        //#[cfg(TILESERVER_VERBOSE)]
        log::debug!(
            "[{}/{}/{}@{}] {} took {}ms (data type: {}) [{:?}]",
            tile.z(),
            tile.x(),
            tile.y(),
            dpi_ratio,
            if meta.source_is_web_mercator {
                "Translate"
            } else {
                "Warp"
            },
            start.elapsed().as_millis(),
            type_string::<T>(),
            std::thread::current().id(),
        );

        if start.elapsed().as_secs() > 10 {
            log::warn!("Slow tile: {}/{}/{}", tile.z(), tile.x(), tile.y());
        }

        if raw_tile_data.is_empty() {
            return Ok(TileData::default());
        }

        raw_tile_to_png::<T>(
            raw_tile_data.as_slice(),
            (Tile::TILE_SIZE * dpi_ratio as u16) as usize,
            (Tile::TILE_SIZE * dpi_ratio as u16) as usize,
            Some(nodata),
            legend,
        )
    }

    pub fn tile_with_legend(
        layer_meta: &LayerMetadata,
        tile: Tile,
        dpi_ratio: u8,
        legend: &Legend,
    ) -> Result<TileData> {
        if !(Range { start: 1, end: 10 }).contains(&dpi_ratio) {
            return Err(crate::Error::InvalidArgument(format!(
                "Invalid dpi ratio {}",
                dpi_ratio
            )));
        }

        if tile.z() < layer_meta.min_zoom || tile.z() > layer_meta.max_zoom {
            return Ok(TileData::default());
        }

        match layer_meta.data_type {
            RasterDataType::Byte => WarpingTileProvider::read_tile::<u8>(layer_meta, tile, dpi_ratio, legend),
            RasterDataType::Int32 => WarpingTileProvider::read_tile::<i32>(layer_meta, tile, dpi_ratio, legend),
            RasterDataType::UInt32 => WarpingTileProvider::read_tile::<u32>(layer_meta, tile, dpi_ratio, legend),
            RasterDataType::Float => WarpingTileProvider::read_tile::<f32>(layer_meta, tile, dpi_ratio, legend),
        }
    }

    pub fn raster_pixel(layer_meta: &LayerMetadata, coord: Coordinate) -> Result<Option<f32>> {
        raster_pixel(layer_meta.path.as_path(), coord, None)
    }

    pub fn value_range_for_extent(
        layer_meta: &LayerMetadata,
        extent: LatLonBounds,
        _zoom: Option<i32>,
    ) -> Result<Range<f64>> {
        detect_raster_range(&layer_meta.path, extent)
    }

    fn check_layer_id(&self, id: LayerId) -> Result<()> {
        if id != self.meta.id {
            return Err(Error::InvalidArgument(format!("Invalid layer id: {}", id)));
        }

        Ok(())
    }
}

impl TileProvider for WarpingTileProvider {
    fn layers(&self) -> Vec<LayerMetadata> {
        vec![self.meta.clone()]
    }

    fn layer(&self, _id: LayerId) -> Result<LayerMetadata> {
        Ok(self.meta.clone())
    }

    fn extent_value_range(&self, id: LayerId, extent: LatLonBounds, zoom: Option<i32>) -> Result<std::ops::Range<f64>> {
        self.check_layer_id(id)?;
        WarpingTileProvider::value_range_for_extent(&self.meta, extent, zoom)
    }

    fn get_raster_value(&self, id: LayerId, coord: Coordinate) -> Result<Option<f32>> {
        self.check_layer_id(id)?;
        WarpingTileProvider::raster_pixel(&self.meta, coord)
    }

    fn get_tile(&self, id: LayerId, tile: Tile, dpi_ratio: u8) -> Result<TileData> {
        // This creates a legend with grayscale colors, is this the desired behavior?
        // Or should we return the raw pixel data?
        self.get_tile_colored(id, tile, dpi_ratio, &Legend::default())
    }

    fn get_tile_colored(&self, id: LayerId, tile: Tile, dpi_ratio: u8, legend: &Legend) -> Result<TileData> {
        self.check_layer_id(id)?;
        WarpingTileProvider::tile_with_legend(&self.meta, tile, dpi_ratio, legend)
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;
    use geo::Tile;

    use crate::{
        tileproviderfactory::TileProviderOptions, warpingtileprovider::WarpingTileProvider, Error, TileProvider,
    };

    fn test_raster() -> std::path::PathBuf {
        [env!("CARGO_MANIFEST_DIR"), "test", "data", "landusebyte.tif"]
            .iter()
            .collect()
    }

    #[test]
    fn test_layer_metadata() -> Result<(), Error> {
        let provider =
            WarpingTileProvider::new(test_raster().as_path(), &TileProviderOptions { calculate_stats: false })?;
        let layer_id = provider.layers().first().unwrap().id;

        let meta = provider.layer(layer_id)?;
        assert_eq!(meta.nodata::<u8>(), Some(255));
        assert_relative_eq!(meta.bounds[0], 2.52542882367258, epsilon = 1e-6);
        assert_relative_eq!(meta.bounds[1], 50.6774001192389, epsilon = 1e-6);
        assert_relative_eq!(meta.bounds[2], 5.91103418055685, epsilon = 1e-6);
        assert_relative_eq!(meta.bounds[3], 51.5002785754381, epsilon = 1e-6);

        Ok(())
    }

    #[test]
    fn test_nodata_outside_of_raster() -> Result<(), Error> {
        let provider =
            WarpingTileProvider::new(test_raster().as_path(), &TileProviderOptions { calculate_stats: false })?;
        let layer_id = provider.layers().first().unwrap().id;

        assert_eq!(provider.meta.nodata::<u8>(), Some(255));

        let tile_data = provider.get_tile(layer_id, Tile { x: 264, y: 171, z: 9 }, 1)?;

        // decode the png data to raw data
        let raw_data = image::load_from_memory(&tile_data.data)
            .expect("Invalid image")
            .to_rgba8();
        // count the number of transparent pixels
        let transparent_count = raw_data.pixels().filter(|p| p[3] == 0).count();
        // The transparent pixel count should be more than 80% of the total pixel count, otherwise there is an issue with the nodata handling
        assert!(transparent_count > (raw_data.pixels().count() as f64 * 0.8) as usize);
        assert!(transparent_count < (raw_data.pixels().count() as f64 * 0.9) as usize);

        Ok(())
    }
}

use std::{ops::Range, path::PathBuf};

use gdal::{
    raster::{GdalDataType, GdalType},
    Dataset,
};

use inf::legend::Legend;

use geo::georaster::{self, io::RasterFormat};
use geo::{crs, CellSize, Coordinate, GeoReference, LatLonBounds, SpatialReference, Tile};
use num::Num;
use raster::{DenseRaster, Nodata, RasterCreation, RasterNum, RasterSize};
use raster_tile::{CompressionAlgorithm, RasterTileIO};

use crate::{
    imageprocessing::{self},
    layermetadata::{to_raster_data_type, LayerId, LayerMetadata, RasterDataType},
    rasterprocessing::{metadata_bounds_wgs84, source_type_for_path},
    tiledata::TileData,
    tileformat::TileFormat,
    tileprovider::{self, ColorMappedTileRequest, TileProvider, TileRequest},
    tileproviderfactory::TileProviderOptions,
    Error, PixelFormat, Result,
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

fn detect_raster_range(raster_path: &std::path::Path, band_nr: usize, bbox: LatLonBounds) -> Result<Range<f64>> {
    let options: Vec<String> = vec![
        "-b".to_string(),
        band_nr.to_string(),
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

    if let Ok(ds) = georaster::algo::translate(&Dataset::open(raster_path)?, output_path.as_path(), &options) {
        if let Ok(Some(stats)) = ds.rasterband(band_nr)?.get_statistics(true, true) {
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
    band_nr: usize,
    tile: Tile,
    dpi_ratio: u8,
) -> Result<Vec<T>> {
    let bounds = tile.web_mercator_bounds();
    let scaled_size = Tile::TILE_SIZE * dpi_ratio as u16;

    let options: Vec<String> = vec![
        "-b".to_string(),
        band_nr.to_string(),
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
    let ds = georaster::algo::translate_file(raster_path, &output_path, &options)?;
    georaster::io::dataset::read_band(&ds, 1, &mut data)?;
    Ok(data)
}

fn read_raster_tile_warped<T: RasterNum<T> + GdalType>(
    raster_path: &std::path::Path,
    band_nr: usize,
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
    let mut dest_ds = georaster::io::dataset::create_in_memory_with_data::<T>(&dest_extent, data.as_mut_slice())?;

    let options = vec![
        "-b".to_string(),
        band_nr.to_string(),
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

    georaster::algo::warp_cli(&src_ds, &mut dest_ds, &options, &key_value_options)?;

    // Avoid returning tiles containing only nodata values
    if data.iter().all(|&val| T::is_nodata(val)) {
        return Ok(vec![]);
    }

    Ok(data)
}

fn raw_tile_to_vito_tile_format<T: RasterNum<T>>(data: Vec<T>, width: usize, height: usize) -> Result<TileData> {
    let raster_tile = DenseRaster::new(RasterSize::with_rows_cols(height, width), data);

    Ok(TileData::new(
        TileFormat::RasterTile,
        PixelFormat::Native,
        RasterTileIO::encode_raster_tile(&raster_tile, CompressionAlgorithm::Lz4Block)?,
    ))
}

pub struct WarpingTileProvider {
    meta: Vec<LayerMetadata>,
}

impl WarpingTileProvider {
    pub fn new(path: &std::path::Path, opts: &TileProviderOptions) -> Result<Self> {
        Ok(WarpingTileProvider {
            meta: WarpingTileProvider::create_metadata_for_file(path, opts)?,
        })
    }

    fn create_metadata_for_file(path: &std::path::Path, opts: &TileProviderOptions) -> Result<Vec<LayerMetadata>> {
        let ds = georaster::io::dataset::open_read_only(path)?;

        let raster_count = ds.raster_count();
        let mut result = Vec::with_capacity(raster_count);

        for band_nr in 1..=raster_count {
            let meta = georaster::io::dataset::read_band_metadata(&ds, band_nr)?;
            let raster_band = ds.rasterband(band_nr)?;
            let over_view_count = raster_band.overview_count()?;

            let mut srs = SpatialReference::from_proj(meta.projection())?;
            let zoom_level = Tile::zoom_level_for_pixel_size(meta.cell_size_x(), opts.max_zoom_round_up);

            let mut name = path
                .file_stem()
                .ok_or(Error::Runtime("No path stem".to_string()))?
                .to_string_lossy()
                .to_string();

            if raster_count > 1 {
                name.push_str(&format!(" - Band {}", band_nr));
            }

            let mut layer_meta = LayerMetadata {
                id: tileprovider::unique_layer_id(),
                data_type: to_raster_data_type(raster_band.band_type()),
                url: String::default(),
                path: path.to_path_buf(),
                name,
                max_zoom: zoom_level,
                min_zoom: if over_view_count > 0 {
                    zoom_level - over_view_count
                } else {
                    0
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
                band_nr: Some(band_nr),
            };

            if opts.calculate_stats {
                let allow_approximation = raster_band.x_size() * raster_band.y_size() > 10000000;
                let force = cfg!(not(debug_assertions));

                match raster_band.get_statistics(force, allow_approximation) {
                    Ok(Some(stats)) => {
                        layer_meta.min_value = stats.min;
                        layer_meta.max_value = stats.max;
                    }
                    Ok(None) => {
                        log::warn!("No statistics available for band {}", band_nr);
                        layer_meta.min_value = 0.0;
                        layer_meta.max_value = f64::MAX;
                    }
                    Err(e) => {
                        log::warn!("Failed to calculate statistics: {}", e);
                    }
                }
            }

            result.push(layer_meta);
        }

        if let Some(layer) = result.first() {
            log::debug!("Serving file: {:?}", layer.path);
        }

        Ok(result)
    }

    pub fn supports_raster_type(raster_type: RasterFormat) -> bool {
        matches!(
            raster_type,
            RasterFormat::GeoTiff | RasterFormat::Vrt | RasterFormat::Netcdf
        )
    }

    /// Read the raw tile data, result is a tuple with the raw data and the nodata value
    fn read_tile_data<T: RasterNum<T> + Num + GdalType>(
        meta: &LayerMetadata,
        band_nr: usize,
        tile: Tile,
        dpi_ratio: u8,
    ) -> Result<(Vec<T>, T)> {
        let raw_tile_data: Vec<T>;
        let start = std::time::Instant::now();

        let mut nodata: T = meta.nodata::<T>().unwrap_or(T::nodata_value());

        if !meta.source_is_web_mercator {
            raw_tile_data = read_raster_tile_warped(meta.path.as_path(), band_nr, tile, dpi_ratio)?;
            nodata = T::nodata_value();
        } else {
            raw_tile_data = read_raster_tile(meta.path.as_path(), band_nr, tile, dpi_ratio)?;
        }

        //[cfg(TILESERVER_VERBOSE)]
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

        Ok((raw_tile_data, nodata))
    }

    fn process_pixel_request<T>(
        meta: &LayerMetadata,
        band_nr: usize,
        tile: Tile,
        dpi_ratio: u8,
        coord: Coordinate,
    ) -> Result<Option<f32>>
    where
        T: RasterNum<T> + Num + GdalType,
    {
        let (raw_tile_data, nodata) = WarpingTileProvider::read_tile_data::<T>(meta, band_nr, tile, dpi_ratio)?;
        if raw_tile_data.is_empty() {
            return Ok(None);
        }

        let tile_size = (Tile::TILE_SIZE * dpi_ratio as u16) as usize;
        let raster_size = RasterSize::with_rows_cols(tile_size, tile_size);
        let pixel_size = Tile::pixel_size_at_zoom_level(tile.z) / dpi_ratio as f64;
        let tile_meta = GeoReference::with_origin(
            "",
            raster_size,
            crs::lat_lon_to_web_mercator(tile.bounds().southwest()),
            CellSize::square(pixel_size),
            Option::<f32>::Some(Nodata::nodata_value()),
        );
        let cell = tile_meta.point_to_cell(crs::lat_lon_to_web_mercator(coord));

        match raw_tile_data.get(cell.row as usize * tile_size + cell.col as usize) {
            Some(&v) if v.is_nan() || v == nodata => Ok(None),
            Some(&v) => Ok(v.to_f32()),
            None => Ok(None),
        }
    }

    fn process_tile_request<T>(meta: &LayerMetadata, band_nr: usize, req: &TileRequest) -> Result<TileData>
    where
        T: RasterNum<T> + Num + GdalType,
    {
        let (raw_tile_data, nodata) = WarpingTileProvider::read_tile_data::<T>(meta, band_nr, req.tile, req.dpi_ratio)?;
        if raw_tile_data.is_empty() {
            return Ok(TileData::default());
        }

        match req.tile_format {
            TileFormat::Png => {
                // The default legend is with grayscale colors in range 0-255
                imageprocessing::raw_tile_to_png_color_mapped::<T>(
                    &raw_tile_data,
                    (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
                    (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
                    Some(nodata),
                    &Legend::default(),
                )
            }
            TileFormat::FloatEncodedPng => imageprocessing::raw_tile_to_float_encoded_png::<T>(
                &raw_tile_data,
                (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
                (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
                Some(nodata),
            ),
            TileFormat::RasterTile => raw_tile_to_vito_tile_format::<T>(
                raw_tile_data,
                (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
                (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
            ),
            _ => Err(Error::InvalidArgument("Invalid pixel format".to_string())),
        }
    }

    fn read_color_mapped_tile_as_png<T>(
        meta: &LayerMetadata,
        band_nr: usize,
        req: &ColorMappedTileRequest,
    ) -> Result<TileData>
    where
        T: RasterNum<T> + Num + GdalType,
    {
        let (raw_tile_data, nodata) = WarpingTileProvider::read_tile_data::<T>(meta, band_nr, req.tile, req.dpi_ratio)?;
        if raw_tile_data.is_empty() {
            return Ok(TileData::default());
        }

        imageprocessing::raw_tile_to_png_color_mapped::<T>(
            raw_tile_data.as_slice(),
            (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
            (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
            Some(nodata),
            req.legend,
        )
    }

    fn verify_tile_dpi(dpi: u8) -> Result<()> {
        if !(Range { start: 1, end: 10 }).contains(&dpi) {
            return Err(crate::Error::InvalidArgument(format!("Invalid dpi ratio {}", dpi)));
        }

        Ok(())
    }

    pub fn tile(layer_meta: &LayerMetadata, tile_req: &TileRequest) -> Result<TileData> {
        Self::verify_tile_dpi(tile_req.dpi_ratio)?;

        let tile = &tile_req.tile;
        if tile.z() < layer_meta.min_zoom || tile.z() > layer_meta.max_zoom {
            return Ok(TileData::default());
        }

        let band_nr = layer_meta.band_nr.unwrap_or(1);
        match layer_meta.data_type {
            RasterDataType::Byte => WarpingTileProvider::process_tile_request::<u8>(layer_meta, band_nr, tile_req),
            RasterDataType::Int32 => WarpingTileProvider::process_tile_request::<i32>(layer_meta, band_nr, tile_req),
            RasterDataType::UInt32 => WarpingTileProvider::process_tile_request::<u32>(layer_meta, band_nr, tile_req),
            RasterDataType::Float => WarpingTileProvider::process_tile_request::<f32>(layer_meta, band_nr, tile_req),
        }
    }

    pub fn color_mapped_tile(layer_meta: &LayerMetadata, tile_req: &ColorMappedTileRequest) -> Result<TileData> {
        Self::verify_tile_dpi(tile_req.dpi_ratio)?;

        let tile = &tile_req.tile;
        if tile.z() < layer_meta.min_zoom || tile.z() > layer_meta.max_zoom {
            return Ok(TileData::default());
        }

        let band_nr = layer_meta.band_nr.unwrap_or(1);
        match layer_meta.data_type {
            RasterDataType::Byte => {
                WarpingTileProvider::read_color_mapped_tile_as_png::<u8>(layer_meta, band_nr, tile_req)
            }
            RasterDataType::Int32 => {
                WarpingTileProvider::read_color_mapped_tile_as_png::<i32>(layer_meta, band_nr, tile_req)
            }
            RasterDataType::UInt32 => {
                WarpingTileProvider::read_color_mapped_tile_as_png::<u32>(layer_meta, band_nr, tile_req)
            }
            RasterDataType::Float => {
                WarpingTileProvider::read_color_mapped_tile_as_png::<f32>(layer_meta, band_nr, tile_req)
            }
        }
    }

    pub fn raster_pixel(meta: &LayerMetadata, coord: Coordinate, dpi_ratio: u8) -> Result<Option<f32>> {
        // We read the entire tile for the corresponding coordinate
        // This is not ideal from a performance perspective, but is needed to get accurate values
        // The result of the gdal warp algorithm is not the same for individual pixels probably due
        // to the resampling algorithm used
        // It is advised that clients perform the value lookop on the tiles themselves when a lot of
        // pixel values will be queried

        let band_nr = meta.band_nr.unwrap_or(1);
        let tile = Tile::for_coordinate(coord, meta.max_zoom);
        match meta.data_type {
            RasterDataType::Byte => {
                WarpingTileProvider::process_pixel_request::<u8>(meta, band_nr, tile, dpi_ratio, coord)
            }
            RasterDataType::Int32 => {
                WarpingTileProvider::process_pixel_request::<i32>(meta, band_nr, tile, dpi_ratio, coord)
            }
            RasterDataType::UInt32 => {
                WarpingTileProvider::process_pixel_request::<u32>(meta, band_nr, tile, dpi_ratio, coord)
            }
            RasterDataType::Float => {
                WarpingTileProvider::process_pixel_request::<f32>(meta, band_nr, tile, dpi_ratio, coord)
            }
        }
    }

    pub fn value_range_for_extent(
        layer_meta: &LayerMetadata,
        extent: LatLonBounds,
        _zoom: Option<i32>,
    ) -> Result<Range<f64>> {
        detect_raster_range(&layer_meta.path, layer_meta.band_nr.unwrap_or(1), extent)
    }

    fn layer_ref(&self, id: LayerId) -> Result<&LayerMetadata> {
        self.meta
            .iter()
            .find(|m| m.id == id)
            .ok_or(Error::InvalidArgument(format!("Invalid layer id: {}", id)))
    }
}

impl TileProvider for WarpingTileProvider {
    fn layers(&self) -> Vec<LayerMetadata> {
        self.meta.clone()
    }

    fn layer(&self, id: LayerId) -> Result<LayerMetadata> {
        self.meta
            .iter()
            .find(|m| m.id == id)
            .cloned()
            .ok_or(Error::InvalidArgument(format!("Invalid layer id: {}", id)))
    }

    fn extent_value_range(&self, id: LayerId, extent: LatLonBounds, zoom: Option<i32>) -> Result<std::ops::Range<f64>> {
        let layer_meta = self.layer_ref(id)?;
        WarpingTileProvider::value_range_for_extent(layer_meta, extent, zoom)
    }

    fn get_raster_value(&self, id: LayerId, coord: Coordinate, dpi_ratio: u8) -> Result<Option<f32>> {
        let layer_meta = self.layer_ref(id)?;
        WarpingTileProvider::raster_pixel(layer_meta, coord, dpi_ratio)
    }

    fn get_tile(&self, id: LayerId, tile_req: &TileRequest) -> Result<TileData> {
        let layer_meta = self.layer_ref(id)?;
        WarpingTileProvider::tile(layer_meta, tile_req)
    }

    fn get_tile_color_mapped(&self, id: LayerId, tile_req: &ColorMappedTileRequest) -> Result<TileData> {
        let layer_meta = self.layer_ref(id)?;
        WarpingTileProvider::color_mapped_tile(layer_meta, tile_req)
    }
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;
    use geo::{crs, Coordinate, Point, Tile};
    use inf::cast;
    use path_macro::path;
    use raster::{Cell, DenseRaster, Raster, RasterCreation, RasterSize};
    use raster_tile::RasterTileIO;

    use crate::{
        tileprovider::TileRequest, tileproviderfactory::TileProviderOptions, warpingtileprovider::WarpingTileProvider,
        Error, TileFormat, TileProvider,
    };

    fn test_raster() -> std::path::PathBuf {
        path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data" / "landusebyte.tif")
    }

    fn test_raster_web_mercator() -> std::path::PathBuf {
        path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data" / "landusebyte_3857.tif")
    }

    #[test]
    fn test_layer_metadata() -> Result<(), Error> {
        let provider = WarpingTileProvider::new(&test_raster(), &TileProviderOptions::default())?;
        let layer_id = provider.layers().first().unwrap().id;

        let meta = provider.layer(layer_id)?;
        assert_eq!(meta.nodata::<u8>(), Some(255));
        assert_eq!(meta.max_zoom, 10);
        assert_relative_eq!(meta.bounds[0], 2.52542882367258, epsilon = 1e-6);
        assert_relative_eq!(meta.bounds[1], 50.6774001192389, epsilon = 1e-6);
        assert_relative_eq!(meta.bounds[2], 5.91103418055685, epsilon = 1e-6);
        assert_relative_eq!(meta.bounds[3], 51.5002785754381, epsilon = 1e-6);

        Ok(())
    }

    #[test]
    fn test_provider_option_max_zoom() -> Result<(), Error> {
        {
            let provider = WarpingTileProvider::new(
                &test_raster(),
                &TileProviderOptions {
                    calculate_stats: false,
                    max_zoom_round_up: false,
                },
            )?;

            assert_eq!(10, provider.layers().first().unwrap().max_zoom);
        }
        {
            let provider = WarpingTileProvider::new(
                &test_raster(),
                &TileProviderOptions {
                    calculate_stats: false,
                    max_zoom_round_up: true,
                },
            )?;

            assert_eq!(11, provider.layers().first().unwrap().max_zoom);
        }

        Ok(())
    }

    #[test]
    #[cfg_attr(not(feature = "slow_tests"), ignore)] // This test is very slow but usefull when debugging the pixel values
    fn read_raster_pixel() -> Result<(), Error> {
        let provider = WarpingTileProvider::new(
            &test_raster(),
            &TileProviderOptions {
                calculate_stats: false,
                max_zoom_round_up: true,
            },
        )?;
        let layer_meta = provider.layers().first().unwrap().clone();

        let zoom = layer_meta.max_zoom;

        let tile = Tile::for_coordinate(Coordinate::latlon(51.046575, 4.344067), zoom);
        let tile_bounds = tile.web_mercator_bounds();
        let cell_size = Tile::pixel_size_at_zoom_level(zoom);

        let request = TileRequest {
            tile,
            dpi_ratio: 1,
            tile_format: TileFormat::RasterTile,
        };

        let tile_data = provider.get_tile(layer_meta.id, &request)?;
        let raster_tile = DenseRaster::<u8>::from_tile_bytes(&tile_data.data)?;
        let mut raster_tile_per_pixel = DenseRaster::<u8>::zeros(RasterSize::with_rows_cols(
            Tile::TILE_SIZE as usize * request.dpi_ratio as usize,
            Tile::TILE_SIZE as usize * request.dpi_ratio as usize,
        ));

        let current_coord = tile_bounds.top_left();

        for y in 0..Tile::TILE_SIZE * request.dpi_ratio as u16 {
            for x in 0..Tile::TILE_SIZE * request.dpi_ratio as u16 {
                let coord = Point::from((
                    current_coord.x() + (x as f64 * cell_size) + (cell_size / 2.0),
                    current_coord.y() - (y as f64 * cell_size) - (cell_size / 2.0),
                ));

                let val = provider.get_raster_value(layer_meta.id, crs::web_mercator_to_lat_lon(coord), 1)?;
                raster_tile_per_pixel.set_cell_value(Cell::from_row_col(y as i32, x as i32), cast::option(val));
            }
        }

        assert_eq!(raster_tile, raster_tile_per_pixel);

        Ok(())
    }

    #[test]
    #[cfg_attr(not(feature = "slow_tests"), ignore)] // This test is very slow but usefull when debugging the pixel values
    fn read_raster_pixel_web_mercator() -> Result<(), Error> {
        let provider = WarpingTileProvider::new(
            &test_raster_web_mercator(),
            &TileProviderOptions {
                calculate_stats: false,
                max_zoom_round_up: true,
            },
        )?;
        let layer_meta = provider.layers().first().unwrap().clone();

        let zoom = layer_meta.max_zoom;

        let tile = Tile::for_coordinate(Coordinate::latlon(51.046575, 4.344067), zoom);
        let tile_bounds = tile.web_mercator_bounds();
        let cell_size = Tile::pixel_size_at_zoom_level(zoom);

        let request = TileRequest {
            tile,
            dpi_ratio: 1,
            tile_format: TileFormat::RasterTile,
        };

        let tile_data = provider.get_tile(layer_meta.id, &request)?;
        let raster_tile = DenseRaster::<u8>::from_tile_bytes(&tile_data.data)?;
        let mut raster_tile_per_pixel = DenseRaster::<u8>::zeros(RasterSize::with_rows_cols(
            Tile::TILE_SIZE as usize * request.dpi_ratio as usize,
            Tile::TILE_SIZE as usize * request.dpi_ratio as usize,
        ));

        let current_coord = tile_bounds.top_left();

        for y in 0..Tile::TILE_SIZE * request.dpi_ratio as u16 {
            for x in 0..Tile::TILE_SIZE * request.dpi_ratio as u16 {
                let coord = Point::from((
                    current_coord.x() + (x as f64 * cell_size) + (cell_size / 2.0),
                    current_coord.y() - (y as f64 * cell_size) - (cell_size / 2.0),
                ));

                let val = provider.get_raster_value(layer_meta.id, crs::web_mercator_to_lat_lon(coord), 1)?;
                raster_tile_per_pixel.set_cell_value(Cell::from_row_col(y as i32, x as i32), cast::option(val));
            }
        }

        assert_eq!(raster_tile, raster_tile_per_pixel);

        Ok(())
    }

    #[test]
    fn test_nodata_outside_of_raster() -> Result<(), Error> {
        let provider = WarpingTileProvider::new(test_raster().as_path(), &TileProviderOptions::default())?;
        let layer_id = provider.layers().first().unwrap().id;

        assert_eq!(provider.meta[0].nodata::<u8>(), Some(255));

        let req = TileRequest {
            tile: Tile { x: 264, y: 171, z: 9 },
            dpi_ratio: 1,
            tile_format: TileFormat::Png,
        };

        let tile_data = provider.get_tile(layer_id, &req)?;

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

    #[test]
    fn test_vito_tile_format() -> Result<(), Error> {
        let provider = WarpingTileProvider::new(&test_raster(), &TileProviderOptions::default())?;
        let layer_id = provider.layers().first().unwrap().id;

        let req = TileRequest {
            tile: Tile { x: 264, y: 171, z: 9 },
            dpi_ratio: 1,
            tile_format: TileFormat::RasterTile,
        };

        let tile_data = provider.get_tile(layer_id, &req)?;

        let raster_tile = DenseRaster::<u8>::from_tile_bytes(&tile_data.data)?;
        assert_eq!(raster_tile.width(), 256);
        assert_eq!(raster_tile.height(), 256);

        Ok(())
    }

    #[test]
    fn test_netcdf_tile() -> Result<(), Error> {
        let netcdf_path = path!(env!("CARGO_MANIFEST_DIR") / "test" / "data" / "winddata.nc");
        let provider = WarpingTileProvider::new(
            &netcdf_path,
            &TileProviderOptions {
                calculate_stats: false,
                max_zoom_round_up: false,
            },
        )?;
        let layer_id = provider.layers().first().unwrap().id;

        let meta = provider.layer(layer_id)?;
        assert_eq!(meta.nodata::<f32>(), Some(1e+20));
        assert_eq!(meta.min_zoom, 0);
        assert_eq!(meta.max_zoom, 18);
        assert_relative_eq!(meta.bounds[0], -180.0, epsilon = 1e-6);
        assert_relative_eq!(meta.bounds[1], -90.0, epsilon = 1e-6);
        assert_relative_eq!(meta.bounds[2], 180.0, epsilon = 1e-6);
        assert_relative_eq!(meta.bounds[3], 90.0, epsilon = 1e-6);

        let req = TileRequest {
            tile: Tile { x: 0, y: 0, z: 0 },
            dpi_ratio: 1,
            tile_format: TileFormat::Png,
        };

        let tile_data = provider.get_tile(layer_id, &req);
        assert!(tile_data.is_ok());

        Ok(())
    }
}

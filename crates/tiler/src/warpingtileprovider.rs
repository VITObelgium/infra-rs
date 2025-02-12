use std::ops::Range;

use gdal::raster::GdalType;

use inf::legend::Legend;

use geo::raster::io::RasterFormat;
use geo::{crs, Columns, Coordinate, GeoReference, LatLonBounds, Rows, Tile};
use geo::{Array, ArrayDataType, ArrayNum, DenseArray, RasterSize};
use num::Num;
use raster_tile::{CompressionAlgorithm, RasterTileIO};

use crate::{
    imageprocessing::{self},
    layermetadata::{LayerId, LayerMetadata},
    tiledata::TileData,
    tileformat::TileFormat,
    tileio::{self, detect_raster_range},
    tileprovider::{ColorMappedTileRequest, TileProvider, TileRequest},
    tileproviderfactory::TileProviderOptions,
    Error, PixelFormat, Result,
};

fn raw_tile_to_vito_tile_format<T: ArrayNum<T>>(data: Vec<T>, width: Columns, height: Rows) -> Result<TileData> {
    let raster_tile = DenseArray::new(RasterSize::with_rows_cols(height, width), data)?;

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
            meta: tileio::create_metadata_for_file(path, opts)?,
        })
    }

    pub fn supports_raster_type(raster_type: RasterFormat) -> bool {
        matches!(raster_type, RasterFormat::GeoTiff | RasterFormat::Vrt | RasterFormat::Netcdf)
    }

    fn process_pixel_request<T>(meta: &LayerMetadata, band_nr: usize, tile: Tile, dpi_ratio: u8, coord: Coordinate) -> Result<Option<f32>>
    where
        T: ArrayNum<T> + Num + GdalType,
    {
        let raw_tile_data = tileio::read_tile_data::<T>(meta, band_nr, tile, dpi_ratio)?;
        if raw_tile_data.is_empty() {
            return Ok(None);
        }

        let tile_size = (Tile::TILE_SIZE * dpi_ratio as u16) as usize;
        let tile_meta = GeoReference::from_tile(&tile, tile_size, dpi_ratio);
        let cell = tile_meta.point_to_cell(crs::lat_lon_to_web_mercator(coord));

        match raw_tile_data.cell_value(cell) {
            Some(v) => Ok(v.to_f32()),
            None => Ok(None),
        }
    }

    fn process_tile_request<T>(meta: &LayerMetadata, band_nr: usize, req: &TileRequest) -> Result<TileData>
    where
        T: ArrayNum<T> + Num + GdalType,
    {
        let raw_tile_data = tileio::read_tile_data::<T>(meta, band_nr, req.tile, req.dpi_ratio)?;
        if raw_tile_data.is_empty() {
            return Ok(TileData::default());
        }

        match req.tile_format {
            TileFormat::Png => {
                // The default legend is with grayscale colors in range 0-255
                imageprocessing::raw_tile_to_png_color_mapped::<T>(
                    raw_tile_data.as_ref(),
                    (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
                    (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
                    Some(T::nodata_value()),
                    &Legend::default(),
                )
            }
            TileFormat::FloatEncodedPng => imageprocessing::raw_tile_to_float_encoded_png::<T>(
                raw_tile_data.as_ref(),
                (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
                (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
                Some(T::nodata_value()),
            ),
            TileFormat::RasterTile => {
                let (size, data) = raw_tile_data.into_raw_parts();
                raw_tile_to_vito_tile_format::<T>(data, size.cols, size.rows)
            }
            _ => Err(Error::InvalidArgument("Invalid pixel format".to_string())),
        }
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
            ArrayDataType::Int8 => WarpingTileProvider::process_tile_request::<i8>(layer_meta, band_nr, tile_req),
            ArrayDataType::Uint8 => WarpingTileProvider::process_tile_request::<u8>(layer_meta, band_nr, tile_req),
            ArrayDataType::Int16 => WarpingTileProvider::process_tile_request::<i16>(layer_meta, band_nr, tile_req),
            ArrayDataType::Uint16 => WarpingTileProvider::process_tile_request::<u16>(layer_meta, band_nr, tile_req),
            ArrayDataType::Int32 => WarpingTileProvider::process_tile_request::<i32>(layer_meta, band_nr, tile_req),
            ArrayDataType::Uint32 => WarpingTileProvider::process_tile_request::<u32>(layer_meta, band_nr, tile_req),
            ArrayDataType::Int64 => WarpingTileProvider::process_tile_request::<i64>(layer_meta, band_nr, tile_req),
            ArrayDataType::Uint64 => WarpingTileProvider::process_tile_request::<u64>(layer_meta, band_nr, tile_req),
            ArrayDataType::Float32 => WarpingTileProvider::process_tile_request::<f32>(layer_meta, band_nr, tile_req),
            ArrayDataType::Float64 => WarpingTileProvider::process_tile_request::<f64>(layer_meta, band_nr, tile_req),
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
            ArrayDataType::Int8 => tileio::read_color_mapped_tile_as_png::<i8>(layer_meta, band_nr, tile_req),
            ArrayDataType::Uint8 => tileio::read_color_mapped_tile_as_png::<u8>(layer_meta, band_nr, tile_req),
            ArrayDataType::Int16 => tileio::read_color_mapped_tile_as_png::<i16>(layer_meta, band_nr, tile_req),
            ArrayDataType::Uint16 => tileio::read_color_mapped_tile_as_png::<u16>(layer_meta, band_nr, tile_req),
            ArrayDataType::Int32 => tileio::read_color_mapped_tile_as_png::<i32>(layer_meta, band_nr, tile_req),
            ArrayDataType::Uint32 => tileio::read_color_mapped_tile_as_png::<u32>(layer_meta, band_nr, tile_req),
            ArrayDataType::Int64 => tileio::read_color_mapped_tile_as_png::<i64>(layer_meta, band_nr, tile_req),
            ArrayDataType::Uint64 => tileio::read_color_mapped_tile_as_png::<u64>(layer_meta, band_nr, tile_req),
            ArrayDataType::Float32 => tileio::read_color_mapped_tile_as_png::<f32>(layer_meta, band_nr, tile_req),
            ArrayDataType::Float64 => tileio::read_color_mapped_tile_as_png::<f64>(layer_meta, band_nr, tile_req),
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
            ArrayDataType::Int8 => WarpingTileProvider::process_pixel_request::<i8>(meta, band_nr, tile, dpi_ratio, coord),
            ArrayDataType::Uint8 => WarpingTileProvider::process_pixel_request::<u8>(meta, band_nr, tile, dpi_ratio, coord),
            ArrayDataType::Int16 => WarpingTileProvider::process_pixel_request::<i16>(meta, band_nr, tile, dpi_ratio, coord),
            ArrayDataType::Uint16 => WarpingTileProvider::process_pixel_request::<u16>(meta, band_nr, tile, dpi_ratio, coord),
            ArrayDataType::Int32 => WarpingTileProvider::process_pixel_request::<i32>(meta, band_nr, tile, dpi_ratio, coord),
            ArrayDataType::Uint32 => WarpingTileProvider::process_pixel_request::<u32>(meta, band_nr, tile, dpi_ratio, coord),
            ArrayDataType::Int64 => WarpingTileProvider::process_pixel_request::<i64>(meta, band_nr, tile, dpi_ratio, coord),
            ArrayDataType::Uint64 => WarpingTileProvider::process_pixel_request::<u64>(meta, band_nr, tile, dpi_ratio, coord),
            ArrayDataType::Float32 => WarpingTileProvider::process_pixel_request::<f32>(meta, band_nr, tile, dpi_ratio, coord),
            ArrayDataType::Float64 => WarpingTileProvider::process_pixel_request::<f64>(meta, band_nr, tile, dpi_ratio, coord),
        }
    }

    pub fn value_range_for_extent(layer_meta: &LayerMetadata, extent: LatLonBounds, _zoom: Option<i32>) -> Result<Range<f64>> {
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
    use geo::{crs, Columns, Coordinate, Point, Rows, Tile, ZoomLevelStrategy};
    use geo::{Array, Cell, DenseArray, RasterSize};
    use inf::cast;
    use path_macro::path;
    use raster_tile::RasterTileIO;

    use crate::{
        tileprovider::TileRequest, tileproviderfactory::TileProviderOptions, warpingtileprovider::WarpingTileProvider, Error, TileFormat,
        TileProvider,
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
                    zoom_level_strategy: ZoomLevelStrategy::PreferLower,
                },
            )?;

            assert_eq!(10, provider.layers().first().unwrap().max_zoom);
        }
        {
            let provider = WarpingTileProvider::new(
                &test_raster(),
                &TileProviderOptions {
                    calculate_stats: false,
                    zoom_level_strategy: ZoomLevelStrategy::PreferHigher,
                },
            )?;

            assert_eq!(11, provider.layers().first().unwrap().max_zoom);
        }

        Ok(())
    }

    #[test]
    // Marked as slow_test using the name convention to support filtering
    fn slow_test_read_raster_pixel() -> Result<(), Error> {
        let provider = WarpingTileProvider::new(
            &test_raster(),
            &TileProviderOptions {
                calculate_stats: false,
                zoom_level_strategy: ZoomLevelStrategy::PreferHigher,
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
        let raster_tile = DenseArray::<u8>::from_tile_bytes(&tile_data.data)?;
        let mut raster_tile_per_pixel = DenseArray::<u8>::zeros(RasterSize::with_rows_cols(
            Rows(Tile::TILE_SIZE as i32 * request.dpi_ratio as i32),
            Columns(Tile::TILE_SIZE as i32 * request.dpi_ratio as i32),
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
    // Marked as slow_test using the name convention to support filtering
    fn slow_test_read_raster_pixel_web_mercator() -> Result<(), Error> {
        let provider = WarpingTileProvider::new(
            &test_raster_web_mercator(),
            &TileProviderOptions {
                calculate_stats: false,
                zoom_level_strategy: ZoomLevelStrategy::PreferHigher,
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
        let raster_tile = DenseArray::<u8>::from_tile_bytes(&tile_data.data)?;
        let mut raster_tile_per_pixel = DenseArray::<u8>::zeros(RasterSize::with_rows_cols(
            Rows(Tile::TILE_SIZE as i32 * request.dpi_ratio as i32),
            Columns(Tile::TILE_SIZE as i32 * request.dpi_ratio as i32),
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
        let raw_data = image::load_from_memory(&tile_data.data).expect("Invalid image").to_rgba8();
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

        let raster_tile = DenseArray::<u8>::from_tile_bytes(&tile_data.data)?;
        assert_eq!(raster_tile.columns(), Columns(256));
        assert_eq!(raster_tile.rows(), Rows(256));

        Ok(())
    }

    #[test]
    fn test_netcdf_tile() -> Result<(), Error> {
        let netcdf_path = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data" / "winddata.nc");
        let provider = WarpingTileProvider::new(
            &netcdf_path,
            &TileProviderOptions {
                calculate_stats: false,
                zoom_level_strategy: ZoomLevelStrategy::PreferLower,
            },
        )?;
        let layer_id = provider.layers().first().unwrap().id;

        let meta = provider.layer(layer_id)?;
        assert_eq!(meta.nodata::<f32>(), Some(1e+20));
        assert_eq!(meta.min_zoom, 0);
        assert_eq!(meta.max_zoom, 1);
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

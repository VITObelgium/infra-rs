use std::{ops::Range, path::PathBuf};

use crate::{
    imageprocessing,
    layermetadata::to_raster_data_type,
    rasterprocessing::{metadata_bounds_wgs84, source_type_for_path},
    tileprovider,
    tileproviderfactory::TileProviderOptions,
    ColorMappedTileRequest, Error, LayerMetadata, Result, TileData, TileFormat,
};
use gdal::{
    raster::{GdalDataType, GdalType},
    Dataset,
};
use geo::{constants, crs, georaster, CellSize, GeoReference, LatLonBounds, SpatialReference, Tile};
use num::Num;
use raster::{DenseRaster, Raster, RasterCreation, RasterNum, RasterSize};

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

pub fn detect_raster_range(raster_path: &std::path::Path, band_nr: usize, bbox: LatLonBounds) -> Result<Range<f64>> {
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

pub fn read_raster_tile<T: RasterNum<T> + GdalType>(
    raster_path: &std::path::Path,
    band_nr: usize,
    tile: Tile,
    dpi_ratio: u8,
) -> Result<DenseRaster<T>> {
    let bounds = tile.web_mercator_bounds();
    let scaled_size = (Tile::TILE_SIZE * dpi_ratio as u16) as usize;

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
    let mut data = DenseRaster::zeros(RasterSize::with_rows_cols(scaled_size, scaled_size));
    let ds = georaster::algo::translate_file(raster_path, &output_path, &options)?;
    georaster::io::dataset::read_band(&ds, 1, data.as_mut())?;
    Ok(data)
}

pub fn read_raster_tile_warped<T: RasterNum<T> + GdalType>(
    raster_path: &std::path::Path,
    band_nr: usize,
    tile: Tile,
    dpi_ratio: u8,
) -> Result<DenseRaster<T>> {
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

    let src_ds = if raster_path.extension().is_some_and(|ext| ext == "nc") {
        let opts = vec!["PRESERVE_AXIS_UNIT_IN_CRS=YES"];
        geo::georaster::io::dataset::open_read_only_with_options(raster_path, &opts)?
    } else {
        geo::georaster::io::dataset::open_read_only(raster_path)?
    };

    let data = DenseRaster::filled_with_nodata(RasterSize::with_rows_cols(scaled_size, scaled_size));
    let mut dest_ds = georaster::io::dataset::create_in_memory_with_data::<T>(&dest_extent, data.as_ref())?;

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
    if !data.contains_data() {
        return Ok(DenseRaster::empty());
    }

    Ok(data)
}

#[cfg(feature = "vector-tiles")]
#[allow(dead_code)]
fn diff_tiles_as_vector<T: RasterNum<T> + GdalType>(
    layer_meta1: &LayerMetadata,
    layer_meta2: &LayerMetadata,
    tile: Tile,
) -> Result<TileData> {
    use gdal::vector::LayerAccess;
    use geo::Point;

    use crate::PixelFormat;

    let tile1 = read_tile_data::<T>(layer_meta1, 1, tile, 1)?;
    let tile2 = read_tile_data::<T>(layer_meta2, 1, tile, 1)?;

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

/// Read the raw tile data, result is a tuple with the raw data and the nodata value
pub fn read_tile_data<T: RasterNum<T> + Num + GdalType>(
    meta: &LayerMetadata,
    band_nr: usize,
    tile: Tile,
    dpi_ratio: u8,
) -> Result<DenseRaster<T>> {
    let start = std::time::Instant::now();

    let raw_tile_data = if !meta.source_is_web_mercator {
        read_raster_tile_warped(meta.path.as_path(), band_nr, tile, dpi_ratio)?
    } else {
        read_raster_tile(meta.path.as_path(), band_nr, tile, dpi_ratio)?
    };

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

    Ok(raw_tile_data)
}

pub fn read_color_mapped_tile_as_png<T>(
    meta: &LayerMetadata,
    band_nr: usize,
    req: &ColorMappedTileRequest,
) -> Result<TileData>
where
    T: RasterNum<T> + Num + GdalType,
{
    let raw_tile_data = read_tile_data::<T>(meta, band_nr, req.tile, req.dpi_ratio)?;
    if raw_tile_data.is_empty() {
        return Ok(TileData::default());
    }

    imageprocessing::raw_tile_to_png_color_mapped::<T>(
        raw_tile_data.as_slice(),
        (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
        (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
        Some(T::nodata_value()),
        req.legend,
    )
}

pub fn create_metadata_for_file(path: &std::path::Path, opts: &TileProviderOptions) -> Result<Vec<LayerMetadata>> {
    let ds = georaster::io::dataset::open_read_only(path)?;

    let raster_count = ds.raster_count();
    let mut result = Vec::with_capacity(raster_count);

    for band_nr in 1..=raster_count {
        let meta = georaster::io::dataset::read_band_metadata(&ds, band_nr)?;
        let raster_band = ds.rasterband(band_nr)?;
        let over_view_count = raster_band.overview_count()?;

        let (epsg, source_is_web_mercator, cell_size) = {
            if let Ok(mut srs) = SpatialReference::from_proj(meta.projection()) {
                let cell_size = if srs.is_projected() {
                    meta.cell_size_x()
                } else {
                    meta.cell_size_x() * constants::EARTH_CIRCUMFERENCE_M / 360.0
                };

                (
                    srs.epsg_cs(),
                    srs.is_projected() && srs.epsg_cs() == Some(crs::epsg::WGS84_WEB_MERCATOR),
                    cell_size,
                )
            } else {
                let cell_size = if meta.cell_size().x() < 1.0 {
                    // This is probably in degrees and not in meter
                    meta.cell_size().x() * constants::EARTH_CIRCUMFERENCE_M / 360.0
                } else {
                    meta.cell_size().x()
                };

                (None, false, cell_size)
            }
        };

        let zoom_level = Tile::zoom_level_for_pixel_size(cell_size, opts.zoom_level_strategy);

        let mut name = path
            .file_stem()
            .ok_or(Error::Runtime("No path stem".to_string()))?
            .to_string_lossy()
            .to_string();

        if raster_count > 1 {
            name.push_str(&format!(" - Band {:05}", band_nr));
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
            source_is_web_mercator,
            epsg,
            bounds: metadata_bounds_wgs84(meta).unwrap_or(LatLonBounds::world()).array(),
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

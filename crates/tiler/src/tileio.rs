use std::{ops::Range, path::PathBuf};

use crate::{
    imageprocessing,
    layermetadata::to_raster_data_type,
    rasterprocessing::{metadata_bounds_wgs84, source_type_for_path},
    tileprovider,
    tileproviderfactory::TileProviderOptions,
    ColorMappedTileRequest, Error, LayerMetadata, Result, TileData, TileFormat, TileRequest,
};
use gdal::{
    raster::{GdalDataType, GdalType},
    Dataset,
};
use geo::{crs, georaster, CellSize, GeoReference, LatLonBounds, SpatialReference, Tile};
use inf::Legend;
use num::Num;
use raster::{RasterNum, RasterSize};

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

pub fn read_raster_tile_warped<T: RasterNum<T> + GdalType>(
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

pub fn read_raster_tile_warped_no_resample<T: RasterNum<T> + GdalType>(
    raster_path: &std::path::Path,
    band_nr: usize,
    tile: Tile,
) -> Result<(Vec<T>, RasterSize)> {
    let bounds = tile.web_mercator_bounds();

    let src_ds = gdal::Dataset::open(raster_path)?;
    let mut dest_ds = georaster::io::dataset::create_in_memory(RasterSize { rows: 0, cols: 0 })?;

    let (xmin, ymin) = bounds.bottom_left().into();
    let (xmax, ymax) = bounds.top_right().into();

    let options = vec![
        "-b".to_string(),
        band_nr.to_string(),
        "-ovr".to_string(),
        "AUTO".to_string(),
        "-r".to_string(),
        "near".to_string(),
        "-tr".to_string(),
        "square".to_string(),
        "-te".to_string(),
        xmin.to_string(),
        ymin.to_string(),
        xmax.to_string(),
        ymax.to_string(),
    ];
    let key_value_options: Vec<(String, String)> = vec![
        ("INIT_DEST".to_string(), "NO_DATA".to_string()),
        ("SKIP_NOSOURCE".to_string(), "YES".to_string()),
        ("NUM_THREADS".to_string(), "ALL_CPUS".to_string()),
    ];

    georaster::algo::warp_cli(&src_ds, &mut dest_ds, &options, &key_value_options)?;

    let (shape, data) = dest_ds.rasterband(1)?.read_band_as::<T>()?.into_shape_and_vec();
    let size = RasterSize {
        rows: shape.0,
        cols: shape.1,
    };

    Ok((data, size))
}

/// Read the raw tile data, result is a tuple with the raw data and the nodata value
pub fn read_tile_data<T: RasterNum<T> + Num + GdalType>(
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

/// Read the raw tile data without resampling to TILE_SIZExTILE_SIZE, result is a tuple with the raw data and the nodata value
pub fn read_tile_data_no_resample<T: RasterNum<T> + Num + GdalType>(
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

pub fn read_tile_as_png<T>(meta: &LayerMetadata, band_nr: usize, req: &TileRequest) -> Result<TileData>
where
    T: RasterNum<T> + Num + GdalType,
{
    let (raw_tile_data, nodata) = read_tile_data::<T>(meta, band_nr, req.tile, req.dpi_ratio)?;
    if raw_tile_data.is_empty() {
        return Ok(TileData::default());
    }

    match req.tile_format {
        TileFormat::Png => {
            // The default legend is with grayscale colors in range 0-255
            imageprocessing::raw_tile_to_png_color_mapped::<T>(
                raw_tile_data.as_slice(),
                (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
                (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
                Some(nodata),
                &Legend::default(),
            )
        }
        TileFormat::FloatEncodedPng => imageprocessing::raw_tile_to_float_encoded_png::<T>(
            raw_tile_data.as_slice(),
            (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
            (Tile::TILE_SIZE * req.dpi_ratio as u16) as usize,
            Some(nodata),
        ),
        TileFormat::RasterTile => todo!(),
        _ => Err(Error::InvalidArgument("Invalid pixel format".to_string())),
    }
}

pub fn read_color_mapped_tile_as_png<T>(
    meta: &LayerMetadata,
    band_nr: usize,
    req: &ColorMappedTileRequest,
) -> Result<TileData>
where
    T: RasterNum<T> + Num + GdalType,
{
    let (raw_tile_data, nodata) = read_tile_data::<T>(meta, band_nr, req.tile, req.dpi_ratio)?;
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

pub fn create_metadata_for_file(
    path: &std::path::Path,
    opts: &TileProviderOptions,
    tile_format: TileFormat,
) -> Result<Vec<LayerMetadata>> {
    let ds = georaster::io::dataset::open_read_only(path)?;

    let raster_count = ds.raster_count();
    let mut result = Vec::with_capacity(raster_count);

    for band_nr in 1..=raster_count {
        let meta = georaster::io::dataset::read_band_metadata(&ds, band_nr)?;
        let raster_band = ds.rasterband(band_nr)?;
        let over_view_count = raster_band.overview_count()?;

        let mut srs = SpatialReference::from_definition(meta.projection())?;
        let zoom_level = Tile::zoom_level_for_pixel_size(meta.cell_size_x(), geo::ZoomLevelStrategy::PreferHigher);

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
            tile_format,
            source_is_web_mercator: srs.is_projected() && srs.epsg_cs() == Some(crs::epsg::WGS84_WEB_MERCATOR),
            epsg: srs.epsg_cs(),
            bounds: metadata_bounds_wgs84(meta)?.array(),
            description: String::new(),
            min_value: f64::NEG_INFINITY,
            max_value: f64::INFINITY,
            source_format: source_type_for_path(path),
            scheme: "xyz".to_string(),
            additional_data: Default::default(),
            band_nr: Some(band_nr),
            provider_data: None,
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

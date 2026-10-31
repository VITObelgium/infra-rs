use std::{ops::Range, path::PathBuf, sync::Arc};

use crate::{
    ColorMappedTileRequest, Error, LayerMetadata, Result, TileData, TileFormat, imageprocessing,
    layermetadata::to_raster_data_type,
    rasterprocessing::{metadata_bounds_wgs84, source_type_for_path},
    tileprovider,
    tileproviderfactory::TileProviderOptions,
};
use gdal::{
    Dataset, Metadata as _,
    raster::{GdalDataType, GdalType},
};
use geo::{
    Array, ArrayInterop, ArrayMetadata, ArrayNum, DenseArray, RasterMetadata, RasterSize,
    raster::formats::{self, RasterFileFormat, RasterOpenOptions},
};
use geo::{CellSize, Columns, GeoReference, LatLonBounds, Rows, Tile, constants, crs, raster, srs::SpatialReference};
use inf::allocate::AlignedVecUnderConstruction;
use num::Num;

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

    let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos();
    let output_path = PathBuf::from(format!(
        "/vsimem/range_{}_{}.mem",
        raster_path
            .file_stem()
            .ok_or(Error::Runtime("No filename".to_string()))?
            .to_string_lossy(),
        timestamp
    ));

    if let Ok(ds) = raster::algo::gdal::translate(&Dataset::open(raster_path)?, output_path.as_path(), &options)
        && let Ok(Some(stats)) = ds.rasterband(band_nr)?.get_statistics(true, true)
    {
        log::info!("Value range: [{:.2} <-> {:.2}]", stats.min, stats.max);
        return Ok(Range {
            start: stats.min,
            end: stats.max,
        });
    }

    Err(Error::Runtime(format!(
        "Failed to obtain value range for bbox: {} - {}",
        bbox.northwest(),
        bbox.southeast(),
    )))
}

pub fn read_raster_tile<T: ArrayNum + GdalType>(
    raster_path: &std::path::Path,
    band_nr: usize,
    tile: Tile,
    dpi_ratio: u8,
    tile_size: u32,
) -> Result<DenseArray<T>> {
    let bounds = tile.web_mercator_bounds();
    let scaled_size = (tile_size * dpi_ratio as u32) as i32;
    let src_ds = Dataset::open(raster_path)?;
    // Translate otherwise pads out-of-extent pixels with valid zero values.
    let nodata = src_ds
        .rasterband(band_nr)?
        .no_data_value()
        .map_or_else(|| T::NODATA.to_string(), |value| value.to_string());

    let options: Vec<String> = vec![
        "-b".to_string(),
        band_nr.to_string(),
        "-a_nodata".to_string(),
        nodata,
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
    let meta = RasterMetadata::sized_for_type::<T>(RasterSize::square(scaled_size));
    let ds = raster::algo::gdal::translate(&src_ds, &output_path, &options)?;

    let mut data = AlignedVecUnderConstruction::new(meta.raster_size.cell_count());
    let meta = formats::gdal::GdalRasterIO::from_dataset(ds).read_raster_band_as(1, data.as_uninit_slice_mut())?;
    Ok(DenseArray::new_init_nodata(RasterMetadata::with_geo_reference(meta), unsafe {
        data.assume_init()
    })?)
}

pub fn read_raster_tile_warped<T: ArrayNum + GdalType>(
    raster_path: &std::path::Path,
    band_nr: usize,
    tile: Tile,
    dpi_ratio: u8,
    tile_size: u32,
) -> Result<DenseArray<T>> {
    let bounds = tile.web_mercator_bounds();
    let scaled_size = (tile_size * dpi_ratio as u32) as i32;

    let projection = SpatialReference::from_epsg(crs::epsg::WGS84_WEB_MERCATOR)?;
    let dest_extent = GeoReference::with_bottom_left_origin(
        projection.to_wkt()?,
        RasterSize {
            rows: Rows(scaled_size),
            cols: Columns(scaled_size),
        },
        bounds.bottom_left(),
        CellSize::square(bounds.width() / scaled_size as f64),
        Some(T::NODATA),
    );

    let src_ds = if raster_path.extension().is_some_and(|ext| ext == "nc") {
        let options = RasterOpenOptions {
            driver_specific_options: Some(vec!["PRESERVE_AXIS_UNIT_IN_CRS=YES".to_string()]),
            ..Default::default()
        };

        formats::gdal::open_dataset_read_only_with_options(raster_path, &options)?
    } else {
        formats::gdal::open_dataset_read_only(raster_path)?
    };

    let meta = RasterMetadata::sized_for_type::<T>(RasterSize::square(scaled_size));
    let data = DenseArray::filled_with_nodata(meta);
    let mut dest_ds = raster::formats::gdal::create_in_memory_dataset_with_data::<T>(&dest_extent, data.as_ref())?;

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

    raster::algo::gdal::warp_cli(&src_ds, &mut dest_ds, &options, &key_value_options)?;

    // Avoid returning tiles containing only nodata values
    if !data.contains_data() {
        return Ok(DenseArray::empty());
    }

    Ok(data)
}

/// Synthesize a COG tile below its native minimum zoom with one warp for all bands.
/// The caller validates the band range, tile size and DPI ratio.
pub(crate) fn read_multiband_cog_tile_warped(
    path: &std::path::Path,
    req: &crate::TileRequest,
    bands: Range<usize>,
) -> Result<Vec<DenseArray<f32>>> {
    let src = formats::gdal::open_dataset_read_only(path)?;
    let size = req.tile_size as usize;
    let mut dst = multiband_tile_destination(req, bands.len())?;
    let mut options = vec![
        "-r".into(),
        "near".into(),
        "-ovr".into(),
        "AUTO".into(),
        "-dstnodata".into(),
        "nan".into(),
    ];
    for band in bands.clone() {
        options.extend(["-b".into(), band.to_string()]);
    }
    let warp_options = vec![("INIT_DEST".into(), "NO_DATA".into()), ("UNIFIED_SRC_NODATA".into(), "NO".into())];
    raster::algo::gdal::warp_cli(&src, &mut dst, &options, &warp_options)?;
    (1..=bands.len())
        .map(|band| {
            let buffer = dst.rasterband(band)?.read_as::<f32>((0, 0), (size, size), (size, size), None)?;
            let array = DenseArray::new_init_nodata(
                RasterMetadata::sized_with_nodata(RasterSize::square(req.tile_size as i32), Some(f64::NAN)),
                inf::allocate::aligned_vec_from_iter(buffer.into_shape_and_vec().1),
            )?;
            Ok(if array.contains_data() { array } else { DenseArray::empty() })
        })
        .collect()
}

fn multiband_tile_destination(req: &crate::TileRequest, band_count: usize) -> Result<Dataset> {
    let size = req.tile_size as usize;
    let georef = GeoReference::from_tile(&req.tile, size, 1);
    let mut dst = gdal::DriverManager::get_driver_by_name("MEM")?.create_with_band_type::<f32, _>("", size, size, band_count)?;
    dst.set_geo_transform(&georef.geo_transform().into())?;
    dst.set_projection(georef.projection())?;
    for band in 1..=band_count {
        let mut band = dst.rasterband(band)?;
        band.set_no_data_value(Some(f64::NAN))?;
        // GDAL can skip fully masked source regions, leaving the destination untouched.
        band.fill(f64::NAN, None)?;
    }
    Ok(dst)
}

/// Read the raw tile data, result is a tuple with the raw data and the nodata value
pub fn read_tile_data<T: ArrayNum + Num + GdalType>(
    meta: &LayerMetadata,
    band_nr: usize,
    tile: Tile,
    dpi_ratio: u8,
    tile_size: u32,
) -> Result<DenseArray<T>> {
    let start = std::time::Instant::now();

    let raw_tile_data = if !meta.source_is_web_mercator {
        read_raster_tile_warped(meta.path.as_path(), band_nr, tile, dpi_ratio, tile_size)?
    } else {
        read_raster_tile(meta.path.as_path(), band_nr, tile, dpi_ratio, tile_size)?
    };

    //[cfg(TILESERVER_VERBOSE)]
    log::debug!(
        "[{}/{}/{}@{}] {} took {}ms (data type: {}) [{:?}]",
        tile.z(),
        tile.x(),
        tile.y(),
        dpi_ratio,
        if meta.source_is_web_mercator { "Translate" } else { "Warp" },
        start.elapsed().as_millis(),
        type_string::<T>(),
        std::thread::current().id(),
    );

    if start.elapsed().as_secs() > 10 {
        log::warn!("Slow tile: {}/{}/{}", tile.z(), tile.x(), tile.y());
    }

    Ok(raw_tile_data)
}

pub fn read_color_mapped_tile_as_png<T>(meta: &LayerMetadata, band_nr: usize, req: &ColorMappedTileRequest) -> Result<TileData>
where
    T: ArrayNum + Num + GdalType,
{
    let raw_tile_data = read_tile_data::<T>(meta, band_nr, req.tile, req.dpi_ratio, req.tile_size)?;
    if raw_tile_data.is_empty() {
        return Ok(TileData::default());
    }

    imageprocessing::raw_tile_to_png_color_mapped::<T>(
        raw_tile_data.as_slice(),
        (req.tile_size * req.dpi_ratio as u32) as usize,
        (req.tile_size * req.dpi_ratio as u32) as usize,
        Some(T::NODATA),
        req.legend,
    )
}

pub fn create_metadata_for_file(path: &std::path::Path, opts: &TileProviderOptions) -> Result<Vec<LayerMetadata>> {
    let ds = formats::gdal::open_dataset_read_only(path)?;
    let raster_count = ds.raster_count();
    if raster_count == 0
        && RasterFileFormat::guess_from_path(path) == RasterFileFormat::Netcdf
        && let Some(subdatasets) = ds.metadata_domain("SUBDATASETS")
    {
        return Ok(metadata_for_subdatasets(subdatasets, opts));
    }
    if raster_count == 0 {
        return Ok(Vec::new());
    }
    // GDAL's affine transform, CRS and dimensions belong to the dataset, not
    // individual bands. Building a PROJ transformation per time slice can
    // dominate opening large NetCDF time series with non-Mercator CRSs.
    let spatial = raster_spatial_metadata(formats::gdal::read_band_metadata(&ds, 1)?, opts);
    let mut result = Vec::with_capacity(raster_count);
    for band_nr in 1..=raster_count {
        result.push(metadata_for_band(&ds, path, band_nr, &spatial, opts)?);
    }
    log::debug!("Serving file: {path:?}");
    Ok(result)
}

fn metadata_for_subdatasets(subdatasets: Vec<String>, opts: &TileProviderOptions) -> Vec<LayerMetadata> {
    let mut result = Vec::new();
    for meta in subdatasets {
        if let Some((_, path)) = meta.split_once('=') {
            match create_metadata_for_file(std::path::Path::new(path), opts) {
                Err(error) => log::warn!("Failed to read subdataset {path}: {error}"),
                Ok(metadata) => result.extend(metadata),
            }
        }
    }
    result
}

struct RasterSpatialMetadata {
    epsg: Option<crs::Epsg>,
    source_is_web_mercator: bool,
    zoom_level: i32,
    bounds: [f64; 4],
    geo_reference: Arc<GeoReference>,
}

fn raster_spatial_metadata(meta: GeoReference, opts: &TileProviderOptions) -> RasterSpatialMetadata {
    let (epsg, source_is_web_mercator, cell_size) = if let Ok(srs) = SpatialReference::from_definition(meta.projection()) {
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
            // This is probably in degrees and not in meter.
            meta.cell_size().x() * constants::EARTH_CIRCUMFERENCE_M / 360.0
        } else {
            meta.cell_size().x()
        };
        (None, false, cell_size)
    };
    RasterSpatialMetadata {
        epsg,
        source_is_web_mercator,
        zoom_level: Tile::zoom_level_for_pixel_size(cell_size, opts.zoom_level_strategy, Tile::TILE_SIZE),
        bounds: metadata_bounds_wgs84(meta.clone()).unwrap_or(LatLonBounds::world()).array(),
        geo_reference: Arc::new(meta),
    }
}

fn metadata_for_band(
    ds: &Dataset,
    path: &std::path::Path,
    band_nr: usize,
    spatial: &RasterSpatialMetadata,
    opts: &TileProviderOptions,
) -> Result<LayerMetadata> {
    let raster_band = ds.rasterband(band_nr)?;
    let over_view_count = raster_band.overview_count()?;
    let mut name = path
        .file_stem()
        .ok_or(Error::Runtime("No path stem".to_string()))?
        .to_string_lossy()
        .to_string();
    if ds.raster_count() > 1 {
        name.push_str(&format!(" - Band {band_nr:05}"));
    }
    let mut metadata = LayerMetadata {
        id: tileprovider::unique_layer_id(),
        data_type: to_raster_data_type(raster_band.band_type()),
        url: String::default(),
        path: path.to_path_buf(),
        name,
        max_zoom: spatial.zoom_level,
        min_zoom: if over_view_count > 0 {
            spatial.zoom_level - over_view_count
        } else {
            0
        },
        nodata: raster_band.no_data_value(),
        scale: match (raster_band.scale(), raster_band.offset()) {
            (None, None) => None,
            (scale, offset) => Some(geo::RasterScale {
                scale: scale.unwrap_or(1.0),
                offset: offset.unwrap_or(0.0),
            }),
        },
        supports_dpi_ratio: true,
        tile_size: None, // Tile size is determined by the request.
        tile_format: TileFormat::Png,
        source_is_web_mercator: spatial.source_is_web_mercator,
        epsg: spatial.epsg,
        bounds: spatial.bounds,
        description: String::new(),
        min_value: f32::NAN,
        max_value: f32::NAN,
        source_format: source_type_for_path(path),
        scheme: "xyz".to_string(),
        additional_data: Default::default(),
        band_nr: Some(band_nr),
        geo_reference: Some(spatial.geo_reference.clone()),
        tileprovider_data: None,
    };
    if opts.calculate_stats {
        update_band_statistics(&raster_band, &mut metadata);
    }
    Ok(metadata)
}

fn update_band_statistics(band: &gdal::raster::RasterBand<'_>, metadata: &mut LayerMetadata) {
    let allow_approximation = band.x_size() * band.y_size() > 10000000;
    let force = cfg!(not(debug_assertions));
    match band.get_statistics(force, allow_approximation) {
        Ok(Some(stats)) => {
            metadata.min_value = stats.min as f32;
            metadata.max_value = stats.max as f32;
        }
        Ok(None) => {
            log::warn!("No statistics available for band {:?}", metadata.band_nr);
            metadata.min_value = 0.0;
            metadata.max_value = f32::MAX;
        }
        Err(error) => log::warn!("Failed to calculate statistics: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_spatial_metadata_keeps_each_bands_identity_nodata_scale_and_statistics() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("projected-series.vrt");
        write_multiband_metadata_fixture(&path)?;
        let options = TileProviderOptions {
            calculate_stats: true,
            ..Default::default()
        };
        let layers = create_metadata_for_file(&path, &options)?;
        assert_eq!(layers.len(), 3);
        let bounds = layers[0].bounds;
        assert!((-10.0..-8.0).contains(&bounds[0]) && (38.0..40.0).contains(&bounds[1]));
        for (index, layer) in layers.iter().enumerate() {
            let band = index + 1;
            assert_eq!(layer.band_nr, Some(band));
            assert_eq!(layer.path, path);
            assert_eq!(layer.epsg, Some(crs::Epsg::new(3035)));
            assert_eq!(layer.bounds, bounds);
            assert_eq!(layer.max_zoom, layers[0].max_zoom);
            assert_eq!(layer.nodata, Some(-9999.0 - band as f64));
            assert_eq!(
                layer.scale,
                Some(geo::RasterScale {
                    scale: band as f64 * 2.0,
                    offset: band as f64 * 100.0
                })
            );
            assert_eq!((layer.min_value, layer.max_value), (band as f32 * 10.0, band as f32 * 10.0 + 5.0));
        }
        assert!(layers.windows(2).all(|pair| pair[0].id != pair[1].id));
        assert_eq!(layers[1].data_type, geo::ArrayDataType::Float64);
        Ok(())
    }

    fn write_multiband_metadata_fixture(path: &std::path::Path) -> Result<()> {
        let bands: String = (1..=3)
            .map(|band| {
                let data_type = if band == 2 { "Float64" } else { "Float32" };
                let min = band * 10;
                format!(
                    "<VRTRasterBand dataType=\"{data_type}\" band=\"{band}\">\
                     <NoDataValue>{}</NoDataValue><Scale>{}</Scale><Offset>{}</Offset>\
                     <Metadata><MDI key=\"STATISTICS_MINIMUM\">{min}</MDI>\
                     <MDI key=\"STATISTICS_MAXIMUM\">{}</MDI><MDI key=\"STATISTICS_MEAN\">{min}</MDI>\
                     <MDI key=\"STATISTICS_STDDEV\">1</MDI></Metadata></VRTRasterBand>",
                    -9999 - band,
                    band * 2,
                    band * 100,
                    min + 5,
                )
            })
            .collect();
        std::fs::write(
            path,
            format!(
                "<VRTDataset rasterXSize=\"301\" rasterYSize=\"301\"><SRS>EPSG:3035</SRS>\
             <GeoTransform>2635554,300,0,1997575,0,-300</GeoTransform>{bands}</VRTDataset>"
            ),
        )?;
        Ok(())
    }
}

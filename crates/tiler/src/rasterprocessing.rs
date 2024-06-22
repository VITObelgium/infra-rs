use raster::io::{guess_raster_format_from_filename, RasterFormat};
use std::path::Path;

use inf::{
    crs::{self, web_mercator_to_lat_lon},
    spatialreference::SpatialReference,
    Coordinate, CoordinateTransformer, GeoMetadata, LatLonBounds, Point, RasterSize,
};

use crate::{layermetadata::LayerSourceType, Result};

fn read_pixel_from_file(raster_path: &Path, coord: Point<f64>) -> Result<Option<f32>> {
    let ds = raster::io::open_read_only(raster_path)?;
    let mut meta = raster::io::metadata_from_dataset_band(&ds, 1)?;
    let cell = meta.point_to_cell(coord);
    if !meta.is_cell_on_map(cell) {
        return Ok(None);
    }

    // Modify the metadata to only contain the pixel at the given coordinate
    let ll = meta.cell_lower_left(cell);
    meta.set_extent(ll, RasterSize { rows: 1, cols: 1 }, meta.cell_size());
    let mut data = [0.0];

    raster::io::data_from_dataset_with_extent(&ds, &meta, 1, &mut data)?;
    if Some(f64::from(data[0])) == meta.nodata() {
        return Ok(None);
    }

    Ok(Some(data[0]))
}

pub fn raster_pixel(raster_path: &Path, mut coord: Coordinate, layer_name: Option<&str>) -> Result<Option<f32>> {
    let mut open_opt: Vec<String> = Vec::new();
    if let Some(layer_name) = layer_name {
        open_opt.push(format!("TABLE={}", layer_name));
    }

    let meta = raster::io::metadata_from_file_with_options(raster_path, &open_opt)?;
    let srs = SpatialReference::from_proj(meta.projection())?;
    if !srs.is_geographic() || srs.epsg_geog_cs() != Some(crs::epsg::WGS84) {
        let transformer = CoordinateTransformer::new(
            SpatialReference::from_epsg(crs::epsg::WGS84)?,
            SpatialReference::from_proj(meta.projection())?,
        )?;
        transformer.transform_coordinate_in_place(&mut coord)?;
    }

    read_pixel_from_file(raster_path, coord.into())
}

pub fn metadata_bounds_wgs84(meta: GeoMetadata) -> Result<LatLonBounds> {
    let mut srs = SpatialReference::from_proj(meta.projection())?;
    let mut result = LatLonBounds::hull(meta.top_left().into(), meta.bottom_right().into());

    if srs.is_projected() {
        if srs.epsg_cs() == Some(crs::epsg::WGS84_WEB_MERCATOR) {
            result = LatLonBounds::hull(
                web_mercator_to_lat_lon(meta.top_left()),
                web_mercator_to_lat_lon(meta.bottom_right()),
            );
        } else {
            let transformer = CoordinateTransformer::new(srs, SpatialReference::from_epsg(crs::epsg::WGS84)?)?;
            result = LatLonBounds::hull(
                transformer.transform_point(meta.top_left())?.into(),
                transformer.transform_point(meta.bottom_right())?.into(),
            );
        }
    } else if srs.epsg_geog_cs() != Some(crs::epsg::WGS84) {
        let transformer = CoordinateTransformer::new(srs, SpatialReference::from_epsg(crs::epsg::WGS84)?)?;
        result = LatLonBounds::hull(
            transformer.transform_point(meta.top_left())?.into(),
            transformer.transform_point(meta.bottom_right())?.into(),
        );
    }

    Ok(result)
}

pub fn source_type_for_path(path: &std::path::Path) -> LayerSourceType {
    match guess_raster_format_from_filename(path) {
        RasterFormat::ArcAscii => LayerSourceType::ArcAscii,
        RasterFormat::GeoTiff => LayerSourceType::GeoTiff,
        RasterFormat::MBTiles => LayerSourceType::Mbtiles,
        RasterFormat::GeoPackage => LayerSourceType::GeoPackage,
        _ => LayerSourceType::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use inf::{gdalinterop, CellSize};

    fn test_raster() -> std::path::PathBuf {
        [env!("CARGO_MANIFEST_DIR"), "test", "data", "landusebyte.tif"]
            .iter()
            .collect()
    }

    #[ctor::ctor]
    fn init() {
        let data_dir = [env!("CARGO_MANIFEST_DIR"), "..", "..", "target", "data"]
            .iter()
            .collect();

        let gdal_config = gdalinterop::Config {
            debug_logging: false,
            proj_db_search_location: data_dir,
        };

        gdal_config.apply().expect("Failed to configure GDAL");
    }

    #[test]
    fn raster_bounds_invalid_projection_info() {
        let projection = "LOCAL_CS[\"Amersfoort / RD New\",UNIT[\"metre\",1,AUTHORITY[\"EPSG\",\"9001\"]],AXIS[\"Easting\",EAST],AXIS[\"Northing\",NORTH]]";

        let meta = GeoMetadata::with_origin(
            projection,
            RasterSize { rows: 3250, cols: 2700 },
            Point::new(10000.0, 300000.0),
            CellSize::square(100.0),
            Option::<f64>::None,
        );

        assert!(metadata_bounds_wgs84(meta).is_err());
    }

    #[test]
    fn test_raster_pixel() {
        let result = raster_pixel(test_raster().as_path(), Coordinate::latlon(51.06, 4.52), None).unwrap();
        assert_eq!(result, Some(83.0));
        let result = raster_pixel(test_raster().as_path(), Coordinate::latlon(51.06, 3.8), None).unwrap();
        assert_eq!(result, Some(42.0));
    }

    #[test]
    fn test_raster_pixel_outside_of_raster_extent() {
        assert!(
            raster_pixel(test_raster().as_path(), Coordinate::latlon(50.3, 4.7), None)
                .unwrap()
                .is_none()
        );
        assert!(
            raster_pixel(test_raster().as_path(), Coordinate::latlon(52.0, 4.2), None)
                .unwrap()
                .is_none()
        );
        assert!(
            raster_pixel(test_raster().as_path(), Coordinate::latlon(51.0, 7.0), None)
                .unwrap()
                .is_none()
        );
        assert!(
            raster_pixel(test_raster().as_path(), Coordinate::latlon(51.0, 1.8), None)
                .unwrap()
                .is_none()
        );
    }
}

use geo::{
    Columns, RasterSize, Rows,
    raster::{self, io::RasterFormat},
};
use std::path::Path;

use geo::{
    Coordinate, CoordinateTransformer, GeoReference, LatLonBounds, Point, SpatialReference,
    crs::{self, web_mercator_to_lat_lon},
};

use crate::{Error, Result, layermetadata::LayerSourceType};

fn read_pixel_from_file(raster_path: &Path, band_nr: usize, coord: Point<f64>) -> Result<Option<f32>> {
    let ds = raster::io::dataset::open_read_only(raster_path)?;
    let mut meta = raster::io::dataset::read_band_metadata(&ds, band_nr)?;
    let cell = meta.point_to_cell(coord);
    if !meta.is_cell_on_map(cell) {
        return Ok(None);
    }

    // Modify the metadata to only contain the pixel at the given coordinate
    let ll = meta.cell_lower_left(cell);
    meta.set_extent(ll, RasterSize::with_rows_cols(Rows(1), Columns(1)), meta.cell_size());
    let mut data = [0.0];

    raster::io::dataset::read_band_region(&ds, band_nr, &meta, &mut data)?;
    if Some(f64::from(data[0])) == meta.nodata() {
        return Ok(None);
    }

    Ok(Some(data[0]))
}

pub fn raster_pixel(raster_path: &Path, band_nr: usize, mut coord: Coordinate, layer_name: Option<&str>) -> Result<Option<f32>> {
    let mut open_opt: Vec<String> = Vec::new();
    if let Some(layer_name) = layer_name {
        open_opt.push(format!("TABLE={layer_name}"));
    }

    let meta = raster::io::dataset::read_file_metadata_with_options(raster_path, &open_opt)?;
    let srs = SpatialReference::from_definition(meta.projection())?;
    if !srs.is_geographic() || srs.epsg_geog_cs() != Some(crs::epsg::WGS84) {
        let transformer = CoordinateTransformer::new(
            SpatialReference::from_epsg(crs::epsg::WGS84)?,
            SpatialReference::from_definition(meta.projection())?,
        )?;
        transformer.transform_coordinate_in_place(&mut coord)?;
    }

    read_pixel_from_file(raster_path, band_nr, coord.into())
}

pub fn metadata_bounds_wgs84(meta: GeoReference) -> Result<LatLonBounds> {
    if meta.projection().is_empty() {
        let top_left: Coordinate = meta.top_left().into();
        let bottom_right: Coordinate = meta.bottom_right().into();
        if top_left.is_valid() && bottom_right.is_valid() {
            Ok(LatLonBounds::hull(meta.top_left().into(), meta.bottom_right().into()))
        } else {
            Err(Error::Runtime("Could not calculate bounds".to_string()))
        }
    } else {
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
}

pub fn source_type_for_path(path: &std::path::Path) -> LayerSourceType {
    match RasterFormat::guess_from_path(path) {
        RasterFormat::ArcAscii => LayerSourceType::ArcAscii,
        RasterFormat::GeoTiff => LayerSourceType::GeoTiff,
        RasterFormat::MBTiles => LayerSourceType::Mbtiles,
        RasterFormat::GeoPackage => LayerSourceType::GeoPackage,
        RasterFormat::Netcdf => LayerSourceType::Netcdf,
        _ => LayerSourceType::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use geo::CellSize;
    use path_macro::path;

    fn test_raster() -> std::path::PathBuf {
        path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "tests" / "data" / "landusebyte.tif")
    }

    #[test]
    fn raster_bounds_invalid_projection_info() {
        let projection = "LOCAL_CS[\"Amersfoort / RD New\",UNIT[\"metre\",1,AUTHORITY[\"EPSG\",\"9001\"]],AXIS[\"Easting\",EAST],AXIS[\"Northing\",NORTH]]";

        let meta = GeoReference::with_origin(
            projection,
            RasterSize::with_rows_cols(Rows(3250), Columns(2700)),
            Point::new(10000.0, 300000.0),
            CellSize::square(100.0),
            Option::<f64>::None,
        );

        assert!(metadata_bounds_wgs84(meta).is_err());
    }

    #[test]
    fn test_raster_pixel() {
        let result = raster_pixel(&test_raster(), 1, Coordinate::latlon(51.06, 4.52), None).unwrap();
        assert_eq!(result, Some(83.0));
        let result = raster_pixel(&test_raster(), 1, Coordinate::latlon(51.06, 3.8), None).unwrap();
        assert_eq!(result, Some(42.0));
    }

    #[test]
    fn test_raster_pixel_outside_of_raster_extent() {
        assert!(
            raster_pixel(&test_raster(), 1, Coordinate::latlon(50.3, 4.7), None)
                .unwrap()
                .is_none()
        );
        assert!(
            raster_pixel(&test_raster(), 1, Coordinate::latlon(52.0, 4.2), None)
                .unwrap()
                .is_none()
        );
        assert!(
            raster_pixel(&test_raster(), 1, Coordinate::latlon(51.0, 7.0), None)
                .unwrap()
                .is_none()
        );
        assert!(
            raster_pixel(&test_raster(), 1, Coordinate::latlon(51.0, 1.8), None)
                .unwrap()
                .is_none()
        );
    }
}

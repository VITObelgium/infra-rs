use crate::{
    Array, ArrayNum, Cell, CellIterator, CellSize, CoordinateTransformer, GeoReference, Point, RasterSize, Rect, Result,
    coordinatetransformer::Points, crs, point, raster::DenseRaster,
};

const DEFAULT_EDGE_POINTS: usize = 10;

// enum WarpTarget {
//     Sized(RasterSize),
//     CellSize(CellSize)
//     Regular,
// }

/// Reproject a bounding box to a different coordinate system with configurable edge sampling
///
/// This function transforms a bounding box from one coordinate system to another by sampling
/// points along the edges and finding the bounding box of all transformed points. This is more
/// accurate than just transforming the four corners, especially when the transformation involves
/// significant curvature or distortion.
fn reproject_bounding_box_with_edge_sampling(
    bbox: &Rect<f64>,
    coord_trans: &CoordinateTransformer,
    edge_points: usize,
) -> Result<Rect<f64>> {
    // Ensure we have at least 2 points per edge (corners)
    let points_per_edge = edge_points.max(2);

    let mut all_points = Points::with_capacity(points_per_edge * 4);

    // Generate points along each edge
    // Top edge (left to right)
    for i in 0..points_per_edge {
        let mut t = i as f64 / (points_per_edge - 1) as f64;
        if t > 0.99 {
            t = 1.0;
        }

        let x = bbox.top_left().x() + t * (bbox.top_right().x() - bbox.top_left().x());
        let y = bbox.top_left().y();
        all_points.push(Point::new(x, y));
    }

    // Right edge (top to bottom, excluding corners already added)
    for i in 1..points_per_edge - 1 {
        let mut t = i as f64 / (points_per_edge - 1) as f64;
        if t > 0.99 {
            t = 1.0;
        }
        let x = bbox.top_right().x();
        let y = bbox.top_right().y() + t * (bbox.bottom_right().y() - bbox.top_right().y());
        all_points.push(Point::new(x, y));
    }

    // Bottom edge (right to left, excluding corner already added)
    for i in 1..points_per_edge {
        let mut t = i as f64 / (points_per_edge - 1) as f64;
        if t > 0.99 {
            t = 1.0;
        }
        let x = bbox.bottom_right().x() - t * (bbox.bottom_right().x() - bbox.bottom_left().x());
        let y = bbox.bottom_right().y();
        all_points.push(Point::new(x, y));
    }

    // Left edge (bottom to top, excluding corners already added)
    for i in 1..points_per_edge - 1 {
        let mut t = i as f64 / (points_per_edge - 1) as f64;
        if t > 0.99 {
            t = 1.0;
        }
        let x = bbox.bottom_left().x();
        let y = bbox.bottom_left().y() - t * (bbox.bottom_left().y() - bbox.top_left().y());
        all_points.push(Point::new(x, y));
    }

    // Transform all points
    coord_trans.transform_points_in_place(&mut all_points)?;

    // Find the bounding box of all transformed points
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;

    for point in all_points.iter() {
        min_x = min_x.min(point.x());
        max_x = max_x.max(point.x());
        min_y = min_y.min(point.y());
        max_y = max_y.max(point.y());
    }

    let top_left = Point::new(min_x, max_y);
    let bottom_right = Point::new(max_x, min_y);
    Ok(Rect::from_nw_se(top_left, bottom_right))
}

pub fn reproject_georef_to_epsg(georef: &GeoReference, epsg: crs::Epsg) -> Result<GeoReference> {
    reproject_georef_to_epsg_with_edge_points(georef, epsg, DEFAULT_EDGE_POINTS)
}

/// Reproject a `GeoReference` to a different EPSG with configurable edge sampling
///
/// # Arguments
/// * `georef` - The source `GeoReference` to reproject
/// * `epsg` - The target EPSG coordinate system
/// * `edge_points` - Number of points to sample along each edge of the bounding box for more accurate reprojection
fn reproject_georef_to_epsg_with_edge_points(georef: &GeoReference, epsg: crs::Epsg, edge_points: usize) -> Result<GeoReference> {
    let coord_trans = CoordinateTransformer::from_epsg(georef.projected_epsg().unwrap(), epsg)?;
    let src_bbox = georef.bounding_box();

    // First reproject the source bounding box by sampling edge points
    // This is more accurate than just transforming the four corners, especially for complex projections
    let bbox = reproject_bounding_box_with_edge_sampling(&src_bbox, &coord_trans, edge_points)?;

    // Calculate optimal resolution by determining the diagonal distance in the source and destination coordinate systems
    let resolution = calculate_optimal_resolution(georef, &coord_trans)?;
    let aligned_bbox = calculate_reprojected_bounds(&bbox, resolution);

    // Calculate dimensions with slight tolerance to handle floating point precision
    let height_pixels = aligned_bbox.height() / resolution;
    let width_pixels = aligned_bbox.width() / resolution;

    // Add small epsilon to handle cases where we're very close to the next integer
    const EPSILON: f64 = 1e-10;
    let rows = if (height_pixels - height_pixels.round()).abs() < EPSILON {
        height_pixels.round() as i32
    } else {
        height_pixels.ceil() as i32
    };

    let cols = if (width_pixels - width_pixels.round()).abs() < EPSILON {
        width_pixels.round() as i32
    } else {
        width_pixels.ceil() as i32
    };

    Ok(GeoReference::with_origin(
        epsg.to_string(),
        RasterSize::with_rows_cols(rows.into(), cols.into()),
        aligned_bbox.bottom_left(),
        CellSize::square(resolution),
        georef.nodata(),
    ))
}

fn calculate_optimal_resolution(georef: &GeoReference, coord_trans: &CoordinateTransformer) -> Result<f64> {
    let src_bbox = georef.bounding_box();
    let src_cell_size = georef.cell_size().x();

    // calculate the covered pixels of the diagonal of the source bounding box
    let src_diagonal = point::euclidenan_distance(src_bbox.top_left(), src_bbox.bottom_right());
    let src_diagonal_pixels = src_diagonal / src_cell_size;

    // Calculate the diagonal distance in the destination coordinate system to cover the same amount of pixels
    let dst_tl = coord_trans.transform_point(src_bbox.top_left())?;
    let dst_br = coord_trans.transform_point(src_bbox.bottom_right())?;
    let dst_diagonal = point::euclidenan_distance(dst_tl, dst_br);

    Ok(dst_diagonal / src_diagonal_pixels)
}

fn calculate_reprojected_bounds(bbox: &Rect<f64>, resolution: f64) -> Rect<f64> {
    let width_pixels = (bbox.width() / resolution).round();
    let height_pixels = (bbox.height() / resolution).round();

    // Calculate new dimensions based on exact pixel counts
    let new_width = width_pixels * resolution;
    let new_height = height_pixels * resolution;

    // Center the bounds to maintain the same overall coverage
    let center_x = (bbox.top_left().x() + bbox.bottom_right().x()) / 2.0;
    let center_y = (bbox.top_left().y() + bbox.bottom_right().y()) / 2.0;

    let min_x = center_x - new_width / 2.0;
    let max_x = center_x + new_width / 2.0;
    let min_y = center_y - new_height / 2.0;
    let max_y = center_y + new_height / 2.0;

    Rect::from_nw_se(Point::new(min_x, max_y), Point::new(max_x, min_y))
}

pub fn reproject_to_epsg<T: ArrayNum>(src: &DenseRaster<T>, epsg: crs::Epsg) -> Result<DenseRaster<T>> {
    // let src_srs = Proj::from_proj_string(src.metadata().projection())?;
    // let dst_srs = Proj::from_epsg_code(epsg.into())?;

    // let src_bbox = src.metadata().bounding_box();

    // let mut top_left = src_bbox.top_left();
    // let mut bottom_right = src_bbox.bottom_right();

    // let dst_top_left = proj4rs::transform::transform(&src_srs, &dst_srs, &mut top_left)?;
    // let dst_bottom_right = proj4rs::transform::transform(&src_srs, &dst_srs, &mut bottom_right)?;

    // let cell_size_x = dst_bottom_right.x

    // let target_georef = GeoReference::with_origin(dst_srs.projname(), dst_top_left, dst_bottom_right, epsg)?;

    let target_georef = reproject_georef_to_epsg(src.metadata(), epsg)?;
    reproject(src, target_georef)
}

pub fn reproject<T: ArrayNum>(src: &DenseRaster<T>, target_georef: GeoReference) -> Result<DenseRaster<T>> {
    let source_georef = src.metadata();
    let coord_trans = CoordinateTransformer::from_epsg(target_georef.projected_epsg().unwrap(), source_georef.projected_epsg().unwrap())?;

    let mut result = DenseRaster::<T>::filled_with_nodata(target_georef);
    let mut points = Points::with_capacity(result.rows().count() as usize);

    for row in 0..result.size().rows.count() {
        for cell in CellIterator::for_single_row_from_raster_with_size(result.size(), row) {
            points.push(result.metadata().cell_center(cell));
        }

        coord_trans.transform_points_in_place(&mut points)?;

        for (col, point) in points.iter().enumerate() {
            let src_cell = source_georef.point_to_cell(point);
            if source_georef.is_cell_on_map(src_cell) {
                result.set_cell_value(Cell::from_row_col(row, col as i32), src.cell_value(src_cell));
            }
        }

        points.clear();
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;

    use super::*;
    use crate::{
        raster::{DenseRaster, RasterIO, algo},
        testutils,
    };

    #[test_log::test]
    fn reproject_to_epsg() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let src = DenseRaster::<u8>::read(&input).unwrap();

        let start = std::time::Instant::now();
        let opts = algo::WarpOptions {
            all_cpus: false,
            ..Default::default()
        };
        let gdal = src.warped_to_epsg_with_opts(crs::epsg::WGS84_WEB_MERCATOR, &opts)?;
        let gdal_duration = start.elapsed();
        log::info!("GDAL warp took: {:?}", gdal_duration);

        let start = std::time::Instant::now();
        let result = super::reproject_to_epsg(&src, crs::epsg::WGS84_WEB_MERCATOR)?;
        let reproject_duration = start.elapsed();
        log::info!("Reproject took: {:?}", reproject_duration);

        assert_eq!(gdal.metadata().projected_epsg(), result.metadata().projected_epsg());
        assert_relative_eq!(gdal.metadata().cell_size(), result.metadata().cell_size(), epsilon = 1e-4,);
        assert_eq!(gdal.metadata().raster_size(), result.metadata().raster_size());
        assert!(result.size() == gdal.size());

        let gdal_bbox = gdal.metadata().bounding_box();
        let result_bbox = result.metadata().bounding_box();

        assert_relative_eq!(gdal_bbox.width(), result_bbox.width(), epsilon = 1e-4);
        assert_relative_eq!(gdal_bbox.height(), result_bbox.height(), epsilon = 1e-4);
        assert_relative_eq!(gdal_bbox, result_bbox, epsilon = 20.0); // Small shifts are allowed

        Ok(())
    }

    #[test]
    fn reproject_georef_to_epsg() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let src = DenseRaster::<u8>::read(&input).unwrap();

        let georef_gdal = src.metadata().warped_to_epsg(crs::epsg::WGS84_WEB_MERCATOR)?;
        let georef = super::reproject_georef_to_epsg(src.metadata(), crs::epsg::WGS84_WEB_MERCATOR)?;

        assert_eq!(georef_gdal.raster_size(), georef.raster_size());
        assert_eq!(georef_gdal.projected_epsg(), georef.projected_epsg());
        assert_relative_eq!(georef_gdal.cell_size(), georef.cell_size(), epsilon = 1e-4);

        let gdal_bbox = georef_gdal.bounding_box();
        let bbox = georef.bounding_box();

        assert_relative_eq!(gdal_bbox.width(), bbox.width(), epsilon = 1e-4);
        assert_relative_eq!(gdal_bbox.height(), bbox.height(), epsilon = 1e-4);
        assert_relative_eq!(gdal_bbox, bbox, epsilon = 20.0); // Small shifts are allowed

        Ok(())
    }
}

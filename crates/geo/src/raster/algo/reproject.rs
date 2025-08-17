use crate::{
    Array, ArrayNum, Cell, CellSize, Columns, CoordinateTransformer, Error, GeoReference, Point, RasterSize, Rect, Result, Rows, crs,
    point, raster::DenseRaster,
};

const DEFAULT_EDGE_SAMPLE_COUNT: usize = 25;
const MIN_EDGE_POINTS: usize = 2;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub enum TargetPixelAlignment {
    /// Does not align the target extent to any pixel grid.
    #[default]
    No,
    /// Align the coordinates of the extent of the output file to the pixel grid
    Yes,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum WarpTargetSize {
    #[default]
    /// Tries to use the same amount of pixels for the target as for the source.
    Source,
    /// Uses the exact raster size for the reprojection target.
    Sized(RasterSize),
    /// Uses the provided cell size for the reprojection target.
    CellSize(CellSize, TargetPixelAlignment),
}

#[derive(Debug, Clone)]
pub enum TargetSrs {
    Epsg(crs::Epsg),
    Proj4(String),
}

impl std::fmt::Display for TargetSrs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TargetSrs::Epsg(epsg) => write!(f, "{epsg}"),
            TargetSrs::Proj4(s) => write!(f, "{s}"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WarpOptions {
    /// The strategy to use to determine the warp target output size (default = `WarpTargetSize::Source`)
    pub target_size: WarpTargetSize,
    /// Linear interpolation threshold in pixels, if the linear interpolation method for cells is bigger than this threshold exact calculations will be used (default = 0.125)
    pub error_threshold: f64,
    /// Process chunks in parallel
    pub all_cpus: bool,
    /// The target SRS to reproject to
    pub target_srs: TargetSrs,
}

impl Default for WarpOptions {
    fn default() -> Self {
        Self {
            target_size: Default::default(),
            error_threshold: 0.125,
            all_cpus: Default::default(),
            target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
        }
    }
}

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
    // Helper function to calculate normalized parameter with clamping
    let calculate_t = |i: usize, points_per_edge: usize| -> f64 {
        let t = i as f64 / (points_per_edge - 1) as f64;
        if t > 0.99 { 1.0 } else { t }
    };

    // Ensure we have at least 2 points per edge (corners)
    let points_per_edge = edge_points.max(MIN_EDGE_POINTS);

    // Add points along an edge
    let add_edge_points = |points: &mut Vec<Point>, start: Point, end: Point, include_start: bool, include_end: bool| {
        let range_start = if include_start { 0 } else { 1 };
        let range_end = if include_end { points_per_edge } else { points_per_edge - 1 };

        for i in range_start..range_end {
            let t = calculate_t(i, points_per_edge);
            let x = start.x() + t * (end.x() - start.x());
            let y = start.y() + t * (end.y() - start.y());
            points.push(Point::new(x, y));
        }
    };

    let mut all_points = Vec::with_capacity(points_per_edge * 4);

    // Generate points along each edge
    // Top edge (left to right) - include both corners
    add_edge_points(&mut all_points, bbox.top_left(), bbox.top_right(), true, true);
    // Right edge (top to bottom) - exclude corners (already added)
    add_edge_points(&mut all_points, bbox.top_right(), bbox.bottom_right(), false, false);
    // Bottom edge (right to left) - exclude start corner (already added)
    add_edge_points(&mut all_points, bbox.bottom_right(), bbox.bottom_left(), false, true);
    // Left edge (bottom to top) - exclude corners (already added)
    add_edge_points(&mut all_points, bbox.bottom_left(), bbox.top_left(), false, false);

    coord_trans.transform_points_in_place(&mut all_points)?;

    // Find the bounding box of all transformed points in a single pass
    let (min_x, max_x, min_y, max_y) = all_points.iter().fold(
        (f64::INFINITY, f64::NEG_INFINITY, f64::INFINITY, f64::NEG_INFINITY),
        |(min_x, max_x, min_y, max_y), point| {
            (
                min_x.min(point.x()),
                max_x.max(point.x()),
                min_y.min(point.y()),
                max_y.max(point.y()),
            )
        },
    );

    let top_left = Point::new(min_x, max_y);
    let bottom_right = Point::new(max_x, min_y);
    Ok(Rect::from_nw_se(top_left, bottom_right))
}

pub fn reproject_georeference(georef: &GeoReference, opts: &WarpOptions) -> Result<GeoReference> {
    let source_epsg = georef
        .projected_epsg()
        .ok_or_else(|| Error::InvalidArgument("Source georef has no EPSG code".to_string()))?;
    let coord_trans = CoordinateTransformer::new(&source_epsg.to_string(), &opts.target_srs.to_string())?;
    let target_georef = reproject_georef_with_edge_points(georef, &coord_trans, DEFAULT_EDGE_SAMPLE_COUNT)?;

    match opts.target_size {
        WarpTargetSize::Source => Ok(target_georef),
        WarpTargetSize::Sized(raster_size) => {
            let bbox = target_georef.bounding_box();

            // Calculate pixel size to fit exact requested dimensions
            let pixel_width = bbox.width() / raster_size.cols.count() as f64;
            let pixel_height = bbox.height() / raster_size.rows.count() as f64;
            let cell_size = CellSize::new(pixel_width, -pixel_height);

            Ok(GeoReference::with_bottom_left_origin(
                opts.target_srs.to_string(),
                raster_size,
                bbox.bottom_left(),
                cell_size,
                georef.nodata(),
            ))
        }
        WarpTargetSize::CellSize(cell_size, alignment) => {
            let bbox = match alignment {
                TargetPixelAlignment::Yes => calculate_target_aligned_bounds(&target_georef.bounding_box(), target_georef.cell_size()),
                TargetPixelAlignment::No => target_georef.bounding_box(),
            };

            let raster_size = RasterSize::with_rows_cols(
                Rows((bbox.height() / cell_size.y().abs()).round() as i32),
                Columns((bbox.width() / cell_size.x()).round() as i32),
            );

            Ok(GeoReference::with_bottom_left_origin(
                opts.target_srs.to_string(),
                raster_size,
                bbox.bottom_left(),
                cell_size,
                georef.nodata(),
            ))
        }
    }
}

/// Reproject a `GeoReference` to a different EPSG with configurable edge sampling
///
/// * `edge_points` - Number of points to sample along each edge of the bounding box for more accurate reprojection
fn reproject_georef_with_edge_points(
    georef: &GeoReference,
    coord_trans: &CoordinateTransformer,
    edge_points: usize,
) -> Result<GeoReference> {
    let src_bbox = georef.bounding_box();

    // First reproject the source bounding box by sampling edge points
    // This is more accurate than just transforming the four corners, especially for complex projections
    let bbox = reproject_bounding_box_with_edge_sampling(&src_bbox, coord_trans, edge_points)?;

    // Calculate optimal resolution by determining the diagonal distance in the source and destination coordinate systems
    let resolution = calculate_optimal_resolution(georef, coord_trans)?;

    // Calculate dimensions with slight tolerance to handle floating point precision
    let height_pixels = bbox.height() / resolution;
    let width_pixels = bbox.width() / resolution;

    let rows = height_pixels.round() as i32;
    let cols = width_pixels.round() as i32;

    Ok(GeoReference::with_top_left_origin(
        coord_trans.target_srs(),
        RasterSize::with_rows_cols(rows.into(), cols.into()),
        bbox.top_left(),
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

fn calculate_target_aligned_bounds(bbox: &Rect<f64>, cell_size: CellSize) -> Rect<f64> {
    // Align to pixel grid
    // This means xmin/resx, ymin/resy, xmax/resx, ymax/resy should all be integers
    // The aligned extent should include the minimum (original) extent

    let min_x = (bbox.top_left().x() / cell_size.x()).floor() * cell_size.x();
    let max_x = (bbox.bottom_right().x() / cell_size.x()).ceil() * cell_size.x();

    // Note: cell_size.y() is negative for standard raster coordinates
    let max_y = (bbox.top_left().y() / cell_size.y().abs()).ceil() * cell_size.y().abs();
    let min_y = (bbox.bottom_right().y() / cell_size.y().abs()).floor() * cell_size.y().abs();

    Rect::from_nw_se(Point::new(min_x, max_y), Point::new(max_x, min_y))
}

pub fn reproject<T: ArrayNum>(src: &DenseRaster<T>, opts: &WarpOptions) -> Result<DenseRaster<T>> {
    let source_epsg = src
        .metadata()
        .projected_epsg()
        .ok_or_else(|| Error::InvalidArgument("Source georef has no EPSG code".to_string()))?;

    let target_georef = reproject_georeference(&src.meta, opts)?;
    let mut dst = DenseRaster::<T>::filled_with_nodata(target_georef);

    let coord_trans = CoordinateTransformer::new(&opts.target_srs.to_string(), &source_epsg.to_string())?;
    if opts.error_threshold > 0.0 {
        reproject_with_interpolation(src, &mut dst, &coord_trans, opts)?;
    } else {
        reproject_exact(src, &mut dst, &coord_trans)?;
    }

    Ok(dst)
}

fn reproject_exact<T: ArrayNum>(src: &DenseRaster<T>, dst: &mut DenseRaster<T>, coord_trans: &CoordinateTransformer) -> Result<()> {
    let mut points = Vec::with_capacity(dst.size().cols.count() as usize);
    for row in 0..dst.size().rows.count() {
        transform_row_exact(dst, row, coord_trans, src, &mut points)?;
    }

    Ok(())
}

/// Optimized reproject function using error threshold strategy for linear interpolation
fn reproject_with_interpolation<T: ArrayNum>(
    src: &DenseRaster<T>,
    dst: &mut DenseRaster<T>,
    coord_trans: &CoordinateTransformer,
    opts: &WarpOptions,
) -> Result<()> {
    let source_georef = src.metadata();
    let meta = dst.metadata();

    let mut points = Vec::with_capacity(dst.size().cols.count() as usize);

    // First gather all the start middle end pixels for each row so we can transform them in a single batch
    let row_width = dst.size().cols.count();
    let mut sample_points = Vec::with_capacity(dst.size().cols.count() as usize * 3);
    for row in 0..dst.size().rows.count() {
        // Transform first, middle, and last pixels
        sample_points.extend([
            meta.cell_center(Cell::from_row_col(row, 0)),
            meta.cell_center(Cell::from_row_col(row, row_width / 2)),
            meta.cell_center(Cell::from_row_col(row, row_width - 1)),
        ]);
    }

    coord_trans.transform_points_in_place(&mut sample_points)?;

    let (row_points_chunks, []) = sample_points.as_chunks::<3>() else {
        panic!("slice didn't have even length")
    };

    let error_threshold = opts.error_threshold * meta.cell_size().x();
    for (row, row_points) in (0..dst.size().rows.count()).zip(row_points_chunks) {
        let row_width = dst.size().cols.count();

        if row_width <= 2 {
            // For very narrow rows, fall back to exact transformation
            transform_row_exact(dst, row, coord_trans, src, &mut points)?;
            continue;
        }

        let first_transformed = row_points[0];
        let middle_transformed = row_points[1];
        let last_transformed = row_points[2];

        // Check if linear interpolation is accurate enough for middle pixel
        let interpolated_middle = linear_interpolate(first_transformed, last_transformed, 0.5);
        let error = point::euclidenan_distance(middle_transformed, interpolated_middle);

        if error < error_threshold {
            // Use linear interpolation for the entire row
            interpolate_row(dst, row, first_transformed, last_transformed, source_georef, src);
        } else {
            // Use recursive subdivision or fall back to exact transformation
            subdivide_and_transform_row(dst, row, coord_trans, source_georef, src, error_threshold)?;
        }
    }

    Ok(())
}

/// Transform a row exactly by transforming each pixel
fn transform_row_exact<T: ArrayNum>(
    result: &mut DenseRaster<T>,
    row: i32,
    coord_trans: &CoordinateTransformer,
    src: &DenseRaster<T>,
    points: &mut Vec<Point>,
) -> Result<()> {
    let num_columns = result.size().cols.count();
    points.clear();
    points.extend((0..num_columns).map(|col| result.metadata().cell_center(Cell::from_row_col(row, col))));
    coord_trans.transform_points_in_place(points)?;

    let source_georef = src.metadata();
    for (col, point) in points.iter().enumerate() {
        let src_cell = source_georef.point_to_cell(*point);
        if source_georef.is_cell_on_map(src_cell) {
            result.set_cell_value(Cell::from_row_col(row, col as i32), src.cell_value(src_cell));
        }
    }

    Ok(())
}

/// Linear interpolation between two points
#[inline]
fn linear_interpolate(start: Point, end: Point, t: f64) -> Point {
    Point::new(start.x() + t * (end.x() - start.x()), start.y() + t * (end.y() - start.y()))
}

/// Interpolate values across a row using linear interpolation between endpoints
fn interpolate_row<T: ArrayNum>(
    result: &mut DenseRaster<T>,
    row: i32,
    first_transformed: Point,
    last_transformed: Point,
    source_georef: &GeoReference,
    src: &DenseRaster<T>,
) {
    let row_width = result.size().cols.count();

    for col in 0..row_width {
        let t = if row_width == 1 { 0.0 } else { col as f64 / (row_width - 1) as f64 };
        let interpolated_point = linear_interpolate(first_transformed, last_transformed, t);

        let src_cell = source_georef.point_to_cell(interpolated_point);
        if source_georef.is_cell_on_map(src_cell) {
            result.set_cell_value(Cell::from_row_col(row, col), src.cell_value(src_cell));
        }
    }
}

/// Recursively subdivide a row and use interpolation where error threshold is met
fn subdivide_and_transform_row<T: ArrayNum>(
    result: &mut DenseRaster<T>,
    row: i32,
    coord_trans: &CoordinateTransformer,
    source_georef: &GeoReference,
    src: &DenseRaster<T>,
    error_threshold: f64,
) -> Result<()> {
    let row_width = result.size().cols.count();
    subdivide_segment(result, row, 0, row_width - 1, coord_trans, source_georef, src, error_threshold)
}

/// Recursively subdivide a segment of a row
fn subdivide_segment<T: ArrayNum>(
    result: &mut DenseRaster<T>,
    row: i32,
    start_col: i32,
    end_col: i32,
    coord_trans: &CoordinateTransformer,
    source_georef: &GeoReference,
    src: &DenseRaster<T>,
    error_threshold: f64,
) -> Result<()> {
    if end_col - start_col <= 1 {
        // Base case: transform remaining pixels exactly
        for col in start_col..=end_col {
            let cell = Cell::from_row_col(row, col);
            let pixel = result.metadata().cell_center(cell);
            let transformed = coord_trans.transform_point(pixel)?;
            let src_cell = source_georef.point_to_cell(transformed);
            if source_georef.is_cell_on_map(src_cell) {
                result.set_cell_value(cell, src.cell_value(src_cell));
            }
        }
        return Ok(());
    }

    let middle_col = (start_col + end_col) / 2;

    // Transform the three points
    let start_cell = Cell::from_row_col(row, start_col);
    let middle_cell = Cell::from_row_col(row, middle_col);
    let end_cell = Cell::from_row_col(row, end_col);

    let start_pixel = result.metadata().cell_center(start_cell);
    let middle_pixel = result.metadata().cell_center(middle_cell);
    let end_pixel = result.metadata().cell_center(end_cell);

    let start_transformed = coord_trans.transform_point(start_pixel)?;
    let middle_transformed = coord_trans.transform_point(middle_pixel)?;
    let end_transformed = coord_trans.transform_point(end_pixel)?;

    // Check interpolation error
    let t = (middle_col - start_col) as f64 / (end_col - start_col) as f64;
    let interpolated_middle = linear_interpolate(start_transformed, end_transformed, t);
    let error = point::euclidenan_distance(middle_transformed, interpolated_middle);

    if error < error_threshold {
        // Use linear interpolation for this segment
        for col in start_col..=end_col {
            let t = if end_col == start_col {
                0.0
            } else {
                (col - start_col) as f64 / (end_col - start_col) as f64
            };

            let interpolated_point = linear_interpolate(start_transformed, end_transformed, t);

            let src_cell = source_georef.point_to_cell(interpolated_point);
            if source_georef.is_cell_on_map(src_cell) {
                result.set_cell_value(Cell::from_row_col(row, col), src.cell_value(src_cell));
            }
        }
    } else {
        // Recursively subdivide
        subdivide_segment(result, row, start_col, middle_col, coord_trans, source_georef, src, error_threshold)?;
        subdivide_segment(result, row, middle_col, end_col, coord_trans, source_georef, src, error_threshold)?;
    }

    Ok(())
}

#[cfg(feature = "gdal")]
#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;

    use super::*;
    use crate::{
        raster::{DenseRaster, RasterIO, algo},
        testutils,
    };

    #[test]
    fn reproject_georef() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let src = DenseRaster::<u8>::read(&input).unwrap();

        let georef_gdal = src.metadata().warped_to_epsg(crs::epsg::WGS84_WEB_MERCATOR)?;
        let opts = WarpOptions::default();
        let georef = super::reproject_georeference(src.metadata(), &opts)?;

        let gdal_bbox = georef_gdal.bounding_box();
        let bbox = georef.bounding_box();

        assert_eq!(georef_gdal.raster_size(), georef.raster_size());
        assert_eq!(georef_gdal.projected_epsg(), georef.projected_epsg());
        assert_relative_eq!(georef_gdal.cell_size(), georef.cell_size(), epsilon = 1e-4);

        assert_relative_eq!(gdal_bbox.width(), bbox.width(), epsilon = 1e-4);
        assert_relative_eq!(gdal_bbox.height(), bbox.height(), epsilon = 1e-4);
        assert_relative_eq!(gdal_bbox, bbox, epsilon = 1.0); // <1m shifts are allowed

        Ok(())
    }

    // #[test_log::test]
    // fn reproject_to_epsg_fixed_size_fg() -> Result<()> {
    //     let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
    //     let target_size = RasterSize::with_rows_cols(Rows(1000), Columns(1800));
    //     let opts = super::WarpOptions {
    //         target_size: super::WarpTargetSize::Sized(target_size),
    //         all_cpus: false,
    //         error_threshold: 0.25,
    //     };

    //     let src = DenseRaster::<u8>::read(&input)?;
    //     let result = super::reproject_to_epsg_optimized(&src, crs::epsg::WGS84_WEB_MERCATOR, &opts)?;
    //     assert_eq!(target_size, result.metadata().raster_size());
    //     Ok(())
    // }

    #[test_log::test]
    fn reproject_performance_benchmark() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let src = DenseRaster::<u8>::read(&input).unwrap();

        // Measure GDAL performance
        let start = std::time::Instant::now();
        let opts = algo::GdalWarpOptions {
            all_cpus: false,
            ..Default::default()
        };
        let _gdal_result = src.warped_to_epsg_with_opts(crs::epsg::WGS84_WEB_MERCATOR, &opts)?;
        let gdal_duration = start.elapsed();

        let mut opts = super::WarpOptions {
            target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
            ..Default::default()
        };

        // Measure with interpolation performance
        let start = std::time::Instant::now();
        let _ = super::reproject(&src, &opts)?;
        let optimized_duration = start.elapsed();

        // Measure standard implementation performance
        opts.error_threshold = 0.0;
        let start = std::time::Instant::now();
        let _ = super::reproject(&src, &opts)?;
        let standard_duration = start.elapsed();

        log::info!("Performance Benchmark Results:");
        log::info!("GDAL:           {:?}", gdal_duration);
        log::info!("Standard:       {:?}", standard_duration);
        log::info!("Optimized:      {:?}", optimized_duration);
        log::info!(
            "Optimized vs Standard: {:.2}x faster",
            standard_duration.as_secs_f64() / optimized_duration.as_secs_f64()
        );
        log::info!(
            "Optimized vs GDAL:     {:.2}x faster",
            gdal_duration.as_secs_f64() / optimized_duration.as_secs_f64()
        );

        Ok(())
    }

    // #[test]
    // fn reproject_target_aligned_pixels() -> Result<()> {
    //     let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
    //     let src = GeoReference::from_file(&input)?;

    //     // Test without target_aligned_pixels
    //     let opts_no_tap = WarpOptions {
    //         target_size: WarpTargetSize::CellSize(CellSize::square(100.0), TargetPixelAlignment::No),
    //         target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
    //         ..Default::default()
    //     };
    //     let result_no_tap = super::reproject_georeference(&src, &opts_no_tap)?;

    //     // Test with target_aligned_pixels
    //     let opts_tap = WarpOptions {
    //         target_size: WarpTargetSize::CellSize(CellSize::square(100.0), TargetPixelAlignment::Yes),
    //         ..Default::default()
    //     };
    //     let result_tap = super::reproject_georeference(&src, &opts_tap)?;

    //     // The results should have the same cell size
    //     assert_relative_eq!(result_no_tap.cell_size(), result_tap.cell_size(), epsilon = 1e-4);

    //     // But different bounds alignment - TAP version should have aligned bounds
    //     let bbox_no_tap = result_no_tap.bounding_box();
    //     let bbox_tap = result_tap.bounding_box();

    //     let cell_size = result_tap.cell_size();

    //     // With TAP, the bounds should be aligned to the pixel grid
    //     // xmin / cell_size.x should be an integer (or very close to one)
    //     let x_alignment_error = (bbox_tap.top_left().x() / cell_size.x()).fract().abs();
    //     let y_alignment_error = (bbox_tap.top_left().y() / cell_size.y().abs()).fract().abs();

    //     log::info!("X alignment error (TAP): {}", x_alignment_error);
    //     log::info!("Y alignment error (TAP): {}", y_alignment_error);
    //     log::info!("No-TAP bbox: {:?}", bbox_no_tap);
    //     log::info!("TAP bbox: {:?}", bbox_tap);
    //     log::info!("Cell size: {:?}", cell_size);

    //     // Debug: show the exact alignment calculations
    //     log::info!("TAP top_left.x / cell_size.x = {}", bbox_tap.top_left().x() / cell_size.x());
    //     log::info!("TAP top_left.y / cell_size.y = {}", bbox_tap.top_left().y() / cell_size.y().abs());

    //     assert!(
    //         x_alignment_error < 1e-6,
    //         "TAP bounds should be aligned to pixel grid in X, error: {}",
    //         x_alignment_error
    //     );
    //     assert!(
    //         y_alignment_error < 1e-6,
    //         "TAP bounds should be aligned to pixel grid in Y, error: {}",
    //         y_alignment_error
    //     );

    //     // The non-TAP version may or may not be aligned (typically not)
    //     //

    //     log::error!("Conoare results to gdal");

    //     Ok(())
    // }
}

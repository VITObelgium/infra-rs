use std::ops::RangeInclusive;

use crate::{
    Array, ArrayNum, Cell, CellSize, Columns, Error, GeoReference, GeoTransform, Point, RasterSize, Rect, Result, Rows, crs, point,
    raster::DenseRaster, srs::CoordinateTransformer,
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

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum NumThreads {
    AllCpus,
    Count(usize),
}

#[derive(Debug, Clone, Copy, Default)]
pub enum WarpTargetSize {
    #[default]
    /// Tries to use the same amount of pixels for the target as for the source.
    Source,
    /// Uses the exact raster size for the warr target.
    Sized(RasterSize),
    /// Uses the provided cell size for the warp target.
    CellSize(CellSize, TargetPixelAlignment),
    /// Uses the provided geotransform and rastersize.
    Exact(GeoTransform, RasterSize),
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
    /// Configure how many threads to use for the warp operation
    pub num_threads: NumThreads,
    /// The target SRS to warp to
    pub target_srs: TargetSrs,
}

impl Default for WarpOptions {
    fn default() -> Self {
        Self {
            target_size: Default::default(),
            error_threshold: 0.125,
            num_threads: NumThreads::Count(1),
            target_srs: TargetSrs::Epsg(crs::epsg::WGS84_WEB_MERCATOR),
        }
    }
}

/// Warp a bounding box to a different coordinate system with configurable edge sampling
///
/// This function transforms a bounding box from one coordinate system to another by sampling
/// points along the edges and finding the bounding box of all transformed points. This is more
/// accurate than just transforming the four corners, especially when the transformation involves
/// significant curvature or distortion.
fn warp_bounding_box_with_edge_sampling(bbox: &Rect<f64>, coord_trans: &CoordinateTransformer, edge_points: usize) -> Result<Rect<f64>> {
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

pub fn warp_georeference(georef: &GeoReference, opts: &WarpOptions) -> Result<GeoReference> {
    let source_epsg = georef
        .projected_epsg()
        .ok_or_else(|| Error::InvalidArgument("Source georef has no EPSG code".to_string()))?;
    let coord_trans = CoordinateTransformer::new(&source_epsg.to_string(), &opts.target_srs.to_string())?;
    let target_georef = warp_georef_with_edge_points(georef, &coord_trans, DEFAULT_EDGE_SAMPLE_COUNT)?;

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
                TargetPixelAlignment::Yes => calculate_target_aligned_bounds(&target_georef.bounding_box(), cell_size),
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
        WarpTargetSize::Exact(geotrans, raster_size) => Ok(GeoReference::new(
            opts.target_srs.to_string(),
            raster_size,
            geotrans,
            georef.nodata(),
        )),
    }
}

/// Warp a `GeoReference` to a different EPSG with configurable edge sampling
///
/// * `edge_points` - Number of points to sample along each edge of the bounding box for more accurate reprojection
fn warp_georef_with_edge_points(georef: &GeoReference, coord_trans: &CoordinateTransformer, edge_points: usize) -> Result<GeoReference> {
    let src_bbox = georef.bounding_box();

    // First reproject the source bounding box by sampling edge points
    // This is more accurate than just transforming the four corners, especially for complex projections
    let bbox = warp_bounding_box_with_edge_sampling(&src_bbox, coord_trans, edge_points)?;

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

    let max_y = (bbox.top_left().y() / cell_size.y().abs()).floor() * cell_size.y().abs();
    let min_y = (bbox.bottom_right().y() / cell_size.y().abs()).floor() * cell_size.y().abs();

    Rect::from_nw_se(Point::new(min_x, max_y), Point::new(max_x, min_y))
}

pub fn warp<T: ArrayNum>(src: &DenseRaster<T>, opts: &WarpOptions) -> Result<DenseRaster<T>> {
    let target_georef = warp_georeference(&src.meta, opts)?;
    let mut dst = DenseRaster::<T>::filled_with_nodata(target_georef);

    let coord_trans = CoordinateTransformer::new(&opts.target_srs.to_string(), src.metadata().projection())?;
    if opts.error_threshold > 0.0 {
        warp_with_interpolation(src, &mut dst, &coord_trans, opts)?;
    } else {
        warp_exact(src, &mut dst, &coord_trans, opts)?;
    }

    Ok(dst)
}

#[cfg(all(feature = "rayon", feature = "proj4rs"))]
fn create_scoped_thread_pool(thread_count: Option<usize>) -> Result<rayon::ThreadPool> {
    let mut pool_builder = rayon::ThreadPoolBuilder::new();
    if let Some(count) = thread_count {
        pool_builder = pool_builder.num_threads(count);
    }
    pool_builder
        .build()
        .map_err(|e| Error::Runtime(format!("Failed to create threadpool: {e}")))
}

fn warp_exact<T: ArrayNum>(
    src: &DenseRaster<T>,
    dst: &mut DenseRaster<T>,
    coord_trans: &CoordinateTransformer,
    opts: &WarpOptions,
) -> Result<()> {
    let cols = dst.size().cols.count() as usize;
    let meta = dst.metadata().clone();

    let thread_count = match opts.num_threads {
        NumThreads::AllCpus => None,
        NumThreads::Count(val) => Some(val),
    };

    if thread_count.is_some_and(|count| count <= 1) || !cfg!(feature = "rayon") {
        // Working buffer for transformed points outside the loop to avoid allocation overhead
        let mut points = Vec::with_capacity(dst.size().cols.count() as usize);
        for (row, row_slice) in dst.as_mut_slice().chunks_mut(cols).enumerate() {
            transform_row_exact(row_slice, &meta, row as i32, coord_trans, src, &mut points)?;
        }
    } else {
        // proj is not threadsafe so only allow parallel warp with proj4rs
        #[cfg(all(feature = "rayon", feature = "proj4rs"))]
        {
            use rayon::prelude::*;
            let _pool = create_scoped_thread_pool(thread_count)?;
            dst.as_mut_slice().par_chunks_mut(cols).enumerate().try_for_each(|(r, row)| {
                let mut points = Vec::with_capacity(cols);
                transform_row_exact(row, &meta, r as i32, coord_trans, src, &mut points)
            })?;
        }
    }

    Ok(())
}

/// Optimized warp function using error threshold strategy for row based linear interpolation
fn warp_with_interpolation<T: ArrayNum>(
    src: &DenseRaster<T>,
    dst: &mut DenseRaster<T>,
    coord_trans: &CoordinateTransformer,
    opts: &WarpOptions,
) -> Result<()> {
    let target_georef = dst.metadata().clone();

    // First gather all the start middle and end pixels for each row so we can transform them in a single batch
    let row_width = dst.size().cols.count();
    let mut sample_points = Vec::with_capacity(dst.size().cols.count() as usize * 3);
    for row in 0..dst.size().rows.count() {
        // Collect first, middle, and last pixels
        sample_points.extend([
            target_georef.cell_center(Cell::from_row_col(row, 0)),
            target_georef.cell_center(Cell::from_row_col(row, row_width / 2)),
            target_georef.cell_center(Cell::from_row_col(row, row_width - 1)),
        ]);
    }

    coord_trans.transform_points_in_place(&mut sample_points)?;

    // chunks of the first middle and last points of each row
    let (row_points_chunks, []) = sample_points.as_chunks::<3>() else {
        panic!("row points size error")
    };

    let cols = dst.size().cols.count() as usize;
    let error_threshold = opts.error_threshold * target_georef.cell_size().x();
    let thread_count = match opts.num_threads {
        NumThreads::AllCpus => None,
        NumThreads::Count(val) => Some(val),
    };

    if thread_count.is_some_and(|count| count <= 1) || !cfg!(feature = "rayon") {
        for (row, (row_slice, row_points)) in dst.as_mut_slice().chunks_mut(cols).zip(row_points_chunks).enumerate() {
            process_row_with_interpolation(row_slice, row_points, row, &target_georef, coord_trans, src, error_threshold, cols)?;
        }
    } else {
        #[cfg(all(feature = "rayon", feature = "proj4rs"))]
        {
            use rayon::prelude::*;
            let _pool = create_scoped_thread_pool(thread_count)?;

            dst.as_mut_slice()
                .par_chunks_mut(cols)
                .zip(row_points_chunks)
                .enumerate()
                .try_for_each(|(row, (row_slice, row_points))| {
                    process_row_with_interpolation(row_slice, row_points, row, &target_georef, coord_trans, src, error_threshold, cols)
                })?;
        }
    }

    Ok(())
}

/// Transform a row exactly by transforming each pixel
fn transform_row_exact<T: ArrayNum>(
    row_slice: &mut [T],
    target_georef: &GeoReference,
    row: i32,
    coord_trans: &CoordinateTransformer,
    src: &DenseRaster<T>,
    points: &mut Vec<Point>,
) -> Result<()> {
    let num_columns = row_slice.len() as i32;
    let source_georef = src.metadata();

    points.clear();
    points.extend((0..num_columns).map(|col| target_georef.cell_center(Cell::from_row_col(row, col))));
    coord_trans.transform_points_in_place(points)?;

    for (col, point) in points.iter().enumerate() {
        let src_cell = source_georef.point_to_cell(*point);
        if source_georef.is_cell_on_map(src_cell) {
            row_slice[col] = src.cell_value(src_cell).unwrap_or(T::NODATA);
        }
    }

    Ok(())
}

/// Linear interpolation between two points
#[inline]
fn linear_interpolate(start: Point, end: Point, t: f64) -> Point {
    Point::new(start.x() + t * (end.x() - start.x()), start.y() + t * (end.y() - start.y()))
}

fn process_row_with_interpolation<T: ArrayNum>(
    row_slice: &mut [T],
    row_points: &[Point; 3],
    row: usize,
    target_georef: &GeoReference,
    coord_trans: &CoordinateTransformer,
    src: &DenseRaster<T>,
    error_threshold: f64,
    cols: usize,
) -> Result<()> {
    if cols <= 2 {
        // For very narrow rows, fall back to exact transformation
        let mut points = Vec::with_capacity(cols);
        transform_row_exact(row_slice, target_georef, row as i32, coord_trans, src, &mut points)?;
        return Ok(());
    }

    let first_transformed = row_points[0];
    let middle_transformed = row_points[1];
    let last_transformed = row_points[2];

    // Check if linear interpolation is accurate enough for middle pixel
    let interpolated_middle = linear_interpolate(first_transformed, last_transformed, 0.5);
    let error = point::euclidenan_distance(middle_transformed, interpolated_middle);

    if error < error_threshold {
        // Use linear interpolation for the entire row
        interpolate_row(row_slice, first_transformed, last_transformed, src);
    } else {
        // Use recursive subdivision or fall back to exact transformation
        subdivide_and_transform_row(row_slice, target_georef, row as i32, coord_trans, src, error_threshold)?;
    }

    Ok(())
}

/// Interpolate values across a row using linear interpolation between endpoints
fn interpolate_row<T: ArrayNum>(dest: &mut [T], first_transformed: Point, last_transformed: Point, src: &DenseRaster<T>) {
    let row_width = dest.len();
    let source_georef = src.metadata();

    for (col, dest_cell) in dest.iter_mut().enumerate() {
        let t = if row_width == 1 { 0.0 } else { col as f64 / (row_width - 1) as f64 };
        let interpolated_point = linear_interpolate(first_transformed, last_transformed, t);

        let src_cell = source_georef.point_to_cell(interpolated_point);
        if source_georef.is_cell_on_map(src_cell) {
            *dest_cell = src.cell_value(src_cell).unwrap_or(T::NODATA);
        }
    }
}

/// Recursively subdivide a row and use interpolation where error threshold is met
fn subdivide_and_transform_row<T: ArrayNum>(
    result: &mut [T],
    result_georef: &GeoReference,
    row: i32,
    coord_trans: &CoordinateTransformer,
    src: &DenseRaster<T>,
    error_threshold: f64,
) -> Result<()> {
    let start_col = 0;
    let end_col = result.len() as i32 - 1;

    subdivide_segment(result, result_georef, row, start_col..=end_col, coord_trans, src, error_threshold)
}

/// Recursively subdivide a segment of a row
fn subdivide_segment<T: ArrayNum>(
    result: &mut [T],
    result_georef: &GeoReference,
    row: i32,
    columns: RangeInclusive<i32>,
    coord_trans: &CoordinateTransformer,
    src: &DenseRaster<T>,
    error_threshold: f64,
) -> Result<()> {
    let start_col = *columns.start();
    let middle_col = (columns.start() + columns.end()) / 2;
    let end_col = *columns.end();
    let column_count = (end_col - start_col + 1) as usize;

    debug_assert!(result.len() == column_count);

    let source_georef = src.metadata();

    if column_count <= 2 {
        // Transform remaining pixels exactly
        for (i, col) in columns.enumerate() {
            let cell = Cell::from_row_col(row, col);
            let pixel = result_georef.cell_center(cell);
            let transformed = coord_trans.transform_point(pixel)?;
            let src_cell = source_georef.point_to_cell(transformed);
            if source_georef.is_cell_on_map(src_cell) {
                result[i] = src.cell_value(src_cell).unwrap_or(T::NODATA);
            }
        }
        return Ok(());
    }

    // Transform the three points
    let mut points = [
        result_georef.cell_center(Cell::from_row_col(row, start_col)),
        result_georef.cell_center(Cell::from_row_col(row, middle_col)),
        result_georef.cell_center(Cell::from_row_col(row, end_col)),
    ];

    coord_trans.transform_points_in_place(&mut points)?;

    let start_pixel = points[0];
    let middle_pixel = points[1];
    let end_pixel = points[2];

    // Check interpolation error
    let t = (middle_col - start_col) as f64 / (end_col - start_col) as f64;
    let interpolated_middle = linear_interpolate(start_pixel, end_pixel, t);
    let error = point::euclidenan_distance(middle_pixel, interpolated_middle);

    if error < error_threshold {
        // Use linear interpolation for this segment
        for (i, col) in columns.enumerate() {
            let t = if end_col == start_col {
                0.0
            } else {
                (col - start_col) as f64 / (end_col - start_col) as f64
            };

            let interpolated_point = linear_interpolate(start_pixel, end_pixel, t);
            let src_cell = source_georef.point_to_cell(interpolated_point);
            if source_georef.is_cell_on_map(src_cell) {
                result[i] = src.cell_value(src_cell).unwrap_or(T::NODATA);
            }
        }
    } else {
        // Recursively subdivide
        let first_half_split_pos = (middle_col - start_col) as usize + 1;
        subdivide_segment(
            &mut result[0..first_half_split_pos],
            result_georef,
            row,
            start_col..=middle_col,
            coord_trans,
            src,
            error_threshold,
        )?;
        subdivide_segment(
            &mut result[first_half_split_pos..],
            result_georef,
            row,
            middle_col + 1..=end_col,
            coord_trans,
            src,
            error_threshold,
        )?;
    }

    Ok(())
}

#[cfg(feature = "gdal")]
#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;

    use super::*;
    use crate::testutils;

    #[test]
    fn warp_georef() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");

        let opts = WarpOptions::default();
        let georef = super::warp_georeference(&GeoReference::from_file(&input)?, &opts)?;

        //let georef_gdal = algo::gdal::warp_georeference(src.metadata(), &opts)?;
        // georef obtained by performing the same warp operation with GDAL
        let georef_gdal = GeoReference::new(
            r#"PROJCS["WGS 84 / Pseudo-Mercator",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Mercator_1SP"],PARAMETER["central_meridian",0],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["X",EAST],AXIS["Y",NORTH],EXTENSION["PROJ4","+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs"],AUTHORITY["EPSG","3857"]]"#,
            RasterSize::with_rows_cols(Rows(938), Columns(2390)),
            [
                281129.4506858873,
                158.95752146313262,
                0.0,
                6712820.038056537,
                0.0,
                -158.95752146313262,
            ]
            .into(),
            Some(255.0),
        );

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

    #[test]
    fn reproject_target_aligned_pixels() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let src = GeoReference::from_file(&input)?;

        let opts_tap = WarpOptions {
            target_size: WarpTargetSize::CellSize(CellSize::square(100.0), TargetPixelAlignment::Yes),
            ..Default::default()
        };

        let georef = super::warp_georeference(&src, &opts_tap)?;

        // georef obtained by performing the same warp operation with GDAL
        let georef_gdal = GeoReference::new(
            r#"PROJCS["WGS 84 / Pseudo-Mercator",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]],PROJECTION["Mercator_1SP"],PARAMETER["central_meridian",0],PARAMETER["scale_factor",1],PARAMETER["false_easting",0],PARAMETER["false_northing",0],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["X",EAST],AXIS["Y",NORTH],EXTENSION["PROJ4","+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs"],AUTHORITY["EPSG","3857"]]"#,
            RasterSize::with_rows_cols(Rows(1491), Columns(3800)),
            [281100.0, 100.0, 0.0, 6712800.0, 0.0, -100.0].into(),
            Some(255.0),
        );

        let gdal_bbox = georef_gdal.bounding_box();
        let bbox = georef.bounding_box();

        assert_eq!(georef_gdal.raster_size(), georef.raster_size());
        assert_eq!(georef_gdal.projected_epsg(), georef.projected_epsg());
        assert_relative_eq!(georef_gdal.cell_size(), georef.cell_size(), epsilon = 1e-4);

        assert_relative_eq!(gdal_bbox.width(), bbox.width(), epsilon = 1e-4);
        assert_relative_eq!(gdal_bbox.height(), bbox.height(), epsilon = 1e-4);
        assert_relative_eq!(gdal_bbox, bbox, epsilon = 1e-4); // No shifts are allowed because of TAP

        Ok(())
    }
}

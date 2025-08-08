use crate::{
    Array, ArrayNum, Cell, CellIterator, CoordinateTransformer, GeoReference, Result, coordinatetransformer::Points, crs,
    raster::DenseRaster,
};

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

    let target_georef = src.metadata().warped_to_epsg(epsg)?;
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
    use super::*;
    use crate::{
        raster::{
            DenseRaster, RasterIO,
            algo::{self, raster_diff},
        },
        testutils,
    };

    #[test]
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
        println!("GDAL warp took: {:?}", gdal_duration);

        let start = std::time::Instant::now();
        let result = super::reproject_to_epsg(&src, crs::epsg::WGS84_WEB_MERCATOR)?;
        let reproject_duration = start.elapsed();
        println!("Custom reproject took: {:?}", reproject_duration);

        let diff = raster_diff(&gdal, &result)?;
        // We are happy if 99% of the cells match with a gdal warp

        assert!(
            diff.matches as f64 / result.len() as f64 > 0.99,
            "Raster diff has too many mismatches: only {:.2}% matched",
            100.0 * diff.matches as f64 / result.len() as f64
        );

        assert!(
            diff.mismatches.len() < 25,
            "Raster diff has too many mismatches: {}",
            diff.mismatches.len()
        );

        Ok(())
    }
}

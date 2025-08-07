use crate::{Array, ArrayNum, CellIterator, CoordinateTransformer, GeoReference, Result, crs, raster::DenseRaster};

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

    for cell in CellIterator::for_raster_with_size(result.size()) {
        let mut cell_coordinate = result.metadata().cell_center(cell);
        assert_eq!(result.metadata().point_to_cell(cell_coordinate), cell);
        coord_trans.transform_point_in_place(&mut cell_coordinate)?;

        let src_cell = source_georef.point_to_cell(cell_coordinate);
        if source_georef.is_cell_on_map(src_cell) {
            result.set_cell_value(cell, src.cell_value(src_cell));
        }
    }

    Ok(result)
}

// pub fn reproject<T: ArrayNum>(src: &DenseRaster<T>, target_georef: GeoReference) -> Result<DenseRaster<T>> {
//     let source_georef = src.metadata();

//     let inv_geotrans = source_georef.inverse_geotransform()?;

//     //let src_srs = Proj::from_epsg_code(source_georef.projected_epsg().unwrap().into())?;
//     //let dst_srs = Proj::from_epsg_code(target_georef.projected_epsg().unwrap().into())?;

//     let mut result = DenseRaster::<T>::filled_with_nodata(target_georef);
//     let target_georef = result.metadata().clone();

//     let coord_trans = CoordinateTransformer::from_epsg(target_georef.projected_epsg().unwrap(), source_georef.projected_epsg().unwrap())?;

//     for cell in CellIterator::for_raster_with_size(result.size()) {
//         let mut cell_coordinate = target_georef.coordinate_for_cell(cell);
//         //assert_eq!(result.metadata().point_to_cell(cell_coordinate), cell);

//         coord_trans.transform_point_in_place(&mut cell_coordinate)?;
//         //proj4rs::transform::transform(&dst_srs, &src_srs, &mut cell_coordinate)?;

//         let src_cell = crate::Cell::from_row_col(
//             (inv_geotrans[3] + cell_coordinate.x() * inv_geotrans[4] + cell_coordinate.y() * inv_geotrans[5]).round() as i32,
//             (inv_geotrans[0] + cell_coordinate.x() * inv_geotrans[1] + cell_coordinate.y() * inv_geotrans[2]).round() as i32,
//         );

//         //let src_cell = source_georef.point_to_nearest_cell(cell_coordinate);
//         if source_georef.is_cell_on_map(src_cell) {
//             result.set_cell_value(cell, src.cell_value(src_cell));
//         }
//     }

//     Ok(result)
// }

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

        //result.write("/Users/dirk/reproject.tif")?;

        //assert_eq!(gdal, result);
        Ok(())
    }

    // #[test]
    // fn test_reproject_to_same_epsg() -> Result<()> {
    //     let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
    //     let src = DenseRaster::<u8>::read(&input).unwrap();

    //     let mut gdal = src.warped_to_epsg(crs::epsg::BELGIAN_LAMBERT72)?;
    //     gdal.write("/Users/dirk/reproject_gdal_same.tif")?;

    //     let mut result = reproject_to_epsg(&src, crs::epsg::BELGIAN_LAMBERT72)?;
    //     result.write("/Users/dirk/reproject_same.tif")?;
    //     Ok(())
    // }
}

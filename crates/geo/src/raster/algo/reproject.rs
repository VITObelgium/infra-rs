use crate::{Array, ArrayNum, CellIterator, GeoReference, Result, crs, raster::DenseRaster};
use proj4rs::proj::Proj;

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

    let src_srs = Proj::from_epsg_code(source_georef.projected_epsg().unwrap().into())?;
    let dst_srs = Proj::from_epsg_code(target_georef.projected_epsg().unwrap().into())?;

    let mut result = DenseRaster::<T>::filled_with_nodata(target_georef);

    for cell in CellIterator::for_raster_with_size(result.size()) {
        let mut cell_coordinate = result.metadata().cell_center(cell);
        assert_eq!(result.metadata().point_to_cell(cell_coordinate), cell);
        proj4rs::transform::transform(&dst_srs, &src_srs, &mut cell_coordinate)?;

        if source_georef.is_point_on_map(cell_coordinate) {
            let src_cell = source_georef.point_to_cell(cell_coordinate);
            result.set_cell_value(cell, src.cell_value(src_cell));
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        raster::{DenseRaster, RasterIO},
        testutils,
    };

    #[test]
    fn test_reproject_to_epsg() -> Result<()> {
        let input = testutils::workspace_test_data_dir().join("landusebyte.tif");
        let src = DenseRaster::<u8>::read(&input).unwrap();

        let mut gdal = src.warped_to_epsg(crs::epsg::WGS84_WEB_MERCATOR)?;
        gdal.write("/Users/dirk/reproject_gdal.tif")?;

        let mut result = reproject_to_epsg(&src, crs::epsg::WGS84_WEB_MERCATOR)?;
        result.write("/Users/dirk/reproject.tif")?;

        let mut result_same = reproject_to_epsg(&result, crs::epsg::WGS84_WEB_MERCATOR)?;
        result_same.write("/Users/dirk/reproject_same.tif")?;
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

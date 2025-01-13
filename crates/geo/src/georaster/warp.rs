use inf::{crs::Epsg, Point};
use proj4rs::{adaptors::transform_vertex_2d, Proj};

use super::{Raster, RasterNum};
use crate::Result;

pub fn warp<T: RasterNum<T>, R: Raster<T>>(raster: R, epsg: Epsg) -> Result<R> {
    let input_meta = raster.geo_metadata();

    let input_srs = Proj::from_epsg_code(input_meta.projected_epsg().unwrap().into())?;
    let output_srs = Proj::from_epsg_code(epsg.into())?;

    let top_left: Point = transform_vertex_2d(&input_srs, &output_srs, input_meta.top_left().into())?.into();
    let bottom_right: Point = transform_vertex_2d(&input_srs, &output_srs, input_meta.bottom_right().into())?.into();
    let bottom_left: Point = transform_vertex_2d(&input_srs, &output_srs, input_meta.bottom_left().into())?.into();
    let top_right: Point = transform_vertex_2d(&input_srs, &output_srs, input_meta.top_right().into())?.into();

    println!("{:?} {:?}", top_left, bottom_right);
    println!("{:?} {:?}", bottom_left, top_right);

    let result = R::zeros(raster.geo_metadata().clone());

    Ok(result)
}

#[cfg(test)]
mod tests {
    use inf::{crs, spatialreference};

    use crate::raster::{io, DenseRaster, RasterIO};

    use super::*;

    #[test]
    fn test_warp() {
        let path: std::path::PathBuf = [env!("CARGO_MANIFEST_DIR"), "test", "data", "epsg31370.tif"]
            .iter()
            .collect();

        let path_wgs84: std::path::PathBuf = [env!("CARGO_MANIFEST_DIR"), "test", "data", "epsg3857.tif"]
            .iter()
            .collect();

        let meta_31370 = io::dataset::read_file_metadata(&path).unwrap();
        let meta_wgs84 = io::dataset::read_file_metadata(&path_wgs84).unwrap();
        println!(
            "GDAL WGS84 TL {:?} TR {:?}",
            meta_wgs84.top_left(),
            meta_wgs84.bottom_right()
        );
        println!(
            "GDAL WGS84 BL {:?} BR {:?}",
            meta_wgs84.bottom_left(),
            meta_wgs84.top_right()
        );

        let trans = spatialreference::CoordinateWarpTransformer::for_epsgs(
            crs::epsg::BELGIAN_LAMBERT72,
            crs::epsg::WGS84_WEB_MERCATOR,
        )
        .unwrap();

        println!(
            "Gdal  WARP TL {:?} BR {:?}",
            trans.transform_point(meta_31370.top_left()).unwrap(),
            trans.transform_point(meta_31370.bottom_right()).unwrap()
        );

        println!(
            "Gdal  WARP BL {:?} TR {:?}",
            trans.transform_point(meta_31370.bottom_left()).unwrap(),
            trans.transform_point(meta_31370.top_right()).unwrap()
        );

        let ras31370 = DenseRaster::<f32>::read(&path).unwrap();
        let result = warp(ras31370, crs::epsg::WGS84_WEB_MERCATOR).unwrap();

        assert_eq!(
            result.geo_metadata().projected_epsg().unwrap(),
            crs::epsg::WGS84_WEB_MERCATOR
        );
    }
}

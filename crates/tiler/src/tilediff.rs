use raster::AnyDenseRaster;

use crate::{Error, Result, TileData, TileFormat};

pub fn diff_tiles(tile1: &AnyDenseRaster, tile2: &AnyDenseRaster, format: TileFormat) -> Result<TileData> {
    if tile1.data_type() != tile2.data_type() {
        return Err(Error::InvalidArgument("Diff tile data types do not match".into()));
    }

    #[cfg(feature = "vector-tiles")]
    use raster::RasterDataType;

    match format {
        #[cfg(feature = "vector-tiles")]
        #[allow(clippy::unwrap_used)] // Types are checked prior to unwrapping
        TileFormat::Protobuf => match tile1.data_type() {
            RasterDataType::Uint8 => diff_tiles_as_mvt::<u8>(tile1.try_into().unwrap(), tile2.try_into().unwrap()),
            RasterDataType::Uint16 => diff_tiles_as_mvt::<u16>(tile1.try_into().unwrap(), tile2.try_into().unwrap()),
            RasterDataType::Uint32 => diff_tiles_as_mvt::<u32>(tile1.try_into().unwrap(), tile2.try_into().unwrap()),
            RasterDataType::Uint64 => diff_tiles_as_mvt::<u64>(tile1.try_into().unwrap(), tile2.try_into().unwrap()),
            RasterDataType::Int8 => diff_tiles_as_mvt::<i8>(tile1.try_into().unwrap(), tile2.try_into().unwrap()),
            RasterDataType::Int16 => diff_tiles_as_mvt::<i16>(tile1.try_into().unwrap(), tile2.try_into().unwrap()),
            RasterDataType::Int32 => diff_tiles_as_mvt::<i32>(tile1.try_into().unwrap(), tile2.try_into().unwrap()),
            RasterDataType::Int64 => diff_tiles_as_mvt::<i64>(tile1.try_into().unwrap(), tile2.try_into().unwrap()),
            RasterDataType::Float32 => diff_tiles_as_mvt::<f32>(tile1.try_into().unwrap(), tile2.try_into().unwrap()),
            RasterDataType::Float64 => diff_tiles_as_mvt::<f64>(tile1.try_into().unwrap(), tile2.try_into().unwrap()),
        },
        _ => Err(Error::InvalidArgument("Unsupported tile format".into())),
    }
}

#[cfg(feature = "vector-tiles")]
fn diff_tiles_as_mvt<T: raster::RasterNum<T> + gdal::raster::GdalType>(
    tile1: &raster::DenseRaster<T>,
    tile2: &raster::DenseRaster<T>,
) -> Result<TileData> {
    use gdal::vector::LayerAccess;
    use geo::{georaster, CellSize, GeoReference, Point, Tile};
    use raster::Raster;

    use crate::PixelFormat;

    if tile1.len() != tile2.len() {
        return Err(Error::InvalidArgument("Tile data length mismatch".to_string()));
    }

    if tile1.is_empty() {
        return Ok(TileData::default());
    }

    let diff = tile2 - tile1;

    let geo_ref = GeoReference::with_origin(
        "EPSG:4326",
        diff.size(),
        Point::new(0.0, -(Tile::TILE_SIZE as f64)),
        CellSize::square(1.0),
        Option::<f64>::None,
    );

    let vec_ds = georaster::algo::polygonize(&geo_ref, diff.as_ref())?;

    let mut tile = mvt::Tile::new(Tile::TILE_SIZE as u32);

    let mut idx = 0;
    for feature in vec_ds.layer(0)?.features() {
        if let Some(geom) = feature.geometry() {
            if let Ok(geo_types::Geometry::Polygon(geom)) = geom.to_geo() {
                let mut cell_geom = mvt::GeomEncoder::new(mvt::GeomType::Polygon);
                for point in geom.exterior().points() {
                    cell_geom.add_point(point.x(), -point.y())?;
                }

                cell_geom.complete_geom()?;

                for interior in geom.interiors() {
                    for point in interior.points() {
                        cell_geom.add_point(point.x(), -point.y())?;
                    }

                    cell_geom.complete_geom()?;
                }

                let layer = tile.create_layer(&idx.to_string());
                let mut mvt_feat = layer.into_feature(cell_geom.encode()?);
                mvt_feat.set_id(idx as u64);
                mvt_feat.add_tag_double(
                    "diff",
                    feature.field_as_double_by_name("Value")?.expect("Value not found"),
                );
                tile.add_layer(mvt_feat.into_layer())?;
                idx += 1;
            }
        }
    }

    Ok(TileData::new(
        TileFormat::Protobuf,
        PixelFormat::Unknown,
        tile.to_bytes()?,
    ))
}

// #[cfg(test)]
// mod tests {
//     use crate::{tileio, TileRequest};

//     use super::*;
//     use geo::{crs, GeoReference, RuntimeConfiguration, Tile};
//     use path_macro::path;

//     #[ctor::ctor]
//     fn init() {
//         let mut data_dir = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / "target" / "data");
//         if !data_dir.exists() {
//             // Infra used as a subcrate, try the parent directory
//             data_dir = path!(env!("CARGO_MANIFEST_DIR") / ".." / ".." / ".." / "target" / "data");
//             if !data_dir.exists() {
//                 panic!("Proj.db data directory not found");
//             }
//         }

//         let config = RuntimeConfiguration::builder().proj_db(&data_dir).build();
//         config.apply().expect("Failed to configure runtime");
//     }

//     #[test]
//     fn test_diff_tile_provider() {
//         let path1 = path!(env!("CARGO_MANIFEST_DIR") / "test" / "data" / "potgeo_lim_bebouwd.tif");
//         let path2 = path!(env!("CARGO_MANIFEST_DIR") / "test" / "data" / "residentieel_dakopp_50m_lim.tif");
//         assert!(path1.exists());
//         assert!(path2.exists());

//         let meta = GeoReference::from_file(&path1).unwrap();
//         let tile = Tile::for_coordinate(
//             meta.warped_to_epsg(crs::epsg::WGS84).unwrap().latlonbounds().center(),
//             10,
//         );

//         let req = TileRequest {
//             tile,
//             dpi_ratio: 1,
//             tile_format: TileFormat::Protobuf,
//         };

//         let tile1 = tileio::read_raster_tile_warped::<u8>(&path1, 1, req.tile, 1).unwrap();
//         let tile2 = tileio::read_raster_tile_warped::<u8>(&path2, 1, req.tile, 1).unwrap();

//         let mvt = diff_tiles_as_mvt::<u8>(&tile1, &tile2).unwrap();

//         // write mvt to file for debugging
//         std::fs::write("/Users/dirk/tile.mvt", mvt.data).unwrap();
//     }
// }

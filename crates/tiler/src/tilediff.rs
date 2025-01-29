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
        "",
        diff.size(),
        Point::new(0.0, Tile::TILE_SIZE as f64),
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
                    cell_geom.add_point(point.x(), point.y())?;
                }

                let layer = tile.create_layer(&idx.to_string());
                let mut mvt_feat = layer.into_feature(cell_geom.encode()?);
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

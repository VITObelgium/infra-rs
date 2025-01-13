use num::NumCast;
use raster::RasterNum;

use crate::georaster::{GeoRaster, GeoRasterCreation};

pub fn cast<TDest: RasterNum<TDest>, TSrc: RasterNum<TSrc>, RDest, RSrc>(src: &RSrc) -> RDest
where
    RDest: GeoRaster<TDest> + GeoRasterCreation<TDest>,
    RSrc: GeoRaster<TSrc>,
    for<'a> &'a RSrc: IntoIterator<Item = Option<TSrc>>,
{
    RDest::from_iter(
        src.geo_metadata().clone(),
        src.into_iter().map(|x| x.and_then(|x| NumCast::from(x))),
    )
}

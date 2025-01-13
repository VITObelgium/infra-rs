use num::NumCast;

use crate::{raster::RasterCreation, Raster, RasterNum};

pub fn cast<TDest: RasterNum<TDest>, TSrc: RasterNum<TSrc>, RDest, RSrc>(src: &RSrc) -> RDest
where
    RDest: Raster<TDest> + RasterCreation<TDest>,
    RSrc: Raster<TSrc>,
    for<'a> &'a RSrc: IntoIterator<Item = Option<TSrc>>,
{
    RDest::from_iter(src.size(), src.into_iter().map(|x| x.and_then(|x| NumCast::from(x))))
}

pub fn assert_dimensions<R1, R2, T1, T2>(r1: &R1, r2: &R2)
where
    R1: Raster<T1>,
    R2: Raster<T2>,
    T1: RasterNum<T1>,
    T2: RasterNum<T2>,
{
    assert_eq!(r1.width(), r2.width(), "Raster widths do not match");
    assert_eq!(r1.height(), r2.height(), "Raster heights do not match");
}

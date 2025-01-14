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

#[cfg(test)]
mod tests {
    use raster::Raster;

    use super::*;
    use crate::georaster::{
        testutils::{create_vec, test_metadata_3x3, NOD},
        DenseGeoRaster, GeoRasterCreation,
    };

    #[test]
    fn test_cast() {
        let i32_raster = DenseGeoRaster::<i32>::new(
            test_metadata_3x3(),
            create_vec(&[NOD, 2.0, 2.0, 3.0, NOD, 3.0, 1.0, 1.0, 0.0]),
        );

        let expected = DenseGeoRaster::<i16>::new(
            test_metadata_3x3(),
            create_vec(&[NOD, 2.0, 2.0, 3.0, NOD, 3.0, 1.0, 1.0, 0.0]),
        );

        let result: DenseGeoRaster<i16> = cast(&i32_raster);
        assert_eq!(result, expected);
        assert!(!result.index_has_data(0));
        assert!(!result.index_has_data(4));

        for index in [0, 4] {
            assert!(!result.index_has_data(index));
        }

        for index in [1, 2, 3, 5, 6, 7, 8] {
            assert!(result.index_has_data(index));
        }
    }
}

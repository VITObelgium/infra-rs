#[cfg(feature = "proj")]
mod proj;
#[cfg(feature = "proj")]
pub use proj::CoordinateTransformer;

#[cfg(feature = "proj4rs")]
mod proj4rs;
#[cfg(all(feature = "proj4rs", not(feature = "proj")))]
// proj takes precedence over proj4rs if both are enabled
pub use proj4rs::CoordinateTransformer;

#[cfg(all(feature = "proj", feature = "proj4rs"))]
#[cfg(test)]
mod tests {
    use approx::assert_relative_eq;

    use super::*;
    use crate::{Point, Result, crs};

    #[test]
    fn compare_transformers() -> Result<()> {
        let source_crs = crs::epsg::BELGIAN_LAMBERT72;
        let target_crs = crs::epsg::WGS84_WEB_MERCATOR;

        let source_wkt = r#"PROJCRS["BD72 / Belgian Lambert 72",BASEGEOGCRS["BD72",DATUM["Reseau National Belge 1972",ELLIPSOID["International 1924",6378388,297,LENGTHUNIT["metre",1]]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],ID["EPSG",4313]],CONVERSION["Belgian Lambert 72",METHOD["Lambert Conic Conformal (2SP)",ID["EPSG",9802]],PARAMETER["Latitude of false origin",90,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8821]],PARAMETER["Longitude of false origin",4.36748666666667,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8822]],PARAMETER["Latitude of 1st standard parallel",51.1666672333333,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8823]],PARAMETER["Latitude of 2nd standard parallel",49.8333339,ANGLEUNIT["degree",0.0174532925199433],ID["EPSG",8824]],PARAMETER["Easting at false origin",150000.013,LENGTHUNIT["metre",1],ID["EPSG",8826]],PARAMETER["Northing at false origin",5400088.438,LENGTHUNIT["metre",1],ID["EPSG",8827]]],CS[Cartesian,2],AXIS["easting (X)",east,ORDER[1],LENGTHUNIT["metre",1]],AXIS["northing (Y)",north,ORDER[2],LENGTHUNIT["metre",1]],USAGE[SCOPE["Engineering survey, topographic mapping."],AREA["Belgium - onshore."],BBOX[49.5,2.5,51.51,6.4]],ID["EPSG",31370]]"#;

        //let proj_transformer = proj::CoordinateTransformer::from_epsg(source_crs, target_crs)?;
        let proj4rs_transformer = proj4rs::CoordinateTransformer::from_epsg(source_crs, target_crs)?;
        let proj_transformer = proj::CoordinateTransformer::new(source_wkt, &target_crs.to_string())?;

        // let proj_transformer = proj::CoordinateTransformer::new(
        //     "+proj=lcc +lat_1=51.16666723333333 +lat_2=49.8333339 +lat_0=90 +lon_0=4.367486666666666 +x_0=150000.013 +y_0=5400088.438 +ellps=intl +towgs84=-106.869,52.2978,-103.724,0.3366,-0.457,1.8422,-1.2747 +units=m",
        //     "+proj=webmerc +ellps=WGS84 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +towgs84=0,0,0,0,0,0,0",
        // )?;
        // let proj4rs_transformer = proj4rs::CoordinateTransformer::new(
        //     "+proj=lcc +lat_1=51.16666723333333 +lat_2=49.8333339 +lat_0=90 +lon_0=4.367486666666666 +x_0=150000.013 +y_0=5400088.438 +ellps=intl +towgs84=-106.869,52.2978,-103.724,0.3366,-0.457,1.8422,-1.2747 +units=m",
        //     "+proj=webmerc +ellps=WGS84 +lat_0=0 +lon_0=0 +x_0=0 +y_0=0 +towgs84=0,0,0,0,0,0,0",
        // )?;

        let point = Point::new(22000.0, 245000.0);

        assert_relative_eq!(
            proj_transformer.transform_point(point)?,
            proj4rs_transformer.transform_point(point)?,
            epsilon = 1e-5
        );

        Ok(())
    }
}

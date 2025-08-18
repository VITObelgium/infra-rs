use crate::{Error, Result, crs::Epsg};
use gdal::spatial_ref::AxisMappingStrategy;

#[derive(Debug, Clone, PartialEq)]
pub struct SpatialReference {
    srs: gdal::spatial_ref::SpatialRef,
}

impl SpatialReference {
    pub fn new(srs: gdal::spatial_ref::SpatialRef) -> Self {
        SpatialReference { srs }
    }

    pub fn from_proj(projection: &str) -> Result<Self> {
        if projection.is_empty() {
            return Err(Error::InvalidArgument("Empty projection string".into()));
        }

        let mut srs = gdal::spatial_ref::SpatialRef::from_proj4(projection)?;
        srs.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);
        Ok(SpatialReference { srs })
    }

    pub fn from_epsg(epsg: Epsg) -> Result<Self> {
        let mut srs = gdal::spatial_ref::SpatialRef::from_epsg(epsg.code() as u32)?;
        srs.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);
        Ok(SpatialReference { srs })
    }

    pub fn from_definition(def: &str) -> Result<Self> {
        let mut srs = gdal::spatial_ref::SpatialRef::from_definition(def)?;
        srs.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);
        Ok(SpatialReference { srs })
    }

    pub fn to_wkt(&self) -> Result<String> {
        Ok(self.srs.to_wkt()?)
    }

    pub fn to_proj(&self) -> Result<String> {
        Ok(self.srs.to_proj4()?)
    }

    pub fn is_projected(&self) -> bool {
        self.srs.is_projected()
    }

    pub fn is_geographic(&self) -> bool {
        self.srs.is_geographic()
    }

    pub fn epsg_cs(&mut self) -> Option<Epsg> {
        if self.srs.auto_identify_epsg().is_ok() {
            SpatialReference::epsg_conv(self.srs.auth_code().ok())
        } else {
            None
        }
    }

    pub fn epsg_geog_cs(&self) -> Option<Epsg> {
        if let Ok(geogcs) = self.srs.geog_cs() {
            SpatialReference::epsg_conv(geogcs.auth_code().ok())
        } else {
            None
        }
    }

    pub fn srs(&self) -> &gdal::spatial_ref::SpatialRef {
        &self.srs
    }

    fn epsg_conv(epsg: Option<i32>) -> Option<Epsg> {
        epsg.map(|epsg| Epsg::new(epsg as u16))
    }
}

/// Single shot version of `SpatialReference::to_wkt`
pub fn projection_from_epsg(epsg: Epsg) -> Result<String> {
    if let Err(e) = SpatialReference::from_epsg(epsg) {
        log::error!("Error creating spatial reference: {e}");
    }

    let spatial_ref = SpatialReference::from_epsg(epsg)?;
    spatial_ref.to_wkt()
}

/// Single shot version of `SpatialReference::epsg_geog_cs`
pub fn projection_to_geo_epsg(projection: &str) -> Option<Epsg> {
    let spatial_ref = SpatialReference::from_definition(projection).ok()?;
    spatial_ref.epsg_geog_cs()
}

/// Single shot version of `SpatialReference::epsg_cs`
pub fn projection_to_epsg(projection: &str) -> Option<Epsg> {
    let mut spatial_ref = SpatialReference::from_definition(projection).ok()?;
    spatial_ref.epsg_cs()
}

#[cfg(test)]
mod tests {
    use super::SpatialReference;

    #[test]
    fn epsg_import() {
        let srs = SpatialReference::from_epsg(31370.into()).unwrap();
        assert!(srs.is_projected());
        assert!(!srs.is_geographic());
        assert_eq!(srs.epsg_geog_cs(), Some(4313.into()));

        // let mut srs = SpatialReference::from_definition(&srs.to_proj().unwrap()).unwrap();
        // assert!(srs.is_projected());
        // assert_eq!(srs.epsg_cs(), Some(31370.into()));

        let mut srs = SpatialReference::from_definition(&srs.to_wkt().unwrap()).unwrap();
        assert!(srs.is_projected());
        assert_eq!(srs.epsg_cs(), Some(31370.into()));
    }
}

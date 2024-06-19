use gdal::spatial_ref::AxisMappingStrategy;

use crate::{crs::Epsg, Error};

pub struct SpatialReference {
    srs: gdal::spatial_ref::SpatialRef,
}

impl SpatialReference {
    pub fn from_proj(projection: &str) -> Result<Self, Error> {
        let mut srs = gdal::spatial_ref::SpatialRef::from_proj4(projection)?;
        srs.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);
        Ok(SpatialReference { srs })
    }

    pub fn from_epsg(epsg: Epsg) -> Result<Self, Error> {
        let mut srs = gdal::spatial_ref::SpatialRef::from_epsg(epsg.into())?;
        srs.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);
        Ok(SpatialReference { srs })
    }

    pub fn from_definition(def: &str) -> Result<Self, Error> {
        let mut srs = gdal::spatial_ref::SpatialRef::from_definition(def)?;
        srs.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);
        Ok(SpatialReference { srs })
    }

    pub fn to_wkt(&self) -> Result<String, Error> {
        Ok(self.srs.to_wkt()?)
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
        epsg.map(|epsg| Epsg::new(epsg as u32))
    }
}

/// Single shot version of `SpatialReference::to_wkt`
pub fn projection_from_epsg(epsg: Epsg) -> Result<String, Error> {
    if let Err(e) = SpatialReference::from_epsg(epsg) {
        log::error!("Error creating spatial reference: {}", e);
    }

    let spatial_ref = SpatialReference::from_epsg(epsg)?;
    spatial_ref.to_wkt()
}

/// Single shot version of `SpatialReference::epsg_geog_cs`
pub fn projection_to_geo_epsg(projection: &str) -> Option<Epsg> {
    let spatial_ref = SpatialReference::from_proj(projection).ok()?;
    spatial_ref.epsg_geog_cs()
}

/// Single shot version of `SpatialReference::epsg_cs`
pub fn projection_to_epsg(projection: &str) -> Option<Epsg> {
    let mut spatial_ref = SpatialReference::from_proj(projection).ok()?;
    spatial_ref.epsg_cs()
}

use crate::crs;

#[derive(Debug, Clone, Default)]
pub enum ModelType {
    #[default]
    Projected,
    Geographic,
    Geocentric,
}

#[derive(Debug, Clone, Default)]
pub struct ProjectionInfo {
    pub model_type: ModelType,
    pub projected_epsg: Option<crs::Epsg>,
    pub geographic_epsg: Option<crs::Epsg>,
}

impl ProjectionInfo {
    pub fn epsg(&self) -> Option<crs::Epsg> {
        match self.model_type {
            ModelType::Projected => self.projected_epsg,
            ModelType::Geographic => self.geographic_epsg,
            ModelType::Geocentric => None,
        }
    }
}

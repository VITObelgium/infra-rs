use crate::gdalinterop;
use crate::Result;
use bon::bon;

pub struct RuntimeConfiguration {
    gdal_config: gdalinterop::Config,
}

#[bon]
impl RuntimeConfiguration {
    #[builder]
    pub fn new(proj_db: &std::path::Path, gdal_debug_log: Option<bool>) -> Self {
        Self {
            gdal_config: gdalinterop::Config {
                debug_logging: gdal_debug_log.unwrap_or(false),
                proj_db_search_location: proj_db.to_path_buf(),
            },
        }
    }

    pub fn apply(&self) -> Result<()> {
        self.gdal_config.apply()?;
        Ok(())
    }
}

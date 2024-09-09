use crate::gdalinterop;
use crate::Result;

pub struct RuntimeConfiguration {
    gdal_config: gdalinterop::Config,
}

impl RuntimeConfiguration {
    pub fn new(proj_db: &std::path::Path) -> Self {
        Self {
            gdal_config: gdalinterop::Config {
                debug_logging: false,
                proj_db_search_location: proj_db.to_path_buf(),
            },
        }
    }

    pub fn apply(&self) -> Result<()> {
        self.gdal_config.apply()?;
        Ok(())
    }
}

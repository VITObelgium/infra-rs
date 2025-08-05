use crate::Result;
use crate::gdalinterop;
use bon::bon;

pub struct RuntimeConfiguration {
    gdal_config: gdalinterop::Config,
}

#[bon]
impl RuntimeConfiguration {
    #[builder]
    pub fn new(proj_db: Option<&std::path::Path>, gdal_debug_log: Option<bool>, config_options: Option<Vec<(String, String)>>) -> Self {
        Self {
            gdal_config: gdalinterop::Config {
                debug_logging: gdal_debug_log.unwrap_or(false),
                proj_db_search_location: proj_db.map(|db| db.to_path_buf()),
                config_options: config_options.unwrap_or_default(),
            },
        }
    }

    pub fn apply(&self) -> Result<()> {
        self.gdal_config.apply()?;
        Ok(())
    }
}

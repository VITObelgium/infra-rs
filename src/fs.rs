use std::path::Path;

use crate::{Error, Result};

pub fn create_directory_for_file(p: &Path) -> Result {
    if let Some(parent_dir) = p.parent() {
        std::fs::create_dir_all(parent_dir)
            .map_err(|e| Error::Runtime(format!("Failed to create output directory for file ({e})")))?;
    }

    Ok(())
}

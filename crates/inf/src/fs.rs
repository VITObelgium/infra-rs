use std::path::Path;

use crate::{Error, Result};

pub fn create_directory_for_file(p: &Path) -> Result {
    if let Some(parent_dir) = p.parent() {
        std::fs::create_dir_all(parent_dir).map_err(|e| {
            Error::Runtime(format!(
                "Failed to create output directory for file '{}' ({e})",
                p.to_string_lossy()
            ))
        })?;
    }

    Ok(())
}

pub fn sanitize_filename(name: &str, replacement_char: char) -> String {
    let forbidden = ['<', '>', ':', '"', '/', '\\', '|', '?', '*'];
    name.chars()
        .map(|c| if forbidden.contains(&c) { replacement_char } else { c })
        .collect()
}

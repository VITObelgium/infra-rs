use geo::{ZoomLevelStrategy, raster::io::RasterFormat};

use crate::Result;
use std::path::Path;

use crate::{
    Error, directorytileprovider::DirectoryTileProvider, mbtilestileprovider::MbtilesTileProvider, tileprovider::TileProvider,
    warpingtileprovider::WarpingTileProvider,
};

#[derive(Clone, Default)]
pub struct TileProviderOptions {
    pub calculate_stats: bool,
    // when calculating the max zoom levelm prefer the higher value when the cellsize is between two zoom levels
    pub zoom_level_strategy: ZoomLevelStrategy,
}

/// Create a tile provider for hosting a single file
pub fn create_single_file_tile_provider(path: &Path, opts: &TileProviderOptions) -> Result<Box<dyn TileProvider + Send + Sync>> {
    let raster_type = RasterFormat::guess_from_path(path);

    if raster_type == RasterFormat::MBTiles {
        Ok(Box::new(MbtilesTileProvider::new(path)?))
    } else if WarpingTileProvider::supports_raster_type(raster_type) {
        Ok(Box::new(WarpingTileProvider::new(path, opts)?))
    } else {
        Err(Error::Runtime(format!(
            "No raster provider available for: {}",
            path.to_string_lossy()
        )))
    }
}

/// Create a suitable tile provider for hosting a file or directory
/// In case of a directory, all supported files in the directory are hosted as separate layers
/// In case of a file, the file is hosted as a single layer
pub fn create_tile_provider(path: &Path, opts: &TileProviderOptions) -> Result<Box<dyn TileProvider + Send + Sync>> {
    if path.is_file() {
        if let Ok(provider) = create_single_file_tile_provider(path, opts) {
            return Ok(provider);
        }

        return Err(Error::Runtime(format!("Not a supported file: {}", path.to_string_lossy())));
    } else if path.is_dir() {
        return Ok(Box::new(DirectoryTileProvider::new(path, opts.clone())?));
    }

    Err(Error::Runtime(format!("Invalid location provided: {}", path.to_string_lossy())))
}

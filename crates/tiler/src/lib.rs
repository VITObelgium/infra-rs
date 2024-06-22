#![warn(clippy::unwrap_used)]

mod layermetadata;
mod tiledata;
mod tileformat;
mod tileprovider;
pub mod tileproviderfactory;

mod directorytileprovider;
mod imageprocessing;
mod mbtilestileprovider;
mod rasterprocessing;
mod warpingtileprovider;

pub use directorytileprovider::DirectoryTileProvider;
#[cfg(feature = "rest")]
use layermetadata::layer_metadata_to_tile_json;
pub use layermetadata::LayerId;
pub use layermetadata::LayerMetadata;
pub use layermetadata::TileJson;
pub use tiledata::TileData;
pub use tileformat::TileFormat;
pub use tileprovider::TileProvider;

pub type Error = inf::Error;
pub type Result<T> = inf::Result<T>;

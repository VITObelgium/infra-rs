use core::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
pub enum TileFormat {
    #[default]
    Unknown,
    Png,
    FloatEncodedPng,
    Jpeg,
    Protobuf,
    RasterTile,
}

impl TileFormat {
    pub fn extension(&self) -> &str {
        match self {
            TileFormat::Protobuf => "pbf",
            TileFormat::Png | TileFormat::FloatEncodedPng => "png",
            TileFormat::Jpeg => "jpg",
            TileFormat::RasterTile => "vrt",
            TileFormat::Unknown => "",
        }
    }
}

impl fmt::Display for TileFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                TileFormat::Protobuf => "pbf",
                TileFormat::Png => "png",
                TileFormat::FloatEncodedPng => "float_png",
                TileFormat::Jpeg => "jpeg",
                TileFormat::RasterTile => "vrt",
                TileFormat::Unknown => "",
            }
        )
    }
}

impl From<&str> for TileFormat {
    fn from(s: &str) -> Self {
        match s {
            "png" => TileFormat::Png,
            "float_png" => TileFormat::FloatEncodedPng,
            "jpeg" => TileFormat::Jpeg,
            "pbf" => TileFormat::Protobuf,
            "vrt" => TileFormat::RasterTile,
            _ => TileFormat::Unknown,
        }
    }
}

use crate::tileformat::TileFormat;
use gdal::raster::GdalDataType;
use inf::{crs::Epsg, Coordinate, LatLonBounds};
use num::NumCast;
use std::{collections::HashMap, path::PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct LayerId(u64);

impl std::fmt::Display for LayerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl LayerId {
    pub fn new(epsg: u64) -> Self {
        Self(epsg)
    }
}

impl From<u64> for LayerId {
    fn from(val: u64) -> LayerId {
        LayerId::new(val)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum RasterDataType {
    Byte,
    Int32,
    UInt32,
    Float,
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
pub enum LayerSourceType {
    GeoPackage,
    Mbtiles,
    ArcAscii,
    GeoTiff,
    Unknown,
}

#[derive(Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct LayerMetadata {
    pub id: LayerId,
    pub name: String,
    pub url: String,
    pub description: String,
    pub path: PathBuf,
    pub source_is_web_mercator: bool,
    pub min_value: f64,
    pub max_value: f64,
    pub min_zoom: i32,
    pub max_zoom: i32,
    pub nodata: Option<f64>,
    pub bounds: [f64; 4], // left bottom right top
    pub data_type: RasterDataType,
    pub epsg: Epsg,
    pub tile_format: TileFormat,
    pub supports_dpi_ratio: bool,
    pub source_format: LayerSourceType,
    pub scheme: String,
    #[cfg_attr(feature = "serde", serde(skip))]
    pub additional_data: HashMap<String, String>,
}

impl LayerMetadata {
    pub fn bounds(&self) -> LatLonBounds {
        let top_left = Coordinate {
            latitude: self.bounds[3],
            longitude: self.bounds[0],
        };
        let bottom_right = Coordinate {
            latitude: self.bounds[1],
            longitude: self.bounds[2],
        };
        LatLonBounds::hull(top_left, bottom_right)
    }

    pub fn set_bounds(&mut self, bounds: LatLonBounds) {
        self.set_bounds_from_coordinates(bounds.northwest(), bounds.southeast());
    }

    pub fn set_bounds_from_coordinates(&mut self, top_left: Coordinate, bottom_right: Coordinate) {
        self.bounds[0] = top_left.longitude;
        self.bounds[1] = bottom_right.latitude;
        self.bounds[2] = bottom_right.longitude;
        self.bounds[3] = top_left.latitude;
    }

    pub fn url(&self, server_root: &str) -> String {
        format!(
            "http://{}/api/{}/{{z}}/{{x}}/{{y}}{}.{}",
            server_root,
            self.id,
            if self.supports_dpi_ratio { "{ratio}" } else { "" },
            self.tile_format.extension()
        )
    }

    pub fn nodata<T>(&self) -> Option<T>
    where
        T: NumCast,
    {
        match self.nodata {
            Some(nodata) => NumCast::from(nodata),
            None => None,
        }
    }

    pub fn to_tile_json(&self, server_root: &str) -> TileJson {
        TileJson {
            tilejson: "2.0.0".to_string(),
            scheme: self.scheme.clone(),
            r#type: "overlay".to_string(),
            name: self.name.clone(),
            description: self.description.clone(),
            minzoom: self.min_zoom,
            maxzoom: self.max_zoom,
            bounds: self.bounds,
            minvalue: self.min_value,
            maxvalue: self.max_value,
            tiles: [self.url(server_root)].to_vec(),
            additional_data: self.additional_data.clone(),
        }
    }
}

pub fn to_raster_data_type(type_info: GdalDataType) -> RasterDataType {
    match type_info {
        GdalDataType::UInt8 => RasterDataType::Byte,
        GdalDataType::Int32 => RasterDataType::Int32,
        GdalDataType::UInt32 => RasterDataType::UInt32,
        _ => RasterDataType::Float,
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct TileJson {
    tilejson: String,
    scheme: String,
    r#type: String,
    name: String,
    description: String,
    minzoom: i32,
    maxzoom: i32,
    bounds: [f64; 4],
    minvalue: f64,
    maxvalue: f64,
    tiles: Vec<String>,
    #[cfg_attr(feature = "serde", serde(flatten))]
    additional_data: HashMap<String, String>,
}

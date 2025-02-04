use crate::tileformat::TileFormat;
use gdal::raster::GdalDataType;
use geo::RasterDataType;
use geo::{crs::Epsg, Coordinate, LatLonBounds};
use num::NumCast;
use std::{collections::HashMap, path::PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "specta", derive(specta::Type))]
pub struct LayerId(u64);

impl std::fmt::Display for LayerId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl LayerId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

impl From<u64> for LayerId {
    fn from(val: u64) -> LayerId {
        LayerId::new(val)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
#[cfg_attr(feature = "specta", derive(specta::Type))]
pub enum LayerSourceType {
    GeoPackage,
    Mbtiles,
    ArcAscii,
    GeoTiff,
    Netcdf,
    Unknown,
}

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "specta", derive(specta::Type))]
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
    pub epsg: Option<Epsg>,
    pub tile_format: TileFormat,
    pub supports_dpi_ratio: bool,
    pub source_format: LayerSourceType,
    pub scheme: String,
    #[cfg_attr(feature = "serde", serde(skip))]
    pub additional_data: HashMap<String, String>,
    #[cfg_attr(feature = "serde", serde(skip))]
    pub band_nr: Option<usize>,
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
            "{}/api/{}/{{z}}/{{x}}/{{y}}{}.{}",
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
        GdalDataType::Int8 => RasterDataType::Int8,
        GdalDataType::UInt8 => RasterDataType::Uint8,
        GdalDataType::Int16 => RasterDataType::Int16,
        GdalDataType::UInt16 => RasterDataType::Uint16,
        GdalDataType::Int32 => RasterDataType::Int32,
        GdalDataType::UInt32 => RasterDataType::Uint32,
        GdalDataType::Int64 => RasterDataType::Int64,
        GdalDataType::UInt64 => RasterDataType::Uint64,
        GdalDataType::Float64 => RasterDataType::Float64,
        GdalDataType::Float32 | GdalDataType::Unknown => RasterDataType::Float32,
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct TileJson {
    pub tilejson: String,
    pub scheme: String,
    pub r#type: String,
    pub name: String,
    pub description: String,
    pub minzoom: i32,
    pub maxzoom: i32,
    pub bounds: [f64; 4],
    pub minvalue: f64,
    pub maxvalue: f64,
    pub tiles: Vec<String>,
    #[cfg_attr(feature = "serde", serde(flatten))]
    pub additional_data: HashMap<String, String>,
}

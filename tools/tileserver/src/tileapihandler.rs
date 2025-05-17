use std::{collections::HashMap, sync::Arc};

use axum::{
    Json,
    body::Body,
    http::{self, StatusCode},
    routing::get,
};

use geo::{Coordinate, LatLonBounds, Tile, ZoomLevelStrategy};
use inf::{Color, Legend, colormap::ColorMap, legend};
use std::ops::Range;
use tiler::{
    ColorMappedTileRequest, DirectoryTileProvider, LayerId, LayerMetadata, TileData, TileFormat, TileJson, TileProvider, TileRequest,
    tileproviderfactory::{TileProviderOptions, create_tile_provider},
};
use tower::ServiceBuilder;
use tower_http::trace::TraceLayer;

use crate::{AppError, Error, Result};

#[derive(Clone)]
pub enum StatusEvent {
    Layers(Vec<LayerMetadata>),
    TileServed(LayerId),
}

#[derive(serde::Serialize)]
pub struct RasterValueResponse {
    value: f32,
}

#[derive(serde::Serialize)]
pub struct LayersResponse {
    layers: Vec<LayerMetadata>,
}

struct State {
    api: TileApiHandler,
}

struct TileResponse {
    data: TileData,
}

impl From<TileData> for TileResponse {
    fn from(data: TileData) -> Self {
        Self { data }
    }
}

impl State {
    fn new(gis_dir: &std::path::Path, status_tx: tokio::sync::broadcast::Sender<StatusEvent>) -> Self {
        match TileApiHandler::new(gis_dir, status_tx) {
            Ok(api) => Self { api },
            Err(err) => {
                log::error!("Failed to create tile server api handler: {err}");
                std::process::exit(1);
            }
        }
    }
}

fn parse_layer_id(layer: &str) -> Result<LayerId> {
    Ok(layer
        .parse::<u64>()
        .map_err(|_| Error::InvalidArgument(format!("Invalid layer id: {}", layer)))?
        .into())
}

async fn list_layers(state: axum::Extension<Arc<State>>, headers: http::HeaderMap) -> std::result::Result<Json<LayersResponse>, AppError> {
    Ok(state.api.list_layers(headers)?)
}

async fn layer_json(
    state: axum::Extension<Arc<State>>,
    axum::extract::Path(layer): axum::extract::Path<String>,
    headers: http::HeaderMap,
) -> std::result::Result<Json<TileJson>, AppError> {
    Ok(state.api.get_tile_json(layer, headers)?)
}

async fn layer_tile(
    state: axum::Extension<Arc<State>>,
    axum::extract::Path((layer, z, x, y)): axum::extract::Path<(String, i32, i32, String)>,
    headers: http::HeaderMap,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> std::result::Result<TileResponse, AppError> {
    let (tile, dpi) = parse_tile(z, x, &y)?;
    Ok(state.api.get_tile(layer.as_str(), tile, dpi, headers, params).await?.into())
}

async fn tile_diff(
    state: axum::Extension<Arc<State>>,
    axum::extract::Path((layer1, layer2, z, x, y)): axum::extract::Path<(String, String, i32, i32, String)>,
    headers: http::HeaderMap,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> std::result::Result<TileResponse, AppError> {
    let (tile, dpi) = parse_tile(z, x, &y)?;
    Ok(state
        .api
        .diff_tile(layer1.as_str(), layer2.as_str(), tile, dpi, headers, params)
        .await?
        .into())
}

async fn layer_value_range(
    state: axum::Extension<Arc<State>>,
    axum::extract::Path(layer): axum::extract::Path<String>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> std::result::Result<Json<std::ops::Range<f64>>, AppError> {
    Ok(state.api.get_value_range(layer.as_str(), params).await?)
}

async fn layer_raster_value(
    state: axum::Extension<Arc<State>>,
    axum::extract::Path(layer): axum::extract::Path<String>,
    axum::extract::Query(params): axum::extract::Query<HashMap<String, String>>,
) -> std::result::Result<Json<RasterValueResponse>, AppError> {
    Ok(state.api.get_raster_value(layer.as_str(), params).await?)
}

pub fn create_router(gis_dir: &std::path::Path) -> (axum::routing::Router, tokio::sync::broadcast::Receiver<StatusEvent>) {
    let (status_tx, status_rx) = tokio::sync::broadcast::channel(100);

    (
        axum::Router::new()
            .route("/api/layers", get(list_layers))
            .route("/api/{layer}", get(layer_json))
            .route("/api/{layer}/{z}/{x}/{y}", get(layer_tile))
            .route("/api/{layer}/valuerange", get(layer_value_range))
            .route("/api/{layer}/rastervalue", get(layer_raster_value))
            .route("/api/diff/{layer1}/{layer2}/{z}/{x}/{y}", get(tile_diff))
            .layer(axum::Extension(Arc::new(State::new(gis_dir, status_tx))))
            .layer(ServiceBuilder::new().layer(TraceLayer::new_for_http())),
        status_rx,
    )
}

const fn tile_format_content_type(tile_format: TileFormat) -> &'static str {
    match tile_format {
        TileFormat::Protobuf => "application/protobuf",
        TileFormat::Png | TileFormat::FloatEncodedPng => "image/png",
        TileFormat::Jpeg => "image/jpeg",
        TileFormat::RasterTile | TileFormat::Unknown => "application/octet-stream",
    }
}

fn parse_coordinate(val: &str) -> Result<f64> {
    if let Ok(val) = val.parse::<f64>() {
        Ok(val)
    } else {
        Err(Error::InvalidArgument(format!("Invalid coordinate value: {}", val)))
    }
}

fn parse_coordinate_param(query_params: &HashMap<String, String>, lat_name: &str, lon_name: &str) -> Result<geo::Coordinate> {
    if let (Some(lat), Some(lon)) = (query_params.get(lat_name), query_params.get(lon_name)) {
        return Ok(geo::Coordinate {
            latitude: parse_coordinate(lat)?,
            longitude: parse_coordinate(lon)?,
        });
    }

    Err(Error::InvalidArgument(format!("Missing {} or {} parameter", lat_name, lon_name)))
}

fn host_header(headers: &axum::http::HeaderMap) -> Result<&str> {
    if let Some(host) = headers.get("host") {
        match host.to_str() {
            Ok(host) => Ok(host),
            Err(err) => Err(Error::Runtime(format!("Failed to parse the HOST header: {}", err))),
        }
    } else {
        Err(Error::Runtime("Failed to extract the HOST header".to_string()))
    }
}

impl axum::response::IntoResponse for TileResponse {
    fn into_response(self) -> axum::response::Response {
        if self.data.is_empty() {
            return (StatusCode::OK, "").into_response();
        }

        let mut response = axum::response::Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", tile_format_content_type(self.data.format));

        if self.data.format == TileFormat::Protobuf {
            response = response.header("Content-Encoding", "gzip");
        }

        response
            .body(Body::from(self.data.data))
            .unwrap_or((StatusCode::INTERNAL_SERVER_ERROR, "Failed to create response").into_response())
    }
}

pub struct TileApiHandler {
    tile_provider: Box<dyn TileProvider + Send + Sync>,
    status_tx: tokio::sync::broadcast::Sender<StatusEvent>,
}

impl TileApiHandler {
    pub fn new(gis_dir: &std::path::Path, status_tx: tokio::sync::broadcast::Sender<StatusEvent>) -> Result<Self> {
        let opts = TileProviderOptions {
            calculate_stats: true,
            zoom_level_strategy: ZoomLevelStrategy::PreferHigher,
        };

        let tile_provider = create_tile_provider(gis_dir, &opts)?;

        let _ = status_tx.send(StatusEvent::Layers(tile_provider.layers().clone()));
        Ok(TileApiHandler { tile_provider, status_tx })
    }

    pub fn get_tile_json(&self, layer: String, headers: axum::http::HeaderMap) -> Result<Json<TileJson>> {
        let layer = parse_layer_id(layer.strip_suffix(".tilejson").unwrap_or_default())?;
        Ok(Json(self.tile_provider.layer(layer)?.to_tile_json(host_header(&headers)?)))
    }

    pub fn list_layers(&self, headers: axum::http::HeaderMap) -> Result<Json<LayersResponse>> {
        let host = host_header(&headers)?;
        let layers = self
            .tile_provider
            .layers()
            .iter()
            .map(|meta| {
                let mut meta_with_url = meta.clone();
                meta_with_url.url = meta_with_url.url(host);
                meta_with_url
            })
            .collect::<Vec<LayerMetadata>>();
        Ok(Json(LayersResponse { layers }))
    }

    async fn fetch_tile(layer_meta: LayerMetadata, tile: Tile, dpi: u8, tile_format: TileFormat, tile_size: u16) -> Result<TileData> {
        let (send, recv) = tokio::sync::oneshot::channel();

        rayon::spawn(move || {
            let tile_request = TileRequest {
                tile,
                dpi_ratio: dpi,
                tile_format,
                tile_size,
            };

            let tile = DirectoryTileProvider::get_tile_for_layer(&layer_meta, &tile_request);
            let _ = send.send(tile);
        });

        recv.await.expect("Panic in rayon::spawn")
    }

    async fn fetch_diff_tile(
        layer_meta1: LayerMetadata,
        layer_meta2: LayerMetadata,
        tile: Tile,
        dpi: u8,
        tile_format: TileFormat,
        tile_size: u16,
    ) -> Result<TileData> {
        let (send, recv) = tokio::sync::oneshot::channel();

        rayon::spawn(move || {
            let tile_request = TileRequest {
                tile,
                dpi_ratio: dpi,
                tile_format,
                tile_size,
            };

            let tile = DirectoryTileProvider::diff_tile(&layer_meta1, &layer_meta2, &tile_request);
            let _ = send.send(tile);
        });

        recv.await.expect("Panic in rayon::spawn")
    }

    async fn fetch_tile_color_mapped(
        layer_meta: LayerMetadata,
        value_range: Range<Option<f64>>,
        cmap: String,
        tile: Tile,
        dpi: u8,
        tile_size: u16,
    ) -> Result<TileData> {
        let (send, recv) = tokio::sync::oneshot::channel();

        rayon::spawn(move || {
            let legend = create_legend(
                &cmap,
                value_range.start.unwrap_or(layer_meta.min_value),
                value_range.end.unwrap_or(layer_meta.max_value),
            );

            match legend {
                Ok(legend) => {
                    let tile_request = ColorMappedTileRequest {
                        tile,
                        dpi_ratio: dpi,
                        tile_size,
                        legend: &legend,
                    };

                    let tile = DirectoryTileProvider::get_tile_color_mapped_for_layer(&layer_meta, &tile_request);
                    let _ = send.send(tile);
                }
                Err(err) => {
                    let _ = send.send(Err(err));
                }
            }
        });

        recv.await.expect("Panic in rayon::spawn")
    }

    async fn fetch_extent_value_range(
        layer_meta: LayerMetadata,
        top_left: Coordinate,
        bottom_right: Coordinate,
        zoom: Option<i32>,
    ) -> Result<Range<f64>> {
        let (send, recv) = tokio::sync::oneshot::channel();

        rayon::spawn(move || {
            let _ = send.send(DirectoryTileProvider::extent_value_range_for_layer(
                &layer_meta,
                LatLonBounds::hull(top_left, bottom_right),
                zoom,
            ));
        });

        recv.await.expect("Panic in rayon::spawn")
    }

    async fn fetch_raster_value(layer_meta: LayerMetadata, coord: Coordinate) -> Result<Option<f32>> {
        let (send, recv) = tokio::sync::oneshot::channel();

        rayon::spawn(move || {
            let _ = send.send(DirectoryTileProvider::get_raster_value_for_layer(&layer_meta, coord, 1));
        });

        recv.await.expect("Panic in rayon::spawn")
    }

    pub async fn get_tile(
        &self,
        layer: &str,
        tile: Tile,
        dpi_ratio: u8,
        headers: axum::http::HeaderMap,
        params: HashMap<String, String>,
    ) -> Result<TileData> {
        let mut cmap = String::from("gray");
        let mut min_value = Option::<f64>::None;
        let mut max_value = Option::<f64>::None;
        let mut tile_format = Option::<TileFormat>::None;
        let mut tile_size = 256;

        if let Some(cmap_str) = params.get("cmap") {
            cmap = cmap_str.to_string();
        }

        if let Some(min_str) = params.get("min") {
            min_value = min_str.parse::<f64>().ok();
        }

        if let Some(max_str) = params.get("max") {
            max_value = max_str.parse::<f64>().ok();
        }

        if let Some(format) = params.get("tile_format") {
            tile_format = Some(TileFormat::from(format.as_str()));
        }

        if let Some(size) = params.get("tile_size") {
            tile_size = size.parse::<u16>().unwrap_or(256);
        }

        log::debug!(
            "Tile request {}/{}/{}/{}: cmap({}) min({:?}) max({:?}) format({:?})",
            layer,
            tile.z,
            tile.x,
            tile.y,
            cmap,
            min_value,
            max_value,
            tile_format,
        );

        let layer_id = parse_layer_id(layer)?;
        let layer_meta = self.tile_provider.layer(layer_id)?;
        let tile = match tile_format {
            Some(TileFormat::FloatEncodedPng | TileFormat::RasterTile) => {
                Self::fetch_tile(layer_meta, tile, dpi_ratio, tile_format.unwrap(), tile_size).await?
            }
            _ => Self::fetch_tile_color_mapped(layer_meta, min_value..max_value, cmap, tile, dpi_ratio, tile_size).await?,
        };

        if tile.format == TileFormat::Protobuf {
            if let Some(host) = headers.get("accept-encoding") {
                if !(host.to_str().unwrap_or_default().contains("gzip")) {
                    log::warn!("Requester does not accept gzip compression");
                }
            }
        }

        let _ = self.status_tx.send(StatusEvent::TileServed(layer_id));
        Ok(tile)
    }

    pub async fn diff_tile(
        &self,
        layer1: &str,
        layer2: &str,
        tile: Tile,
        dpi_ratio: u8,
        headers: axum::http::HeaderMap,
        params: HashMap<String, String>,
    ) -> Result<TileData> {
        let mut cmap = String::from("gray");
        let mut min_value = Option::<f64>::None;
        let mut max_value = Option::<f64>::None;
        let mut tile_format = Option::<TileFormat>::None;
        let mut tile_size = 256;

        if let Some(cmap_str) = params.get("cmap") {
            cmap = cmap_str.to_string();
        }

        if let Some(min_str) = params.get("min") {
            min_value = min_str.parse::<f64>().ok();
        }

        if let Some(max_str) = params.get("max") {
            max_value = max_str.parse::<f64>().ok();
        }

        if let Some(format) = params.get("tile_format") {
            tile_format = Some(TileFormat::from(format.as_str()));
        }

        if let Some(size) = params.get("tile_size") {
            tile_size = size.parse::<u16>().unwrap_or(256);
        }

        log::debug!(
            "Diff request {}-{}/{}/{}/{}: cmap({}) min({:?}) max({:?}) format({:?})",
            layer1,
            layer2,
            tile.z,
            tile.x,
            tile.y,
            cmap,
            min_value,
            max_value,
            tile_format,
        );

        let layer_id1 = parse_layer_id(layer1)?;
        let layer_id2 = parse_layer_id(layer2)?;

        let layer_meta1 = self.tile_provider.layer(layer_id1)?;
        let layer_meta2 = self.tile_provider.layer(layer_id2)?;
        let tile = match tile_format {
            Some(TileFormat::FloatEncodedPng | TileFormat::RasterTile) => {
                Self::fetch_diff_tile(layer_meta1, layer_meta2, tile, dpi_ratio, tile_format.unwrap(), tile_size).await?
            }
            _ => {
                return Err(Error::InvalidArgument("Diff tile does not support color mapping".to_string()));
            }
        };

        if tile.format == TileFormat::Protobuf {
            if let Some(host) = headers.get("accept-encoding") {
                if !(host.to_str().unwrap_or_default().contains("gzip")) {
                    log::warn!("Requester does not accept gzip compression");
                }
            }
        }

        let _ = self.status_tx.send(StatusEvent::TileServed(layer_id1));
        let _ = self.status_tx.send(StatusEvent::TileServed(layer_id2));
        Ok(tile)
    }

    pub async fn get_value_range(&self, layer: &str, query_params: HashMap<String, String>) -> Result<Json<Range<f64>>> {
        let mut zoom: Option<i32> = None;
        let top_left = parse_coordinate_param(&query_params, "topleft_lat", "topleft_lon")?;
        let bottom_right = parse_coordinate_param(&query_params, "bottomright_lat", "bottomright_lon")?;

        if let Some(zoom_str) = query_params.get("zoom") {
            if let Ok(zoom_int) = zoom_str.parse::<i32>() {
                if zoom_int <= u8::MAX as i32 {
                    zoom = Some(zoom_int);
                } else {
                    return Err(Error::InvalidArgument(format!("Invalid zoom level: {}", zoom_int)));
                }
            }
        }

        let layer_meta = self.tile_provider.layer(parse_layer_id(layer)?)?;

        Ok(Json(
            Self::fetch_extent_value_range(layer_meta, top_left, bottom_right, zoom).await?,
        ))
    }

    pub async fn get_raster_value(&self, layer: &str, query_params: HashMap<String, String>) -> Result<Json<RasterValueResponse>> {
        let layer_meta = self.tile_provider.layer(parse_layer_id(layer)?)?;
        let coord = parse_coordinate_param(&query_params, "lat", "lon")?;

        let val = Self::fetch_raster_value(layer_meta, coord).await?;
        Ok(Json(RasterValueResponse {
            value: val.unwrap_or(f32::NAN),
        }))
    }
}

fn parse_classified_color_map_specification(cmap_name: &str) -> Result<inf::legend::BandedLegend> {
    use inf::MappedLegend;

    if !cmap_name.starts_with('[') || !cmap_name.ends_with(']') {
        return Err(Error::Runtime("Invalid classified color map description".to_string()));
    }

    let mut bands = Vec::new();
    let classes: Vec<&str> = cmap_name[1..cmap_name.len() - 1].split(',').collect();
    for cl in classes {
        let split: Vec<&str> = cl.split(';').collect();
        if split.len() != 3 {
            return Err(Error::Runtime("Invalid classification".to_string()));
        }

        let start = split[0]
            .parse()
            .map_err(|_| Error::InvalidArgument(format!("Invalid lower bound: {}", split[0])))?;

        let end = split[1]
            .parse()
            .map_err(|_| Error::InvalidArgument(format!("Invalid upper bound: {}", split[1])))?;

        let color = Color::from_hex_string(format!("#{}", split[2]).as_str())?;

        bands.push(legend::mapper::LegendBand::new(Range { start, end }, color, String::default()));
    }

    let legend = MappedLegend::with_mapper(legend::mapper::Banded::new(bands), legend::MappingConfig::default());

    Ok(legend)
}

fn parse_tile(z: i32, x: i32, y: &str) -> Result<(Tile, u8)> {
    let (y, dpi, extension) = parse_tile_filename(y)?;

    if extension != "png" && extension != "pbf" && extension != "vrt" {
        return Err(Error::InvalidArgument("Invalid tile extension".to_string()));
    }

    Ok((Tile { z, x, y }, dpi))
}

fn parse_tile_filename(filename: &str) -> Result<(i32, u8, String)> {
    let mut dpi_ratio = 1;

    let splitted: Vec<&str> = filename.split('.').collect();
    if splitted.len() == 2 {
        let num_ratio_split = splitted[0].split('@').collect::<Vec<&str>>();
        if num_ratio_split.len() == 2 {
            if num_ratio_split[1].len() == 2 && num_ratio_split[1][1..] == *"x" {
                dpi_ratio = num_ratio_split[1][0..1]
                    .parse::<u8>()
                    .map_err(|_| Error::InvalidArgument(format!("Invalid DPI ratio: {}", num_ratio_split[1])))?;
            } else {
                return Err(Error::InvalidArgument(format!("Invalid DPI ratio: {}", num_ratio_split[1])));
            }
        } else if num_ratio_split.len() != 1 {
            return Err(Error::InvalidArgument(format!("Invalid tile filename {}", filename)));
        }

        let y_index = num_ratio_split[0]
            .parse::<i32>()
            .map_err(|_| Error::InvalidArgument(format!("Invalid tile y index: {}", num_ratio_split[0])))?;
        let extension = splitted[1];
        return Ok((y_index, dpi_ratio, extension.to_string()));
    }

    Err(Error::InvalidArgument(format!("Invalid tile filename {}", filename)))
}

fn create_legend(cmap_name: &str, min: f64, max: f64) -> Result<Legend> {
    if min > max {
        return Err(Error::Runtime("Minimum value is bigger than maximum value".to_string()));
    }

    if !cmap_name.is_empty() && cmap_name.starts_with('[') {
        Ok(Legend::Banded(parse_classified_color_map_specification(cmap_name)?))
    } else {
        Ok(Legend::Linear(legend::create_linear(
            &ColorMap::Named(cmap_name.into()),
            Range { start: min, end: max },
            None,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Result;

    #[test]
    fn test_parse_filename() -> Result<()> {
        assert_eq!(parse_tile_filename("1.png")?, (1, 1, "png".to_string()));
        assert_eq!(parse_tile_filename("2@2x.png")?, (2, 2, "png".to_string()));
        assert_eq!(parse_tile_filename("5@3x.pbf")?, (5, 3, "pbf".to_string()));
        Ok(())
    }
}

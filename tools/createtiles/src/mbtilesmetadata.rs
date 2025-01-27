use tiler::LayerMetadata;

pub struct Metadata {
    pub name: String,
    pub format: String, //application/octet-stream
    pub bounds: [f64; 4],
    pub min_zoom: i32,
    pub max_zoom: i32,
    pub attribution: Option<String>, // (HTML string): An attribution string, which explains the sources of data and/or style for the map.
    pub description: Option<String>, // A description of the tileset's content.
    pub map_type: Option<String>,    // overlay or baselayer
    pub version: Option<i32>,        // The version of the tiles
    pub additional_data: Vec<(String, String)>,
}

impl From<Metadata> for Vec<(String, String)> {
    fn from(value: Metadata) -> Self {
        let mut res = vec![
            ("name".into(), value.name),
            ("format".into(), value.format),
            (
                "bounds".into(),
                value
                    .bounds
                    .iter()
                    .map(|b| b.to_string())
                    .collect::<Vec<String>>()
                    .join(","),
            ),
            ("minzoom".into(), value.min_zoom.to_string()),
            ("maxzoom".into(), value.max_zoom.to_string()),
        ];
        if let Some(attribution) = value.attribution {
            res.push(("attribution".into(), attribution));
        }
        if let Some(description) = value.description {
            res.push(("description".into(), description));
        }
        if let Some(map_type) = value.map_type {
            res.push(("maptype".into(), map_type));
        }
        if let Some(version) = value.version {
            res.push(("version".into(), version.to_string()));
        }
        res.extend(value.additional_data);

        res
    }
}

impl Metadata {
    pub fn new(
        layer_meta: &LayerMetadata,
        min_zoom: i32,
        max_zoom: i32,
        additional_data: Vec<(String, String)>,
    ) -> Self {
        Metadata {
            name: layer_meta.name.clone(),
            format: layer_meta.tile_format.to_string(),
            bounds: layer_meta.bounds,
            min_zoom,
            max_zoom,
            attribution: None,
            description: if layer_meta.description.is_empty() {
                None
            } else {
                Some(layer_meta.description.clone())
            },
            map_type: Some("overlay".into()),
            version: None,
            additional_data,
        }
    }
}

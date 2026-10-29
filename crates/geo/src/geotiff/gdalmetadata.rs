use crate::{Error, RasterScale};
use xml::{
    common::is_whitespace_str,
    reader::{ParserConfig, XmlEvent},
};

use super::gdalghostdata::Interleave;

#[derive(Debug, Clone, Default)]
#[cfg_attr(
    target_arch = "wasm32",
    derive(tsify::Tsify, serde::Serialize, serde::Deserialize),
    tsify(from_wasm_abi, into_wasm_abi)
)]
pub struct TiffStats {
    pub minimum_value: f64,
    pub maximum_value: f64,
    pub mean: f64,
    pub standard_deviation: f64,
    pub valid_pixel_percentage: f64,
}

/// Band-specific metadata including offset, scale, and description values
#[derive(Debug, Clone, Default)]
pub struct BandMetadata {
    /// Human-readable band description
    pub description: Option<String>,
    /// The scale and offset of the raster values. The value of a pixel can be calculated as `value * scale + offset`.
    /// Missing components default to scale 1 and offset 0; `None` means neither component was parsed.
    pub scale: Option<RasterScale>,
}

struct XmlItem {
    attributes: Vec<(String, String)>,
    text: String,
}

impl XmlItem {
    fn attribute(&self, name: &str) -> Option<&str> {
        self.attributes.iter().find(|(key, _)| key == name).map(|(_, value)| value.as_str())
    }
}

/// High-level GDAL metadata parsed from TIFF tags
#[derive(Debug, Clone, Default)]
pub struct GdalMetadata {
    /// Statistical information (min, max, mean, stddev, etc.)
    pub statistics: Option<TiffStats>,
    /// Per-band metadata (description, offset, scale), indexed by zero-based band/sample index.
    pub band_metadata: Vec<BandMetadata>,
    /// Interleave mode from `IMAGE_STRUCTURE` domain
    pub interleave: Option<Interleave>,
    /// Maximum zoom level from `TILING_SCHEME` domain
    pub max_zoom: Option<i32>,
    /// Valid-data AABB in WGS84: `[west, south, east, north]`.
    pub data_bounds: Option<[f64; 4]>,
}

/// Parse a JSON-style AABB `[a,b,c,d]` as written by GDAL `metadataOptions`.
fn parse_bounds_array(raw: &str) -> Option<[f64; 4]> {
    let trimmed = raw.trim();
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return None;
    }
    let inner = &trimmed[1..trimmed.len() - 1];
    let mut values = [0.0_f64; 4];
    let mut count = 0usize;
    for part in inner.split(',') {
        if count >= 4 {
            return None;
        }
        values[count] = part.trim().parse().ok()?;
        count += 1;
    }
    if count != 4 {
        return None;
    }
    let [a, b, c, d] = values;
    if !values.iter().all(|value| value.is_finite()) || (a >= c || b >= d) {
        return None;
    }
    Some(values)
}

// GDAL metadata can have various formats:
// <GDALMetadata>
//   <Item name="STATISTICS_MAXIMUM" sample="0">254</Item>
//   <Item name="STATISTICS_MEAN" sample="0">119.11901635438</Item>
//   <Item name="STATISTICS_MINIMUM" sample="0">0</Item>
//   <Item name="STATISTICS_STDDEV" sample="0">58.60474035626</Item>
//   <Item name="STATISTICS_VALID_PERCENT" sample="0">45.34</Item>
//   <Item name="NAME" domain="TILING_SCHEME">GoogleMapsCompatible</Item>
//   <Item name="ZOOM_LEVEL" domain="TILING_SCHEME">10</Item>
//   <Item name="DESCRIPTION" sample="0" role="description">Red</Item>
//   <Item name="OFFSET" sample="0" role="offset">0</Item>
//   <Item name="SCALE" sample="0" role="scale">0.031372549019600002</Item>
//   <Item name="INTERLEAVE" domain="IMAGE_STRUCTURE">TILE</Item>
// </GDALMetadata>

/// Parse GDAL metadata XML into a structured format
///
/// Extracts statistics, band descriptions/offset/scale values, interleave mode,
/// zoom level, and data bounds. Item text is accumulated across text, CDATA, and
/// entity boundaries before deriving these values; unknown metadata is ignored.
/// Scale and offset are combined per band, defaulting missing components to 1 and 0,
/// respectively. Invalid scale/offset numbers are treated as absent.
/// Band metadata contains exactly `band_count` entries in sample order, with defaults
/// for bands without metadata. Sample indices must be less than `band_count`.
/// Only an unnamespaced `GDALMetadata` root without attributes and direct, text-only
/// `Item` children are supported. Malformed XML and invalid sample indices return errors.
///
/// # Arguments
/// * `xml` - The XML string containing GDAL metadata
/// * `band_count` - The number of raster bands
///
/// # Returns
/// * `Ok(GdalMetadata)` if parsing succeeds
/// * `Err` if XML parsing fails or sample indices are invalid
pub fn parse_gdal_metadata(xml: &str, band_count: u32) -> crate::Result<GdalMetadata> {
    let items = parse_gdal_metadata_items(xml)?;
    let mut metadata = GdalMetadata {
        band_metadata: vec![BandMetadata::default(); band_count as usize],
        ..Default::default()
    };
    let mut band_scaling = vec![(None, None); band_count as usize];
    let mut stats = TiffStats::default();
    let mut has_statistics = false;

    for item in &items {
        let sample = item
            .attribute("sample")
            .map(|value| {
                let sample = value
                    .parse::<u32>()
                    .map_err(|e| Error::InvalidArgument(format!("Invalid GDAL metadata sample attribute {value:?}: {e}")))?;
                if sample >= band_count {
                    return Err(Error::InvalidArgument(format!(
                        "GDAL metadata sample index {sample} is out of range for {band_count} bands"
                    )));
                }
                Ok(sample as usize)
            })
            .transpose()?;
        let domain = item.attribute("domain");
        let role = item.attribute("role");
        let data = &item.text;

        match item.attribute("name") {
            Some("STATISTICS_MINIMUM") => {
                stats.minimum_value = data.parse::<f64>().unwrap_or_default();
                has_statistics = true;
            }
            Some("STATISTICS_MAXIMUM") => {
                stats.maximum_value = data.parse::<f64>().unwrap_or_default();
                has_statistics = true;
            }
            Some("STATISTICS_MEAN") => {
                stats.mean = data.parse::<f64>().unwrap_or_default();
                has_statistics = true;
            }
            Some("STATISTICS_STDDEV") => {
                stats.standard_deviation = data.parse::<f64>().unwrap_or_default();
                has_statistics = true;
            }
            Some("STATISTICS_VALID_PERCENT") => {
                stats.valid_pixel_percentage = data.parse::<f64>().unwrap_or_default();
                has_statistics = true;
            }
            Some("NAME") if domain == Some("TILING_SCHEME") && !data.eq_ignore_ascii_case("GoogleMapsCompatible") => {
                return Err(Error::Runtime(format!("Unsupported TILING_SCHEME: {data}")));
            }
            Some("ZOOM_LEVEL") if domain == Some("TILING_SCHEME") => {
                metadata.max_zoom = data.parse::<i32>().ok();
            }
            Some("DESCRIPTION") => {
                if let Some(sample) = sample {
                    metadata.band_metadata[sample].description = Some(data.clone());
                }
            }
            Some("OFFSET") if role == Some("offset") => {
                if let Some(sample) = sample {
                    band_scaling[sample].1 = data.parse::<f64>().ok();
                }
            }
            Some("SCALE") if role == Some("scale") => {
                if let Some(sample) = sample {
                    band_scaling[sample].0 = data.parse::<f64>().ok();
                }
            }
            Some("INTERLEAVE") if domain == Some("IMAGE_STRUCTURE") => {
                metadata.interleave = super::gdalghostdata::parse_interleave_mode(data);
            }
            Some("data_bounds") => {
                metadata.data_bounds = parse_bounds_array(data);
            }
            _ => {}
        }
    }

    // Combine components after parsing so an invalid duplicate still clears the previous value.
    for (band, (scale, offset)) in metadata.band_metadata.iter_mut().zip(band_scaling) {
        band.scale = (scale.is_some() || offset.is_some()).then_some(RasterScale {
            scale: scale.unwrap_or(1.0),
            offset: offset.unwrap_or(0.0),
        });
    }

    metadata.statistics = has_statistics.then_some(stats);

    Ok(metadata)
}

fn parse_gdal_metadata_items(xml: &str) -> crate::Result<Vec<XmlItem>> {
    let mut items = Vec::new();
    let mut current_item: Option<XmlItem> = None;
    let mut root_seen = false;
    let mut root_open = false;
    let reader = ParserConfig::new()
        .trim_whitespace(false)
        .coalesce_characters(false)
        .create_reader(xml.as_bytes());

    for event in reader {
        match event.map_err(|e| Error::Runtime(format!("XML parse error: {e}")))? {
            XmlEvent::StartElement {
                name,
                attributes,
                namespace,
            } => {
                // GDAL's metadata schema uses unqualified element and attribute names.
                if name.namespace.is_some()
                    || name.prefix.is_some()
                    || !namespace.is_essentially_empty()
                    || attributes
                        .iter()
                        .any(|attr| attr.name.namespace.is_some() || attr.name.prefix.is_some())
                {
                    return Err(Error::Runtime("Unsupported XML namespace in GDAL metadata".into()));
                }
                if !root_seen && name.local_name == "GDALMetadata" && attributes.is_empty() {
                    root_seen = true;
                    root_open = true;
                } else if root_open && current_item.is_none() && name.local_name == "Item" {
                    current_item = Some(XmlItem {
                        attributes: attributes.into_iter().map(|attr| (attr.name.local_name, attr.value)).collect(),
                        text: String::new(),
                    });
                } else {
                    return Err(Error::Runtime(format!("Unsupported GDAL metadata XML element: {name}")));
                }
            }
            XmlEvent::EndElement { name } => {
                if name.local_name == "Item" {
                    let item = current_item
                        .take()
                        .ok_or_else(|| Error::Runtime("Unexpected GDAL metadata Item closing tag".into()))?;
                    items.push(item);
                } else if name.local_name == "GDALMetadata" && current_item.is_none() {
                    root_open = false;
                } else {
                    return Err(Error::Runtime(format!("Unexpected GDAL metadata XML closing tag: {name}")));
                }
            }
            XmlEvent::Characters(data) | XmlEvent::CData(data) | XmlEvent::Whitespace(data) => {
                if let Some(item) = &mut current_item {
                    item.text.push_str(&data);
                } else if !is_whitespace_str(&data) {
                    return Err(Error::Runtime("Unexpected text outside GDAL metadata Item".into()));
                }
            }
            XmlEvent::ProcessingInstruction { .. } => {
                return Err(Error::Runtime("Unsupported XML processing instruction in GDAL metadata".into()));
            }
            XmlEvent::Doctype { .. } => {
                return Err(Error::Runtime("Unsupported XML document type in GDAL metadata".into()));
            }
            XmlEvent::StartDocument { .. } | XmlEvent::EndDocument | XmlEvent::Comment(_) => {}
        }
    }

    if !root_seen || root_open || current_item.is_some() {
        return Err(Error::Runtime("Incomplete GDAL metadata XML document".into()));
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_abs_diff_eq;

    #[test]
    fn parse_statistics_xml() {
        let xml = r#"
<GDALMetadata>
    <Item name="STATISTICS_MAXIMUM" sample="0">254</Item>
    <Item name="STATISTICS_MEAN" sample="0">119.11901635438</Item>
    <Item name="STATISTICS_MINIMUM" sample="0">0</Item>
    <Item name="STATISTICS_STDDEV" sample="0">58.60474035626</Item>
    <Item name="STATISTICS_VALID_PERCENT" sample="0">45.34</Item>
    <Item name="NAME" domain="TILING_SCHEME">GoogleMapsCompatible</Item>
    <Item name="ZOOM_LEVEL" domain="TILING_SCHEME">10</Item>
</GDALMetadata>
       "#;
        let metadata = parse_gdal_metadata(xml, 2).expect("Should parse successfully");

        assert_eq!(metadata.band_metadata.len(), 2);
        assert!(
            metadata
                .band_metadata
                .iter()
                .all(|band| band.description.is_none() && band.scale.is_none())
        );
        assert!(metadata.statistics.is_some());
        let stats = metadata.statistics.unwrap();
        assert_eq!(stats.minimum_value, 0.0);
        assert_eq!(stats.maximum_value, 254.0);
        assert_abs_diff_eq!(stats.mean, 119.11901635438, epsilon = 1e-10);
        assert_abs_diff_eq!(stats.standard_deviation, 58.60474035626, epsilon = 1e-10);
        assert_abs_diff_eq!(stats.valid_pixel_percentage, 45.34, epsilon = 1e-10);
        assert_eq!(metadata.max_zoom, Some(10));
    }

    #[test]
    fn parse_gdal_metadata_with_band_metadata() {
        let xml = r#"
<GDALMetadata>
  <Item name="OFFSET" sample="0" role="offset">0</Item>
  <Item name="SCALE" sample="0" role="scale">0.0313725490196</Item>
  <Item name="DESCRIPTION" sample="0" role="description">Red</Item>
  <Item name="DESCRIPTION" sample="1" role="description">Green</Item>
  <Item name="OFFSET" sample="1" role="offset">0</Item>
  <Item name="SCALE" sample="1" role="scale">0.0313725490196</Item>
  <Item name="OFFSET" sample="2" role="offset">0</Item>
  <Item name="SCALE" sample="2" role="scale">0.0313725490196</Item>
  <Item name="DESCRIPTION" sample="2" role="description">Blue</Item>
</GDALMetadata>
        "#;
        let metadata = parse_gdal_metadata(xml, 3).expect("Should parse successfully");

        assert_eq!(metadata.band_metadata.len(), 3);

        for band in &metadata.band_metadata {
            let scale = band.scale.unwrap();
            assert_eq!(scale.offset, 0.0);
            assert_abs_diff_eq!(scale.scale, 0.0313725490196);
        }
        assert_eq!(metadata.band_metadata[0].description.as_deref(), Some("Red"));
        assert_eq!(metadata.band_metadata[1].description.as_deref(), Some("Green"));
        assert_eq!(metadata.band_metadata[2].description.as_deref(), Some("Blue"));
    }

    #[test]
    fn parse_gdal_metadata_with_interleave() {
        let xml = r#"
<GDALMetadata>
  <Item name="INTERLEAVE" domain="IMAGE_STRUCTURE">TILE</Item>
</GDALMetadata>
        "#;
        let metadata = parse_gdal_metadata(xml, 1).expect("Should parse successfully");

        assert_eq!(metadata.interleave, Some(Interleave::Tile));
    }

    #[test]
    fn parse_gdal_metadata_comprehensive() {
        let xml = r#"
<GDALMetadata>
  <Item name="OFFSET" sample="0" role="offset">0</Item>
  <Item name="SCALE" sample="0" role="scale">0.0313725490196</Item>
  <Item name="OFFSET" sample="1" role="offset">0</Item>
  <Item name="SCALE" sample="1" role="scale">0.0313725490196</Item>
  <Item name="OFFSET" sample="2" role="offset">0</Item>
  <Item name="SCALE" sample="2" role="scale">0.0313725490196</Item>
  <Item name="OFFSET" sample="3" role="offset">0</Item>
  <Item name="SCALE" sample="3" role="scale">0.0313725490196</Item>
  <Item name="OFFSET" sample="4" role="offset">0</Item>
  <Item name="SCALE" sample="4" role="scale">0.0313725490196</Item>
  <Item name="INTERLEAVE" domain="IMAGE_STRUCTURE">TILE</Item>
</GDALMetadata>
        "#;
        let metadata = parse_gdal_metadata(xml, 5).expect("Should parse successfully");

        assert_eq!(metadata.band_metadata.len(), 5);
        assert_eq!(metadata.interleave, Some(Interleave::Tile));

        for band in &metadata.band_metadata {
            let scale = band.scale.unwrap();
            assert_eq!(scale.offset, 0.0);
            assert_abs_diff_eq!(scale.scale, 0.0313725490196);
        }
    }

    #[test]
    fn parse_gdal_metadata_with_statistics_and_band_data() {
        let xml = r#"
<GDALMetadata>
    <Item name="STATISTICS_MAXIMUM" sample="0">254</Item>
    <Item name="STATISTICS_MEAN" sample="0">119.11901635438</Item>
    <Item name="STATISTICS_MINIMUM" sample="0">0</Item>
    <Item name="STATISTICS_STDDEV" sample="0">58.60474035626</Item>
    <Item name="STATISTICS_VALID_PERCENT" sample="0">45.34</Item>
    <Item name="OFFSET" sample="0" role="offset">10</Item>
    <Item name="SCALE" sample="0" role="scale">2.5</Item>
    <Item name="INTERLEAVE" domain="IMAGE_STRUCTURE">PIXEL</Item>
</GDALMetadata>
        "#;
        let metadata = parse_gdal_metadata(xml, 1).expect("Should parse successfully");

        assert!(metadata.statistics.is_some());
        assert_eq!(metadata.band_metadata.len(), 1);
        assert_eq!(metadata.interleave, Some(Interleave::Pixel));

        let band = &metadata.band_metadata[0];
        let scale = band.scale.unwrap();
        assert_eq!(scale.offset, 10.0);
        assert_abs_diff_eq!(scale.scale, 2.5, epsilon = 1e-10);
    }

    #[test]
    fn parse_gdal_metadata_partial_band_data_fills_gaps_and_trailing_bands() {
        let xml = r#"
<GDALMetadata>
  <Item name="OFFSET" sample="0" role="offset">5</Item>
  <Item name="SCALE" sample="2" role="scale">1.5</Item>
</GDALMetadata>
        "#;
        let metadata = parse_gdal_metadata(xml, 5).expect("Should parse successfully");

        assert_eq!(
            metadata
                .band_metadata
                .iter()
                .map(|band| (band.description.as_deref(), band.scale))
                .collect::<Vec<_>>(),
            vec![
                (None, Some(RasterScale { scale: 1.0, offset: 5.0 })),
                (None, None),
                (None, Some(RasterScale { scale: 1.5, offset: 0.0 })),
                (None, None),
                (None, None)
            ]
        );
    }

    #[test]
    fn parse_gdal_metadata_descriptions_without_scaling() {
        let xml = r#"<GDALMetadata>
  <Item name="DESCRIPTION" sample="0" role="description">Red</Item>
  <Item name="DESCRIPTION" sample="2" role="description">Blue</Item>
</GDALMetadata>"#;
        let metadata = parse_gdal_metadata(xml, 3).expect("Should parse successfully");

        assert_eq!(
            metadata
                .band_metadata
                .iter()
                .map(|band| (band.description.as_deref(), band.scale))
                .collect::<Vec<_>>(),
            vec![(Some("Red"), None), (None, None), (Some("Blue"), None)]
        );
    }

    #[test]
    fn parse_gdal_metadata_combines_scaling_in_either_order_per_band() {
        let xml = r#"<GDALMetadata>
  <Item name="DESCRIPTION" sample="2" role="description">Blue</Item>
  <Item name="SCALE" sample="2" role="scale">3.5</Item>
  <Item name="OFFSET" sample="0" role="offset">-10</Item>
  <Item name="DESCRIPTION" sample="0" role="description">Red</Item>
  <Item name="SCALE" sample="0" role="scale">2.5</Item>
  <Item name="OFFSET" sample="2" role="offset">20</Item>
</GDALMetadata>"#;
        let metadata = parse_gdal_metadata(xml, 3).expect("Should parse successfully");

        assert_eq!(
            metadata
                .band_metadata
                .iter()
                .map(|band| (band.description.as_deref(), band.scale))
                .collect::<Vec<_>>(),
            vec![
                (Some("Red"), Some(RasterScale { scale: 2.5, offset: -10.0 })),
                (None, None),
                (Some("Blue"), Some(RasterScale { scale: 3.5, offset: 20.0 }))
            ]
        );
    }

    #[test]
    fn parse_gdal_metadata_preserves_explicit_identity_scaling() {
        for (name, role, value) in [("SCALE", "scale", 1), ("OFFSET", "offset", 0)] {
            let xml = format!(r#"<GDALMetadata><Item name="{name}" sample="0" role="{role}">{value}</Item></GDALMetadata>"#);
            let metadata = parse_gdal_metadata(&xml, 1).expect("Should parse successfully");

            assert_eq!(metadata.band_metadata[0].scale, Some(RasterScale::default()));
        }
    }

    #[test]
    fn parse_gdal_metadata_invalid_scaling_values_are_absent() {
        for (scale, offset, expected) in [
            ("invalid", "invalid", None),
            ("", "", None),
            (" 2.5 ", " 10 ", None),
            ("2.5", "invalid", Some(RasterScale { scale: 2.5, offset: 0.0 })),
            ("invalid", "10", Some(RasterScale { scale: 1.0, offset: 10.0 })),
        ] {
            let xml = format!(
                r#"<GDALMetadata>
  <Item name="OFFSET" sample="0" role="offset">{offset}</Item>
  <Item name="SCALE" sample="0" role="scale">{scale}</Item>
</GDALMetadata>"#
            );
            let metadata = parse_gdal_metadata(&xml, 1).expect("Invalid scaling should not fail parsing");

            assert_eq!(metadata.band_metadata.len(), 1);
            assert_eq!(metadata.band_metadata[0].scale, expected, "scale={scale:?}, offset={offset:?}");
        }
    }

    #[test]
    fn parse_gdal_metadata_duplicate_scaling_uses_last_value_even_if_invalid() {
        for (scale, offset, expected) in [
            ("invalid", "invalid", None),
            ("3.5", "invalid", Some(RasterScale { scale: 3.5, offset: 0.0 })),
            ("invalid", "20", Some(RasterScale { scale: 1.0, offset: 20.0 })),
        ] {
            let xml = format!(
                r#"<GDALMetadata>
  <Item name="OFFSET" sample="0" role="offset">10</Item>
  <Item name="SCALE" sample="0" role="scale">2.5</Item>
  <Item name="SCALE" sample="0" role="scale">{scale}</Item>
  <Item name="OFFSET" sample="0" role="offset">{offset}</Item>
</GDALMetadata>"#
            );
            let metadata = parse_gdal_metadata(&xml, 1).expect("Invalid scaling should not fail parsing");

            assert_eq!(metadata.band_metadata[0].scale, expected, "scale={scale:?}, offset={offset:?}");
        }
    }

    #[test]
    fn parse_gdal_metadata_empty() {
        for band_count in [0, 1, 3] {
            let xml = r#"<GDALMetadata></GDALMetadata>"#;
            let metadata = parse_gdal_metadata(xml, band_count).expect("Should parse successfully");

            assert!(metadata.statistics.is_none());
            assert_eq!(metadata.band_metadata.len(), band_count as usize);
            assert!(
                metadata
                    .band_metadata
                    .iter()
                    .all(|band| band.description.is_none() && band.scale.is_none())
            );
            assert!(metadata.interleave.is_none());
            assert!(metadata.data_bounds.is_none());
        }
    }

    #[test]
    fn parse_gdal_metadata_data_bounds() {
        let xml = r#"
<GDALMetadata>
  <Item name="data_bounds">[4.6600340395095525,52.22028643027196,5.132071086172366,52.50897297186259]</Item>
</GDALMetadata>
        "#;
        let metadata = parse_gdal_metadata(xml, 1).expect("Should parse successfully");

        let wgs84 = metadata.data_bounds.expect("data_bounds_4326");
        assert_abs_diff_eq!(wgs84[0], 4.6600340395095525, epsilon = 1e-12);
        assert_abs_diff_eq!(wgs84[1], 52.22028643027196, epsilon = 1e-12);
        assert_abs_diff_eq!(wgs84[2], 5.132071086172366, epsilon = 1e-12);
        assert_abs_diff_eq!(wgs84[3], 52.50897297186259, epsilon = 1e-12);
    }

    #[test]
    fn parse_bounds_array_rejects_invalid() {
        assert!(parse_bounds_array("").is_none());
        assert!(parse_bounds_array("[1,2,3]").is_none());
        assert!(parse_bounds_array("[1,2,0,4]").is_none()); // west >= east
    }

    #[test]
    fn parse_gdal_metadata_interleave_band() {
        let xml = r#"
<GDALMetadata>
  <Item name="INTERLEAVE" domain="IMAGE_STRUCTURE">BAND</Item>
</GDALMetadata>
        "#;
        let metadata = parse_gdal_metadata(xml, 1).expect("Should parse successfully");

        assert_eq!(metadata.interleave, Some(Interleave::Band));
    }

    #[test]
    fn parse_gdal_metadata_invalid_interleave() {
        let xml = r#"
<GDALMetadata>
  <Item name="INTERLEAVE" domain="IMAGE_STRUCTURE">INVALID</Item>
</GDALMetadata>
        "#;
        let metadata = parse_gdal_metadata(xml, 1).expect("Should parse successfully");

        assert_eq!(metadata.interleave, None);
    }

    #[test]
    fn parse_gdal_metadata_interleave_wrong_domain() {
        let xml = r#"
<GDALMetadata>
  <Item name="INTERLEAVE" domain="OTHER_DOMAIN">TILE</Item>
</GDALMetadata>
        "#;
        let metadata = parse_gdal_metadata(xml, 1).expect("Should parse successfully");

        // Should not parse interleave from wrong domain
        assert_eq!(metadata.interleave, None);
    }

    #[test]
    fn parse_attribute_names_are_case_sensitive() {
        let xml = r#"<GDALMetadata>
  <Item NAME="DESCRIPTION" sample="0">Ignored</Item>
  <Item name="DESCRIPTION" SAMPLE="0">Ignored</Item>
  <Item name="DESCRIPTION" sample="1">Green</Item>
</GDALMetadata>"#;
        let metadata = parse_gdal_metadata(xml, 3).unwrap();
        assert_eq!(
            metadata
                .band_metadata
                .iter()
                .map(|band| band.description.as_deref())
                .collect::<Vec<_>>(),
            vec![None, Some("Green"), None]
        );
        assert!(metadata.band_metadata.iter().all(|band| band.scale.is_none()));
    }

    #[test]
    fn parse_ignores_unknown_domains_roles_and_attributes() {
        let xml = r#"<GDALMetadata>
  <Item domain="VENDOR_DOMAIN" name="CUSTOM" sample="02" role="vendor_role" units="m&amp;s" extra=""> first &lt;value&gt; </Item>
  <Item role="another_role" sample="1" name="CUSTOM" domain="OTHER_DOMAIN">second</Item>
  <Item custom="nameless">third</Item>
</GDALMetadata>"#;
        let metadata = parse_gdal_metadata(xml, 3).expect("Should accept unknown metadata");
        assert!(metadata.statistics.is_none());
        assert_eq!(metadata.band_metadata.len(), 3);
        assert!(
            metadata
                .band_metadata
                .iter()
                .all(|band| band.description.is_none() && band.scale.is_none())
        );
        assert!(metadata.interleave.is_none());
        assert!(metadata.max_zoom.is_none());
        assert!(metadata.data_bounds.is_none());
    }

    #[test]
    fn parse_assigns_band_metadata_by_sample_regardless_of_item_order() {
        let xml = r#"<GDALMetadata>
  <Item name="DESCRIPTION" sample="2" role="description">Blue</Item>
  <Item name="STATISTICS_MINIMUM" sample="2">3.25</Item>
  <Item name="DESCRIPTION" sample="0" role="description">Red</Item>
  <Item name="STATISTICS_MINIMUM" sample="0">1.5</Item>
  <Item name="DESCRIPTION" sample="1" role="description">Green</Item>
</GDALMetadata>"#;
        let metadata = parse_gdal_metadata(xml, 4).unwrap();
        assert_eq!(
            metadata
                .band_metadata
                .iter()
                .map(|band| band.description.as_deref())
                .collect::<Vec<_>>(),
            vec![Some("Red"), Some("Green"), Some("Blue"), None]
        );
    }

    #[test]
    fn parse_accumulates_characters_cdata_entities_and_whitespace() {
        let xml = "<GDALMetadata><Item name=\"DESCRIPTION\" sample=\"0\"> \t<![CDATA[<raw>&]]>before<!-- split -->after&amp;end<![CDATA[\n\t]]> </Item></GDALMetadata>";
        let metadata = parse_gdal_metadata(xml, 1).unwrap();
        assert_eq!(
            metadata.band_metadata[0].description.as_deref(),
            Some(" \t<raw>&beforeafter&end\n\t ")
        );
    }

    #[test]
    fn parse_derives_convenience_fields_from_complete_text() {
        let xml = r#"<GDALMetadata>
  <Item name="STATISTICS_MINIMUM" sample="0">1<![CDATA[2]]>.5</Item>
  <Item name="NAME" domain="TILING_SCHEME">GoogleMaps<![CDATA[Compatible]]></Item>
  <Item name="ZOOM_LEVEL" domain="TILING_SCHEME">1<!-- split -->2</Item>
  <Item name="DESCRIPTION" sample="0">R<![CDATA[ed]]> &amp; green</Item>
  <Item name="OFFSET" sample="0" role="offset">1<![CDATA[0]]></Item>
  <Item name="SCALE" sample="0" role="scale">2<![CDATA[.5]]></Item>
  <Item name="INTERLEAVE" domain="IMAGE_STRUCTURE">PI<![CDATA[X]]>EL</Item>
  <Item name="data_bounds">[1,2,<![CDATA[3,4]]>]</Item>
</GDALMetadata>"#;
        let metadata = parse_gdal_metadata(xml, 1).unwrap();
        assert_eq!(metadata.statistics.unwrap().minimum_value, 12.5);
        assert_eq!(metadata.max_zoom, Some(12));
        assert_eq!(metadata.band_metadata[0].description.as_deref(), Some("Red & green"));
        assert_eq!(metadata.band_metadata[0].scale, Some(RasterScale { scale: 2.5, offset: 10.0 }));
        assert_eq!(metadata.interleave, Some(Interleave::Pixel));
        assert_eq!(metadata.data_bounds, Some([1.0, 2.0, 3.0, 4.0]));
    }

    #[test]
    fn parse_retains_empty_and_whitespace_only_descriptions() {
        let xml = "<GDALMetadata><Item name=\"CUSTOM\"/><Item/><Item name=\"DESCRIPTION\" sample=\"0\"></Item><Item name=\"DESCRIPTION\" sample=\"1\"> \t\n&#xD;</Item></GDALMetadata>";
        let metadata = parse_gdal_metadata(xml, 2).unwrap();
        assert_eq!(metadata.band_metadata.len(), 2);
        assert_eq!(metadata.band_metadata[0].description.as_deref(), Some(""));
        assert_eq!(metadata.band_metadata[1].description.as_deref(), Some(" \t\n\r"));
    }

    #[test]
    fn parse_rejects_malformed_sample_attributes_even_for_unknown_items() {
        for sample in ["", "invalid", "-1", "4294967296", "1.5", " 0", "0 "] {
            let xml = format!("<GDALMetadata><Item name=\"UNKNOWN\" sample=\"{sample}\"/></GDALMetadata>");
            let error = parse_gdal_metadata(&xml, 1).expect_err("Malformed sample should not be silently ignored");
            assert!(
                matches!(&error, Error::InvalidArgument(_)),
                "Unexpected error for {sample:?}: {error}"
            );
            assert!(
                error.to_string().contains("sample attribute"),
                "Unexpected error for {sample:?}: {error}"
            );
        }
    }

    #[test]
    fn parse_rejects_out_of_range_samples_even_for_unknown_items() {
        for (name, role) in [
            ("DESCRIPTION", "description"),
            ("SCALE", "scale"),
            ("OFFSET", "offset"),
            ("STATISTICS_MINIMUM", ""),
            ("UNKNOWN", ""),
        ] {
            for (sample, band_count) in [("0", 0), ("1", 1), ("3", 3), ("4294967295", 3), ("18446744073709551615", 3)] {
                let xml = format!(r#"<GDALMetadata><Item name="{name}" sample="{sample}" role="{role}">2.5</Item></GDALMetadata>"#);
                let error = parse_gdal_metadata(&xml, band_count).expect_err("Out-of-range samples should be rejected");

                assert!(
                    matches!(&error, Error::InvalidArgument(_)),
                    "Unexpected error for {name}, sample={sample}, band_count={band_count}: {error}"
                );
                assert!(error.to_string().contains(sample), "Error should identify sample {sample}: {error}");
            }
        }
    }

    #[test]
    fn parse_rejects_nested_and_unsupported_structures() {
        for xml in [
            "<Other/>",
            "<Item/>",
            "<GDALMetadata extra=\"lost\"/>",
            "<GDALMetadata><Other/></GDALMetadata>",
            "<GDALMetadata><Other><Item/></Other></GDALMetadata>",
            "<GDALMetadata><Item><Item/></Item></GDALMetadata>",
            "<GDALMetadata><Item>before<Other/>after</Item></GDALMetadata>",
            "<GDALMetadata><GDALMetadata/></GDALMetadata>",
            "<GDALMetadata>lost text<Item/></GDALMetadata>",
            "<GDALMetadata xmlns=\"urn:custom\"><Item/></GDALMetadata>",
            "<GDALMetadata xmlns:c=\"urn:custom\"><c:Item/></GDALMetadata>",
            "<GDALMetadata><Item xmlns:c=\"urn:custom\" c:extra=\"lost\"/></GDALMetadata>",
            "<GDALMetadata><Item xml:lang=\"en\"/></GDALMetadata>",
            "<GDALMetadata><?custom lost?></GDALMetadata>",
            "<!DOCTYPE GDALMetadata><GDALMetadata/>",
        ] {
            assert!(parse_gdal_metadata(xml, 0).is_err(), "Should reject unsupported XML: {xml}");
        }
    }

    #[test]
    fn parse_rejects_malformed_xml() {
        for xml in [
            "",
            "<GDALMetadata>",
            "<GDALMetadata><Item></GDALMetadata>",
            "<GDALMetadata><Item>&unknown;</Item></GDALMetadata>",
            "<GDALMetadata><Item name=\"A\" name=\"B\"/></GDALMetadata>",
            "<GDALMetadata/><GDALMetadata/>",
            "<GDALMetadata/>trailing text",
        ] {
            assert!(parse_gdal_metadata(xml, 0).is_err(), "Should reject malformed XML: {xml}");
        }
    }
}

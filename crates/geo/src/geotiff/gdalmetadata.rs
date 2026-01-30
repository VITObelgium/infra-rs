use crate::Error;
use xml::reader::{EventReader, XmlEvent};

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
    #[cfg_attr(target_arch = "wasm32", serde(skip))]
    pub max_zoom: Option<i32>,
}

/// Band-specific metadata including offset and scale values
#[derive(Debug, Clone, Default)]
pub struct BandMetadata {
    /// The band/sample index (0-based)
    pub sample: u32,
    /// Offset value for the band
    pub offset: Option<f64>,
    /// Scale value for the band
    pub scale: Option<f64>,
}

/// Complete GDAL metadata parsed from TIFF tags
#[derive(Debug, Clone, Default)]
pub struct GdalMetadata {
    /// Statistical information (min, max, mean, stddev, etc.)
    pub statistics: Option<TiffStats>,
    /// Per-band metadata (offset, scale)
    pub band_metadata: Vec<BandMetadata>,
    /// Interleave mode from IMAGE_STRUCTURE domain
    pub interleave: Option<Interleave>,
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
//   <Item name="OFFSET" sample="0" role="offset">0</Item>
//   <Item name="SCALE" sample="0" role="scale">0.031372549019600002</Item>
//   <Item name="INTERLEAVE" domain="IMAGE_STRUCTURE">TILE</Item>
// </GDALMetadata>

/// Parse GDAL metadata XML into a structured format
///
/// This function parses the complete GDAL metadata XML from TIFF tags,
/// including statistics, band-specific offset/scale values, and interleave mode.
///
/// # Arguments
/// * `xml` - The XML string containing GDAL metadata
///
/// # Returns
/// * `Ok(GdalMetadata)` if parsing succeeds
/// * `Err` if XML parsing fails
pub fn parse_gdal_metadata(xml: &str) -> crate::Result<GdalMetadata> {
    let mut metadata = GdalMetadata::default();
    let mut stats = TiffStats::default();
    let mut has_statistics = false;

    let mut current_name: Option<String> = None;
    let mut current_domain: Option<String> = None;
    let mut current_sample: Option<u32> = None;
    let mut current_role: Option<String> = None;

    for e in EventReader::from_str(xml) {
        match e {
            Ok(XmlEvent::StartElement { name, attributes, .. }) if name.local_name == "Item" => {
                current_name = None;
                current_domain = None;
                current_sample = None;
                current_role = None;

                for attr in attributes {
                    match attr.name.local_name.as_str() {
                        "name" => current_name = Some(attr.value.clone()),
                        "domain" => current_domain = Some(attr.value.clone()),
                        "sample" => current_sample = attr.value.parse::<u32>().ok(),
                        "role" => current_role = Some(attr.value.clone()),
                        _ => {}
                    }
                }
            }
            Ok(XmlEvent::Characters(data)) => {
                if let Some(ref name) = current_name {
                    match name.as_str() {
                        "STATISTICS_MINIMUM" => {
                            stats.minimum_value = data.parse::<f64>().unwrap_or_default();
                            has_statistics = true;
                        }
                        "STATISTICS_MAXIMUM" => {
                            stats.maximum_value = data.parse::<f64>().unwrap_or_default();
                            has_statistics = true;
                        }
                        "STATISTICS_MEAN" => {
                            stats.mean = data.parse::<f64>().unwrap_or_default();
                            has_statistics = true;
                        }
                        "STATISTICS_STDDEV" => {
                            stats.standard_deviation = data.parse::<f64>().unwrap_or_default();
                            has_statistics = true;
                        }
                        "STATISTICS_VALID_PERCENT" => {
                            stats.valid_pixel_percentage = data.parse::<f64>().unwrap_or_default();
                            has_statistics = true;
                        }
                        "NAME" => {
                            if let Some(ref domain) = current_domain
                                && domain == "TILING_SCHEME"
                                && data != "GoogleMapsCompatible"
                            {
                                return Err(Error::Runtime(format!("Unsupported TILING_SCHEME: {data}")));
                            }
                        }
                        "ZOOM_LEVEL" => {
                            if let Some(ref domain) = current_domain
                                && domain == "TILING_SCHEME"
                            {
                                stats.max_zoom = data.parse::<i32>().ok();
                                has_statistics = true;
                            }
                        }
                        "OFFSET" => {
                            if let Some(role) = &current_role
                                && role == "offset"
                                && let Some(sample) = current_sample
                            {
                                let offset_value = data.parse::<f64>().ok();
                                if let Some(band) = metadata.band_metadata.iter_mut().find(|b| b.sample == sample) {
                                    band.offset = offset_value;
                                } else {
                                    metadata.band_metadata.push(BandMetadata {
                                        sample,
                                        offset: offset_value,
                                        scale: None,
                                    });
                                }
                            }
                        }
                        "SCALE" => {
                            if let Some(role) = &current_role
                                && role == "scale"
                                && let Some(sample) = current_sample
                            {
                                let scale_value = data.parse::<f64>().ok();
                                if let Some(band) = metadata.band_metadata.iter_mut().find(|b| b.sample == sample) {
                                    band.scale = scale_value;
                                } else {
                                    metadata.band_metadata.push(BandMetadata {
                                        sample,
                                        offset: None,
                                        scale: scale_value,
                                    });
                                }
                            }
                        }
                        "INTERLEAVE" => {
                            if let Some(ref domain) = current_domain
                                && domain == "IMAGE_STRUCTURE"
                            {
                                metadata.interleave = super::gdalghostdata::parse_interleave_mode(&data);
                            }
                        }
                        _ => {}
                    }
                }
            }
            Ok(XmlEvent::EndElement { name }) if name.local_name == "Item" => {
                current_name = None;
                current_domain = None;
                current_sample = None;
                current_role = None;
            }
            Err(e) => {
                return Err(Error::Runtime(format!("XML parse error: {e}")));
            }
            _ => {}
        }
    }

    // Sort band metadata by sample index for consistent ordering
    metadata.band_metadata.sort_by_key(|b| b.sample);

    if has_statistics {
        metadata.statistics = Some(stats);
    }

    Ok(metadata)
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
        let metadata = parse_gdal_metadata(xml).expect("Should parse successfully");

        assert!(metadata.statistics.is_some());
        let stats = metadata.statistics.unwrap();
        assert_eq!(stats.minimum_value, 0.0);
        assert_eq!(stats.maximum_value, 254.0);
        assert_abs_diff_eq!(stats.mean, 119.11901635438, epsilon = 1e-10);
        assert_abs_diff_eq!(stats.standard_deviation, 58.60474035626, epsilon = 1e-10);
        assert_abs_diff_eq!(stats.valid_pixel_percentage, 45.34, epsilon = 1e-10);
        assert_eq!(stats.max_zoom, Some(10));
    }

    #[test]
    fn parse_gdal_metadata_with_offset_scale() {
        let xml = r#"
<GDALMetadata>
  <Item name="OFFSET" sample="0" role="offset">0</Item>
  <Item name="SCALE" sample="0" role="scale">0.031372549019600002</Item>
  <Item name="OFFSET" sample="1" role="offset">0</Item>
  <Item name="SCALE" sample="1" role="scale">0.031372549019600002</Item>
  <Item name="OFFSET" sample="2" role="offset">0</Item>
  <Item name="SCALE" sample="2" role="scale">0.031372549019600002</Item>
</GDALMetadata>
        "#;
        let metadata = parse_gdal_metadata(xml).expect("Should parse successfully");

        assert_eq!(metadata.band_metadata.len(), 3);

        for (i, band) in metadata.band_metadata.iter().enumerate() {
            assert_eq!(band.sample, i as u32);
            assert_eq!(band.offset, Some(0.0));
            assert_abs_diff_eq!(band.scale.unwrap(), 0.031372549019600002, epsilon = 1e-15);
        }
    }

    #[test]
    fn parse_gdal_metadata_with_interleave() {
        let xml = r#"
<GDALMetadata>
  <Item name="INTERLEAVE" domain="IMAGE_STRUCTURE">TILE</Item>
</GDALMetadata>
        "#;
        let metadata = parse_gdal_metadata(xml).expect("Should parse successfully");

        assert_eq!(metadata.interleave, Some(Interleave::Tile));
    }

    #[test]
    fn parse_gdal_metadata_comprehensive() {
        let xml = r#"
<GDALMetadata>
  <Item name="OFFSET" sample="0" role="offset">0</Item>
  <Item name="SCALE" sample="0" role="scale">0.031372549019600002</Item>
  <Item name="OFFSET" sample="1" role="offset">0</Item>
  <Item name="SCALE" sample="1" role="scale">0.031372549019600002</Item>
  <Item name="OFFSET" sample="2" role="offset">0</Item>
  <Item name="SCALE" sample="2" role="scale">0.031372549019600002</Item>
  <Item name="OFFSET" sample="3" role="offset">0</Item>
  <Item name="SCALE" sample="3" role="scale">0.031372549019600002</Item>
  <Item name="OFFSET" sample="4" role="offset">0</Item>
  <Item name="SCALE" sample="4" role="scale">0.031372549019600002</Item>
  <Item name="INTERLEAVE" domain="IMAGE_STRUCTURE">TILE</Item>
</GDALMetadata>
        "#;
        let metadata = parse_gdal_metadata(xml).expect("Should parse successfully");

        assert_eq!(metadata.band_metadata.len(), 5);
        assert_eq!(metadata.interleave, Some(Interleave::Tile));

        for (i, band) in metadata.band_metadata.iter().enumerate() {
            assert_eq!(band.sample, i as u32);
            assert_eq!(band.offset, Some(0.0));
            assert_abs_diff_eq!(band.scale.unwrap(), 0.031372549019600002, epsilon = 1e-15);
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
        let metadata = parse_gdal_metadata(xml).expect("Should parse successfully");

        assert!(metadata.statistics.is_some());
        assert_eq!(metadata.band_metadata.len(), 1);
        assert_eq!(metadata.interleave, Some(Interleave::Pixel));

        let band = &metadata.band_metadata[0];
        assert_eq!(band.sample, 0);
        assert_eq!(band.offset, Some(10.0));
        assert_abs_diff_eq!(band.scale.unwrap(), 2.5, epsilon = 1e-10);
    }

    #[test]
    fn parse_gdal_metadata_partial_band_data() {
        let xml = r#"
<GDALMetadata>
  <Item name="OFFSET" sample="0" role="offset">5</Item>
  <Item name="SCALE" sample="2" role="scale">1.5</Item>
</GDALMetadata>
        "#;
        let metadata = parse_gdal_metadata(xml).expect("Should parse successfully");

        assert_eq!(metadata.band_metadata.len(), 2);

        // Band 0 has only offset
        assert_eq!(metadata.band_metadata[0].sample, 0);
        assert_eq!(metadata.band_metadata[0].offset, Some(5.0));
        assert_eq!(metadata.band_metadata[0].scale, None);

        // Band 2 has only scale
        assert_eq!(metadata.band_metadata[1].sample, 2);
        assert_eq!(metadata.band_metadata[1].offset, None);
        assert_abs_diff_eq!(metadata.band_metadata[1].scale.unwrap(), 1.5, epsilon = 1e-10);
    }

    #[test]
    fn parse_gdal_metadata_empty() {
        let xml = r#"<GDALMetadata></GDALMetadata>"#;
        let metadata = parse_gdal_metadata(xml).expect("Should parse successfully");

        assert!(metadata.statistics.is_none());
        assert!(metadata.band_metadata.is_empty());
        assert!(metadata.interleave.is_none());
    }

    #[test]
    fn parse_gdal_metadata_interleave_band() {
        let xml = r#"
<GDALMetadata>
  <Item name="INTERLEAVE" domain="IMAGE_STRUCTURE">BAND</Item>
</GDALMetadata>
        "#;
        let metadata = parse_gdal_metadata(xml).expect("Should parse successfully");

        assert_eq!(metadata.interleave, Some(Interleave::Band));
    }

    #[test]
    fn parse_gdal_metadata_invalid_interleave() {
        let xml = r#"
<GDALMetadata>
  <Item name="INTERLEAVE" domain="IMAGE_STRUCTURE">INVALID</Item>
</GDALMetadata>
        "#;
        let metadata = parse_gdal_metadata(xml).expect("Should parse successfully");

        assert_eq!(metadata.interleave, None);
    }

    #[test]
    fn parse_gdal_metadata_interleave_wrong_domain() {
        let xml = r#"
<GDALMetadata>
  <Item name="INTERLEAVE" domain="OTHER_DOMAIN">TILE</Item>
</GDALMetadata>
        "#;
        let metadata = parse_gdal_metadata(xml).expect("Should parse successfully");

        // Should not parse interleave from wrong domain
        assert_eq!(metadata.interleave, None);
    }
}

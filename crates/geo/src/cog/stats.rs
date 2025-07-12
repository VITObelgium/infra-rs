use crate::Error;
use xml::reader::{EventReader, XmlEvent};

#[derive(Debug, Clone, Default)]
#[cfg_attr(
    target_arch = "wasm32",
    derive(tsify::Tsify, serde::Serialize, serde::Deserialize),
    tsify(from_wasm_abi, into_wasm_abi)
)]
pub struct CogStats {
    pub minimum_value: f64,
    pub maximum_value: f64,
    pub mean: f64,
    pub standard_deviation: f64,
    pub valid_pixel_percentage: f64,
    #[cfg_attr(target_arch = "wasm32", serde(skip))]
    pub max_zoom: Option<i32>,
}

// Statistics have the following format
// <GDALMetadata>
//   <Item name="STATISTICS_MAXIMUM" sample="0">254</Item>
//   <Item name="STATISTICS_MEAN" sample="0">119.11901635438</Item>
//   <Item name="STATISTICS_MINIMUM" sample="0">0</Item>
//   <Item name="STATISTICS_STDDEV" sample="0">58.60474035626</Item>
//   <Item name="STATISTICS_VALID_PERCENT" sample="0">45.34</Item>
//   <Item name="NAME" domain="TILING_SCHEME">GoogleMapsCompatible</Item>
//   <Item name="ZOOM_LEVEL" domain="TILING_SCHEME">10</Item>
// </GDALMetadata>

pub fn parse_statistics(xml: &str) -> crate::Result<CogStats> {
    let mut stats = CogStats::default();

    let mut current_name: Option<String> = None;
    let mut current_domain: Option<String> = None;

    for e in EventReader::from_str(xml) {
        match e {
            Ok(XmlEvent::StartElement { name, attributes, .. }) if name.local_name == "Item" => {
                current_name = None;
                current_domain = None;
                for attr in attributes {
                    if attr.name.local_name == "name" {
                        current_name = Some(attr.value.clone());
                    }

                    if attr.name.local_name == "domain" {
                        current_domain = Some(attr.value.clone());
                    }
                }
            }
            Ok(XmlEvent::Characters(data)) => {
                if let Some(ref name) = current_name {
                    match name.as_str() {
                        "STATISTICS_MINIMUM" => {
                            stats.minimum_value = data.parse::<f64>().unwrap_or_default();
                        }
                        "STATISTICS_MAXIMUM" => {
                            stats.maximum_value = data.parse::<f64>().unwrap_or_default();
                        }
                        "STATISTICS_MEAN" => {
                            stats.mean = data.parse::<f64>().unwrap_or_default();
                        }
                        "STATISTICS_STDDEV" => {
                            stats.standard_deviation = data.parse::<f64>().unwrap_or_default();
                        }
                        "STATISTICS_VALID_PERCENT" => {
                            stats.valid_pixel_percentage = data.parse::<f64>().unwrap_or_default();
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
                            }
                        }
                        _ => {}
                    }
                }
            }
            Ok(XmlEvent::EndElement { name }) if name.local_name == "Item" => {
                current_name = None;
                current_domain = None;
            }
            Err(e) => {
                return Err(Error::Runtime(format!("XML parse error: {e}")));
            }
            _ => {}
        }
    }

    Ok(stats)
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
        let stats = parse_statistics(xml).expect("Should parse successfully");
        assert_eq!(stats.minimum_value, 0.0);
        assert_eq!(stats.maximum_value, 254.0);
        assert_abs_diff_eq!(stats.mean, 119.11901635438, epsilon = 1e-10);
        assert_abs_diff_eq!(stats.standard_deviation, 58.60474035626, epsilon = 1e-10);
        assert_abs_diff_eq!(stats.valid_pixel_percentage, 45.34, epsilon = 1e-10);
        assert_eq!(stats.max_zoom, Some(10));
    }
}

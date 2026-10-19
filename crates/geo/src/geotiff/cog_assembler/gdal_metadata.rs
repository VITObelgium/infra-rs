//! Lossless item-level GDAL metadata used only by raw COG assembly.

use crate::{Error, Result};
use xml::{
    common::{is_name_char, is_name_start_char, is_whitespace_str, is_xml10_char},
    reader::{ParserConfig, XmlEvent},
};

/// A complete item, including attributes unknown to this crate.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct GdalMetadataItem {
    /// Attribute names and decoded values, in source order.
    pub(super) attributes: Vec<(String, String)>,
    /// Decoded element text, without trimming whitespace.
    pub(super) text: String,
}

impl GdalMetadataItem {
    pub(super) fn attribute(&self, name: &str) -> Option<&str> {
        self.attributes.iter().find(|(key, _)| key == name).map(|(_, value)| value.as_str())
    }

    /// Replace an attribute in place, or append it if it does not exist.
    pub(super) fn set_attribute(&mut self, name: &str, value: String) {
        if let Some((_, current)) = self.attributes.iter_mut().find(|(key, _)| key == name) {
            *current = value;
        } else {
            self.attributes.push((name.to_owned(), value));
        }
    }
}

#[derive(Debug, Clone, Default)]
pub(super) struct GdalMetadata {
    pub(super) items: Vec<GdalMetadataItem>,
}

/// Retain all items without interpreting statistics, interleave, or band values.
/// Structures that cannot be represented by the flat item model are rejected.
pub(super) fn parse_gdal_metadata(xml: &str) -> Result<GdalMetadata> {
    let mut items = Vec::new();
    let mut current_item: Option<GdalMetadataItem> = None;
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
                // Namespace bindings cannot be represented by the flat item model.
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
                    current_item = Some(GdalMetadataItem {
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
                    if let Some(sample) = item.attribute("sample") {
                        sample
                            .parse::<u32>()
                            .map_err(|e| Error::Runtime(format!("Invalid GDAL metadata sample attribute {sample:?}: {e}")))?;
                    }
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
    Ok(GdalMetadata { items })
}

/// Retain item and attribute order and decoded text. Character references protect
/// whitespace from XML normalization. Invalid characters and attributes are errors.
pub(super) fn serialize_gdal_metadata(items: &[GdalMetadataItem]) -> Result<String> {
    let mut xml = String::from("<GDALMetadata>");
    for item in items {
        xml.push_str("<Item");
        for (index, (name, value)) in item.attributes.iter().enumerate() {
            let mut chars = name.chars();
            if name == "xmlns"
                || name.contains(':')
                || !chars.next().is_some_and(is_name_start_char)
                || !chars.all(is_name_char)
                || item.attributes[..index].iter().any(|(previous, _)| previous == name)
            {
                return Err(Error::Runtime(format!(
                    "Invalid or duplicate GDAL metadata attribute name: {name:?}"
                )));
            }
            xml.push(' ');
            xml.push_str(name);
            xml.push_str("=\"");
            push_xml_value(&mut xml, value, true)?;
            xml.push('"');
        }
        xml.push('>');
        push_xml_value(&mut xml, &item.text, false)?;
        xml.push_str("</Item>");
    }
    xml.push_str("</GDALMetadata>");
    Ok(xml)
}

fn push_xml_value(xml: &mut String, value: &str, attribute: bool) -> Result<()> {
    for ch in value.chars() {
        match ch {
            '&' => xml.push_str("&amp;"),
            '<' => xml.push_str("&lt;"),
            '>' => xml.push_str("&gt;"),
            '"' => xml.push_str("&quot;"),
            '\'' => xml.push_str("&apos;"),
            // XML normalizes literal whitespace before resolving character references.
            '\r' => xml.push_str("&#xD;"),
            '\n' if attribute => xml.push_str("&#xA;"),
            '\t' if attribute => xml.push_str("&#x9;"),
            ch if !is_xml10_char(ch) => {
                return Err(Error::Runtime(format!(
                    "Invalid GDAL metadata XML character: U+{:04X}",
                    u32::from(ch)
                )));
            }
            ch => xml.push(ch),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn item_attributes_are_case_sensitive_and_updated_in_place() {
        let mut item = GdalMetadataItem {
            attributes: vec![("name".into(), "CUSTOM".into()), ("sample".into(), "0".into())],
            text: "unchanged".into(),
        };
        assert_eq!(item.attribute("name"), Some("CUSTOM"));
        assert_eq!(item.attribute("NAME"), None);
        assert_eq!(item.attribute("domain"), None);

        item.set_attribute("sample", "2".into());
        item.set_attribute("domain", "CUSTOM_DOMAIN".into());
        assert_eq!(
            item.attributes,
            vec![
                ("name".into(), "CUSTOM".into()),
                ("sample".into(), "2".into()),
                ("domain".into(), "CUSTOM_DOMAIN".into()),
            ]
        );
        assert_eq!(item.text, "unchanged");
    }

    #[test]
    fn parse_preserves_unknown_domains_roles_and_attributes() {
        let xml = r#"<GDALMetadata>
  <Item domain="VENDOR_DOMAIN" name="CUSTOM" sample="02" role="vendor_role" units="m&amp;s" extra=""> first &lt;value&gt; </Item>
  <Item role="another_role" sample="1" name="CUSTOM" domain="OTHER_DOMAIN">second</Item>
  <Item custom="nameless">third</Item>
</GDALMetadata>"#;
        let metadata = parse_gdal_metadata(xml).expect("Should retain unknown metadata");
        assert_eq!(
            metadata.items,
            vec![
                GdalMetadataItem {
                    attributes: vec![
                        ("domain".into(), "VENDOR_DOMAIN".into()),
                        ("name".into(), "CUSTOM".into()),
                        ("sample".into(), "02".into()),
                        ("role".into(), "vendor_role".into()),
                        ("units".into(), "m&s".into()),
                        ("extra".into(), "".into()),
                    ],
                    text: " first <value> ".into(),
                },
                GdalMetadataItem {
                    attributes: vec![
                        ("role".into(), "another_role".into()),
                        ("sample".into(), "1".into()),
                        ("name".into(), "CUSTOM".into()),
                        ("domain".into(), "OTHER_DOMAIN".into()),
                    ],
                    text: "second".into(),
                },
                GdalMetadataItem {
                    attributes: vec![("custom".into(), "nameless".into())],
                    text: "third".into(),
                },
            ]
        );
        let serialized = serialize_gdal_metadata(&metadata.items).expect("Should serialize unknown metadata");
        assert_eq!(parse_gdal_metadata(&serialized).unwrap().items, metadata.items);
    }

    #[test]
    fn parse_preserves_per_sample_values_and_source_order() {
        let xml = r#"<GDALMetadata>
  <Item name="DESCRIPTION" sample="2" role="description">Blue</Item>
  <Item name="STATISTICS_MINIMUM" sample="2">3.25</Item>
  <Item name="DESCRIPTION" sample="0" role="description">Red</Item>
  <Item name="STATISTICS_MINIMUM" sample="0">1.5</Item>
  <Item name="DESCRIPTION" sample="1" role="description">Green</Item>
</GDALMetadata>"#;
        let metadata = parse_gdal_metadata(xml).unwrap();
        let values: Vec<_> = metadata
            .items
            .iter()
            .map(|item| (item.attribute("sample"), item.text.as_str()))
            .collect();
        assert_eq!(
            values,
            vec![
                (Some("2"), "Blue"),
                (Some("2"), "3.25"),
                (Some("0"), "Red"),
                (Some("0"), "1.5"),
                (Some("1"), "Green"),
            ]
        );
        let serialized = serialize_gdal_metadata(&metadata.items).unwrap();
        assert_eq!(parse_gdal_metadata(&serialized).unwrap().items, metadata.items);
    }

    #[test]
    fn parse_accumulates_characters_cdata_entities_and_whitespace() {
        let xml = "<GDALMetadata><Item name=\"CUSTOM\"> \t<![CDATA[<raw>&]]>before<!-- split -->after&amp;end<![CDATA[\n\t]]> </Item></GDALMetadata>";
        let metadata = parse_gdal_metadata(xml).unwrap();
        assert_eq!(metadata.items[0].text, " \t<raw>&beforeafter&end\n\t ");
        let serialized = serialize_gdal_metadata(&metadata.items).unwrap();
        assert_eq!(parse_gdal_metadata(&serialized).unwrap().items, metadata.items);
    }

    #[test]
    fn parse_retains_known_item_text_without_convenience_field_interpretation() {
        let xml = r#"<GDALMetadata>
  <Item name="STATISTICS_MINIMUM" sample="0">not a statistic</Item>
  <Item name="NAME" domain="TILING_SCHEME">CustomScheme</Item>
  <Item name="ZOOM_LEVEL" domain="TILING_SCHEME">1<!-- split -->2</Item>
  <Item name="DESCRIPTION" sample="0">R<![CDATA[ed]]> &amp; green</Item>
  <Item name="OFFSET" sample="0" role="offset">not an offset</Item>
  <Item name="SCALE" sample="0" role="scale">2<![CDATA[.5]]></Item>
  <Item name="INTERLEAVE" domain="IMAGE_STRUCTURE">VENDOR</Item>
  <Item name="data_bounds">custom bounds</Item>
</GDALMetadata>"#;
        let metadata = parse_gdal_metadata(xml).unwrap();
        assert_eq!(
            metadata.items.iter().map(|item| item.text.as_str()).collect::<Vec<_>>(),
            [
                "not a statistic",
                "CustomScheme",
                "12",
                "Red & green",
                "not an offset",
                "2.5",
                "VENDOR",
                "custom bounds"
            ]
        );
        let serialized = serialize_gdal_metadata(&metadata.items).unwrap();
        assert_eq!(parse_gdal_metadata(&serialized).unwrap().items, metadata.items);
    }

    #[test]
    fn parse_retains_empty_and_whitespace_only_items() {
        let xml = "<GDALMetadata><Item name=\"CUSTOM\"/><Item/><Item name=\"DESCRIPTION\" sample=\"0\"></Item><Item> \t\n&#xD;</Item></GDALMetadata>";
        let metadata = parse_gdal_metadata(xml).unwrap();
        assert_eq!(
            metadata.items.iter().map(|item| item.text.as_str()).collect::<Vec<_>>(),
            vec!["", "", "", " \t\n\r"]
        );
        assert!(metadata.items[1].attributes.is_empty());
        let serialized = serialize_gdal_metadata(&metadata.items).unwrap();
        assert_eq!(parse_gdal_metadata(&serialized).unwrap().items, metadata.items);
    }

    #[test]
    fn parse_rejects_malformed_sample_attributes_even_for_unknown_items() {
        for sample in ["", "invalid", "-1", "4294967296", "1.5", " 0", "0 "] {
            let xml = format!("<GDALMetadata><Item name=\"UNKNOWN\" sample=\"{sample}\"/></GDALMetadata>");
            let error = parse_gdal_metadata(&xml).expect_err("Malformed sample should not be silently ignored");
            assert!(
                error.to_string().contains("sample attribute"),
                "Unexpected error for {sample:?}: {error}"
            );
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
            assert!(parse_gdal_metadata(xml).is_err(), "Should reject unsupported XML: {xml}");
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
            assert!(parse_gdal_metadata(xml).is_err(), "Should reject malformed XML: {xml}");
        }
    }

    #[test]
    fn serialize_roundtrips_escaped_whitespace_and_entities() {
        let xml = r#"<GDALMetadata><Item name="CUSTOM" domain="a&#x9;b&#xA;c&#xD;d" extra="&quot;&apos;&lt;&gt;&amp;">&#xD; start&#x9;&#xA;&lt;&amp;&gt;&quot;&apos; café &#xD;end </Item></GDALMetadata>"#;
        let metadata = parse_gdal_metadata(xml).unwrap();
        assert_eq!(metadata.items[0].attribute("domain"), Some("a\tb\nc\rd"));
        assert_eq!(metadata.items[0].attribute("extra"), Some("\"'<>&"));
        assert_eq!(metadata.items[0].text, "\r start\t\n<&>\"' café \rend ");

        let serialized = serialize_gdal_metadata(&metadata.items).unwrap();
        assert!(serialized.contains("domain=\"a&#x9;b&#xA;c&#xD;d\""));
        assert!(serialized.contains("&#xD; start\t\n&lt;&amp;&gt;&quot;&apos; café &#xD;end "));
        assert!(!serialized.contains('\r'));
        assert_eq!(parse_gdal_metadata(&serialized).unwrap().items, metadata.items);
    }

    #[test]
    fn serialize_roundtrips_constructed_items_without_normalizing_whitespace() {
        let items = vec![GdalMetadataItem {
            attributes: vec![("custom".into(), " \t\n\r\r\n<&>\"' café ".into())],
            text: " \t\n\r\r\n<&>\"' café ]]> ".into(),
        }];
        let serialized = serialize_gdal_metadata(&items).unwrap();
        assert_eq!(parse_gdal_metadata(&serialized).unwrap().items, items);
    }

    #[test]
    fn serialize_empty_metadata() {
        let serialized = serialize_gdal_metadata(&[]).unwrap();
        assert_eq!(serialized, "<GDALMetadata></GDALMetadata>");
        assert!(parse_gdal_metadata(&serialized).unwrap().items.is_empty());
        assert!(parse_gdal_metadata("<GDALMetadata/>").unwrap().items.is_empty());
    }

    #[test]
    fn serialize_rejects_invalid_or_duplicate_attribute_names() {
        for name in ["", "1invalid", "with space", "a\"injection", "xmlns", "custom:attribute"] {
            let item = GdalMetadataItem {
                attributes: vec![(name.into(), "value".into())],
                text: String::new(),
            };
            assert!(serialize_gdal_metadata(&[item]).is_err(), "Should reject attribute name {name:?}");
        }
        let item = GdalMetadataItem {
            attributes: vec![("name".into(), "first".into()), ("name".into(), "second".into())],
            text: String::new(),
        };
        assert!(serialize_gdal_metadata(&[item]).is_err());
    }

    #[test]
    fn serialize_rejects_invalid_xml_characters() {
        for invalid in ['\0', '\u{1}', '\u{b}', '\u{fffe}'] {
            let item = GdalMetadataItem {
                attributes: Vec::new(),
                text: invalid.to_string(),
            };
            assert!(serialize_gdal_metadata(&[item]).is_err());
            let item = GdalMetadataItem {
                attributes: vec![("custom".into(), invalid.to_string())],
                text: String::new(),
            };
            assert!(serialize_gdal_metadata(&[item]).is_err());
        }
    }
}

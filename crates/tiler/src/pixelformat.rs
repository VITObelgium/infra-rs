use core::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
pub enum PixelFormat {
    #[default]
    Unknown,
    // Regular rbga pixel format
    Rgba,
    // The source data is converted to float and stored in the pixel data
    RawFloat,
    // The pixel data is stored in the native format of the source
    Native,
}

impl fmt::Display for PixelFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                PixelFormat::Rgba => "rgba",
                PixelFormat::RawFloat => "rawfloat",
                PixelFormat::Native => "native",
                PixelFormat::Unknown => "",
            }
        )
    }
}

impl From<&str> for PixelFormat {
    fn from(s: &str) -> Self {
        match s {
            "rgba" => PixelFormat::Rgba,
            "rawfloat" => PixelFormat::RawFloat,
            "native" => PixelFormat::Native,
            _ => PixelFormat::Unknown,
        }
    }
}

use core::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "lowercase"))]
pub enum PixelFormat {
    #[default]
    Unknown,
    Rgba,
    RawFloat,
}

impl fmt::Display for PixelFormat {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                PixelFormat::Rgba => "rgba",
                PixelFormat::RawFloat => "rawfloat",
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
            _ => PixelFormat::Unknown,
        }
    }
}

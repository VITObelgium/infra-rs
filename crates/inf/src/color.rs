use crate::Error;

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C, packed)]
pub struct Color {
    /// Red
    pub r: u8,
    /// Green
    pub g: u8,
    /// Blue
    pub b: u8,
    /// Alpha
    pub a: u8,
}

impl Default for Color {
    fn default() -> Self {
        TRANSPARENT
    }
}

impl AsRef<u8> for Color {
    fn as_ref(&self) -> &u8 {
        &self.r
    }
}

impl Color {
    pub const fn rgb(r: u8, g: u8, b: u8) -> Color {
        Color::rgba(r, g, b, 255)
    }

    pub const fn rgba(r: u8, g: u8, b: u8, a: u8) -> Color {
        Color { r, g, b, a }
    }

    fn parse_hex(str: &str) -> Result<u8, Error> {
        match u8::from_str_radix(str, 16) {
            Ok(value) => Ok(value),
            Err(_) => Err(Error::InvalidArgument(format!("Invalid hex color value: {}", str))),
        }
    }

    /// Parse a color from a hex string, the alpha channel is optionsl (e.g. #ff4d9b #43ff64d9).
    pub fn from_hex_string(hex_string: &str) -> Result<Color, Error> {
        if hex_string.is_empty() {
            return Err(Error::InvalidArgument("Empty color string".to_string()));
        }

        if !hex_string.starts_with('#') || (hex_string.len() != 7 && hex_string.len() != 9) {
            return Err(Error::InvalidArgument(format!("Invalid color string: {}", hex_string)));
        }

        let mut offset = 1;
        let a: u8;

        if hex_string.len() == 9 {
            a = Color::parse_hex(&hex_string[offset..offset + 2])?;
            offset += 2;
        } else {
            a = 255;
        }

        let r = Color::parse_hex(&hex_string[offset..offset + 2])?;
        let g = Color::parse_hex(&hex_string[offset + 2..offset + 4])?;
        let b = Color::parse_hex(&hex_string[offset + 4..offset + 6])?;

        Ok(Color { r, g, b, a })
    }

    pub fn to_hex_rgb(&self) -> String {
        format!("#{:02X}{:02X}{:02X}", self.r, self.g, self.b)
    }

    pub fn to_hex_argb(&self) -> String {
        format!("#{:02X}{:02X}{:02X}{:02X}", self.a, self.r, self.g, self.b)
    }

    pub const fn to_bits(self) -> u32 {
        // SAFETY: `u32` is a plain old datatype so we can always transmute to it since Color is four bytes.
        unsafe { std::mem::transmute(self) }
    }
}

pub const BLACK: Color = Color { r: 0, g: 0, b: 0, a: 255 };
pub const WHITE: Color = Color {
    r: 255,
    g: 255,
    b: 255,
    a: 255,
};
pub const BLUE: Color = Color {
    r: 0,
    g: 0,
    b: 255,
    a: 255,
};
pub const GREEN: Color = Color {
    r: 0,
    g: 255,
    b: 0,
    a: 255,
};
pub const RED: Color = Color {
    r: 255,
    g: 0,
    b: 0,
    a: 255,
};
pub const TRANSPARENT: Color = Color { r: 0, g: 0, b: 0, a: 0 };
pub const DARK_GREY: Color = Color {
    r: 107,
    g: 110,
    b: 119,
    a: 255,
};
pub const LIGHT_GREY: Color = Color {
    r: 170,
    g: 174,
    b: 177,
    a: 255,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_hex_rgb() {
        assert_eq!(Color::from_hex_string("#000000").unwrap(), Color::rgba(0, 0, 0, 255));
        assert_eq!(Color::from_hex_string("#FFFFFF").unwrap(), Color::rgba(255, 255, 255, 255));
        assert_eq!(Color::from_hex_string("#FF0000").unwrap(), Color::rgba(255, 0, 0, 255));
        assert_eq!(Color::from_hex_string("#00FF00").unwrap(), Color::rgba(0, 255, 0, 255));
        assert_eq!(Color::from_hex_string("#0000FF").unwrap(), Color::rgba(0, 0, 255, 255));

        assert_eq!(Color::from_hex_string("#000000").unwrap(), Color::rgba(0, 0, 0, 255));
        assert_eq!(Color::from_hex_string("#fffFFF").unwrap(), Color::rgba(255, 255, 255, 255));
        assert_eq!(Color::from_hex_string("#ff0000").unwrap(), Color::rgba(255, 0, 0, 255));
        assert_eq!(Color::from_hex_string("#00fF00").unwrap(), Color::rgba(0, 255, 0, 255));
        assert_eq!(Color::from_hex_string("#0000FF").unwrap(), Color::rgba(0, 0, 255, 255));

        assert_eq!(Color::from_hex_string("#19E624").unwrap(), Color::rgba(25, 230, 36, 255));
    }

    #[test]
    fn from_hex_argb() {
        assert_eq!(Color::from_hex_string("#ff000000").unwrap(), Color::rgba(0, 0, 0, 255));
        assert_eq!(Color::from_hex_string("#ffffFFFF").unwrap(), Color::rgba(255, 255, 255, 255));
        assert_eq!(Color::from_hex_string("#ffff0000").unwrap(), Color::rgba(255, 0, 0, 255));
        assert_eq!(Color::from_hex_string("#ff00FF00").unwrap(), Color::rgba(0, 255, 0, 255));
        assert_eq!(Color::from_hex_string("#ff0000FF").unwrap(), Color::rgba(0, 0, 255, 255));

        assert_eq!(Color::from_hex_string("#00000000").unwrap(), Color::rgba(0, 0, 0, 0));
        assert_eq!(Color::from_hex_string("#64ffFFFF").unwrap(), Color::rgba(255, 255, 255, 100));
        assert_eq!(Color::from_hex_string("#96ff0000").unwrap(), Color::rgba(255, 0, 0, 150));
        assert_eq!(Color::from_hex_string("#c800FF00").unwrap(), Color::rgba(0, 255, 0, 200));
        assert_eq!(Color::from_hex_string("#ff0000FF").unwrap(), Color::rgba(0, 0, 255, 255));

        assert_eq!(Color::from_hex_string("#FF19E624").unwrap(), Color::rgba(25, 230, 36, 255));
    }

    #[test]
    fn from_invalid_hex() {
        assert!(Color::from_hex_string("").is_err());
        assert!(Color::from_hex_string("#").is_err());
        assert!(Color::from_hex_string("#########").is_err());
        assert!(Color::from_hex_string("#0011ZZ").is_err());
        assert!(Color::from_hex_string("#FFFFFFF").is_err());
    }

    #[test]
    fn to_hex_rgb() {
        assert_eq!(Color::rgb(0, 0, 0).to_hex_rgb(), "#000000");
        assert_eq!(Color::rgb(255, 255, 255).to_hex_rgb(), "#FFFFFF");
        assert_eq!(Color::rgb(255, 0, 0).to_hex_rgb(), "#FF0000");
        assert_eq!(Color::rgb(0, 255, 0).to_hex_rgb(), "#00FF00");
        assert_eq!(Color::rgb(0, 0, 255).to_hex_rgb(), "#0000FF");

        assert_eq!(Color::rgba(0, 0, 0, 0).to_hex_rgb(), "#000000");
        assert_eq!(Color::rgba(255, 255, 255, 100).to_hex_rgb(), "#FFFFFF");
        assert_eq!(Color::rgba(255, 0, 0, 150).to_hex_rgb(), "#FF0000");
        assert_eq!(Color::rgba(0, 255, 0, 200).to_hex_rgb(), "#00FF00");
        assert_eq!(Color::rgba(0, 0, 255, 255).to_hex_rgb(), "#0000FF");

        assert_eq!(Color::rgba(25, 230, 36, 255).to_hex_rgb(), "#19E624");
    }

    #[test]
    fn to_hex_argb() {
        assert_eq!(Color::rgb(0, 0, 0).to_hex_argb(), "#FF000000");
        assert_eq!(Color::rgb(255, 255, 255).to_hex_argb(), "#FFFFFFFF");
        assert_eq!(Color::rgb(255, 0, 0).to_hex_argb(), "#FFFF0000");
        assert_eq!(Color::rgb(0, 255, 0).to_hex_argb(), "#FF00FF00");
        assert_eq!(Color::rgb(0, 0, 255).to_hex_argb(), "#FF0000FF");

        assert_eq!(Color::rgba(0, 0, 0, 0).to_hex_argb(), "#00000000");
        assert_eq!(Color::rgba(255, 255, 255, 100).to_hex_argb(), "#64FFFFFF");
        assert_eq!(Color::rgba(255, 0, 0, 150).to_hex_argb(), "#96FF0000");
        assert_eq!(Color::rgba(0, 255, 0, 200).to_hex_argb(), "#C800FF00");
        assert_eq!(Color::rgba(0, 0, 255, 255).to_hex_argb(), "#FF0000FF");

        assert_eq!(Color::rgba(25, 230, 36, 255).to_hex_argb(), "#FF19E624");
    }
}

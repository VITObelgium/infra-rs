use crate::{color::Color, Error};

pub struct ColorDictEntry {
    pub x: f64,
    pub y0: f64,
    pub y1: f64,
}

pub struct ColorDict {
    pub red: Vec<ColorDictEntry>,
    pub green: Vec<ColorDictEntry>,
    pub blue: Vec<ColorDictEntry>,
}

pub struct ColorMapper {
    pub red: fn(f64) -> u8,
    pub green: fn(f64) -> u8,
    pub blue: fn(f64) -> u8,
}

pub struct ColorInfo {
    pub start: f64,
    pub color: Color,
}

impl ColorInfo {
    pub const fn new(start: f64, color: Color) -> ColorInfo {
        ColorInfo { start, color }
    }
}

#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Clone, Debug)]
pub struct ColorMap {
    cmap: Vec<Color>,
}

impl Default for ColorMap {
    fn default() -> ColorMap {
        ColorMap {
            cmap: vec![Color::default(); 256],
        }
    }
}

pub enum ColorMapPreset {
    Bone,
    Cool,
    Copper,
    Gray,
    Hot,
    Hsv,
    Pink,
    Jet,
    Spring,
    Summer,
    Autumn,
    Winter,
    Wistia,
    NipySpectral,
    GistEarth,
    GistNcar,
    GistStern,
}

impl ColorMap {
    pub fn new(cdict: &ColorDict, reverse: bool) -> ColorMap {
        let map_value = |index: usize| -> Color {
            let value = index as f64 / (256 - 1) as f64;
            Color::rgb(
                ColorMap::process_band(value, &cdict.red),
                ColorMap::process_band(value, &cdict.green),
                ColorMap::process_band(value, &cdict.blue),
            )
        };

        let mut cmap = vec![Color::default(); 256];
        let mut index = 0;
        if reverse {
            for iter in cmap.iter_mut().rev() {
                *iter = map_value(index);
                index += 1;
            }
        } else {
            for iter in cmap.iter_mut() {
                *iter = map_value(index);
                index += 1;
            }
        }

        ColorMap { cmap }
    }

    pub fn from_color_list(clist: &[Color], reverse: bool) -> ColorMap {
        let cdict = color_list_to_dict(clist);
        ColorMap::new(&cdict, reverse)
    }

    pub fn from_color_info_list(clist: &[ColorInfo], reverse: bool) -> ColorMap {
        let cdict = colorinfo_list_to_dict(clist);
        ColorMap::new(&cdict, reverse)
    }

    pub fn from_color_array(mut cmap: Vec<Color>, reverse: bool) -> ColorMap {
        if reverse {
            cmap.reverse();
        }

        ColorMap { cmap }
    }

    pub fn from_color_mapper(cmap: &ColorMapper, reverse: bool) -> ColorMap {
        let mut cmap_values = vec![Color::default(); 256];
        for (i, cmap_value) in cmap_values.iter_mut().enumerate() {
            let map_val = i as f64 / 255.0;
            *cmap_value = Color::rgb((cmap.red)(map_val), (cmap.green)(map_val), (cmap.blue)(map_val));
        }

        if reverse {
            cmap_values.reverse();
        }

        ColorMap { cmap: cmap_values }
    }

    pub fn qualitative(clist: &[Color]) -> ColorMap {
        let mut cmap = vec![Color::default(); 256];
        for (i, color) in cmap.iter_mut().enumerate() {
            let index = (i as f64 / 255.0 * (clist.len() - 1) as f64) as usize;
            *color = clist[index];
        }

        ColorMap { cmap }
    }

    fn create_colordict_cmap(name: &str) -> Option<ColorDict> {
        match name {
            "bone" => Some(cmap::bone()),
            "cool" => Some(cmap::cool()),
            "copper" => Some(cmap::copper()),
            "gray" => Some(cmap::gray()),
            "hot" => Some(cmap::hot()),
            "hsv" => Some(cmap::hsv()),
            "pink" => Some(cmap::pink()),
            "jet" => Some(cmap::jet()),
            "spring" => Some(cmap::spring()),
            "summer" => Some(cmap::summer()),
            "autumn" => Some(cmap::autumn()),
            "winter" => Some(cmap::winter()),
            "wistia" => Some(cmap::wistia()),
            "nipy_spectral" => Some(cmap::nipy_spectral()),
            "gist_earth" => Some(cmap::gist_earth()),
            "gist_ncar" => Some(cmap::gist_ncar()),
            "gist_stern" => Some(cmap::gist_stern()),
            _ => None,
        }
    }

    pub fn create(name: &str) -> Result<ColorMap, Error> {
        let mut reverse = false;
        let mut lowername = name.to_lowercase();
        if lowername.ends_with("_r") {
            reverse = true;
            lowername.truncate(lowername.len() - 2);
        }

        static CMAP_LOOKUP2: phf::Map<&str, &'static [Color]> = phf::phf_map! {
            "blues" => &cmap::BLUES,
            "brbg" => &cmap::BR_BG,
            "bugn" => &cmap::BU_GN,
            "bupu" => &cmap::BU_PU,
            "gnbu" => &cmap::GN_BU,
            "greens" => &cmap::GREENS,
            "greys" => &cmap::GREYS,
            "oranges" => &cmap::ORANGES,
            "orrd" => &cmap::OR_RD,
            "piyg" => &cmap::PI_YG,
            "prgn" => &cmap::PR_GN,
            "pubu" => &cmap::PU_BU,
            "pubugn" => &cmap::PU_BU_GN,
            "puor" => &cmap::PU_OR,
            "purd" => &cmap::PU_RD,
            "purples" => &cmap::PURPLES,
            "rdbu" => &cmap::RD_BU,
            "rdgy" => &cmap::RD_GY,
            "rdpu" => &cmap::RD_PU,
            "rdylbu" => &cmap::RD_YL_BU,
            "rdylgn" => &cmap::RD_YL_GN,
            "reds" => &cmap::REDS,
            "spectral" => &cmap::SPECTRAL,
            "ylgn" => &cmap::YL_GN,
            "ylgnbu" => &cmap::YL_GN_BU,
            "ylorbr" => &cmap::YL_OR_BR,
            "ylorrd" => &cmap::YL_OR_RD,
            "turbo" => &cmap::TURBO,
            "accent" => &cmap::ACCENT,
            "dark2" => &cmap::DARK2,
            "paired" => &cmap::PAIRED,
            "pastel1" => &cmap::PASTEL1,
            "pastel2" => &cmap::PASTEL2,
            "set1" => &cmap::SET1,
            "set2" => &cmap::SET2,
            "set3" => &cmap::SET3,
            "tab10" => &cmap::TAB10,
            "tab20" => &cmap::TAB20,
            "tab20b" => &cmap::TAB20B,
            "tab20c" => &cmap::TAB20C,
        };

        static CMAP_LOOKUP3: phf::Map<&str, &'static [ColorInfo]> = phf::phf_map! {
            "terrain" => &cmap::TERRAIN,
        };

        static CMAP_LOOKUP4: phf::Map<&str, &'static ColorMapper> = phf::phf_map! {
            "rainbow" => &cmap::RAINBOW,
        };

        if let Some(cmap) = ColorMap::create_colordict_cmap(lowername.as_str()) {
            Ok(ColorMap::new(&cmap, reverse))
        } else if let Some(clist) = CMAP_LOOKUP2.get(lowername.as_str()) {
            Ok(ColorMap::from_color_list(clist, reverse))
        } else if let Some(clist) = CMAP_LOOKUP3.get(lowername.as_str()) {
            Ok(ColorMap::from_color_info_list(clist, reverse))
        } else if let Some(cmap) = CMAP_LOOKUP4.get(lowername.as_str()) {
            Ok(ColorMap::from_color_mapper(cmap, reverse))
        } else {
            Err(Error::Runtime(format!("Unsupported color map: {}", name)))
        }
    }

    pub fn get_color(&self, value: f64) -> Color {
        self.cmap[(value * 255.0).round() as usize]
    }

    pub fn get_color_by_value(&self, value: u8) -> Color {
        self.cmap[value as usize]
    }

    pub fn apply_opacity_fade_in(&mut self, fade_stop: f64) {
        let end_index = (self.cmap.len() as f64 * fade_stop) as usize;
        let transparency_increment = 1.0 / end_index as f64;
        let mut alpha = 0.0;
        for i in 0..end_index {
            self.cmap[i].a = (alpha * 255.0) as u8;
            alpha += transparency_increment;
        }
    }

    pub fn apply_opacity_fade_out(&mut self, fade_start: f64) {
        let start_index = (self.cmap.len() as f64 * fade_start) as usize;
        let transparency_increment = 1.0 / start_index as f64;
        let mut alpha = 1.0;
        for i in start_index..self.cmap.len() {
            self.cmap[i].a = (alpha * 255.0) as u8;
            alpha -= transparency_increment;
        }
    }

    fn process_band(value: f64, dict: &[ColorDictEntry]) -> u8 {
        assert!(dict.len() >= 2);

        for i in 0..dict.len() - 1 {
            if value >= dict[i].x && value < dict[i + 1].x {
                return remap(dict[i].x, dict[i + 1].x, dict[i].y1, dict[i + 1].y0, value);
            }
        }

        if let Some(entry) = dict.last() {
            if value == entry.x {
                let i = dict.len() - 2;
                return remap(dict[i].x, dict[i + 1].x, dict[i].y1, dict[i + 1].y0, value);
            }
        }

        0
    }
}

fn color_list_to_dict(clist: &[Color]) -> ColorDict {
    assert!(clist.len() > 1);

    let mut cdict = ColorDict {
        red: Vec::new(),
        green: Vec::new(),
        blue: Vec::new(),
    };
    let band_width = 1.0 / (clist.len() - 1) as f64;
    let mut val = 0.0;
    for color in clist {
        let r = color.r as f64 / 255.0;
        let g = color.g as f64 / 255.0;
        let b = color.b as f64 / 255.0;
        cdict.red.push(ColorDictEntry { x: val, y0: r, y1: r });
        cdict.green.push(ColorDictEntry { x: val, y0: g, y1: g });
        cdict.blue.push(ColorDictEntry { x: val, y0: b, y1: b });

        val += band_width;
    }

    cdict
}

fn colorinfo_list_to_dict(clist: &[ColorInfo]) -> ColorDict {
    assert!(clist.len() > 1);

    let mut cdict = ColorDict {
        red: Vec::new(),
        green: Vec::new(),
        blue: Vec::new(),
    };
    for color_info in clist {
        let r = color_info.color.r as f64 / 255.0;
        let g = color_info.color.g as f64 / 255.0;
        let b = color_info.color.b as f64 / 255.0;
        cdict.red.push(ColorDictEntry {
            x: color_info.start,
            y0: r,
            y1: r,
        });
        cdict.green.push(ColorDictEntry {
            x: color_info.start,
            y0: g,
            y1: g,
        });
        cdict.blue.push(ColorDictEntry {
            x: color_info.start,
            y0: b,
            y1: b,
        });
    }

    cdict
}

fn remap(start: f64, end: f64, map_start: f64, map_end: f64, value: f64) -> u8 {
    assert!(start < end);
    assert!((0.0..=1.0).contains(&map_start));
    assert!((0.0..=1.0).contains(&map_end));

    if map_start == map_end {
        return (map_start * 255.0) as u8;
    }

    let range_width = end - start;
    let pos = (value - start) / range_width;

    let map_width = map_end - map_start;
    ((map_start + (map_width * pos)) * 255.0).round() as u8
}

pub mod cmap {

    use crate::color::Color;

    use super::{ColorDict, ColorDictEntry, ColorInfo, ColorMapper};

    pub fn bone() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.746032,
                    y0: 0.652778,
                    y1: 0.652778,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.365079,
                    y0: 0.319444,
                    y1: 0.319444,
                },
                ColorDictEntry {
                    x: 0.746032,
                    y0: 0.777778,
                    y1: 0.777778,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.365079,
                    y0: 0.444444,
                    y1: 0.444444,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
        }
    }

    pub fn cool() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.0,
                    y1: 0.0,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
        }
    }

    pub fn copper() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.809524,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.7812,
                    y1: 0.7812,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.4975,
                    y1: 0.4975,
                },
            ],
        }
    }

    pub fn gray() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
        }
    }

    pub fn hot() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0416,
                    y1: 0.0416,
                },
                ColorDictEntry {
                    x: 0.365079,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.365079,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.746032,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.746032,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
        }
    }

    pub fn hsv() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 0.158730,
                    y0: 1.000000,
                    y1: 1.000000,
                },
                ColorDictEntry {
                    x: 0.174603,
                    y0: 0.968750,
                    y1: 0.968750,
                },
                ColorDictEntry {
                    x: 0.333333,
                    y0: 0.031250,
                    y1: 0.031250,
                },
                ColorDictEntry {
                    x: 0.349206,
                    y0: 0.000000,
                    y1: 0.000000,
                },
                ColorDictEntry {
                    x: 0.666667,
                    y0: 0.000000,
                    y1: 0.000000,
                },
                ColorDictEntry {
                    x: 0.682540,
                    y0: 0.031250,
                    y1: 0.031250,
                },
                ColorDictEntry {
                    x: 0.841270,
                    y0: 0.968750,
                    y1: 0.968750,
                },
                ColorDictEntry {
                    x: 0.857143,
                    y0: 1.000000,
                    y1: 1.000000,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.158730,
                    y0: 0.937500,
                    y1: 0.937500,
                },
                ColorDictEntry {
                    x: 0.174603,
                    y0: 1.000000,
                    y1: 1.000000,
                },
                ColorDictEntry {
                    x: 0.507937,
                    y0: 1.000000,
                    y1: 1.000000,
                },
                ColorDictEntry {
                    x: 0.666667,
                    y0: 0.062500,
                    y1: 0.062500,
                },
                ColorDictEntry {
                    x: 0.682540,
                    y0: 0.000000,
                    y1: 0.000000,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.0,
                    y1: 0.0,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.333333,
                    y0: 0.000000,
                    y1: 0.000000,
                },
                ColorDictEntry {
                    x: 0.349206,
                    y0: 0.062500,
                    y1: 0.062500,
                },
                ColorDictEntry {
                    x: 0.507937,
                    y0: 1.000000,
                    y1: 1.000000,
                },
                ColorDictEntry {
                    x: 0.841270,
                    y0: 1.000000,
                    y1: 1.000000,
                },
                ColorDictEntry {
                    x: 0.857143,
                    y0: 0.937500,
                    y1: 0.937500,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.09375,
                    y1: 0.09375,
                },
            ],
        }
    }

    pub fn pink() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.1178,
                    y1: 0.1178,
                },
                ColorDictEntry {
                    x: 0.015873,
                    y0: 0.195857,
                    y1: 0.195857,
                },
                ColorDictEntry {
                    x: 0.031746,
                    y0: 0.250661,
                    y1: 0.250661,
                },
                ColorDictEntry {
                    x: 0.047619,
                    y0: 0.295468,
                    y1: 0.295468,
                },
                ColorDictEntry {
                    x: 0.063492,
                    y0: 0.334324,
                    y1: 0.334324,
                },
                ColorDictEntry {
                    x: 0.079365,
                    y0: 0.369112,
                    y1: 0.369112,
                },
                ColorDictEntry {
                    x: 0.095238,
                    y0: 0.400892,
                    y1: 0.400892,
                },
                ColorDictEntry {
                    x: 0.111111,
                    y0: 0.430331,
                    y1: 0.430331,
                },
                ColorDictEntry {
                    x: 0.126984,
                    y0: 0.457882,
                    y1: 0.457882,
                },
                ColorDictEntry {
                    x: 0.142857,
                    y0: 0.483867,
                    y1: 0.483867,
                },
                ColorDictEntry {
                    x: 0.158730,
                    y0: 0.508525,
                    y1: 0.508525,
                },
                ColorDictEntry {
                    x: 0.174603,
                    y0: 0.532042,
                    y1: 0.532042,
                },
                ColorDictEntry {
                    x: 0.190476,
                    y0: 0.554563,
                    y1: 0.554563,
                },
                ColorDictEntry {
                    x: 0.206349,
                    y0: 0.576204,
                    y1: 0.576204,
                },
                ColorDictEntry {
                    x: 0.222222,
                    y0: 0.597061,
                    y1: 0.597061,
                },
                ColorDictEntry {
                    x: 0.238095,
                    y0: 0.617213,
                    y1: 0.617213,
                },
                ColorDictEntry {
                    x: 0.253968,
                    y0: 0.636729,
                    y1: 0.636729,
                },
                ColorDictEntry {
                    x: 0.269841,
                    y0: 0.655663,
                    y1: 0.655663,
                },
                ColorDictEntry {
                    x: 0.285714,
                    y0: 0.674066,
                    y1: 0.674066,
                },
                ColorDictEntry {
                    x: 0.301587,
                    y0: 0.691980,
                    y1: 0.691980,
                },
                ColorDictEntry {
                    x: 0.317460,
                    y0: 0.709441,
                    y1: 0.709441,
                },
                ColorDictEntry {
                    x: 0.333333,
                    y0: 0.726483,
                    y1: 0.726483,
                },
                ColorDictEntry {
                    x: 0.349206,
                    y0: 0.743134,
                    y1: 0.743134,
                },
                ColorDictEntry {
                    x: 0.365079,
                    y0: 0.759421,
                    y1: 0.759421,
                },
                ColorDictEntry {
                    x: 0.380952,
                    y0: 0.766356,
                    y1: 0.766356,
                },
                ColorDictEntry {
                    x: 0.396825,
                    y0: 0.773229,
                    y1: 0.773229,
                },
                ColorDictEntry {
                    x: 0.412698,
                    y0: 0.780042,
                    y1: 0.780042,
                },
                ColorDictEntry {
                    x: 0.428571,
                    y0: 0.786796,
                    y1: 0.786796,
                },
                ColorDictEntry {
                    x: 0.444444,
                    y0: 0.793492,
                    y1: 0.793492,
                },
                ColorDictEntry {
                    x: 0.460317,
                    y0: 0.800132,
                    y1: 0.800132,
                },
                ColorDictEntry {
                    x: 0.476190,
                    y0: 0.806718,
                    y1: 0.806718,
                },
                ColorDictEntry {
                    x: 0.492063,
                    y0: 0.813250,
                    y1: 0.813250,
                },
                ColorDictEntry {
                    x: 0.507937,
                    y0: 0.819730,
                    y1: 0.819730,
                },
                ColorDictEntry {
                    x: 0.523810,
                    y0: 0.826160,
                    y1: 0.826160,
                },
                ColorDictEntry {
                    x: 0.539683,
                    y0: 0.832539,
                    y1: 0.832539,
                },
                ColorDictEntry {
                    x: 0.555556,
                    y0: 0.838870,
                    y1: 0.838870,
                },
                ColorDictEntry {
                    x: 0.571429,
                    y0: 0.845154,
                    y1: 0.845154,
                },
                ColorDictEntry {
                    x: 0.587302,
                    y0: 0.851392,
                    y1: 0.851392,
                },
                ColorDictEntry {
                    x: 0.603175,
                    y0: 0.857584,
                    y1: 0.857584,
                },
                ColorDictEntry {
                    x: 0.619048,
                    y0: 0.863731,
                    y1: 0.863731,
                },
                ColorDictEntry {
                    x: 0.634921,
                    y0: 0.869835,
                    y1: 0.869835,
                },
                ColorDictEntry {
                    x: 0.650794,
                    y0: 0.875897,
                    y1: 0.875897,
                },
                ColorDictEntry {
                    x: 0.666667,
                    y0: 0.881917,
                    y1: 0.881917,
                },
                ColorDictEntry {
                    x: 0.682540,
                    y0: 0.887896,
                    y1: 0.887896,
                },
                ColorDictEntry {
                    x: 0.698413,
                    y0: 0.893835,
                    y1: 0.893835,
                },
                ColorDictEntry {
                    x: 0.714286,
                    y0: 0.899735,
                    y1: 0.899735,
                },
                ColorDictEntry {
                    x: 0.730159,
                    y0: 0.905597,
                    y1: 0.905597,
                },
                ColorDictEntry {
                    x: 0.746032,
                    y0: 0.911421,
                    y1: 0.911421,
                },
                ColorDictEntry {
                    x: 0.761905,
                    y0: 0.917208,
                    y1: 0.917208,
                },
                ColorDictEntry {
                    x: 0.777778,
                    y0: 0.922958,
                    y1: 0.922958,
                },
                ColorDictEntry {
                    x: 0.793651,
                    y0: 0.928673,
                    y1: 0.928673,
                },
                ColorDictEntry {
                    x: 0.809524,
                    y0: 0.934353,
                    y1: 0.934353,
                },
                ColorDictEntry {
                    x: 0.825397,
                    y0: 0.939999,
                    y1: 0.939999,
                },
                ColorDictEntry {
                    x: 0.841270,
                    y0: 0.945611,
                    y1: 0.945611,
                },
                ColorDictEntry {
                    x: 0.857143,
                    y0: 0.951190,
                    y1: 0.951190,
                },
                ColorDictEntry {
                    x: 0.873016,
                    y0: 0.956736,
                    y1: 0.956736,
                },
                ColorDictEntry {
                    x: 0.888889,
                    y0: 0.962250,
                    y1: 0.962250,
                },
                ColorDictEntry {
                    x: 0.904762,
                    y0: 0.967733,
                    y1: 0.967733,
                },
                ColorDictEntry {
                    x: 0.920635,
                    y0: 0.973185,
                    y1: 0.973185,
                },
                ColorDictEntry {
                    x: 0.936508,
                    y0: 0.978607,
                    y1: 0.978607,
                },
                ColorDictEntry {
                    x: 0.952381,
                    y0: 0.983999,
                    y1: 0.983999,
                },
                ColorDictEntry {
                    x: 0.968254,
                    y0: 0.989361,
                    y1: 0.989361,
                },
                ColorDictEntry {
                    x: 0.984127,
                    y0: 0.994695,
                    y1: 0.994695,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.015873,
                    y0: 0.102869,
                    y1: 0.102869,
                },
                ColorDictEntry {
                    x: 0.031746,
                    y0: 0.145479,
                    y1: 0.145479,
                },
                ColorDictEntry {
                    x: 0.047619,
                    y0: 0.178174,
                    y1: 0.178174,
                },
                ColorDictEntry {
                    x: 0.063492,
                    y0: 0.205738,
                    y1: 0.205738,
                },
                ColorDictEntry {
                    x: 0.079365,
                    y0: 0.230022,
                    y1: 0.230022,
                },
                ColorDictEntry {
                    x: 0.095238,
                    y0: 0.251976,
                    y1: 0.251976,
                },
                ColorDictEntry {
                    x: 0.111111,
                    y0: 0.272166,
                    y1: 0.272166,
                },
                ColorDictEntry {
                    x: 0.126984,
                    y0: 0.290957,
                    y1: 0.290957,
                },
                ColorDictEntry {
                    x: 0.142857,
                    y0: 0.308607,
                    y1: 0.308607,
                },
                ColorDictEntry {
                    x: 0.158730,
                    y0: 0.325300,
                    y1: 0.325300,
                },
                ColorDictEntry {
                    x: 0.174603,
                    y0: 0.341178,
                    y1: 0.341178,
                },
                ColorDictEntry {
                    x: 0.190476,
                    y0: 0.356348,
                    y1: 0.356348,
                },
                ColorDictEntry {
                    x: 0.206349,
                    y0: 0.370899,
                    y1: 0.370899,
                },
                ColorDictEntry {
                    x: 0.222222,
                    y0: 0.384900,
                    y1: 0.384900,
                },
                ColorDictEntry {
                    x: 0.238095,
                    y0: 0.398410,
                    y1: 0.398410,
                },
                ColorDictEntry {
                    x: 0.253968,
                    y0: 0.411476,
                    y1: 0.411476,
                },
                ColorDictEntry {
                    x: 0.269841,
                    y0: 0.424139,
                    y1: 0.424139,
                },
                ColorDictEntry {
                    x: 0.285714,
                    y0: 0.436436,
                    y1: 0.436436,
                },
                ColorDictEntry {
                    x: 0.301587,
                    y0: 0.448395,
                    y1: 0.448395,
                },
                ColorDictEntry {
                    x: 0.317460,
                    y0: 0.460044,
                    y1: 0.460044,
                },
                ColorDictEntry {
                    x: 0.333333,
                    y0: 0.471405,
                    y1: 0.471405,
                },
                ColorDictEntry {
                    x: 0.349206,
                    y0: 0.482498,
                    y1: 0.482498,
                },
                ColorDictEntry {
                    x: 0.365079,
                    y0: 0.493342,
                    y1: 0.493342,
                },
                ColorDictEntry {
                    x: 0.380952,
                    y0: 0.503953,
                    y1: 0.503953,
                },
                ColorDictEntry {
                    x: 0.396825,
                    y0: 0.514344,
                    y1: 0.514344,
                },
                ColorDictEntry {
                    x: 0.412698,
                    y0: 0.524531,
                    y1: 0.524531,
                },
                ColorDictEntry {
                    x: 0.428571,
                    y0: 0.534522,
                    y1: 0.534522,
                },
                ColorDictEntry {
                    x: 0.444444,
                    y0: 0.544331,
                    y1: 0.544331,
                },
                ColorDictEntry {
                    x: 0.460317,
                    y0: 0.553966,
                    y1: 0.553966,
                },
                ColorDictEntry {
                    x: 0.476190,
                    y0: 0.563436,
                    y1: 0.563436,
                },
                ColorDictEntry {
                    x: 0.492063,
                    y0: 0.572750,
                    y1: 0.572750,
                },
                ColorDictEntry {
                    x: 0.507937,
                    y0: 0.581914,
                    y1: 0.581914,
                },
                ColorDictEntry {
                    x: 0.523810,
                    y0: 0.590937,
                    y1: 0.590937,
                },
                ColorDictEntry {
                    x: 0.539683,
                    y0: 0.599824,
                    y1: 0.599824,
                },
                ColorDictEntry {
                    x: 0.555556,
                    y0: 0.608581,
                    y1: 0.608581,
                },
                ColorDictEntry {
                    x: 0.571429,
                    y0: 0.617213,
                    y1: 0.617213,
                },
                ColorDictEntry {
                    x: 0.587302,
                    y0: 0.625727,
                    y1: 0.625727,
                },
                ColorDictEntry {
                    x: 0.603175,
                    y0: 0.634126,
                    y1: 0.634126,
                },
                ColorDictEntry {
                    x: 0.619048,
                    y0: 0.642416,
                    y1: 0.642416,
                },
                ColorDictEntry {
                    x: 0.634921,
                    y0: 0.650600,
                    y1: 0.650600,
                },
                ColorDictEntry {
                    x: 0.650794,
                    y0: 0.658682,
                    y1: 0.658682,
                },
                ColorDictEntry {
                    x: 0.666667,
                    y0: 0.666667,
                    y1: 0.666667,
                },
                ColorDictEntry {
                    x: 0.682540,
                    y0: 0.674556,
                    y1: 0.674556,
                },
                ColorDictEntry {
                    x: 0.698413,
                    y0: 0.682355,
                    y1: 0.682355,
                },
                ColorDictEntry {
                    x: 0.714286,
                    y0: 0.690066,
                    y1: 0.690066,
                },
                ColorDictEntry {
                    x: 0.730159,
                    y0: 0.697691,
                    y1: 0.697691,
                },
                ColorDictEntry {
                    x: 0.746032,
                    y0: 0.705234,
                    y1: 0.705234,
                },
                ColorDictEntry {
                    x: 0.761905,
                    y0: 0.727166,
                    y1: 0.727166,
                },
                ColorDictEntry {
                    x: 0.777778,
                    y0: 0.748455,
                    y1: 0.748455,
                },
                ColorDictEntry {
                    x: 0.793651,
                    y0: 0.769156,
                    y1: 0.769156,
                },
                ColorDictEntry {
                    x: 0.809524,
                    y0: 0.789314,
                    y1: 0.789314,
                },
                ColorDictEntry {
                    x: 0.825397,
                    y0: 0.808969,
                    y1: 0.808969,
                },
                ColorDictEntry {
                    x: 0.841270,
                    y0: 0.828159,
                    y1: 0.828159,
                },
                ColorDictEntry {
                    x: 0.857143,
                    y0: 0.846913,
                    y1: 0.846913,
                },
                ColorDictEntry {
                    x: 0.873016,
                    y0: 0.865261,
                    y1: 0.865261,
                },
                ColorDictEntry {
                    x: 0.888889,
                    y0: 0.883229,
                    y1: 0.883229,
                },
                ColorDictEntry {
                    x: 0.904762,
                    y0: 0.900837,
                    y1: 0.900837,
                },
                ColorDictEntry {
                    x: 0.920635,
                    y0: 0.918109,
                    y1: 0.918109,
                },
                ColorDictEntry {
                    x: 0.936508,
                    y0: 0.935061,
                    y1: 0.935061,
                },
                ColorDictEntry {
                    x: 0.952381,
                    y0: 0.951711,
                    y1: 0.951711,
                },
                ColorDictEntry {
                    x: 0.968254,
                    y0: 0.968075,
                    y1: 0.968075,
                },
                ColorDictEntry {
                    x: 0.984127,
                    y0: 0.984167,
                    y1: 0.984167,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.015873,
                    y0: 0.102869,
                    y1: 0.102869,
                },
                ColorDictEntry {
                    x: 0.031746,
                    y0: 0.145479,
                    y1: 0.145479,
                },
                ColorDictEntry {
                    x: 0.047619,
                    y0: 0.178174,
                    y1: 0.178174,
                },
                ColorDictEntry {
                    x: 0.063492,
                    y0: 0.205738,
                    y1: 0.205738,
                },
                ColorDictEntry {
                    x: 0.079365,
                    y0: 0.230022,
                    y1: 0.230022,
                },
                ColorDictEntry {
                    x: 0.095238,
                    y0: 0.251976,
                    y1: 0.251976,
                },
                ColorDictEntry {
                    x: 0.111111,
                    y0: 0.272166,
                    y1: 0.272166,
                },
                ColorDictEntry {
                    x: 0.126984,
                    y0: 0.290957,
                    y1: 0.290957,
                },
                ColorDictEntry {
                    x: 0.142857,
                    y0: 0.308607,
                    y1: 0.308607,
                },
                ColorDictEntry {
                    x: 0.158730,
                    y0: 0.325300,
                    y1: 0.325300,
                },
                ColorDictEntry {
                    x: 0.174603,
                    y0: 0.341178,
                    y1: 0.341178,
                },
                ColorDictEntry {
                    x: 0.190476,
                    y0: 0.356348,
                    y1: 0.356348,
                },
                ColorDictEntry {
                    x: 0.206349,
                    y0: 0.370899,
                    y1: 0.370899,
                },
                ColorDictEntry {
                    x: 0.222222,
                    y0: 0.384900,
                    y1: 0.384900,
                },
                ColorDictEntry {
                    x: 0.238095,
                    y0: 0.398410,
                    y1: 0.398410,
                },
                ColorDictEntry {
                    x: 0.253968,
                    y0: 0.411476,
                    y1: 0.411476,
                },
                ColorDictEntry {
                    x: 0.269841,
                    y0: 0.424139,
                    y1: 0.424139,
                },
                ColorDictEntry {
                    x: 0.285714,
                    y0: 0.436436,
                    y1: 0.436436,
                },
                ColorDictEntry {
                    x: 0.301587,
                    y0: 0.448395,
                    y1: 0.448395,
                },
                ColorDictEntry {
                    x: 0.317460,
                    y0: 0.460044,
                    y1: 0.460044,
                },
                ColorDictEntry {
                    x: 0.333333,
                    y0: 0.471405,
                    y1: 0.471405,
                },
                ColorDictEntry {
                    x: 0.349206,
                    y0: 0.482498,
                    y1: 0.482498,
                },
                ColorDictEntry {
                    x: 0.365079,
                    y0: 0.493342,
                    y1: 0.493342,
                },
                ColorDictEntry {
                    x: 0.380952,
                    y0: 0.503953,
                    y1: 0.503953,
                },
                ColorDictEntry {
                    x: 0.396825,
                    y0: 0.514344,
                    y1: 0.514344,
                },
                ColorDictEntry {
                    x: 0.412698,
                    y0: 0.524531,
                    y1: 0.524531,
                },
                ColorDictEntry {
                    x: 0.428571,
                    y0: 0.534522,
                    y1: 0.534522,
                },
                ColorDictEntry {
                    x: 0.444444,
                    y0: 0.544331,
                    y1: 0.544331,
                },
                ColorDictEntry {
                    x: 0.460317,
                    y0: 0.553966,
                    y1: 0.553966,
                },
                ColorDictEntry {
                    x: 0.476190,
                    y0: 0.563436,
                    y1: 0.563436,
                },
                ColorDictEntry {
                    x: 0.492063,
                    y0: 0.572750,
                    y1: 0.572750,
                },
                ColorDictEntry {
                    x: 0.507937,
                    y0: 0.581914,
                    y1: 0.581914,
                },
                ColorDictEntry {
                    x: 0.523810,
                    y0: 0.590937,
                    y1: 0.590937,
                },
                ColorDictEntry {
                    x: 0.539683,
                    y0: 0.599824,
                    y1: 0.599824,
                },
                ColorDictEntry {
                    x: 0.555556,
                    y0: 0.608581,
                    y1: 0.608581,
                },
                ColorDictEntry {
                    x: 0.571429,
                    y0: 0.617213,
                    y1: 0.617213,
                },
                ColorDictEntry {
                    x: 0.587302,
                    y0: 0.625727,
                    y1: 0.625727,
                },
                ColorDictEntry {
                    x: 0.603175,
                    y0: 0.634126,
                    y1: 0.634126,
                },
                ColorDictEntry {
                    x: 0.619048,
                    y0: 0.642416,
                    y1: 0.642416,
                },
                ColorDictEntry {
                    x: 0.634921,
                    y0: 0.650600,
                    y1: 0.650600,
                },
                ColorDictEntry {
                    x: 0.650794,
                    y0: 0.658682,
                    y1: 0.658682,
                },
                ColorDictEntry {
                    x: 0.666667,
                    y0: 0.666667,
                    y1: 0.666667,
                },
                ColorDictEntry {
                    x: 0.682540,
                    y0: 0.674556,
                    y1: 0.674556,
                },
                ColorDictEntry {
                    x: 0.698413,
                    y0: 0.682355,
                    y1: 0.682355,
                },
                ColorDictEntry {
                    x: 0.714286,
                    y0: 0.690066,
                    y1: 0.690066,
                },
                ColorDictEntry {
                    x: 0.730159,
                    y0: 0.697691,
                    y1: 0.697691,
                },
                ColorDictEntry {
                    x: 0.746032,
                    y0: 0.705234,
                    y1: 0.705234,
                },
                ColorDictEntry {
                    x: 0.761905,
                    y0: 0.727166,
                    y1: 0.727166,
                },
                ColorDictEntry {
                    x: 0.777778,
                    y0: 0.748455,
                    y1: 0.748455,
                },
                ColorDictEntry {
                    x: 0.793651,
                    y0: 0.769156,
                    y1: 0.769156,
                },
                ColorDictEntry {
                    x: 0.809524,
                    y0: 0.789314,
                    y1: 0.789314,
                },
                ColorDictEntry {
                    x: 0.825397,
                    y0: 0.808969,
                    y1: 0.808969,
                },
                ColorDictEntry {
                    x: 0.841270,
                    y0: 0.828159,
                    y1: 0.828159,
                },
                ColorDictEntry {
                    x: 0.857143,
                    y0: 0.846913,
                    y1: 0.846913,
                },
                ColorDictEntry {
                    x: 0.873016,
                    y0: 0.865261,
                    y1: 0.865261,
                },
                ColorDictEntry {
                    x: 0.888889,
                    y0: 0.883229,
                    y1: 0.883229,
                },
                ColorDictEntry {
                    x: 0.904762,
                    y0: 0.900837,
                    y1: 0.900837,
                },
                ColorDictEntry {
                    x: 0.920635,
                    y0: 0.918109,
                    y1: 0.918109,
                },
                ColorDictEntry {
                    x: 0.936508,
                    y0: 0.935061,
                    y1: 0.935061,
                },
                ColorDictEntry {
                    x: 0.952381,
                    y0: 0.951711,
                    y1: 0.951711,
                },
                ColorDictEntry {
                    x: 0.968254,
                    y0: 0.968075,
                    y1: 0.968075,
                },
                ColorDictEntry {
                    x: 0.984127,
                    y0: 0.984167,
                    y1: 0.984167,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
        }
    }

    pub fn jet() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.35,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.66,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 0.89,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.5,
                    y1: 0.5,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.125,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.375,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 0.64,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 0.91,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.0,
                    y1: 0.0,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.5,
                    y1: 0.5,
                },
                ColorDictEntry {
                    x: 0.11,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 0.34,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 0.65,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.0,
                    y1: 0.0,
                },
            ],
        }
    }

    pub fn spring() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.0,
                    y1: 0.0,
                },
            ],
        }
    }

    pub fn summer() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.5,
                    y1: 0.5,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.4,
                    y1: 0.4,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.4,
                    y1: 0.4,
                },
            ],
        }
    }

    pub fn autumn() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.0,
                    y1: 0.0,
                },
            ],
        }
    }

    pub fn winter() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.0,
                    y1: 0.0,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.5,
                    y1: 0.5,
                },
            ],
        }
    }

    pub fn wistia() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.5,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 0.75,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.9882352941176471,
                    y1: 0.9882352941176471,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 0.25,
                    y0: 0.9098039215686274,
                    y1: 0.9098039215686274,
                },
                ColorDictEntry {
                    x: 0.5,
                    y0: 0.7411764705882353,
                    y1: 0.7411764705882353,
                },
                ColorDictEntry {
                    x: 0.75,
                    y0: 0.6274509803921569,
                    y1: 0.6274509803921569,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.4980392156862745,
                    y1: 0.4980392156862745,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.47843137254901963,
                    y1: 0.47843137254901963,
                },
                ColorDictEntry {
                    x: 0.25,
                    y0: 0.10196078431372549,
                    y1: 0.10196078431372549,
                },
                ColorDictEntry {
                    x: 0.5,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.75,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.0,
                    y1: 0.0,
                },
            ],
        }
    }

    pub fn nipy_spectral() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.05,
                    y0: 0.4667,
                    y1: 0.4667,
                },
                ColorDictEntry {
                    x: 0.10,
                    y0: 0.5333,
                    y1: 0.5333,
                },
                ColorDictEntry {
                    x: 0.15,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.20,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.25,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.30,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.35,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.40,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.45,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.50,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.55,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.60,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.65,
                    y0: 0.7333,
                    y1: 0.7333,
                },
                ColorDictEntry {
                    x: 0.70,
                    y0: 0.9333,
                    y1: 0.9333,
                },
                ColorDictEntry {
                    x: 0.75,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 0.80,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 0.85,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 0.90,
                    y0: 0.8667,
                    y1: 0.8667,
                },
                ColorDictEntry {
                    x: 0.95,
                    y0: 0.80,
                    y1: 0.80,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.80,
                    y1: 0.80,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.05,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.10,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.15,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.20,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.25,
                    y0: 0.4667,
                    y1: 0.4667,
                },
                ColorDictEntry {
                    x: 0.30,
                    y0: 0.6000,
                    y1: 0.6000,
                },
                ColorDictEntry {
                    x: 0.35,
                    y0: 0.6667,
                    y1: 0.6667,
                },
                ColorDictEntry {
                    x: 0.40,
                    y0: 0.6667,
                    y1: 0.6667,
                },
                ColorDictEntry {
                    x: 0.45,
                    y0: 0.6000,
                    y1: 0.6000,
                },
                ColorDictEntry {
                    x: 0.50,
                    y0: 0.7333,
                    y1: 0.7333,
                },
                ColorDictEntry {
                    x: 0.55,
                    y0: 0.8667,
                    y1: 0.8667,
                },
                ColorDictEntry {
                    x: 0.60,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 0.65,
                    y0: 1.0,
                    y1: 1.0,
                },
                ColorDictEntry {
                    x: 0.70,
                    y0: 0.9333,
                    y1: 0.9333,
                },
                ColorDictEntry {
                    x: 0.75,
                    y0: 0.8000,
                    y1: 0.8000,
                },
                ColorDictEntry {
                    x: 0.80,
                    y0: 0.6000,
                    y1: 0.6000,
                },
                ColorDictEntry {
                    x: 0.85,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.90,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.95,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.80,
                    y1: 0.80,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.05,
                    y0: 0.5333,
                    y1: 0.5333,
                },
                ColorDictEntry {
                    x: 0.10,
                    y0: 0.6000,
                    y1: 0.6000,
                },
                ColorDictEntry {
                    x: 0.15,
                    y0: 0.6667,
                    y1: 0.6667,
                },
                ColorDictEntry {
                    x: 0.20,
                    y0: 0.8667,
                    y1: 0.8667,
                },
                ColorDictEntry {
                    x: 0.25,
                    y0: 0.8667,
                    y1: 0.8667,
                },
                ColorDictEntry {
                    x: 0.30,
                    y0: 0.8667,
                    y1: 0.8667,
                },
                ColorDictEntry {
                    x: 0.35,
                    y0: 0.6667,
                    y1: 0.6667,
                },
                ColorDictEntry {
                    x: 0.40,
                    y0: 0.5333,
                    y1: 0.5333,
                },
                ColorDictEntry {
                    x: 0.45,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.50,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.55,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.60,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.65,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.70,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.75,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.80,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.85,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.90,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 0.95,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 0.80,
                    y1: 0.80,
                },
            ],
        }
    }

    pub fn gist_earth() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0000,
                },
                ColorDictEntry {
                    x: 0.2824,
                    y0: 0.1882,
                    y1: 0.1882,
                },
                ColorDictEntry {
                    x: 0.4588,
                    y0: 0.2714,
                    y1: 0.2714,
                },
                ColorDictEntry {
                    x: 0.5490,
                    y0: 0.4719,
                    y1: 0.4719,
                },
                ColorDictEntry {
                    x: 0.6980,
                    y0: 0.7176,
                    y1: 0.7176,
                },
                ColorDictEntry {
                    x: 0.7882,
                    y0: 0.7553,
                    y1: 0.7553,
                },
                ColorDictEntry {
                    x: 1.0000,
                    y0: 0.9922,
                    y1: 0.9922,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0000,
                },
                ColorDictEntry {
                    x: 0.0275,
                    y0: 0.0000,
                    y1: 0.0000,
                },
                ColorDictEntry {
                    x: 0.1098,
                    y0: 0.1893,
                    y1: 0.1893,
                },
                ColorDictEntry {
                    x: 0.1647,
                    y0: 0.3035,
                    y1: 0.3035,
                },
                ColorDictEntry {
                    x: 0.2078,
                    y0: 0.3841,
                    y1: 0.3841,
                },
                ColorDictEntry {
                    x: 0.2824,
                    y0: 0.5020,
                    y1: 0.5020,
                },
                ColorDictEntry {
                    x: 0.5216,
                    y0: 0.6397,
                    y1: 0.6397,
                },
                ColorDictEntry {
                    x: 0.6980,
                    y0: 0.7171,
                    y1: 0.7171,
                },
                ColorDictEntry {
                    x: 0.7882,
                    y0: 0.6392,
                    y1: 0.6392,
                },
                ColorDictEntry {
                    x: 0.7922,
                    y0: 0.6413,
                    y1: 0.6413,
                },
                ColorDictEntry {
                    x: 0.8000,
                    y0: 0.6447,
                    y1: 0.6447,
                },
                ColorDictEntry {
                    x: 0.8078,
                    y0: 0.6481,
                    y1: 0.6481,
                },
                ColorDictEntry {
                    x: 0.8157,
                    y0: 0.6549,
                    y1: 0.6549,
                },
                ColorDictEntry {
                    x: 0.8667,
                    y0: 0.6991,
                    y1: 0.6991,
                },
                ColorDictEntry {
                    x: 0.8745,
                    y0: 0.7103,
                    y1: 0.7103,
                },
                ColorDictEntry {
                    x: 0.8824,
                    y0: 0.7216,
                    y1: 0.7216,
                },
                ColorDictEntry {
                    x: 0.8902,
                    y0: 0.7323,
                    y1: 0.7323,
                },
                ColorDictEntry {
                    x: 0.8980,
                    y0: 0.7430,
                    y1: 0.7430,
                },
                ColorDictEntry {
                    x: 0.9412,
                    y0: 0.8275,
                    y1: 0.8275,
                },
                ColorDictEntry {
                    x: 0.9569,
                    y0: 0.8635,
                    y1: 0.8635,
                },
                ColorDictEntry {
                    x: 0.9647,
                    y0: 0.8816,
                    y1: 0.8816,
                },
                ColorDictEntry {
                    x: 0.9961,
                    y0: 0.9733,
                    y1: 0.9733,
                },
                ColorDictEntry {
                    x: 1.0000,
                    y0: 0.9843,
                    y1: 0.9843,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.0039,
                    y0: 0.1684,
                    y1: 0.1684,
                },
                ColorDictEntry {
                    x: 0.0078,
                    y0: 0.2212,
                    y1: 0.2212,
                },
                ColorDictEntry {
                    x: 0.0275,
                    y0: 0.4329,
                    y1: 0.4329,
                },
                ColorDictEntry {
                    x: 0.0314,
                    y0: 0.4549,
                    y1: 0.4549,
                },
                ColorDictEntry {
                    x: 0.2824,
                    y0: 0.5004,
                    y1: 0.5004,
                },
                ColorDictEntry {
                    x: 0.4667,
                    y0: 0.2748,
                    y1: 0.2748,
                },
                ColorDictEntry {
                    x: 0.5451,
                    y0: 0.3205,
                    y1: 0.3205,
                },
                ColorDictEntry {
                    x: 0.7843,
                    y0: 0.3961,
                    y1: 0.3961,
                },
                ColorDictEntry {
                    x: 0.8941,
                    y0: 0.6651,
                    y1: 0.6651,
                },
                ColorDictEntry {
                    x: 1.0000,
                    y0: 0.9843,
                    y1: 0.9843,
                },
            ],
        }
    }

    pub fn gist_ncar() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0000,
                },
                ColorDictEntry {
                    x: 0.3098,
                    y0: 0.0000,
                    y1: 0.0000,
                },
                ColorDictEntry {
                    x: 0.3725,
                    y0: 0.3993,
                    y1: 0.3993,
                },
                ColorDictEntry {
                    x: 0.4235,
                    y0: 0.5003,
                    y1: 0.5003,
                },
                ColorDictEntry {
                    x: 0.5333,
                    y0: 1.0000,
                    y1: 1.0000,
                },
                ColorDictEntry {
                    x: 0.7922,
                    y0: 1.0000,
                    y1: 1.0000,
                },
                ColorDictEntry {
                    x: 0.8471,
                    y0: 0.6218,
                    y1: 0.6218,
                },
                ColorDictEntry {
                    x: 0.8980,
                    y0: 0.9235,
                    y1: 0.9235,
                },
                ColorDictEntry {
                    x: 1.0000,
                    y0: 0.9961,
                    y1: 0.9961,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0000,
                },
                ColorDictEntry {
                    x: 0.0510,
                    y0: 0.3722,
                    y1: 0.3722,
                },
                ColorDictEntry {
                    x: 0.1059,
                    y0: 0.0000,
                    y1: 0.0000,
                },
                ColorDictEntry {
                    x: 0.1569,
                    y0: 0.7202,
                    y1: 0.7202,
                },
                ColorDictEntry {
                    x: 0.1608,
                    y0: 0.7537,
                    y1: 0.7537,
                },
                ColorDictEntry {
                    x: 0.1647,
                    y0: 0.7752,
                    y1: 0.7752,
                },
                ColorDictEntry {
                    x: 0.2157,
                    y0: 1.0000,
                    y1: 1.0000,
                },
                ColorDictEntry {
                    x: 0.2588,
                    y0: 0.9804,
                    y1: 0.9804,
                },
                ColorDictEntry {
                    x: 0.2706,
                    y0: 0.9804,
                    y1: 0.9804,
                },
                ColorDictEntry {
                    x: 0.3176,
                    y0: 1.0000,
                    y1: 1.0000,
                },
                ColorDictEntry {
                    x: 0.3686,
                    y0: 0.8081,
                    y1: 0.8081,
                },
                ColorDictEntry {
                    x: 0.4275,
                    y0: 1.0000,
                    y1: 1.0000,
                },
                ColorDictEntry {
                    x: 0.5216,
                    y0: 1.0000,
                    y1: 1.0000,
                },
                ColorDictEntry {
                    x: 0.6314,
                    y0: 0.7292,
                    y1: 0.7292,
                },
                ColorDictEntry {
                    x: 0.6863,
                    y0: 0.2796,
                    y1: 0.2796,
                },
                ColorDictEntry {
                    x: 0.7451,
                    y0: 0.0000,
                    y1: 0.0000,
                },
                ColorDictEntry {
                    x: 0.7922,
                    y0: 0.0000,
                    y1: 0.0000,
                },
                ColorDictEntry {
                    x: 0.8431,
                    y0: 0.1753,
                    y1: 0.1753,
                },
                ColorDictEntry {
                    x: 0.8980,
                    y0: 0.5000,
                    y1: 0.5000,
                },
                ColorDictEntry {
                    x: 1.0000,
                    y0: 0.9725,
                    y1: 0.9725,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.5020,
                    y1: 0.5020,
                },
                ColorDictEntry {
                    x: 0.0510,
                    y0: 0.0222,
                    y1: 0.0222,
                },
                ColorDictEntry {
                    x: 0.1098,
                    y0: 1.0000,
                    y1: 1.0000,
                },
                ColorDictEntry {
                    x: 0.2039,
                    y0: 1.0000,
                    y1: 1.0000,
                },
                ColorDictEntry {
                    x: 0.2627,
                    y0: 0.6145,
                    y1: 0.6145,
                },
                ColorDictEntry {
                    x: 0.3216,
                    y0: 0.0000,
                    y1: 0.0000,
                },
                ColorDictEntry {
                    x: 0.4157,
                    y0: 0.0000,
                    y1: 0.0000,
                },
                ColorDictEntry {
                    x: 0.4745,
                    y0: 0.2342,
                    y1: 0.2342,
                },
                ColorDictEntry {
                    x: 0.5333,
                    y0: 0.0000,
                    y1: 0.0000,
                },
                ColorDictEntry {
                    x: 0.5804,
                    y0: 0.0000,
                    y1: 0.0000,
                },
                ColorDictEntry {
                    x: 0.6314,
                    y0: 0.0549,
                    y1: 0.0549,
                },
                ColorDictEntry {
                    x: 0.6902,
                    y0: 0.0000,
                    y1: 0.0000,
                },
                ColorDictEntry {
                    x: 0.7373,
                    y0: 0.0000,
                    y1: 0.0000,
                },
                ColorDictEntry {
                    x: 0.7922,
                    y0: 0.9738,
                    y1: 0.9738,
                },
                ColorDictEntry {
                    x: 0.8000,
                    y0: 1.0000,
                    y1: 1.0000,
                },
                ColorDictEntry {
                    x: 0.8431,
                    y0: 1.0000,
                    y1: 1.0000,
                },
                ColorDictEntry {
                    x: 0.8980,
                    y0: 0.9341,
                    y1: 0.9341,
                },
                ColorDictEntry {
                    x: 1.0000,
                    y0: 0.9961,
                    y1: 0.9961,
                },
            ],
        }
    }

    pub fn gist_stern() -> ColorDict {
        ColorDict {
            red: vec![
                ColorDictEntry {
                    x: 0.000,
                    y0: 0.000,
                    y1: 0.000,
                },
                ColorDictEntry {
                    x: 0.0547,
                    y0: 1.000,
                    y1: 1.000,
                },
                ColorDictEntry {
                    x: 0.250,
                    y0: 0.027,
                    y1: 0.250,
                },
                ColorDictEntry {
                    x: 1.000,
                    y0: 1.000,
                    y1: 1.000,
                },
            ],
            green: vec![
                ColorDictEntry {
                    x: 0.0,
                    y0: 0.0,
                    y1: 0.0,
                },
                ColorDictEntry {
                    x: 1.0,
                    y0: 1.0,
                    y1: 1.0,
                },
            ],
            blue: vec![
                ColorDictEntry {
                    x: 0.000,
                    y0: 0.000,
                    y1: 0.000,
                },
                ColorDictEntry {
                    x: 0.500,
                    y0: 1.000,
                    y1: 1.000,
                },
                ColorDictEntry {
                    x: 0.735,
                    y0: 0.000,
                    y1: 0.000,
                },
                ColorDictEntry {
                    x: 1.000,
                    y0: 1.000,
                    y1: 1.000,
                },
            ],
        }
    }

    pub const BLUES: [Color; 9] = [
        Color::rgb(247, 251, 255),
        Color::rgb(222, 235, 247),
        Color::rgb(198, 219, 239),
        Color::rgb(158, 202, 225),
        Color::rgb(107, 174, 214),
        Color::rgb(66, 146, 198),
        Color::rgb(33, 113, 181),
        Color::rgb(8, 81, 156),
        Color::rgb(8, 48, 107),
    ];

    pub const BR_BG: [Color; 11] = [
        Color::rgb(84, 48, 5),
        Color::rgb(140, 81, 10),
        Color::rgb(191, 129, 45),
        Color::rgb(223, 194, 125),
        Color::rgb(246, 232, 195),
        Color::rgb(245, 245, 245),
        Color::rgb(199, 234, 229),
        Color::rgb(128, 205, 193),
        Color::rgb(53, 151, 143),
        Color::rgb(1, 102, 94),
        Color::rgb(0, 60, 48),
    ];

    pub const BU_GN: [Color; 9] = [
        Color::rgb(247, 252, 253),
        Color::rgb(229, 245, 249),
        Color::rgb(204, 236, 230),
        Color::rgb(153, 216, 201),
        Color::rgb(102, 194, 164),
        Color::rgb(65, 174, 118),
        Color::rgb(35, 139, 69),
        Color::rgb(0, 109, 44),
        Color::rgb(0, 68, 27),
    ];

    pub const BU_PU: [Color; 9] = [
        Color::rgb(247, 252, 253),
        Color::rgb(224, 236, 244),
        Color::rgb(191, 211, 230),
        Color::rgb(158, 188, 218),
        Color::rgb(140, 150, 198),
        Color::rgb(140, 107, 177),
        Color::rgb(136, 65, 157),
        Color::rgb(129, 15, 124),
        Color::rgb(77, 0, 75),
    ];

    pub const GN_BU: [Color; 9] = [
        Color::rgb(247, 252, 240),
        Color::rgb(224, 243, 219),
        Color::rgb(204, 235, 197),
        Color::rgb(168, 221, 181),
        Color::rgb(123, 204, 196),
        Color::rgb(78, 179, 211),
        Color::rgb(43, 140, 190),
        Color::rgb(8, 104, 172),
        Color::rgb(8, 64, 129),
    ];

    pub const GREENS: [Color; 9] = [
        Color::rgb(247, 252, 245),
        Color::rgb(229, 245, 224),
        Color::rgb(199, 233, 192),
        Color::rgb(161, 217, 155),
        Color::rgb(116, 196, 118),
        Color::rgb(65, 171, 93),
        Color::rgb(35, 139, 69),
        Color::rgb(0, 109, 44),
        Color::rgb(0, 68, 27),
    ];

    pub const GREYS: [Color; 9] = [
        Color::rgb(255, 255, 255),
        Color::rgb(240, 240, 240),
        Color::rgb(217, 217, 217),
        Color::rgb(189, 189, 189),
        Color::rgb(150, 150, 150),
        Color::rgb(115, 115, 115),
        Color::rgb(82, 82, 82),
        Color::rgb(37, 37, 37),
        Color::rgb(0, 0, 0),
    ];

    pub const ORANGES: [Color; 9] = [
        Color::rgb(255, 245, 235),
        Color::rgb(254, 230, 206),
        Color::rgb(253, 208, 162),
        Color::rgb(253, 174, 107),
        Color::rgb(253, 141, 60),
        Color::rgb(241, 105, 19),
        Color::rgb(217, 72, 1),
        Color::rgb(166, 54, 3),
        Color::rgb(127, 39, 4),
    ];

    pub const OR_RD: [Color; 9] = [
        Color::rgb(255, 247, 236),
        Color::rgb(254, 232, 200),
        Color::rgb(253, 212, 158),
        Color::rgb(253, 187, 132),
        Color::rgb(252, 141, 89),
        Color::rgb(239, 101, 72),
        Color::rgb(215, 48, 31),
        Color::rgb(179, 0, 0),
        Color::rgb(127, 0, 0),
    ];

    pub const PI_YG: [Color; 11] = [
        Color::rgb(142, 1, 82),
        Color::rgb(197, 27, 125),
        Color::rgb(222, 119, 174),
        Color::rgb(241, 182, 218),
        Color::rgb(253, 224, 239),
        Color::rgb(247, 247, 247),
        Color::rgb(230, 245, 208),
        Color::rgb(184, 225, 134),
        Color::rgb(127, 188, 65),
        Color::rgb(77, 146, 33),
        Color::rgb(39, 100, 25),
    ];

    pub const PR_GN: [Color; 11] = [
        Color::rgb(64, 0, 75),
        Color::rgb(118, 42, 131),
        Color::rgb(153, 112, 171),
        Color::rgb(194, 165, 207),
        Color::rgb(231, 212, 232),
        Color::rgb(247, 247, 247),
        Color::rgb(217, 240, 211),
        Color::rgb(166, 219, 160),
        Color::rgb(90, 174, 97),
        Color::rgb(27, 120, 55),
        Color::rgb(0, 68, 27),
    ];

    pub const PU_BU: [Color; 9] = [
        Color::rgb(255, 247, 251),
        Color::rgb(236, 231, 242),
        Color::rgb(208, 209, 230),
        Color::rgb(166, 189, 219),
        Color::rgb(116, 169, 207),
        Color::rgb(54, 144, 192),
        Color::rgb(5, 112, 176),
        Color::rgb(4, 90, 141),
        Color::rgb(2, 56, 88),
    ];

    pub const PU_BU_GN: [Color; 9] = [
        Color::rgb(255, 247, 251),
        Color::rgb(236, 226, 240),
        Color::rgb(208, 209, 230),
        Color::rgb(166, 189, 219),
        Color::rgb(103, 169, 207),
        Color::rgb(54, 144, 192),
        Color::rgb(2, 129, 138),
        Color::rgb(1, 108, 89),
        Color::rgb(1, 70, 54),
    ];

    pub const PU_OR: [Color; 11] = [
        Color::rgb(127, 59, 8),
        Color::rgb(179, 88, 6),
        Color::rgb(224, 130, 20),
        Color::rgb(253, 184, 99),
        Color::rgb(254, 224, 182),
        Color::rgb(247, 247, 247),
        Color::rgb(216, 218, 235),
        Color::rgb(178, 171, 210),
        Color::rgb(128, 115, 172),
        Color::rgb(84, 39, 136),
        Color::rgb(45, 0, 75),
    ];

    pub const PU_RD: [Color; 9] = [
        Color::rgb(247, 244, 249),
        Color::rgb(231, 225, 239),
        Color::rgb(212, 185, 218),
        Color::rgb(201, 148, 199),
        Color::rgb(223, 101, 176),
        Color::rgb(231, 41, 138),
        Color::rgb(206, 18, 86),
        Color::rgb(152, 0, 67),
        Color::rgb(103, 0, 31),
    ];

    pub const PURPLES: [Color; 9] = [
        Color::rgb(252, 251, 253),
        Color::rgb(239, 237, 245),
        Color::rgb(218, 218, 235),
        Color::rgb(188, 189, 220),
        Color::rgb(158, 154, 200),
        Color::rgb(128, 125, 186),
        Color::rgb(106, 81, 163),
        Color::rgb(84, 39, 143),
        Color::rgb(63, 0, 125),
    ];

    pub const RD_BU: [Color; 9] = [
        Color::rgb(252, 251, 253),
        Color::rgb(239, 237, 245),
        Color::rgb(218, 218, 235),
        Color::rgb(188, 189, 220),
        Color::rgb(158, 154, 200),
        Color::rgb(128, 125, 186),
        Color::rgb(106, 81, 163),
        Color::rgb(84, 39, 143),
        Color::rgb(63, 0, 125),
    ];

    pub const RD_GY: [Color; 11] = [
        Color::rgb(103, 0, 31),
        Color::rgb(178, 24, 43),
        Color::rgb(214, 96, 77),
        Color::rgb(244, 165, 130),
        Color::rgb(253, 219, 199),
        Color::rgb(255, 255, 255),
        Color::rgb(224, 224, 224),
        Color::rgb(186, 186, 186),
        Color::rgb(135, 135, 135),
        Color::rgb(77, 77, 77),
        Color::rgb(26, 26, 26),
    ];

    pub const RD_PU: [Color; 9] = [
        Color::rgb(255, 247, 243),
        Color::rgb(253, 224, 221),
        Color::rgb(252, 197, 192),
        Color::rgb(250, 159, 181),
        Color::rgb(247, 104, 161),
        Color::rgb(221, 52, 151),
        Color::rgb(174, 1, 126),
        Color::rgb(122, 1, 119),
        Color::rgb(73, 0, 106),
    ];

    pub const RD_YL_BU: [Color; 11] = [
        Color::rgb(165, 0, 38),
        Color::rgb(215, 48, 39),
        Color::rgb(244, 109, 67),
        Color::rgb(253, 174, 97),
        Color::rgb(254, 224, 144),
        Color::rgb(255, 255, 191),
        Color::rgb(224, 243, 248),
        Color::rgb(171, 217, 233),
        Color::rgb(116, 173, 209),
        Color::rgb(69, 117, 180),
        Color::rgb(49, 54, 149),
    ];

    pub const RD_YL_GN: [Color; 11] = [
        Color::rgb(165, 0, 38),
        Color::rgb(215, 48, 39),
        Color::rgb(244, 109, 67),
        Color::rgb(253, 174, 97),
        Color::rgb(254, 224, 139),
        Color::rgb(255, 255, 191),
        Color::rgb(217, 239, 139),
        Color::rgb(166, 217, 106),
        Color::rgb(102, 189, 99),
        Color::rgb(26, 152, 80),
        Color::rgb(0, 104, 55),
    ];

    pub const REDS: [Color; 9] = [
        Color::rgb(255, 245, 240),
        Color::rgb(254, 224, 210),
        Color::rgb(252, 187, 161),
        Color::rgb(252, 146, 114),
        Color::rgb(251, 106, 74),
        Color::rgb(239, 59, 44),
        Color::rgb(203, 24, 29),
        Color::rgb(165, 15, 21),
        Color::rgb(103, 0, 13),
    ];

    pub const SPECTRAL: [Color; 11] = [
        Color::rgb(158, 1, 66),
        Color::rgb(213, 62, 79),
        Color::rgb(244, 109, 67),
        Color::rgb(253, 174, 97),
        Color::rgb(254, 224, 139),
        Color::rgb(255, 255, 191),
        Color::rgb(230, 245, 152),
        Color::rgb(171, 221, 164),
        Color::rgb(102, 194, 165),
        Color::rgb(50, 136, 189),
        Color::rgb(94, 79, 162),
    ];

    pub const YL_GN: [Color; 9] = [
        Color::rgb(255, 255, 229),
        Color::rgb(247, 252, 185),
        Color::rgb(217, 240, 163),
        Color::rgb(173, 221, 142),
        Color::rgb(120, 198, 121),
        Color::rgb(65, 171, 93),
        Color::rgb(35, 132, 67),
        Color::rgb(0, 104, 55),
        Color::rgb(0, 69, 41),
    ];

    pub const YL_GN_BU: [Color; 9] = [
        Color::rgb(255, 255, 217),
        Color::rgb(237, 248, 177),
        Color::rgb(199, 233, 180),
        Color::rgb(127, 205, 187),
        Color::rgb(65, 182, 196),
        Color::rgb(29, 145, 192),
        Color::rgb(34, 94, 168),
        Color::rgb(37, 52, 148),
        Color::rgb(8, 29, 88),
    ];

    pub const YL_OR_BR: [Color; 9] = [
        Color::rgb(255, 255, 229),
        Color::rgb(255, 247, 188),
        Color::rgb(254, 227, 145),
        Color::rgb(254, 196, 79),
        Color::rgb(254, 153, 41),
        Color::rgb(236, 112, 20),
        Color::rgb(204, 76, 2),
        Color::rgb(153, 52, 4),
        Color::rgb(102, 37, 6),
    ];

    pub const YL_OR_RD: [Color; 9] = [
        Color::rgb(255, 255, 204),
        Color::rgb(255, 237, 160),
        Color::rgb(254, 217, 118),
        Color::rgb(254, 178, 76),
        Color::rgb(253, 141, 60),
        Color::rgb(252, 78, 42),
        Color::rgb(227, 26, 28),
        Color::rgb(189, 0, 38),
        Color::rgb(128, 0, 38),
    ];

    // Qualitative maps
    pub const ACCENT: [Color; 8] = [
        Color::rgb(127, 201, 127),
        Color::rgb(190, 174, 212),
        Color::rgb(253, 192, 134),
        Color::rgb(255, 255, 153),
        Color::rgb(56, 108, 176),
        Color::rgb(240, 2, 127),
        Color::rgb(191, 91, 23),
        Color::rgb(102, 102, 102),
    ];

    pub const DARK2: [Color; 8] = [
        Color::rgb(27, 158, 119),
        Color::rgb(217, 95, 2),
        Color::rgb(117, 112, 179),
        Color::rgb(231, 41, 138),
        Color::rgb(102, 166, 30),
        Color::rgb(230, 171, 2),
        Color::rgb(166, 118, 29),
        Color::rgb(102, 102, 102),
    ];

    pub const PAIRED: [Color; 12] = [
        Color::rgb(166, 206, 227),
        Color::rgb(31, 120, 180),
        Color::rgb(178, 223, 138),
        Color::rgb(51, 160, 44),
        Color::rgb(251, 154, 153),
        Color::rgb(227, 26, 28),
        Color::rgb(253, 191, 111),
        Color::rgb(255, 127, 0),
        Color::rgb(202, 178, 214),
        Color::rgb(106, 61, 154),
        Color::rgb(255, 255, 153),
        Color::rgb(177, 89, 40),
    ];

    pub const PASTEL1: [Color; 9] = [
        Color::rgb(251, 180, 174),
        Color::rgb(179, 205, 227),
        Color::rgb(204, 235, 197),
        Color::rgb(222, 203, 228),
        Color::rgb(254, 217, 166),
        Color::rgb(255, 255, 204),
        Color::rgb(229, 216, 189),
        Color::rgb(253, 218, 236),
        Color::rgb(242, 242, 242),
    ];

    pub const PASTEL2: [Color; 8] = [
        Color::rgb(179, 226, 205),
        Color::rgb(253, 205, 172),
        Color::rgb(203, 213, 232),
        Color::rgb(244, 202, 228),
        Color::rgb(230, 245, 201),
        Color::rgb(255, 242, 174),
        Color::rgb(241, 226, 204),
        Color::rgb(204, 204, 204),
    ];

    pub const SET1: [Color; 9] = [
        Color::rgb(228, 26, 28),
        Color::rgb(55, 126, 184),
        Color::rgb(77, 175, 74),
        Color::rgb(152, 78, 163),
        Color::rgb(255, 127, 0),
        Color::rgb(255, 255, 51),
        Color::rgb(166, 86, 40),
        Color::rgb(247, 129, 191),
        Color::rgb(153, 153, 153),
    ];

    pub const SET2: [Color; 8] = [
        Color::rgb(102, 194, 165),
        Color::rgb(252, 141, 98),
        Color::rgb(141, 160, 203),
        Color::rgb(231, 138, 195),
        Color::rgb(166, 216, 84),
        Color::rgb(255, 217, 47),
        Color::rgb(229, 196, 148),
        Color::rgb(179, 179, 179),
    ];

    pub const SET3: [Color; 12] = [
        Color::rgb(141, 211, 199),
        Color::rgb(255, 255, 179),
        Color::rgb(190, 186, 218),
        Color::rgb(251, 128, 114),
        Color::rgb(128, 177, 211),
        Color::rgb(253, 180, 98),
        Color::rgb(179, 222, 105),
        Color::rgb(252, 205, 229),
        Color::rgb(217, 217, 217),
        Color::rgb(188, 128, 189),
        Color::rgb(204, 235, 197),
        Color::rgb(255, 237, 111),
    ];

    // Qualitative maps from https://github.com/vega/vega/wiki/Scales
    pub const TAB10: [Color; 10] = [
        Color::rgb(31, 119, 180),  // #1f77b4
        Color::rgb(255, 127, 14),  // #ff7f0e
        Color::rgb(44, 160, 44),   // #2ca02c
        Color::rgb(214, 39, 40),   // #d62728
        Color::rgb(148, 103, 189), // #9467bd
        Color::rgb(140, 86, 75),   // #8c564b
        Color::rgb(227, 119, 194), // #e377c2
        Color::rgb(127, 127, 127), // #7f7f7f
        Color::rgb(188, 189, 34),  // #bcbd22
        Color::rgb(23, 190, 207),  // #17becf
    ];

    pub const TAB20: [Color; 20] = [
        Color::rgb(31, 119, 180),  // #1f77b4
        Color::rgb(174, 199, 232), // #aec7e8
        Color::rgb(255, 127, 14),  // #ff7f0e
        Color::rgb(255, 187, 120), // #ffbb78
        Color::rgb(44, 160, 44),   // #2ca02c
        Color::rgb(152, 223, 138), // #98df8a
        Color::rgb(214, 39, 40),   // #d62728
        Color::rgb(255, 152, 150), // #ff9896
        Color::rgb(148, 103, 189), // #9467bd
        Color::rgb(197, 176, 213), // #c5b0d5
        Color::rgb(140, 86, 75),   // #8c564b
        Color::rgb(196, 156, 148), // #c49c94
        Color::rgb(227, 119, 194), // #e377c2
        Color::rgb(247, 182, 210), // #f7b6d2
        Color::rgb(127, 127, 127), // #7f7f7f
        Color::rgb(199, 199, 199), // #c7c7c7
        Color::rgb(188, 189, 34),  // #bcbd22
        Color::rgb(219, 219, 141), // #dbdb8d
        Color::rgb(23, 190, 207),  // #17becf
        Color::rgb(158, 218, 229), // #9edae5
    ];

    pub const TAB20B: [Color; 20] = [
        Color::rgb(57, 59, 121),   // #393b79
        Color::rgb(82, 84, 163),   // #5254a3
        Color::rgb(107, 110, 207), // #6b6ecf
        Color::rgb(156, 158, 222), // #9c9ede
        Color::rgb(99, 121, 57),   // #637939
        Color::rgb(140, 162, 82),  // #8ca252
        Color::rgb(181, 207, 107), // #b5cf6b
        Color::rgb(206, 219, 156), // #cedb9c
        Color::rgb(140, 109, 49),  // #8c6d31
        Color::rgb(189, 158, 57),  // #bd9e39
        Color::rgb(231, 186, 82),  // #e7ba52
        Color::rgb(231, 203, 148), // #e7cb94
        Color::rgb(132, 60, 57),   // #843c39
        Color::rgb(173, 73, 74),   // #ad494a
        Color::rgb(214, 97, 107),  // #d6616b
        Color::rgb(231, 150, 156), // #e7969c
        Color::rgb(123, 65, 115),  // #7b4173
        Color::rgb(165, 81, 148),  // #a55194
        Color::rgb(206, 109, 189), // #ce6dbd
        Color::rgb(222, 158, 214), // #de9ed6
    ];

    pub const TAB20C: [Color; 20] = [
        Color::rgb(49, 130, 189),  // #3182bd
        Color::rgb(107, 174, 214), // #6baed6
        Color::rgb(158, 202, 225), // #9ecae1
        Color::rgb(198, 219, 239), // #c6dbef
        Color::rgb(230, 85, 13),   // #e6550d
        Color::rgb(253, 141, 60),  // #fd8d3c
        Color::rgb(253, 174, 107), // #fdae6b
        Color::rgb(253, 208, 162), // #fdd0a2
        Color::rgb(49, 163, 84),   // #31a354
        Color::rgb(116, 196, 118), // #74c476
        Color::rgb(161, 217, 155), // #a1d99b
        Color::rgb(199, 233, 192), // #c7e9c0
        Color::rgb(117, 107, 177), // #756bb1
        Color::rgb(158, 154, 200), // #9e9ac8
        Color::rgb(188, 189, 220), // #bcbddc
        Color::rgb(218, 218, 235), // #dadaeb
        Color::rgb(99, 99, 99),    // #636363
        Color::rgb(150, 150, 150), // #969696
        Color::rgb(189, 189, 189), // #bdbdbd
        Color::rgb(217, 217, 217), // #d9d9d9
    ];

    pub const TURBO: [Color; 256] = [
        Color::rgb(48, 18, 59),
        Color::rgb(50, 21, 67),
        Color::rgb(51, 24, 74),
        Color::rgb(52, 27, 81),
        Color::rgb(53, 30, 88),
        Color::rgb(54, 33, 95),
        Color::rgb(55, 36, 102),
        Color::rgb(56, 39, 109),
        Color::rgb(57, 42, 115),
        Color::rgb(58, 45, 121),
        Color::rgb(59, 47, 128),
        Color::rgb(60, 50, 134),
        Color::rgb(61, 53, 139),
        Color::rgb(62, 56, 145),
        Color::rgb(63, 59, 151),
        Color::rgb(63, 62, 156),
        Color::rgb(64, 64, 162),
        Color::rgb(65, 67, 167),
        Color::rgb(65, 70, 172),
        Color::rgb(66, 73, 177),
        Color::rgb(66, 75, 181),
        Color::rgb(67, 78, 186),
        Color::rgb(68, 81, 191),
        Color::rgb(68, 84, 195),
        Color::rgb(68, 86, 199),
        Color::rgb(69, 89, 203),
        Color::rgb(69, 92, 207),
        Color::rgb(69, 94, 211),
        Color::rgb(70, 97, 214),
        Color::rgb(70, 100, 218),
        Color::rgb(70, 102, 221),
        Color::rgb(70, 105, 224),
        Color::rgb(70, 107, 227),
        Color::rgb(71, 110, 230),
        Color::rgb(71, 113, 233),
        Color::rgb(71, 115, 235),
        Color::rgb(71, 118, 238),
        Color::rgb(71, 120, 240),
        Color::rgb(71, 123, 242),
        Color::rgb(70, 125, 244),
        Color::rgb(70, 128, 246),
        Color::rgb(70, 130, 248),
        Color::rgb(70, 133, 250),
        Color::rgb(70, 135, 251),
        Color::rgb(69, 138, 252),
        Color::rgb(69, 140, 253),
        Color::rgb(68, 143, 254),
        Color::rgb(67, 145, 254),
        Color::rgb(66, 148, 255),
        Color::rgb(65, 150, 255),
        Color::rgb(64, 153, 255),
        Color::rgb(62, 155, 254),
        Color::rgb(61, 158, 254),
        Color::rgb(59, 160, 253),
        Color::rgb(58, 163, 252),
        Color::rgb(56, 165, 251),
        Color::rgb(55, 168, 250),
        Color::rgb(53, 171, 248),
        Color::rgb(51, 173, 247),
        Color::rgb(49, 175, 245),
        Color::rgb(47, 178, 244),
        Color::rgb(46, 180, 242),
        Color::rgb(44, 183, 240),
        Color::rgb(42, 185, 238),
        Color::rgb(40, 188, 235),
        Color::rgb(39, 190, 233),
        Color::rgb(37, 192, 231),
        Color::rgb(35, 195, 228),
        Color::rgb(34, 197, 226),
        Color::rgb(32, 199, 223),
        Color::rgb(31, 201, 221),
        Color::rgb(30, 203, 218),
        Color::rgb(28, 205, 216),
        Color::rgb(27, 208, 213),
        Color::rgb(26, 210, 210),
        Color::rgb(26, 212, 208),
        Color::rgb(25, 213, 205),
        Color::rgb(24, 215, 202),
        Color::rgb(24, 217, 200),
        Color::rgb(24, 219, 197),
        Color::rgb(24, 221, 194),
        Color::rgb(24, 222, 192),
        Color::rgb(24, 224, 189),
        Color::rgb(25, 226, 187),
        Color::rgb(25, 227, 185),
        Color::rgb(26, 228, 182),
        Color::rgb(28, 230, 180),
        Color::rgb(29, 231, 178),
        Color::rgb(31, 233, 175),
        Color::rgb(32, 234, 172),
        Color::rgb(34, 235, 170),
        Color::rgb(37, 236, 167),
        Color::rgb(39, 238, 164),
        Color::rgb(42, 239, 161),
        Color::rgb(44, 240, 158),
        Color::rgb(47, 241, 155),
        Color::rgb(50, 242, 152),
        Color::rgb(53, 243, 148),
        Color::rgb(56, 244, 145),
        Color::rgb(60, 245, 142),
        Color::rgb(63, 246, 138),
        Color::rgb(67, 247, 135),
        Color::rgb(70, 248, 132),
        Color::rgb(74, 248, 128),
        Color::rgb(78, 249, 125),
        Color::rgb(82, 250, 122),
        Color::rgb(85, 250, 118),
        Color::rgb(89, 251, 115),
        Color::rgb(93, 252, 111),
        Color::rgb(97, 252, 108),
        Color::rgb(101, 253, 105),
        Color::rgb(105, 253, 102),
        Color::rgb(109, 254, 98),
        Color::rgb(113, 254, 95),
        Color::rgb(117, 254, 92),
        Color::rgb(121, 254, 89),
        Color::rgb(125, 255, 86),
        Color::rgb(128, 255, 83),
        Color::rgb(132, 255, 81),
        Color::rgb(136, 255, 78),
        Color::rgb(139, 255, 75),
        Color::rgb(143, 255, 73),
        Color::rgb(146, 255, 71),
        Color::rgb(150, 254, 68),
        Color::rgb(153, 254, 66),
        Color::rgb(156, 254, 64),
        Color::rgb(159, 253, 63),
        Color::rgb(161, 253, 61),
        Color::rgb(164, 252, 60),
        Color::rgb(167, 252, 58),
        Color::rgb(169, 251, 57),
        Color::rgb(172, 251, 56),
        Color::rgb(175, 250, 55),
        Color::rgb(177, 249, 54),
        Color::rgb(180, 248, 54),
        Color::rgb(183, 247, 53),
        Color::rgb(185, 246, 53),
        Color::rgb(188, 245, 52),
        Color::rgb(190, 244, 52),
        Color::rgb(193, 243, 52),
        Color::rgb(195, 241, 52),
        Color::rgb(198, 240, 52),
        Color::rgb(200, 239, 52),
        Color::rgb(203, 237, 52),
        Color::rgb(205, 236, 52),
        Color::rgb(208, 234, 52),
        Color::rgb(210, 233, 53),
        Color::rgb(212, 231, 53),
        Color::rgb(215, 229, 53),
        Color::rgb(217, 228, 54),
        Color::rgb(219, 226, 54),
        Color::rgb(221, 224, 55),
        Color::rgb(223, 223, 55),
        Color::rgb(225, 221, 55),
        Color::rgb(227, 219, 56),
        Color::rgb(229, 217, 56),
        Color::rgb(231, 215, 57),
        Color::rgb(233, 213, 57),
        Color::rgb(235, 211, 57),
        Color::rgb(236, 209, 58),
        Color::rgb(238, 207, 58),
        Color::rgb(239, 205, 58),
        Color::rgb(241, 203, 58),
        Color::rgb(242, 201, 58),
        Color::rgb(244, 199, 58),
        Color::rgb(245, 197, 58),
        Color::rgb(246, 195, 58),
        Color::rgb(247, 193, 58),
        Color::rgb(248, 190, 57),
        Color::rgb(249, 188, 57),
        Color::rgb(250, 186, 57),
        Color::rgb(251, 184, 56),
        Color::rgb(251, 182, 55),
        Color::rgb(252, 179, 54),
        Color::rgb(252, 177, 54),
        Color::rgb(253, 174, 53),
        Color::rgb(253, 172, 52),
        Color::rgb(254, 169, 51),
        Color::rgb(254, 167, 50),
        Color::rgb(254, 164, 49),
        Color::rgb(254, 161, 48),
        Color::rgb(254, 158, 47),
        Color::rgb(254, 155, 45),
        Color::rgb(254, 153, 44),
        Color::rgb(254, 150, 43),
        Color::rgb(254, 147, 42),
        Color::rgb(254, 144, 41),
        Color::rgb(253, 141, 39),
        Color::rgb(253, 138, 38),
        Color::rgb(252, 135, 37),
        Color::rgb(252, 132, 35),
        Color::rgb(251, 129, 34),
        Color::rgb(251, 126, 33),
        Color::rgb(250, 123, 31),
        Color::rgb(249, 120, 30),
        Color::rgb(249, 117, 29),
        Color::rgb(248, 114, 28),
        Color::rgb(247, 111, 26),
        Color::rgb(246, 108, 25),
        Color::rgb(245, 105, 24),
        Color::rgb(244, 102, 23),
        Color::rgb(243, 99, 21),
        Color::rgb(242, 96, 20),
        Color::rgb(241, 93, 19),
        Color::rgb(240, 91, 18),
        Color::rgb(239, 88, 17),
        Color::rgb(237, 85, 16),
        Color::rgb(236, 83, 15),
        Color::rgb(235, 80, 14),
        Color::rgb(234, 78, 13),
        Color::rgb(232, 75, 12),
        Color::rgb(231, 73, 12),
        Color::rgb(229, 71, 11),
        Color::rgb(228, 69, 10),
        Color::rgb(226, 67, 10),
        Color::rgb(225, 65, 9),
        Color::rgb(223, 63, 8),
        Color::rgb(221, 61, 8),
        Color::rgb(220, 59, 7),
        Color::rgb(218, 57, 7),
        Color::rgb(216, 55, 6),
        Color::rgb(214, 53, 6),
        Color::rgb(212, 51, 5),
        Color::rgb(210, 49, 5),
        Color::rgb(208, 47, 5),
        Color::rgb(206, 45, 4),
        Color::rgb(204, 43, 4),
        Color::rgb(202, 42, 4),
        Color::rgb(200, 40, 3),
        Color::rgb(197, 38, 3),
        Color::rgb(195, 37, 3),
        Color::rgb(193, 35, 2),
        Color::rgb(190, 33, 2),
        Color::rgb(188, 32, 2),
        Color::rgb(185, 30, 2),
        Color::rgb(183, 29, 2),
        Color::rgb(180, 27, 1),
        Color::rgb(178, 26, 1),
        Color::rgb(175, 24, 1),
        Color::rgb(172, 23, 1),
        Color::rgb(169, 22, 1),
        Color::rgb(167, 20, 1),
        Color::rgb(164, 19, 1),
        Color::rgb(161, 18, 1),
        Color::rgb(158, 16, 1),
        Color::rgb(155, 15, 1),
        Color::rgb(152, 14, 1),
        Color::rgb(149, 13, 1),
        Color::rgb(146, 11, 1),
        Color::rgb(142, 10, 1),
        Color::rgb(139, 9, 2),
        Color::rgb(136, 8, 2),
        Color::rgb(133, 7, 2),
        Color::rgb(129, 6, 2),
        Color::rgb(126, 5, 2),
        Color::rgb(122, 4, 3),
    ];

    pub const TERRAIN: [ColorInfo; 6] = [
        ColorInfo::new(0.00, Color::rgb(51, 51, 154)),
        ColorInfo::new(0.15, Color::rgb(0, 154, 255)),
        ColorInfo::new(0.25, Color::rgb(0, 204, 102)),
        ColorInfo::new(0.50, Color::rgb(255, 255, 93)),
        ColorInfo::new(0.75, Color::rgb(128, 92, 84)),
        ColorInfo::new(1.00, Color::rgb(255, 255, 255)),
    ];

    const MAPPER33: fn(f64) -> u8 = |x: f64| {
        let val = ((2.0 * x - 0.5).abs() * 255.0).round() as i32;
        val.clamp(0, 255) as u8
    };

    const MAPPER13: fn(f64) -> u8 = |x| {
        let val = ((x * std::f64::consts::PI).sin() * 255.0).round() as i32;
        val.clamp(0, 255) as u8
    };

    const MAPPER10: fn(f64) -> u8 = |x| {
        let val = ((x * std::f64::consts::PI / 2.0).cos() * 255.0).round() as i32;
        val.clamp(0, 255) as u8
    };

    pub const RAINBOW: ColorMapper = ColorMapper {
        red: MAPPER33,
        green: MAPPER13,
        blue: MAPPER10,
    };
}

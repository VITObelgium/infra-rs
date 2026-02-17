#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RasterScale {
    pub scale: f64,
    pub offset: f64,
}

impl Default for RasterScale {
    fn default() -> Self {
        Self { scale: 1.0, offset: 0.0 }
    }
}

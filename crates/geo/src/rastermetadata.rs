use inf::cast;

use crate::{ArrayDataType, ArrayMetadata, GeoReference, RasterScale, RasterSize};

/// Simple raster metadata structure that contains the size of the raster and the optional Nodata value.
/// Usefull for cases where complete georeferencing is not needed
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct RasterMetadata {
    pub raster_size: RasterSize,
    pub nodata: Option<f64>,
    pub scale: Option<RasterScale>,
}

impl std::fmt::Display for RasterMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}, nodata: {:?}", self.raster_size, self.nodata)
    }
}

impl ArrayMetadata for RasterMetadata {
    fn size(&self) -> RasterSize {
        self.raster_size
    }

    fn nodata(&self) -> Option<f64> {
        self.nodata
    }

    fn sized(raster_size: RasterSize, dtype: ArrayDataType) -> Self {
        Self {
            raster_size,
            nodata: Some(dtype.default_nodata_value()),
            scale: None,
        }
    }

    fn sized_with_nodata(raster_size: RasterSize, nodata: Option<f64>) -> Self {
        Self {
            raster_size,
            nodata: cast::option(nodata),
            scale: None,
        }
    }

    fn with_geo_reference(georef: GeoReference) -> Self {
        Self {
            raster_size: georef.size(),
            nodata: georef.nodata(),
            scale: georef.scale(),
        }
    }

    fn with_scale(self, scale: RasterScale) -> Self {
        Self {
            scale: Some(scale),
            ..self
        }
    }

    fn geo_reference(&self) -> GeoReference {
        let mut georef = GeoReference::without_spatial_reference(self.raster_size, self.nodata);
        georef.set_square_cell_size_north_up(1.0); // Otherwise tools will not render anything
        georef.set_scale(self.scale);
        georef
    }
}

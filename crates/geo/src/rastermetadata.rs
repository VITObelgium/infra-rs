use inf::cast;

use crate::{ArrayDataType, ArrayMetadata, GeoReference, RasterSize};

/// Simple raster metadata structure that contains the size of the raster and the optional Nodata value.
/// Usefull for cases where complete georeferencing is not needed
#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub struct RasterMetadata {
    pub raster_size: RasterSize,
    pub nodata: Option<f64>,
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
        }
    }

    fn sized_with_nodata(raster_size: RasterSize, nodata: Option<f64>) -> Self {
        Self {
            raster_size,
            nodata: cast::option(nodata),
        }
    }

    fn with_geo_reference(georef: GeoReference) -> Self {
        Self {
            raster_size: georef.size(),
            nodata: georef.nodata(),
        }
    }

    fn geo_reference(&self) -> GeoReference {
        GeoReference::without_spatial_reference(self.raster_size, self.nodata)
    }
}

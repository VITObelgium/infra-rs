use arrow::{
    array::{Array, ArrayData, PrimitiveArray},
    datatypes::ArrowPrimitiveType,
    pyarrow::PyArrowType,
};

use pyo3::{pyclass, pymethods};

use crate::{
    raster::{ArrowRaster, ArrowRasterNum, Raster},
    GeoMetadata, RasterSize,
};

#[derive(Clone)]
#[pyclass(name = "RasterMetadata")]
pub struct PyRasterMetadata {
    // The raw projection string
    pub projection: String,
    // The EPSG code of the projection
    pub epsg: Option<u32>,
    /// The size of the image in pixels (width, height)
    pub size: (usize, usize),
    /// The cell size of the image (xsize, ysize)
    pub cell_size: (f64, f64),
    /// The affine transformation.
    pub geo_transform: [f64; 6],
    /// The nodata value.
    pub nodata: Option<f64>,
}

impl From<&GeoMetadata> for PyRasterMetadata {
    fn from(meta: &GeoMetadata) -> Self {
        PyRasterMetadata {
            projection: meta.projection().to_string(),
            epsg: meta.projected_epsg().map(|crs| crs.into()),
            size: (meta.columns(), meta.rows()),
            cell_size: (meta.cell_size().x(), meta.cell_size().y()),
            geo_transform: meta.geo_transform(),
            nodata: meta.nodata(),
        }
    }
}

impl From<&PyRasterMetadata> for GeoMetadata {
    fn from(val: &PyRasterMetadata) -> Self {
        GeoMetadata::new(
            val.projection.clone(),
            RasterSize {
                rows: val.size.1,
                cols: val.size.0,
            },
            val.geo_transform,
            val.nodata,
        )
    }
}

#[pymethods]
impl PyRasterMetadata {
    fn __repr__(&self) -> String {
        let mut str = format!(
            "Meta ({}x{}) cell size [x {} y {}]",
            self.size.0, self.size.1, self.cell_size.0, self.cell_size.1
        );
        if self.epsg.is_some() {
            str += &format!(" EPSG: {}\n", self.epsg.unwrap_or_default());
        }
        str
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

#[pyclass(name = "Raster")]
pub struct PyRaster {
    pub meta: PyRasterMetadata,
    pub data: ArrayData,
}

impl PyRaster {
    pub fn new<T: ArrowRasterNum<T>>(arrow_raster: ArrowRaster<T>) -> Self
    where
        T::TArrow: ArrowPrimitiveType<Native = T>,
    {
        let arr = arrow_raster.arrow_array();
        let array: &PrimitiveArray<T::TArrow> = arr.as_any().downcast_ref().expect("Failed to downcast arrow array");

        PyRaster {
            meta: arrow_raster.geo_metadata().into(),
            data: array.into_data(),
        }
    }
}

#[pymethods]
impl PyRaster {
    #[getter]
    fn meta_data(&self) -> PyRasterMetadata {
        self.meta.clone()
    }

    #[getter]
    fn arrow_data(&self) -> PyArrowType<ArrayData> {
        let data = self.data.clone();
        PyArrowType(data)
    }

    fn __repr__(&self) -> String {
        format!(
            "Raster ({}x{}) ({})",
            self.meta.size.0,
            self.meta.size.1,
            self.data.data_type()
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

#[cfg(all(feature = "python", feature = "arrow"))]
use {super::DenseRaster, crate::GeoReference, crate::Result, num::NumCast};

/// Try to convert a python arrow object to a `DenseRaster`.
/// This will only convert the raster data, so no projection information will be available
#[cfg(all(feature = "python", feature = "arrow"))]
impl<T> TryFrom<pyo3::Py<pyo3::PyAny>> for DenseRaster<T>
where
    T: crate::ArrayNum + super::arrow::arrowutil::ArrowType + Send + Sync + arrow::datatypes::ArrowNativeType,
    T::TArrow: arrow::array::ArrowPrimitiveType<Native = T>,
{
    type Error = crate::Error;

    fn try_from(py_obj: pyo3::Py<pyo3::PyAny>) -> std::result::Result<Self, Self::Error> {
        use crate::Array;
        use crate::RasterSize;
        use crate::array::{Columns, Rows};
        use arrow::array::Array as _;
        use arrow::pyarrow::FromPyArrow;

        pyo3::Python::with_gil(|py| -> Result<Self> {
            let py_obj = py_obj.into_bound(py);
            match arrow::array::ArrayData::from_pyarrow_bound(&py_obj) {
                Ok(array) => {
                    if array.data_type() != &T::arrow_data_type() {
                        return Err(crate::Error::InvalidArgument(format!(
                            "Python arrow array type mismatch: Arrow: {} Requested: {}",
                            array.data_type(),
                            T::arrow_data_type()
                        )));
                    }

                    let arrow_array: arrow::array::PrimitiveArray<T::TArrow> = array.into();
                    let geo_reference = GeoReference::without_spatial_reference(
                        RasterSize::with_rows_cols(Rows(1), Columns(arrow_array.len() as i32)),
                        Some(NumCast::from(T::NODATA).expect("Failed to cast nodata value")),
                    );

                    if arrow_array.is_nullable() {
                        let data = arrow_array.iter().map(|v| v.unwrap_or(T::NODATA)).collect();
                        DenseRaster::new(geo_reference, data)
                    } else {
                        DenseRaster::new(geo_reference, arrow_array.values().to_vec())
                    }
                }
                Err(e) => Err(crate::Error::InvalidArgument(format!(
                    "Python object is not a valid arrow array {}",
                    e
                ))),
            }
        })
    }
}

#[cfg(all(feature = "python", feature = "arrow"))]
#[cfg(test)]
mod tests {
    use crate::{
        Array, Nodata,
        array::{Columns, Rows},
    };
    use arrow::{array::Array as _, pyarrow::PyArrowType};
    use pyo3::{IntoPyObject, PyObject};

    use crate::raster::DenseRaster;

    #[ctor::ctor]
    fn init() {
        pyo3::Python::with_gil(|py| {
            if py.import("pyarrow").is_err() {
                panic!("PyArrow is not installed in the current python environment. Run 'pip install pyarrow' to install it.");
            }
        });
    }

    #[test]
    fn test_pyarrow_to_denseraster() {
        let array = arrow::array::Int32Array::from(vec![1, 10, 3, 20]);
        let py_array = pyo3::Python::with_gil(|py| -> PyObject { PyArrowType(array.into_data()).into_pyobject(py).unwrap().into() });

        let raster = DenseRaster::<i32>::try_from(py_array).expect("Arrow array conversion failed");

        // no shape information is present in the arrow array, so the raster will have a single row
        assert_eq!(raster.columns(), Columns(4));
        assert_eq!(raster.rows(), Rows(1));
        assert_eq!(raster.metadata().nodata_as::<i32>().unwrap(), Some(i32::NODATA));
    }

    #[test]
    fn test_pyarrow_with_mask_to_denseraster() {
        let array = arrow::array::Int32Array::from(vec![Some(1), Some(10), None, Some(20)]);
        let py_array = pyo3::Python::with_gil(|py| -> PyObject { PyArrowType(array.into_data()).into_pyobject(py).unwrap().into() });

        let raster = DenseRaster::<i32>::try_from(py_array).expect("Arrow array conversion failed");

        // no shape information is present in the arrow array, so the raster will have a single row
        assert_eq!(raster.columns(), Columns(4));
        assert_eq!(raster.rows(), Rows(1));
        assert_eq!(raster.metadata().nodata_as::<i32>().unwrap(), Some(i32::NODATA));

        assert_eq!(1, raster.nodata_count());
        assert!(raster.index_has_data(0));
        assert!(raster.index_has_data(1));
        assert!(!raster.index_has_data(2));
        assert!(raster.index_has_data(3));

        assert_eq!(raster.value(0), Some(1));
        assert_eq!(raster.value(1), Some(10));
        assert_eq!(raster.value(2), None);
        assert_eq!(raster.value(3), Some(20));
    }
}

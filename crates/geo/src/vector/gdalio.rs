/// Low level functions to work with gdal vector datasets
use std::{ffi::CString, path::Path};

use crate::{Error, LatLonBounds, Result, crs::epsg, gdalinterop};

use super::*;

use ::gdal::{
    spatial_ref::{AxisMappingStrategy, CoordTransform, SpatialRef},
    vector::{Defn, Envelope, Feature, FieldValue, Layer, LayerAccess, OwnedLayer},
};
use gdal_sys::OGRFieldType;

pub mod dataset {
    use std::path::{Path, PathBuf};

    use super::*;

    use ::gdal::{Dataset, DatasetOptions, DriverManager, GdalOpenFlags, errors::GdalError};

    /// Create a new in-memory vector dataset
    /// Useful for working with vector data in memory before actually writing it to disk
    pub fn create_in_memory() -> Result<Dataset> {
        let mem_driver = ::gdal::DriverManager::get_driver_by_name(VectorFileFormat::Memory.gdal_driver_name())?;
        Ok(mem_driver.create_vector_only("in-mem")?)
    }

    fn open_with_options(path: &Path, options: DatasetOptions) -> Result<Dataset> {
        Dataset::open_ex(path, options).map_err(|err| match err {
            // Match on the error to give a cleaner error message when the file does not exist
            GdalError::NullPointer { method_name: _, msg: _ } => {
                let vec_type = VectorFileFormat::guess_from_path(path);
                if vec_type != VectorFileFormat::Unknown && DriverManager::get_driver_by_name(vec_type.gdal_driver_name()).is_err() {
                    return Error::Runtime(format!("Gdal driver not supported: {}", vec_type.gdal_driver_name()));
                }

                Error::InvalidPath(PathBuf::from(path))
            }
            _ => Error::Runtime(format!("Failed to open raster dataset: {} ({})", path.to_string_lossy(), err)),
        })
    }

    /// Open a GDAL vector dataset for reading
    pub fn open_read_only(path: &Path) -> Result<Dataset> {
        let options = DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_READONLY | GdalOpenFlags::GDAL_OF_VECTOR,
            ..Default::default()
        };

        open_with_options(path, options)
    }

    /// Open a GDAL vector dataset for reading with driver open options
    pub fn open_read_only_with_options(path: &Path, open_options: Option<&[&str]>) -> Result<Dataset> {
        let options = DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_READONLY | GdalOpenFlags::GDAL_OF_VECTOR,
            open_options,
            ..Default::default()
        };

        open_with_options(path, options)
    }
}

/// Return a layer's extent in WGS84 longitude/latitude coordinates.
///
/// Uses GDAL's `try_get_extent` (`force = false`), without requesting a feature or
/// tile scan. Returns `None` for empty layers or when no fast extent is available;
/// this does not establish that the layer is empty. The extent's edges are
/// densified during reprojection to account for nonlinear transformations.
///
/// # Errors
/// Returns an error if GDAL cannot retrieve the extent, a spatial extent has no
/// CRS, reprojection fails, or the extent is invalid or outside the WGS84 world.
/// Assign or correct the layer's CRS and extent metadata before retrying.
pub fn layer_bounds(layer: &impl LayerAccess) -> Result<Option<LatLonBounds>> {
    layer
        .try_get_extent()?
        .map(|extent| extent_to_wgs84_bounds(layer, extent))
        .transpose()
}

/// Return a layer's WGS84 bounds, allowing GDAL to scan features if necessary.
///
/// Uses GDAL's `get_extent` (`force = true`). This can be expensive and may reset
/// the layer's reading cursor; use [`layer_bounds`] instead for tiled datasets or
/// whenever scanning is not explicitly desired. Returns `None` when GDAL reports
/// no meaningful extent, including empty layers and layers without geometry.
/// Uses the same densified reprojection and validation as [`layer_bounds`].
///
/// # Errors
/// Returns an error for GDAL errors other than an unavailable extent, a spatial
/// extent without a CRS, reprojection failures, or invalid WGS84 bounds.
pub fn layer_bounds_with_scan(layer: &impl LayerAccess) -> Result<Option<LatLonBounds>> {
    match layer.get_extent() {
        Ok(extent) => extent_to_wgs84_bounds(layer, extent).map(Some),
        Err(::gdal::errors::GdalError::OgrError {
            err: gdal_sys::OGRErr::OGRERR_FAILURE,
            ..
        }) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn extent_to_wgs84_bounds(layer: &impl LayerAccess, extent: Envelope) -> Result<LatLonBounds> {
    let bounds = [extent.MinX, extent.MinY, extent.MaxX, extent.MaxY];
    if !bounds.iter().all(|value| value.is_finite()) || extent.MinX > extent.MaxX || extent.MinY > extent.MaxY {
        return Err(Error::Runtime(format!(
            "Layer '{}' has an invalid extent {bounds:?}; repair its extent metadata before reading bounds",
            layer.name()
        )));
    }

    let mut source = layer.spatial_ref().ok_or_else(|| {
        Error::Runtime(format!(
            "Layer '{}' has a spatial extent but no CRS; assign its source CRS before reading WGS84 bounds",
            layer.name()
        ))
    })?;
    let transformed = if source.auth_code().ok() == Some(i32::from(epsg::WGS84.code())) {
        bounds
    } else {
        source.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);
        let mut target = SpatialRef::from_epsg(u32::from(epsg::WGS84.code()))?;
        target.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);
        CoordTransform::new(&source, &target)
            // GDAL recommends 21 densification points along each edge.
            .and_then(|transform| transform.transform_bounds(&bounds, 21))
            .map_err(|err| {
                Error::Runtime(format!(
                    "Cannot transform extent {bounds:?} of layer '{}' to WGS84; check its source CRS and extent metadata: {err}",
                    layer.name()
                ))
            })?
    };
    let [west, south, east, north] = transformed;
    if !transformed.iter().all(|value| value.is_finite())
        || !(-180.0..=180.0).contains(&west)
        || !(-180.0..=180.0).contains(&east)
        || !(-90.0..=90.0).contains(&south)
        || !(-90.0..=90.0).contains(&north)
        || south > north
    {
        return Err(Error::Runtime(format!(
            "Layer '{}' transformed to invalid WGS84 bounds {transformed:?}; check its source CRS and extent metadata",
            layer.name()
        )));
    }

    Ok(LatLonBounds::from(transformed))
}

/// Open a vector dataset and return the named layer's fast WGS84 bounds.
///
/// Delegates to [`layer_bounds`], without requesting a feature or tile scan.
///
/// # Errors
/// Returns an error if the dataset or named layer cannot be opened, or if
/// [`layer_bounds`] cannot interpret or transform the extent.
pub fn read_layer_bounds(path: &Path, layer_name: &str) -> Result<Option<LatLonBounds>> {
    let ds = dataset::open_read_only(path)?;
    layer_bounds(&ds.layer_by_name(layer_name)?)
}

pub fn read_dataframe(path: &Path, layer: Option<&str>, columns: &[String]) -> Result<Vec<Vec<Option<FieldValue>>>> {
    let ds = dataset::open_read_only(path)?;
    let mut ds_layer;
    if let Some(layer_name) = layer {
        ds_layer = ds.layer_by_name(layer_name)?;
    } else {
        ds_layer = ds.layer(0)?;
    }

    let mut data = Vec::with_capacity(ds_layer.feature_count() as usize);
    let column_indexes = columns
        .iter()
        .map(|name| Ok(ds_layer.defn().field_index(name)?))
        .collect::<Result<Vec<usize>>>()?;

    for feature in ds_layer.features() {
        let mut row = Vec::with_capacity(columns.len());
        for column in &column_indexes {
            row.push(feature.field(*column)?);
        }

        data.push(row);
    }

    Ok(data)
}

pub fn read_dataframe_schema(
    path: &Path,
    layer: Option<&str>,
    data_frame_open_options: Option<&[&str]>,
) -> Result<Vec<(String, OGRFieldType::Type)>> {
    let ds = dataset::open_read_only_with_options(path, data_frame_open_options)?;
    let ds_layer = if let Some(layer_name) = layer {
        ds.layer_by_name(layer_name)?
    } else {
        ds.layer(0)?
    };

    Ok(ds_layer.defn().fields().map(|f| (f.name(), f.field_type())).collect())
}

/// Read rows from a vector dataset and invokes the provided callback function for each row
pub fn read_dataframe_rows_cb(
    path: &Path,
    layer: Option<&str>,
    filter: Option<&str>,
    columns: Option<&[String]>,
    data_frame_open_options: Option<&[&str]>,
    mut callback: impl FnMut(Vec<Option<FieldValue>>),
) -> Result<()> {
    let ds = dataset::open_read_only_with_options(path, data_frame_open_options)?;
    let mut ds_layer;
    if let Some(layer_name) = layer {
        ds_layer = match ds.layer_by_name(layer_name) {
            Ok(layer) => layer,
            Err(err) => {
                log::debug!("{err}");
                return Err(Error::InvalidArgument(format!(
                    "Layer '{}' not found in dataset '{}'",
                    layer_name,
                    path.to_string_lossy()
                )));
            }
        };
    } else {
        ds_layer = ds.layer(0)?;
    }

    if let Some(filter) = filter {
        ds_layer.set_attribute_filter(filter)?;
    }

    let column_indexes: Vec<usize> = match columns {
        Some(columns) => columns
            .iter()
            .map(|name| match ds_layer.defn().field_index(name) {
                Ok(index) => Ok(index),
                Err(_) => Err(Error::InvalidArgument(format!(
                    "Field '{}' not found in layer '{}', available fields: {}",
                    name,
                    ds_layer.name(),
                    ds_layer.defn().fields().map(|f| f.name()).collect::<Vec<String>>().join(", ")
                ))),
            })
            .collect::<Result<Vec<usize>>>()?,
        None =>
        // If no columns are specified, read all fields
        {
            (0..ds_layer.defn().field_count()?)
                .map(|i| Ok(i as usize))
                .collect::<Result<Vec<usize>>>()?
        }
    };

    for feature in ds_layer.features() {
        let mut row = Vec::with_capacity(column_indexes.len());
        let mut valid_field = false;
        for column_idx in &column_indexes {
            valid_field = valid_field || feature.field_is_valid(*column_idx);
            row.push(feature.field(*column_idx)?);
        }

        if valid_field {
            callback(row);
        }
    }

    Ok(())
}

/// [`gdal::vector::LayerAccess`] extenstion trait that implements missing functionality
/// for working with GDAL vector layers
pub trait LayerAccessExtension
where
    Self: LayerAccess,
{
    #[deprecated(
        since = "0.1.0",
        note = "This method is deprecated. Use `field_index` from the `FeatureDefinition` api instead."
    )]
    fn field_index_with_name(&self, field_name: &str) -> Result<usize> {
        let field_name_c_str = CString::new(field_name)?;
        let field_index = unsafe { gdal_sys::OGR_L_FindFieldIndex(self.c_layer(), field_name_c_str.as_ptr(), gdalinterop::TRUE) };

        if field_index == -1 {
            return Err(Error::InvalidArgument(format!(
                "Field '{}' not found in layer '{}'",
                field_name,
                self.name()
            )));
        }

        Ok(field_index as usize)
    }
}

impl LayerAccessExtension for Layer<'_> {}
impl LayerAccessExtension for OwnedLayer {}

/// [`gdal::vector::Defn`] extenstion trait that implements missing functionality
/// for working with GDAL vector layer definitions
pub trait FeatureDefinitionExtension {
    fn field_count(&self) -> Result<i32>;
}

impl FeatureDefinitionExtension for Defn {
    fn field_count(&self) -> Result<i32> {
        let field_count = unsafe { gdal_sys::OGR_FD_GetFieldCount(self.c_defn()) };
        if field_count < 0 {
            return Err(Error::Runtime("Failed to get layer field count".to_string()));
        }

        Ok(field_count)
    }
}

/// [`gdal::vector::Feature`] extenstion trait that implements missing functionality
/// for working with GDAL vector layers
pub trait FeatureExtension {
    fn field_index_from_name(&self, field_name: &str) -> Result<usize>;
    /// The field at the index is set and not null
    fn field_is_valid(&self, field_index: usize) -> bool;
}

impl FeatureExtension for Feature<'_> {
    fn field_index_from_name(&self, field_name: &str) -> Result<usize> {
        let field_name_c_str = CString::new(field_name)?;
        let field_index = unsafe { gdal_sys::OGR_F_GetFieldIndex(self.c_feature(), field_name_c_str.as_ptr()) };

        if field_index == -1 {
            return Err(Error::InvalidArgument(format!("Field '{field_name}' not found in feature")));
        }

        Ok(field_index as usize)
    }

    fn field_is_valid(&self, field_index: usize) -> bool {
        unsafe { gdal_sys::OGR_F_IsFieldSetAndNotNull(self.c_feature(), field_index as i32) == 1 }
    }
}

#[cfg(test)]
mod tests {
    use ::gdal::{
        Dataset, DatasetOptions, DriverManager, GdalOpenFlags,
        vector::{Geometry, LayerOptions, sql::Dialect},
    };
    use approx::assert_abs_diff_eq;
    use gdal_sys::OGRwkbGeometryType;

    use super::*;
    use crate::crs::Epsg;

    fn write_layer(path: &Path, driver: &str, epsg: Option<Epsg>, geometry_type: OGRwkbGeometryType::Type, wkts: &[&str]) -> Result<()> {
        let mut ds = DriverManager::get_driver_by_name(driver)?.create_vector_only(path)?;
        let srs = epsg.map(|epsg| SpatialRef::from_epsg(u32::from(epsg.code()))).transpose()?;
        let mut layer = ds.create_layer(LayerOptions {
            name: "bounds",
            srs: srs.as_ref(),
            ty: geometry_type,
            ..Default::default()
        })?;
        for wkt in wkts {
            layer.create_feature(Geometry::from_wkt(wkt)?)?;
        }
        ds.flush_cache()?;
        Ok(())
    }

    #[test]
    fn read_layer_bounds_uses_recorded_geopackage_extent() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounds.gpkg");
        write_layer(
            &path,
            "GPKG",
            Some(epsg::WGS84),
            OGRwkbGeometryType::wkbPoint,
            &["POINT (4 50)", "POINT (6 52)"],
        )?;

        let ds = dataset::open_read_only(&path)?;
        assert!(ds.layer_by_name("bounds")?.try_get_extent()?.is_some());
        assert_eq!(
            read_layer_bounds(&path, "bounds")?,
            Some(LatLonBounds::from([4.0, 50.0, 6.0, 52.0]))
        );
        Ok(())
    }

    #[test]
    fn layer_bounds_preserves_wgs84_extent_exactly() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("wgs84.gpkg");
        write_layer(
            &path,
            "GPKG",
            Some(epsg::WGS84),
            OGRwkbGeometryType::wkbPoint,
            &[
                "POINT (-4.12345678912345 49.12345678912345)",
                "POINT (6.98765432198765 53.98765432198765)",
            ],
        )?;
        let ds = dataset::open_read_only(&path)?;
        let layer = ds.layer_by_name("bounds")?;
        let extent = layer.try_get_extent()?.expect("recorded WGS84 extent");
        let expected = Some(LatLonBounds::from([extent.MinX, extent.MinY, extent.MaxX, extent.MaxY]));
        assert_eq!(layer_bounds(&layer)?, expected);
        assert_eq!(layer_bounds_with_scan(&layer)?, expected);
        Ok(())
    }

    #[test]
    fn layer_bounds_reprojects_projected_crs() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounds.gpkg");
        write_layer(
            &path,
            "GPKG",
            Some(epsg::WGS84_WEB_MERCATOR),
            OGRwkbGeometryType::wkbPoint,
            &[
                "POINT (1113194.9079327357 2273030.9269876895)",
                "POINT (3339584.723798207 4865942.279503175)",
            ],
        )?;

        let ds = dataset::open_read_only(&path)?;
        let bounds = layer_bounds(&ds.layer_by_name("bounds")?)?.expect("recorded extent");
        assert_abs_diff_eq!(bounds, LatLonBounds::from([10.0, 20.0, 30.0, 40.0]), epsilon = 1e-9);
        Ok(())
    }

    #[test]
    fn layer_bounds_uses_longitude_latitude_not_epsg_axis_order() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounds.gpkg");
        write_layer(
            &path,
            "GPKG",
            Some(epsg::WGS84),
            OGRwkbGeometryType::wkbPoint,
            &["POINT (120 10)", "POINT (130 20)"],
        )?;

        let ds = dataset::open_read_only(&path)?;
        assert_eq!(
            layer_bounds(&ds.layer_by_name("bounds")?)?,
            Some(LatLonBounds::from([120.0, 10.0, 130.0, 20.0]))
        );
        Ok(())
    }

    #[test]
    fn layer_bounds_densifies_nonlinear_extent_edges() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounds.gpkg");
        write_layer(
            &path,
            "GPKG",
            Some(epsg::ETRS89),
            OGRwkbGeometryType::wkbPoint,
            &["POINT (2500000 1500000)", "POINT (6500000 5500000)"],
        )?;
        let ds = dataset::open_read_only(&path)?;
        let layer = ds.layer_by_name("bounds")?;
        let mut source = SpatialRef::from_epsg(u32::from(epsg::ETRS89.code()))?;
        source.set_axis_mapping_strategy(AxisMappingStrategy::TraditionalGisOrder);
        let target = SpatialRef::from_definition("OGC:CRS84")?;
        let mut x = [2500000.0, 2500000.0, 6500000.0, 6500000.0];
        let mut y = [1500000.0, 5500000.0, 1500000.0, 5500000.0];
        CoordTransform::new(&source, &target)?.transform_coords(&mut x, &mut y, &mut [])?;
        let corner_north = y.into_iter().fold(f64::NEG_INFINITY, f64::max);
        let bounds = layer_bounds(&layer)?.expect("recorded extent");

        assert!(
            bounds.north() > corner_north + 0.1,
            "densified bounds {bounds:?} should extend north of the corners ({corner_north})"
        );
        Ok(())
    }

    #[test]
    fn layer_bounds_preserves_antimeridian_crossing() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounds.gpkg");
        write_layer(
            &path,
            "GPKG",
            Some(Epsg::new(32660)),
            OGRwkbGeometryType::wkbPoint,
            &["POINT (700000 1000000)", "POINT (900000 2000000)"],
        )?;
        let bounds = read_layer_bounds(&path, "bounds")?.expect("recorded extent");

        assert!(
            bounds.west() > bounds.east() && bounds.crosses_antimeridian(),
            "expected crossing bounds, got {bounds:?}"
        );
        Ok(())
    }

    #[test]
    fn layer_bounds_returns_none_for_empty_spatial_layer() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounds.gpkg");
        write_layer(&path, "GPKG", Some(epsg::WGS84), OGRwkbGeometryType::wkbPoint, &[])?;

        assert_eq!(read_layer_bounds(&path, "bounds")?, None);
        Ok(())
    }

    #[test]
    fn layer_bounds_returns_none_for_nonspatial_layer_without_crs() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounds.gpkg");
        write_layer(&path, "GPKG", None, OGRwkbGeometryType::wkbNone, &[])?;

        assert_eq!(read_layer_bounds(&path, "bounds")?, None);
        Ok(())
    }

    #[test]
    fn layer_bounds_errors_when_spatial_extent_has_no_crs() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounds.shp");
        write_layer(&path, "ESRI Shapefile", None, OGRwkbGeometryType::wkbPoint, &["POINT (4 50)"])?;
        let ds = dataset::open_read_only(&path)?;
        let layer = ds.layer_by_name("bounds")?;
        assert!(layer.try_get_extent()?.is_some());
        assert!(layer.spatial_ref().is_none());

        let error = layer_bounds(&layer).expect_err("extent without a CRS must not be interpreted as WGS84");
        assert!(error.to_string().contains("no CRS"), "{error}");
        Ok(())
    }

    #[test]
    fn layer_bounds_does_not_force_unknown_geojson_extent() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounds.geojson");
        let feature = r#"{"type":"Feature","properties":{},"geometry":{"type":"Point","coordinates":[4,50]}}"#;
        std::fs::write(
            &path,
            format!(r#"{{"type":"FeatureCollection","name":"bounds","features":[{feature}]}}"#),
        )?;
        let ds = dataset::open_read_only(&path)?;
        let mut layer = ds.layer_by_name("bounds")?;
        // GeoJSON may cache an extent on opening even without a bbox. A filter
        // makes that whole-layer extent unavailable for this query.
        layer.set_attribute_filter("1 = 1")?;
        assert!(layer.try_get_extent()?.is_none(), "fixture must have no fast extent");
        assert_eq!(layer_bounds(&layer)?, None);
        assert!(layer.try_get_extent()?.is_none(), "helper must not populate the extent by scanning");
        assert_eq!(layer_bounds_with_scan(&layer)?, Some(LatLonBounds::from([4.0, 50.0, 4.0, 50.0])));
        Ok(())
    }

    #[test]
    fn layer_bounds_with_scan_returns_none_for_empty_geojson() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounds.geojson");
        std::fs::write(&path, r#"{"type":"FeatureCollection","name":"bounds","features":[]}"#)?;
        let ds = dataset::open_read_only(&path)?;

        assert_eq!(layer_bounds_with_scan(&ds.layer_by_name("bounds")?)?, None);
        Ok(())
    }

    #[test]
    fn layer_bounds_with_scan_returns_none_for_empty_spatial_layer() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounds.gpkg");
        write_layer(&path, "GPKG", Some(epsg::WGS84), OGRwkbGeometryType::wkbPoint, &[])?;
        let ds = dataset::open_read_only(&path)?;

        assert_eq!(layer_bounds_with_scan(&ds.layer_by_name("bounds")?)?, None);
        Ok(())
    }

    #[test]
    fn layer_bounds_with_scan_returns_none_for_features_without_geometry() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounds.geojson");
        std::fs::write(
            &path,
            r#"{"type":"FeatureCollection","name":"bounds","features":[{"type":"Feature","properties":{},"geometry":null}]}"#,
        )?;
        let ds = dataset::open_read_only(&path)?;

        assert_eq!(layer_bounds_with_scan(&ds.layer_by_name("bounds")?)?, None);
        Ok(())
    }

    #[test]
    fn layer_bounds_with_scan_returns_none_for_nonspatial_layer_without_crs() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounds.gpkg");
        write_layer(&path, "GPKG", None, OGRwkbGeometryType::wkbNone, &[])?;
        let ds = dataset::open_read_only(&path)?;
        let layer = ds.layer_by_name("bounds")?;

        assert_eq!(layer_bounds_with_scan(&layer)?, None);
        Ok(())
    }

    #[test]
    fn layer_bounds_rejects_out_of_world_transformed_bounds() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounds.gpkg");
        write_layer(&path, "GPKG", Some(epsg::WGS84), OGRwkbGeometryType::wkbPoint, &["POINT (4 91)"])?;

        let error = read_layer_bounds(&path, "bounds").expect_err("invalid latitude must not be silently clamped");
        assert!(error.to_string().contains("invalid WGS84 bounds"), "{error}");
        Ok(())
    }

    #[test]
    fn layer_bounds_rejects_nonfinite_recorded_extent() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounds.gpkg");
        write_layer(&path, "GPKG", Some(epsg::WGS84), OGRwkbGeometryType::wkbPoint, &["POINT (4 50)"])?;
        let ds = Dataset::open_ex(
            &path,
            DatasetOptions {
                open_flags: GdalOpenFlags::GDAL_OF_UPDATE | GdalOpenFlags::GDAL_OF_VECTOR,
                ..Default::default()
            },
        )?;
        ds.execute_sql(
            "UPDATE gpkg_contents SET min_x = -1e999, max_x = 1e999 WHERE table_name = 'bounds'",
            None,
            Dialect::SQLITE,
        )?;
        drop(ds);

        let error = read_layer_bounds(&path, "bounds").expect_err("nonfinite metadata must be rejected");
        assert!(error.to_string().contains("invalid extent"), "{error}");
        Ok(())
    }

    #[test]
    fn read_layer_bounds_errors_for_missing_layer() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("bounds.gpkg");
        write_layer(&path, "GPKG", Some(epsg::WGS84), OGRwkbGeometryType::wkbPoint, &["POINT (4 50)"])?;

        assert!(read_layer_bounds(&path, "missing").is_err());
        Ok(())
    }
}
